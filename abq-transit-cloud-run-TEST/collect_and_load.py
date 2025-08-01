import os
import time
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import logging
import sys
import threading
from contextlib import contextmanager
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import urllib.parse
from zoneinfo import ZoneInfo
import hmac
# Configure logging for Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.db_name = os.getenv('DB_NAME', 'abq-transit-db')
        self.db_user = os.getenv('DB_USER', 'myuser')
        self.db_password = os.getenv('DB_PASSWORD', 'ENTER_PASSWORD')
        self.db_host = os.getenv('DB_HOST', '34.174.222.188')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.connection = None
    
    def connect(self, max_retries=5, base_delay=1):
        """Connect to Cloud SQL with exponential backoff retry logic."""
        for attempt in range(max_retries):
            try:
                self.connection = psycopg2.connect(
                    dbname=self.db_name,
                    user=self.db_user,
                    password=self.db_password,
                    host=self.db_host,
                    port=self.db_port,
                    sslmode='require',
                    connect_timeout=30
                )
                self.connection.set_session(autocommit=False)
                logger.info("✅ Database connection successful")
                return self.connection
                
            except psycopg2.OperationalError as e:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"⏳ Database connection attempt {attempt + 1} failed. Retrying in {delay}s... ({e})")
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    logger.error("❌ Failed to connect to database after all retries")
                    raise
            except Exception as e:
                logger.error(f"❌ Unexpected database connection error: {e}")
                raise
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor with automatic cleanup."""
        if not self.connection or self.connection.closed:
            self.connect()
        
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            cursor.close()
    
    def close(self):
        """Close database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("Database connection closed")

class APIHandler(BaseHTTPRequestHandler):
    """Secure API handler for serving transit data."""
    
    def __init__(self, *args, db_connection=None, **kwargs):
        self.db = db_connection
        self.api_key = os.getenv('API_KEY', 'default-unsafe-key')
        super().__init__(*args, **kwargs)
    
    def verify_api_key(self):
        """Verify API key from Authorization header."""
        auth_header = self.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return False
        
        provided_key = auth_header.replace('Bearer ', '')
        return hmac.compare_digest(provided_key, self.api_key)
    
    def require_auth(self):
        """Check authentication and send 401 if invalid."""
        if not self.verify_api_key():
            self.send_response(401)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_data = {
                'error': 'Unauthorized - Valid API key required',
                'hint': 'Include header: Authorization: Bearer YOUR_API_KEY'
            }
            self.wfile.write(json.dumps(error_data).encode())
            return False
        return True
    
    def do_GET(self):
        """Handle GET requests with authentication."""
        try:
            # Parse URL and query parameters
            parsed_url = urllib.parse.urlparse(self.path)
            path = parsed_url.path
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # Public endpoint (no authentication required)
            if path == '/health':
                self.send_health_response()
                return
            
            # All other endpoints require authentication
            if not self.require_auth():
                return
            
            # Protected endpoints
            if path == '/api/summary':
                self.send_summary_stats()
            elif path == '/api/recent':
                hours = int(query_params.get('hours', ['1'])[0])
                self.send_recent_data(hours)
            elif path == '/api/routes':
                self.send_route_list()
            elif path == '/api/route':
                route_name = query_params.get('name', [None])[0]
                hours = int(query_params.get('hours', ['1'])[0])
                self.send_route_data(route_name, hours)
            elif path == '/api/vehicles':
                self.send_active_vehicles()
            elif path == '/api/locations':
                self.send_current_locations()
            else:
                self.send_404()
                
        except Exception as e:
            logger.error(f"API request failed: {e}")
            self.send_error_response(str(e))
    
    def send_health_response(self):
        """Send health check response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'service': 'abq-transit-collector',
            'security': 'API key authentication enabled'
        }
        self.wfile.write(json.dumps(health_status).encode())
    
    def send_summary_stats(self):
        """Send database summary statistics."""
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT vehicle_id) as unique_vehicles,
            COUNT(DISTINCT route_short_name) as unique_routes,
            MIN(timestamp_collected) as first_record,
            MAX(timestamp_collected) as latest_record,
            COUNT(DISTINCT DATE(timestamp_collected)) as collection_days
        FROM vehicle_snapshots
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                stats = {
                    'total_records': result[0],
                    'unique_vehicles': result[1],
                    'unique_routes': result[2],
                    'first_record': result[3].isoformat() if result[3] else None,
                    'latest_record': result[4].isoformat() if result[4] else None,
                    'collection_days': result[5]
                }
            else:
                stats = {}
        
        self.send_json_response(stats)
    
    def send_recent_data(self, hours=1):
        """Send recent transit data."""
        query = """
        SELECT 
            snapshot_id,
            timestamp_collected,
            vehicle_id,
            msg_time,
            ST_X(location) as longitude,
            ST_Y(location) as latitude,
            heading,
            speed_mph,
            route_short_name,
            trip_id,
            next_stop_id,
            next_stop_name,
            next_stop_sched_time
        FROM vehicle_snapshots 
        WHERE timestamp_collected >= NOW() - INTERVAL '%s hours'
        ORDER BY timestamp_collected DESC
        LIMIT 1000
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (hours,))
            results = cursor.fetchall()
            
            data = []
            for row in results:
                data.append({
                    'snapshot_id': row[0],
                    'timestamp_collected': row[1].isoformat() if row[1] else None,
                    'vehicle_id': row[2],
                    'msg_time': row[3],
                    'longitude': float(row[4]) if row[4] else None,
                    'latitude': float(row[5]) if row[5] else None,
                    'heading': float(row[6]) if row[6] else None,
                    'speed_mph': float(row[7]) if row[7] else None,
                    'route_short_name': row[8],
                    'trip_id': row[9],
                    'next_stop_id': row[10],
                    'next_stop_name': row[11],
                    'next_stop_sched_time': row[12]
                })
        
        response = {
            'hours_back': hours,
            'record_count': len(data),
            'data': data
        }
        
        self.send_json_response(response)
    
    def send_route_list(self):
        """Send list of available routes."""
        query = """
        SELECT 
            route_short_name,
            COUNT(*) as observation_count,
            COUNT(DISTINCT vehicle_id) as vehicle_count,
            MAX(timestamp_collected) as last_seen
        FROM vehicle_snapshots 
        WHERE route_short_name IS NOT NULL
        GROUP BY route_short_name
        ORDER BY observation_count DESC
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
            routes = []
            for row in results:
                routes.append({
                    'route_name': row[0],
                    'observation_count': row[1],
                    'vehicle_count': row[2],
                    'last_seen': row[3].isoformat() if row[3] else None
                })
        
        self.send_json_response({'routes': routes})
    
    def send_route_data(self, route_name, hours=1):
        """Send data for a specific route."""
        if not route_name:
            self.send_error_response("Route name is required")
            return
        
        query = """
        SELECT 
            timestamp_collected,
            vehicle_id,
            msg_time,
            ST_X(location) as longitude,
            ST_Y(location) as latitude,
            heading,
            speed_mph,
            trip_id,
            next_stop_name
        FROM vehicle_snapshots 
        WHERE route_short_name = %s 
        AND timestamp_collected >= NOW() - INTERVAL '%s hours'
        ORDER BY timestamp_collected DESC
        LIMIT 500
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (route_name, hours))
            results = cursor.fetchall()
            
            data = []
            for row in results:
                data.append({
                    'timestamp_collected': row[0].isoformat() if row[0] else None,
                    'vehicle_id': row[1],
                    'msg_time':row[2],
                    'longitude': float(row[3]) if row[3] else None,
                    'latitude': float(row[4]) if row[4] else None,
                    'heading': float(row[5]) if row[5] else None,
                    'speed_mph': float(row[6]) if row[6] else None,
                    'trip_id': row[7],
                    'next_stop_name': row[8]
                })
        
        response = {
            'route_name': route_name,
            'hours_back': hours,
            'record_count': len(data),
            'data': data
        }
        
        self.send_json_response(response)
    
    def send_active_vehicles(self):
        """Send currently active vehicles."""
        query = """
        SELECT DISTINCT ON (vehicle_id)
            vehicle_id,
            msg_time,
            timestamp_collected,
            route_short_name,
            ST_X(location) as longitude,
            ST_Y(location) as latitude,
            speed_mph
        FROM vehicle_snapshots 
        WHERE timestamp_collected >= NOW() - INTERVAL '10 minutes'
        ORDER BY vehicle_id, timestamp_collected DESC
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
            vehicles = []
            for row in results:
                vehicles.append({
                    'vehicle_id': row[0],
                    'last_seen': row[1].isoformat() if row[1] else None,
                    'route_short_name': row[2],
                    'longitude': float(row[3]) if row[3] else None,
                    'latitude': float(row[4]) if row[4] else None,
                    'speed_mph': float(row[5]) if row[5] else None,
                    'msg_time': [6]
                })
        
        response = {
            'active_vehicles': len(vehicles),
            'vehicles': vehicles
        }
        
        self.send_json_response(response)
    
    def send_current_locations(self):
        """Send current vehicle locations for mapping."""
        query = """
        SELECT DISTINCT ON (vehicle_id)
            vehicle_id,
            msg_time,
            route_short_name,
            ST_X(location) as longitude,
            ST_Y(location) as latitude,
            heading,
            speed_mph,
            next_stop_name
        FROM vehicle_snapshots 
        WHERE timestamp_collected >= NOW() - INTERVAL '5 minutes'
        AND ST_X(location) IS NOT NULL 
        AND ST_Y(location) IS NOT NULL
        ORDER BY vehicle_id, timestamp_collected DESC
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
            locations = []
            for row in results:
                locations.append({
                    'vehicle_id': row[0],
                    'route_short_name': row[1],
                    'longitude': float(row[2]),
                    'latitude': float(row[3]),
                    'heading': float(row[4]) if row[4] else None,
                    'speed_mph': float(row[5]) if row[5] else None,
                    'next_stop_name': row[6],
                    'msg_time': row[7]
                })
        
        response = {
            'timestamp': datetime.utcnow().isoformat(),
            'vehicle_count': len(locations),
            'locations': locations
        }
        
        self.send_json_response(response)
    
    def send_json_response(self, data):
        """Send JSON response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
    
    def send_error_response(self, error_message):
        """Send error response."""
        self.send_response(500)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        error_data = {
            'error': error_message,
            'timestamp': datetime.utcnow().isoformat()
        }
        self.wfile.write(json.dumps(error_data).encode())
    
    def send_404(self):
        """Send 404 response."""
        self.send_response(404)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        error_data = {
            'error': 'Endpoint not found',
            'available_endpoints': [
                '/health',
                '/api/summary',
                '/api/recent?hours=1',
                '/api/routes',
                '/api/route?name=ROUTE_NAME&hours=1',
                '/api/vehicles',
                '/api/locations'
            ]
        }
        self.wfile.write(json.dumps(error_data).encode())
    
    def log_message(self, format, *args):
        # Suppress HTTP server logs to keep collector logs clean
        pass

class ABQTransitCollector:
    def __init__(self):
        self.gtfs_url = "https://data.cabq.gov/transit/realtime/route/allroutes.json"
        self.sleep_seconds = int(os.getenv('COLLECTION_INTERVAL', '30'))
        self.db = DatabaseConnection()
        self.snapshot_counter = 0
        self.consecutive_failures = 0
        self.max_consecutive_failures = 10
        
    def validate_vehicle_data(self, vehicle):
        """Validate required fields in vehicle data."""
        required_fields = ['vehicle_id', 'latitude', 'longitude']
        return all(vehicle.get(field) is not None for field in required_fields)
    
    def fetch_transit_data(self, timeout=15):
        """Fetch data from ABQ Transit API with error handling."""
        try:
            response = requests.get(self.gtfs_url, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            timestamp_collected = datetime.utcnow()
            
            # Extract and validate vehicle data
            raw_vehicles = data.get("allroutes", [])
            valid_vehicles = [v for v in raw_vehicles if self.validate_vehicle_data(v)]
            
            if len(valid_vehicles) < len(raw_vehicles):
                logger.warning(f"Filtered out {len(raw_vehicles) - len(valid_vehicles)} invalid vehicle records")
            
            records = []
            for vehicle in valid_vehicles:
                # DEBUG: Log the first few vehicles to see what we're getting
                if len(records) < 3:  # Only log first 3 to avoid spam
                    logger.info(f"DEBUG: vehicle_id={vehicle.get('vehicle_id')}, msg_time='{vehicle.get('msg_time')}', next_stop_sched_time='{vehicle.get('next_stop_sched_time')}'")
                    logger.info(f"DEBUG: timestamp_collected={timestamp_collected}")
                
                record = {
                    "snapshot_id": self.snapshot_counter,
                    "timestamp_collected": timestamp_collected,
                    "vehicle_id": vehicle.get("vehicle_id"),
                    "msg_time": vehicle.get("msg_time"),
                    "latitude": float(vehicle.get("latitude", 0)),
                    "longitude": float(vehicle.get("longitude", 0)),
                    "heading": vehicle.get("heading"),
                    "speed_mph": vehicle.get("speed_mph"),
                    "route_short_name": vehicle.get("route_short_name"),
                    "trip_id": vehicle.get("trip_id"),
                    "next_stop_id": vehicle.get("next_stop_id"),
                    "next_stop_name": vehicle.get("next_stop_name"),
                    "next_stop_sched_time": vehicle.get("next_stop_sched_time")
                }
                records.append(record)
            
            return records, timestamp_collected
            
        except requests.exceptions.Timeout:
            logger.error("❌ Request timeout while fetching transit data")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Network error fetching transit data: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error processing transit data: {e}")
            raise
    
    def insert_data(self, records):
        """Insert vehicle records into the database."""
        if not records:
            logger.info("No valid records to insert")
            return
        # msg_time add here 
        insert_query = sql.SQL("""
            INSERT INTO vehicle_snapshots (
                snapshot_id, timestamp_collected, vehicle_id, msg_time, location,
                heading, speed_mph, route_short_name, trip_id,
                next_stop_id, next_stop_name, next_stop_sched_time
            ) VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """)
        # msg_time add here 
        with self.db.get_cursor() as cursor:
            for record in records:
                # Store the raw next_stop_sched_time string as-is from ABQ API
                raw_sched_time = record.get('next_stop_sched_time')
                
                cursor.execute(insert_query, (
                record['snapshot_id'], 
                record['timestamp_collected'], 
                record['vehicle_id'], 
                record['msg_time'],                    
                record['longitude'], record['latitude'], 
                record['heading'], 
                record['speed_mph'],
                record['route_short_name'], 
                record['trip_id'], 
                record['next_stop_id'],
                record['next_stop_name'], 
                raw_sched_time                         
                ))

            
        
        logger.info(f"🚌 Successfully inserted {len(records)} vehicle records")
    
    def collect_snapshot(self):
        """Collect a single snapshot of transit data."""
        self.snapshot_counter += 1
        
        try:
            logger.info(f"📸 Collecting snapshot {self.snapshot_counter} at {datetime.now().strftime('%H:%M:%S')}")
            
            # Fetch data
            records, timestamp = self.fetch_transit_data()
            
            if records:
                # Insert into database
                self.insert_data(records)
                self.consecutive_failures = 0  # Reset failure counter on success
                logger.info(f"✅ Snapshot {self.snapshot_counter} completed: {len(records)} vehicles")
            else:
                logger.warning("No vehicle data found in this snapshot")
                
        except Exception as e:
            self.consecutive_failures += 1
            logger.error(f"❌ Snapshot {self.snapshot_counter} failed: {e}")
            
            if self.consecutive_failures >= self.max_consecutive_failures:
                logger.critical(f"💀 Too many consecutive failures ({self.consecutive_failures}). Shutting down.")
                raise
    
    def run_continuous_collection(self):
        """Main loop for continuous data collection."""
        logger.info("🚀 Starting ABQ Transit data collection for Cloud Run")
        logger.info(f"Collection interval: {self.sleep_seconds} seconds")
        logger.info(f"Target database: {self.db.db_host}:{self.db.db_port}/{self.db.db_name}")
        
        # Initial database connection
        self.db.connect()
        
        try:
            while True:
                start_time = time.time()
                
                # Collect snapshot
                self.collect_snapshot()
                
                # Calculate sleep time to maintain consistent intervals
                elapsed_time = time.time() - start_time
                sleep_time = max(0, self.sleep_seconds - elapsed_time)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"⚠️ Collection took {elapsed_time:.1f}s, longer than {self.sleep_seconds}s interval")
                
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
        except Exception as e:
            logger.critical(f"💀 Critical error in collection loop: {e}")
            raise
        finally:
            self.db.close()
            logger.info("🏁 ABQ Transit data collection stopped")

def start_api_server(db_connection):
    """Start API server in a separate thread."""
    port = int(os.getenv('PORT', '8080'))
    
    def handler(*args, **kwargs):
        APIHandler(*args, db_connection=db_connection, **kwargs)
    
    server = HTTPServer(('', port), handler)
    logger.info(f"API server starting on port {port}")
    server.serve_forever()

def main():
    """Entry point for the application."""
    try:
        collector = ABQTransitCollector()
        
        # Start API server in background
        api_thread = threading.Thread(target=start_api_server, args=(collector.db,), daemon=True)
        api_thread.start()
        
        # Start the main collector
        collector.run_continuous_collection()
    except Exception as e:
        logger.critical(f"Application failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()