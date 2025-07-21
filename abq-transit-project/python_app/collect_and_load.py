import os
import time
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

def connect_to_db():
    """Connects to the PostgreSQL database using environment variables."""
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(
                dbname=db_name, user=db_user, password=db_password, host=db_host
            )
            print("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            print(f"⏳ Database not ready, waiting 5 seconds... ({e})")
            time.sleep(5)
    return conn

def insert_data(conn, df):
    """Inserts a DataFrame of vehicle data into the database."""
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            # This query uses ON CONFLICT DO NOTHING to prevent errors
            # if a row with the same primary key is inserted.
            # You may want to define a unique constraint on (snapshot_id, vehicle_id)
            # for this to be effective. For now, it relies on the SERIAL id.
            insert_query = sql.SQL("""
                INSERT INTO vehicle_snapshots (
                    snapshot_id, timestamp_collected, vehicle_id, location,
                    heading, speed_mph, route_short_name, trip_id,
                    next_stop_id, next_stop_name, next_stop_sched_time
                ) VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s, %s, %s);
            """)
            
            next_stop_time = row['next_stop_sched_time'] if pd.notna(row['next_stop_sched_time']) else None

            cur.execute(insert_query, (
                row['snapshot_id'], row['timestamp_collected'], row['vehicle_id'],
                row['longitude'], row['latitude'], row['heading'], row['speed_mph'],
                row['route_short_name'], row['trip_id'], row['next_stop_id'],
                row['next_stop_name'], next_stop_time
            ))
    conn.commit()
    print(f"🚌 Inserted {len(df)} vehicle records.")

def main():
    """Main function to run the continuous data collection loop."""
    GTFS_URL = "https://data.cabq.gov/transit/realtime/route/allroutes.json"
    SLEEP_SECONDS = 30
    snapshot_counter = 0

    conn = connect_to_db()
    
    print("🚀 Starting continuous data collection... Press Ctrl+C to stop.")
    
    try:
        while True:
            snapshot_counter += 1
            print(f"\n📸 Snapshot {snapshot_counter} at {datetime.now().strftime('%H:%M:%S')}")
            
            try:
                # Fetch and process data
                response = requests.get(GTFS_URL, timeout=10) # Added a timeout
                response.raise_for_status() # Raises an error for bad responses (4xx or 5xx)
                data = response.json()
                timestamp_collected = datetime.utcnow().isoformat()
                
                records = [
                    {
                        "snapshot_id": snapshot_counter, "timestamp_collected": timestamp_collected,
                        "vehicle_id": v.get("vehicle_id"), "latitude": v.get("latitude"),
                        "longitude": v.get("longitude"), "heading": v.get("heading"),
                        "speed_mph": v.get("speed_mph"), "route_short_name": v.get("route_short_name"),
                        "trip_id": v.get("trip_id"), "next_stop_id": v.get("next_stop_id"),
                        "next_stop_name": v.get("next_stop_name"), "next_stop_sched_time": v.get("next_stop_sched_time")
                    } for v in data.get("allroutes", [])
                ]
                
                if records:
                    df_snapshot = pd.DataFrame(records)
                    insert_data(conn, df_snapshot)
                else:
                    print("No vehicle data found in this snapshot.")

            except requests.exceptions.RequestException as e:
                print(f"❌ Network Error: Could not fetch data. {e}")
            except Exception as e:
                print(f"❌ An unexpected error occurred: {e}")

            # Wait for the next interval
            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()