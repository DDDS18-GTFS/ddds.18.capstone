import os
import time
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

# --- Database Connection Function ---
def connect_to_db():
    """Connects to the PostgreSQL database using environment variables."""
    # Get credentials from environment variables
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    
    # Retry connection to give the database time to initialize
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                dbname=db_name, user=db_user, password=db_password, host=db_host
            )
            print("✅ Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            retries -= 1
            print(f"⏳ Database not ready, waiting... ({e})")
            time.sleep(5)
    return None

# --- Data Insertion Function ---
def insert_data(conn, df):
    """Inserts a DataFrame of vehicle data into the database."""
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO vehicle_snapshots (
                    snapshot_id, timestamp_collected, vehicle_id, location,
                    heading, speed_mph, route_short_name, trip_id,
                    next_stop_id, next_stop_name, next_stop_sched_time
                ) VALUES (
                    %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                    %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT DO NOTHING;
            """)
            
            # Handle potential null values for time
            next_stop_time = row['next_stop_sched_time'] if pd.notna(row['next_stop_sched_time']) else None

            cur.execute(insert_query, (
                row['snapshot_id'], row['timestamp_collected'], row['vehicle_id'],
                row['longitude'], row['latitude'], row['heading'], row['speed_mph'],
                row['route_short_name'], row['trip_id'], row['next_stop_id'],
                row['next_stop_name'], next_stop_time
            ))
    conn.commit()
    print(f"✅ Inserted {len(df)} records into the database.")

# --- Main Application Logic ---
def main():
    GTFS_URL = "https://data.cabq.gov/transit/realtime/route/allroutes.json"
    NUM_SNAPSHOTS = 10
    SLEEP_SECONDS = 30

    conn = connect_to_db()
    if not conn:
        print("❌ Could not connect to the database. Exiting.")
        return

    print(f"🚍 Starting Albuquerque data collection ({NUM_SNAPSHOTS} snapshots)...")

    for i in range(NUM_SNAPSHOTS):
        try:
            print(f"\n📸 Snapshot {i+1}/{NUM_SNAPSHOTS} at {datetime.now().strftime('%H:%M:%S')}")
            
            # Fetch data from the URL
            response = requests.get(GTFS_URL)
            data = response.json()
            timestamp_collected = datetime.utcnow().isoformat()
            
            # Process records into a DataFrame
            records = []
            for vehicle in data.get("allroutes", []):
                records.append({
                    "snapshot_id": i + 1, "timestamp_collected": timestamp_collected,
                    "vehicle_id": vehicle.get("vehicle_id"), "latitude": vehicle.get("latitude"),
                    "longitude": vehicle.get("longitude"), "heading": vehicle.get("heading"),
                    "speed_mph": vehicle.get("speed_mph"), "route_short_name": vehicle.get("route_short_name"),
                    "trip_id": vehicle.get("trip_id"), "next_stop_id": vehicle.get("next_stop_id"),
                    "next_stop_name": vehicle.get("next_stop_name"),
                    "next_stop_sched_time": vehicle.get("next_stop_sched_time")
                })
            df_snapshot = pd.DataFrame(records)

            # Insert the new data into the database
            if not df_snapshot.empty:
                insert_data(conn, df_snapshot)

            if i < NUM_SNAPSHOTS - 1:
                time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"❌ Error during snapshot {i+1}: {e}")
            continue
    
    conn.close()
    print("\n🎉 Data collection and loading complete.")

if __name__ == "__main__":
    main()