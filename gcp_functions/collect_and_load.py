from datetime import datetime, timezone
import os
import time
import requests
import pandas as pd
from datetime import datetime

# GCP Libraries
from google.cloud.sql.connector import Connector
from google.cloud import secretmanager
import pg8000

# --- GCP Configuration ---
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
SECRET_ID = os.getenv("SECRET_ID")
PROJECT_ID = os.getenv("PROJECT_ID")

# --- Initialize Cloud SQL Connector and Secret Manager Client ---
connector = Connector()
client = secretmanager.SecretManagerServiceClient()

def get_db_password():
    """Retrieves the database password from Secret Manager."""
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

DB_PASS = get_db_password()

def getconn() -> pg8000.dbapi.Connection:
    """Initializes a connection to the database."""
    conn: pg8000.dbapi.Connection = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )
    return conn

def insert_data(conn, df):
    """Inserts a DataFrame into the database."""
    cur = conn.cursor()
    try:
        for _, row in df.iterrows():
            insert_query = """
                INSERT INTO vehicle_snapshots (
                    snapshot_id, timestamp_collected, vehicle_id, location,
                    heading, speed_mph, route_short_name, trip_id,
                    next_stop_id, next_stop_name, next_stop_sched_time
                ) VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s, %s, %s);
            """
            next_stop_time = row['next_stop_sched_time'] if pd.notna(row['next_stop_sched_time']) else None
            cur.execute(insert_query, (
                row['snapshot_id'], row['timestamp_collected'], row['vehicle_id'],
                row['longitude'], row['latitude'], row['heading'], row['speed_mph'],
                row['route_short_name'], row['trip_id'], row['next_stop_id'],
                row['next_stop_name'], next_stop_time
            ))
        conn.commit()
        print(f"🚌 Inserted {len(df)} vehicle records.")
    finally:
        cur.close()

def main(event, context):
    GTFS_URL = "https://data.cabq.gov/transit/realtime/route/allroutes.json"
    conn = None
    try:
        conn = getconn()
        # Get the next snapshot_id from the database
        cur = conn.cursor()
        cur.execute("SELECT MAX(snapshot_id) FROM vehicle_snapshots;")
        last_id = cur.fetchone()[0]
        snapshot_counter = (last_id or 0) + 1
        cur.close()

        print(f"📸 Snapshot {snapshot_counter} at {datetime.now().strftime('%H:%M:%S')}")
        response = requests.get(GTFS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        timestamp_collected = datetime.utcnow().isoformat()
        records = [{"snapshot_id": snapshot_counter, "timestamp_collected": timestamp_collected, **v} for v in data.get("allroutes", [])]
        if records:
            df_snapshot = pd.DataFrame(records)
            insert_data(conn, df_snapshot)
        else:
            print("No vehicle data found in this snapshot.")

    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()