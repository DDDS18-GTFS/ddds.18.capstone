import os
import argparse
import pandas as pd
import psycopg2
from datetime import datetime

def export_data(sql_query):
    """Connects to the database and exports data using a provided query."""
    print("🚀 Starting database export...")
    print(f"Using query: \"{sql_query}\"")

    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    try:
        conn = psycopg2.connect(
            dbname=db_name, user=db_user, password=db_password, host=db_host
        )
        print("✅ Database connection successful.")

        df = pd.read_sql_query(sql_query, conn)
        print(f"Retrieved {len(df)} rows from the database.")

        if not df.empty:
            run_tag = datetime.now().strftime("%Y%m%d_%H%M")
            output_filename = f"db_export_{run_tag}.parquet"
            
            df.to_parquet(output_filename, index=False)
            print(f"\n🎉 Successfully saved data to ./{output_filename}")
        else:
            print("No data to export.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    # Set up the command-line argument parser
    parser = argparse.ArgumentParser(description="Export data from PostgreSQL to a Parquet file.")
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT * FROM vehicle_snapshots;",
        help="The SQL query to execute for the export."
    )
    
    args = parser.parse_args()
    
    # Call the export function with the provided or default query
    export_data(args.query)