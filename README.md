# 🚍 Albuquerque Real-Time Transit Data Collector

This project continuously collects real-time vehicle position data from the City of Albuquerque's public transit API. The data is stored in a PostgreSQL database with the PostGIS extension for powerful geospatial querying. The entire application is containerized using Docker and Docker Compose for easy setup and deployment.

---
## ## Features

* **Continuous Data Collection**: A Python script runs in a loop to fetch data every 30 seconds.
* **Geospatial Database**: Uses PostgreSQL + PostGIS to store location data efficiently.
* **Dockerized Environment**: Fully containerized for one-command setup and consistent runs.
* **Data Export**: Includes a utility script to export the collected data to a Parquet file with custom SQL queries.
* **Robust and Resilient**: Designed to handle network errors and shut down gracefully.

---
## ## Project Structure
/abq-transit-project/
├── docker-compose.yml        # Orchestrates all Docker containers
├── db_init/
│   └── init.sql              # Initializes the database table on first run
└── python_app/
├── Dockerfile              # Defines the Python application container
├── requirements.txt        # Python dependencies
├── collect_and_load.py     # Main script for continuous data collection
└── export_to_parquet.py    # Utility script to export data

---
## ## Requirements

* Docker
* Docker Compose

---
## ## Setup and Running

1.  **Clone or set up the project files** according to the structure above.

2.  **Open a terminal** in the root `abq-transit-project` directory.

3.  **Build and run the services** using Docker Compose. The `-d` flag runs the containers in the background (detached mode).

    ```bash
    docker compose up --build -d
    ```

The first time you run this, Docker will build the Python image, create a persistent volume for the database, and run the `init.sql` script to create the `vehicle_snapshots` table. The `collect_and_load.py` script will then start running and collecting data.

---
## ## Usage

### ### Connecting to the Database

You can connect directly to the PostgreSQL database to run custom queries.

1.  **Open the `psql` shell:**
    ```bash
    docker exec -it postgis_db psql -U myuser -d abq_transit
    ```

2.  **Run a query.** For example, to find the 5 most recently tracked vehicles:
    ```sql
    SELECT vehicle_id, route_short_name, speed_mph, timestamp_collected
    FROM vehicle_snapshots
    ORDER BY timestamp_collected DESC
    LIMIT 5;
    ```
3.  **Exit** by typing `\q`.

### ### Exporting Data to Parquet

Use the `export_to_parquet.py` script to save data from the database to a `.parquet` file. The output file will appear in the `python_app` directory.

1.  **Run the Export Script**
    This command executes the script inside the running `python_app` container.

    * **To export the entire database (default query):**
        ```bash
        docker exec python_app python3 export_to_parquet.py
        ```

    * **To export using a custom query:**
        Use the `--query` flag and wrap your SQL in quotes.
        ```bash
        docker exec python_app python3 export_to_parquet.py --query "SELECT * FROM vehicle_snapshots WHERE speed_mph > 60;"
        ```

---
## ## Troubleshooting

### ### 🔧 Error: `relation "vehicle_snapshots" does not exist`

This is the most common error and occurs if the database starts without running the initialization script. The database logs will contain a line that says `Skipping initialization`.

This happens when a database volume from a previous failed run already exists. To fix this, you must perform a **full reset**.

1.  **Stop the containers:**
    ```bash
    docker compose down
    ```

2.  **Delete the database volume.** This is the critical step. It will not delete your code.
    ```bash
    docker volume rm abq-transit-project_postgres_data
    ```

3.  **Start fresh.** This will force Docker to create a new database and run the `init.sql` script.
    ```bash
    docker compose up --build -d
    ```

