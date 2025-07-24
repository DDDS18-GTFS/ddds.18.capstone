CREATE TABLE vehicle_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_id INT,
    timestamp_collected TIMESTAMPTZ,
    vehicle_id INT,
    location GEOMETRY(Point, 4326),
    heading FLOAT,
    speed_mph FLOAT,
    route_short_name VARCHAR(50),
    trip_id VARCHAR(255),
    next_stop_id VARCHAR(255),
    next_stop_name VARCHAR(255),
    next_stop_sched_time TIME
);