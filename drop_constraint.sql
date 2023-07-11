CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE sensors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER NOT NULL REFERENCES sensors(id),
    value DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('sensor_data', 'time');

INSERT INTO sensors (name) VALUES ('Sensor A'), ('Sensor B'), ('Sensor C');

INSERT INTO sensor_data (time, sensor_id, value) VALUES
    ('2022-01-01 00:00:00', 1, 42.5),
    ('2022-01-01 01:00:00', 2, 35.7),
    ('2022-01-01 02:00:00', 3, 22.3);

-- This line fails
INSERT INTO sensor_data (time, sensor_id, value) VALUES ('2022-01-01 03:00:00', 4, 21.5);

-- Drop constraint
ALTER TABLE sensor_data DROP CONSTRAINT sensor_data_sensor_id_fkey;
-- Now you can insert data without worrying about the foreign key constraint:

INSERT INTO sensor_data (time, sensor_id, value) VALUES ('2022-01-01 03:00:00', 4, 21.5);

