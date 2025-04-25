-- Drop the existing table if it exists
DROP TABLE IF EXISTS temperature_forecasts CASCADE;

-- Create a table for temperature forecasts if it doesn't exist
CREATE TABLE IF NOT EXISTS temperature_forecasts (
    time TIMESTAMPTZ NOT NULL,           -- When the forecast was made
    forecast_time TIMESTAMPTZ NOT NULL,   -- Time being forecasted
    value DOUBLE PRECISION NOT NULL       -- Forecasted temperature value
);

-- Convert to hypertable with time partitioning if not already a hypertable
-- This creates the first dimension indexed on 'time' (when forecast was made)
SELECT create_hypertable('temperature_forecasts', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

-- Add a second dimension for forecast_time (the time being forecasted) if not already added
-- This creates a second time-based dimension
SELECT add_dimension('temperature_forecasts', by_range('forecast_time', INTERVAL '1 day'), if_not_exists => TRUE);

-- Create an index on forecast_time to improve query performance if it doesn't exist
CREATE INDEX IF NOT EXISTS temp_forecast_time_idx ON temperature_forecasts (forecast_time, time DESC);

-- Enable chunk skipping for forecast_time column
-- This allows TimescaleDB to skip chunks that don't contain relevant forecast_time values
ALTER SYSTEM SET timescaledb.enable_chunk_skipping = 'on';
SELECT pg_reload_conf();

-- Wait a moment for the configuration to take effect
SELECT pg_sleep(1);

-- Now try to enable chunk skipping on the table
SELECT enable_chunk_skipping('temperature_forecasts', 'forecast_time');

-- Insert data for multiple days to create multiple chunks
-- Day 1: Feb 1, 2025
INSERT INTO temperature_forecasts (time, forecast_time, value)
VALUES 
    ('2025-02-01 10:00:00', '2025-02-01 20:30:00', 1.5),
    ('2025-02-01 10:00:00', '2025-02-01 20:31:00', 3.3),
    ('2025-02-01 10:00:00', '2025-02-01 20:32:00', 2.8);

-- Day 2: Feb 2, 2025
INSERT INTO temperature_forecasts (time, forecast_time, value)
VALUES 
    ('2025-02-02 10:00:00', '2025-02-02 20:30:00', 2.5),
    ('2025-02-02 10:00:00', '2025-02-02 20:31:00', 4.3),
    ('2025-02-02 10:00:00', '2025-02-02 20:32:00', 3.8);

-- Day 3: Feb 3, 2025
INSERT INTO temperature_forecasts (time, forecast_time, value)
VALUES 
    ('2025-02-03 10:00:00', '2025-02-03 20:30:00', 3.5),
    ('2025-02-03 10:00:00', '2025-02-03 20:31:00', 5.3),
    ('2025-02-03 10:00:00', '2025-02-03 20:32:00', 4.8);

-- Day 4: Feb 4, 2025
INSERT INTO temperature_forecasts (time, forecast_time, value)
VALUES 
    ('2025-02-04 10:00:00', '2025-02-04 20:30:00', 4.5),
    ('2025-02-04 10:00:00', '2025-02-04 20:31:00', 6.3),
    ('2025-02-04 10:00:00', '2025-02-04 20:32:00', 5.8);

-- Day 5: Feb 5, 2025
INSERT INTO temperature_forecasts (time, forecast_time, value)
VALUES 
    ('2025-02-05 10:00:00', '2025-02-05 20:30:00', 5.5),
    ('2025-02-05 10:00:00', '2025-02-05 20:31:00', 7.3),
    ('2025-02-05 10:00:00', '2025-02-05 20:32:00', 6.8);

-- Show the chunks that were created
SELECT show_chunks('temperature_forecasts');

-- Add parameter_id column
ALTER TABLE temperature_forecasts ADD COLUMN IF NOT EXISTS parameter_id INTEGER DEFAULT 1;

-- Insert some multi-parameter data for multiple days
INSERT INTO temperature_forecasts (time, forecast_time, value, parameter_id)
VALUES 
    -- Day 1 parameter data
    ('2025-02-01 10:02:00', '2025-02-01 20:30:00', 1.7, 1), -- Temperature
    ('2025-02-01 10:02:00', '2025-02-01 20:30:00', 60.0, 2), -- Humidity
    
    -- Day 2 parameter data
    ('2025-02-02 10:02:00', '2025-02-02 20:30:00', 2.7, 1), 
    ('2025-02-02 10:02:00', '2025-02-02 20:30:00', 65.0, 2),
    
    -- Day 3 parameter data
    ('2025-02-03 10:02:00', '2025-02-03 20:30:00', 3.7, 1), 
    ('2025-02-03 10:02:00', '2025-02-03 20:30:00', 70.0, 2),
    
    -- Day 4 parameter data
    ('2025-02-04 10:02:00', '2025-02-04 20:30:00', 4.7, 1), 
    ('2025-02-04 10:02:00', '2025-02-04 20:30:00', 75.0, 2),
    
    -- Day 5 parameter data
    ('2025-02-05 10:02:00', '2025-02-05 20:30:00', 5.7, 1), 
    ('2025-02-05 10:02:00', '2025-02-05 20:30:00', 80.0, 2);

-- QUERY 1: Show chunk skipping in action by querying only Feb 2-3
-- This should skip Feb 1, 4, and 5 chunks
EXPLAIN ANALYZE
WITH latest_forecasts AS (
    SELECT DISTINCT ON (forecast_time) 
        forecast_time,
        value
    FROM temperature_forecasts
    WHERE forecast_time BETWEEN '2025-02-02 00:00:00' AND '2025-02-03 23:59:59'
    ORDER BY forecast_time, time DESC
)
SELECT * FROM latest_forecasts ORDER BY forecast_time;

-- QUERY 2: Show chunk skipping when querying with parameter_id
EXPLAIN ANALYZE
WITH latest_forecasts AS (
    SELECT DISTINCT ON (forecast_time, parameter_id) 
        forecast_time,
        parameter_id,
        value
    FROM temperature_forecasts
    WHERE 
        forecast_time BETWEEN '2025-02-02 00:00:00' AND '2025-02-03 23:59:59' AND
        parameter_id = 1
    ORDER BY forecast_time, parameter_id, time DESC
)
SELECT * FROM latest_forecasts ORDER BY forecast_time;

SELECT disable_chunk_skipping('temperature_forecasts', 'forecast_time');

EXPLAIN ANALYZE
WITH latest_forecasts AS (
    SELECT DISTINCT ON (forecast_time) 
        forecast_time,
        value
    FROM temperature_forecasts
    WHERE forecast_time BETWEEN '2025-02-02 00:00:00' AND '2025-02-03 23:59:59'
    ORDER BY forecast_time, time DESC
)
SELECT * FROM latest_forecasts ORDER BY forecast_time;

-- QUERY 2: Show chunk skipping when querying with parameter_id
EXPLAIN ANALYZE
WITH latest_forecasts AS (
    SELECT DISTINCT ON (forecast_time, parameter_id) 
        forecast_time,
        parameter_id,
        value
    FROM temperature_forecasts
    WHERE 
        forecast_time BETWEEN '2025-02-02 00:00:00' AND '2025-02-03 23:59:59' AND
        parameter_id = 1
    ORDER BY forecast_time, parameter_id, time DESC
)
SELECT * FROM latest_forecasts ORDER BY forecast_time;