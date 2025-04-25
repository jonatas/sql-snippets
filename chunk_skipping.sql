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

-- Add parameter_id column
ALTER TABLE temperature_forecasts ADD COLUMN IF NOT EXISTS parameter_id INTEGER DEFAULT 1;

-- Create a much larger dataset: 30 days of data, with readings every hour
-- For each day, we create forecasts every hour for the next 24 hours
-- This will create approximately 30 days × 24 hours × 24 forecast points = 17,280 rows
\timing on
-- First, insert the base forecast data (no parameters yet)
INSERT INTO temperature_forecasts (time, forecast_time, value)
SELECT 
    -- The time the forecast was made (hourly forecasts)
    base_date + (forecast_hour || ' hours')::interval AS time,
    
    -- The time being forecasted (each forecast predicts 24 hours ahead in hourly increments)
    base_date + (forecast_hour || ' hours')::interval + (lead_hour || ' hours')::interval AS forecast_time,
    
    -- The value: base temperature + day effect + hour effect + random noise
    15.0 + -- base temperature of 15°C
    (extract(day from base_date)::integer % 5) + -- 5-day temperature cycle
    sin(forecast_hour/24.0 * 2 * pi()) * 3 + -- daily cycle (±3°C)
    random() * 2 - 1 -- random noise ±1°C
FROM 
    generate_series('2025-03-01'::date, '2025-04-01'::date, '1 day'::interval) AS base_date,
    generate_series(0, 23) AS forecast_hour,
    generate_series(1, 24) AS lead_hour;

-- Now insert multi-parameter data: temperature, humidity, wind speed, and pressure
-- Each forecast will include 4 different parameters
INSERT INTO temperature_forecasts (time, forecast_time, value, parameter_id)
SELECT 
    -- Same forecast times as before, but every 3 hours instead of every hour
    base_date + (forecast_hour * 3 || ' hours')::interval AS time,
    base_date + (forecast_hour * 3 || ' hours')::interval + (lead_hour * 3 || ' hours')::interval AS forecast_time,
    
    -- Different values based on parameter type
    CASE 
        WHEN parameter_id = 1 THEN -- Temperature (°C)
            15.0 + (extract(day from base_date)::integer % 5) + sin(forecast_hour/8.0 * 2 * pi()) * 3 + random() * 2 - 1
            
        WHEN parameter_id = 2 THEN -- Humidity (%)
            60.0 + (extract(day from base_date)::integer % 7) * 2 + cos(forecast_hour/8.0 * 2 * pi()) * 10 + random() * 5
            
        WHEN parameter_id = 3 THEN -- Wind speed (m/s)
            5.0 + (extract(day from base_date)::integer % 4) + sin(forecast_hour/6.0 * 2 * pi()) * 2 + random() * 3
            
        WHEN parameter_id = 4 THEN -- Pressure (hPa)
            1013.0 + (extract(day from base_date)::integer % 10) + cos(forecast_hour/12.0 * 2 * pi()) * 5 + random() * 2
    END AS value,
    parameter_id
FROM 
    generate_series('2025-02-01'::date, '2025-03-02'::date, '1 day'::interval) AS base_date,
    generate_series(0, 7) AS forecast_hour, -- 8 forecasts per day (every 3 hours)
    generate_series(0, 7) AS lead_hour,    -- 8 lead times (covering 24 hours)
    generate_series(1, 4) AS parameter_id;  -- 4 parameters

-- Show how many rows we created
SELECT count(*) FROM temperature_forecasts;

-- Show the chunks that were created
SELECT show_chunks('temperature_forecasts');

-- Create hypertable statistics to optimize query planning
ANALYZE temperature_forecasts;

-- QUERY 1: Show chunk skipping in action by querying only a specific date range
-- Observe execution time with chunk skipping enabled
EXPLAIN ANALYZE
WITH latest_forecasts AS (
    SELECT DISTINCT ON (forecast_time) 
        forecast_time,
        value
    FROM temperature_forecasts
    WHERE 
        forecast_time BETWEEN '2025-02-15 00:00:00' AND '2025-02-16 23:59:59' AND
        parameter_id = 1
    ORDER BY forecast_time, time DESC
)
SELECT * FROM latest_forecasts ORDER BY forecast_time;

-- Disable chunk skipping to compare performance
SELECT disable_chunk_skipping('temperature_forecasts', 'forecast_time');

-- Run the same query with chunk skipping disabled
EXPLAIN ANALYZE
WITH latest_forecasts AS (
    SELECT DISTINCT ON (forecast_time) 
        forecast_time,
        value
    FROM temperature_forecasts
    WHERE 
        forecast_time BETWEEN '2025-02-15 00:00:00' AND '2025-02-16 23:59:59' AND
        parameter_id = 1
    ORDER BY forecast_time, time DESC
)
SELECT * FROM latest_forecasts ORDER BY forecast_time;

-- Re-enable chunk skipping for further tests
SELECT enable_chunk_skipping('temperature_forecasts', 'forecast_time');

-- Test a more complex query that gets statistics for multiple parameters
EXPLAIN ANALYZE
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    min(value) AS min_value,
    max(value) AS max_value,
    stddev(value) AS stddev_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN '2025-02-10' AND '2025-02-15' AND
    parameter_id IN (1, 2) -- Only temperature and humidity
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Disable chunk skipping again for comparison
SELECT disable_chunk_skipping('temperature_forecasts', 'forecast_time');

-- Run the same complex query with chunk skipping disabled
EXPLAIN ANALYZE
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    min(value) AS min_value,
    max(value) AS max_value,
    stddev(value) AS stddev_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN '2025-02-10' AND '2025-02-15' AND
    parameter_id IN (1, 2) -- Only temperature and humidity
GROUP BY day, parameter_id
ORDER BY day, parameter_id;
\timing off