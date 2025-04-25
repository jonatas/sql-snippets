-- Performance testing script to identify when chunk skipping becomes beneficial
-- This script creates datasets of different sizes and tests chunk skipping effectiveness

-- Drop the existing table if it exists
DROP TABLE IF EXISTS temperature_forecasts CASCADE;

-- Create a table for temperature forecasts
CREATE TABLE temperature_forecasts (
    time TIMESTAMPTZ NOT NULL,           -- When the forecast was made
    forecast_time TIMESTAMPTZ NOT NULL,   -- Time being forecasted
    value DOUBLE PRECISION NOT NULL,      -- Forecasted temperature value
    parameter_id INTEGER NOT NULL,        -- Parameter type (1=temperature, 2=humidity, etc.)
    location_id INTEGER NOT NULL          -- Location identifier
);

-- Convert to hypertable with time partitioning (1-day chunks)
SELECT create_hypertable('temperature_forecasts', by_range('time', INTERVAL '1 day'));

-- Add a second dimension for forecast_time
SELECT add_dimension('temperature_forecasts', by_range('forecast_time', INTERVAL '1 day'));

-- Create indexes to improve query performance
CREATE INDEX ON temperature_forecasts (forecast_time, time DESC);
CREATE INDEX ON temperature_forecasts (parameter_id);
CREATE INDEX ON temperature_forecasts (location_id);

-- Enable compression on the hypertable
ALTER TABLE temperature_forecasts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'parameter_id, location_id',
    timescaledb.compress_orderby = 'time, forecast_time'
);

-- Create a function to generate test data
CREATE OR REPLACE FUNCTION generate_test_data(
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    num_locations INTEGER,
    num_parameters INTEGER
) RETURNS VOID AS $$
DECLARE
    i INTEGER;
    j INTEGER;
BEGIN
    -- Generate diverse data across all parameters and locations
    FOR i IN 1..num_parameters LOOP
        FOR j IN 1..num_locations LOOP
            -- Insert forecast data for each parameter and location
            INSERT INTO temperature_forecasts (time, forecast_time, value, parameter_id, location_id)
            SELECT 
                -- The time the forecast was made (every 4 hours)
                base_date + (forecast_hour || ' hours')::interval AS time,
                
                -- The time being forecasted (6, 12, 18, 24 hours ahead)
                base_date + (forecast_hour || ' hours')::interval + (lead_hour || ' hours')::interval AS forecast_time,
                
                -- The value with various influences
                15.0 + -- base temperature
                (extract(day from base_date)::integer % 5) + -- cyclic pattern
                sin(forecast_hour/24.0 * 2 * pi()) * 3 + -- daily cycle
                random() * 2 - 1 + -- random noise
                (i * 10) + -- parameter-based offset
                (j / 10.0), -- location-based offset
                
                i,  -- parameter_id
                j   -- location_id
            FROM 
                generate_series(start_date, end_date, '1 day'::interval) AS base_date,
                generate_series(0, 20, 4) AS forecast_hour,  -- Every 4 hours
                generate_series(6, 24, 6) AS lead_hour;      -- 6, 12, 18, 24 hours ahead
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Enable chunk skipping globally
ALTER SYSTEM SET timescaledb.enable_chunk_skipping = 'on';
SELECT pg_reload_conf();
SELECT pg_sleep(1);

\timing on

-- Test 1: Small dataset (1 month, 5 locations, 3 parameters)
TRUNCATE temperature_forecasts;
SELECT 'Generating small dataset (1 month, 5 locations, 3 parameters)...';
SELECT generate_test_data(
    CURRENT_DATE - INTERVAL '30 days',
    CURRENT_DATE,
    5,
    3
);

SELECT count(*) AS row_count FROM temperature_forecasts;
SELECT count(DISTINCT forecast_time::date) AS days FROM temperature_forecasts;
SELECT count(DISTINCT location_id) AS locations FROM temperature_forecasts;
SELECT count(DISTINCT parameter_id) AS parameters FROM temperature_forecasts;

-- Compress chunks older than 7 days
SELECT add_compression_policy('temperature_forecasts', INTERVAL '7 days');
DO $$
DECLARE
    chunk_name TEXT;
BEGIN
    FOR chunk_name IN SELECT show_chunks('temperature_forecasts', older_than => INTERVAL '7 days')
    LOOP
        EXECUTE format('SELECT compress_chunk(%L)', chunk_name);
    END LOOP;
END $$;

-- Number of chunks created
SELECT count(*) AS total_chunks FROM show_chunks('temperature_forecasts');
SELECT count(*) AS compressed_chunks 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'temperature_forecasts' AND is_compressed = true;

-- Enable chunk skipping on specific columns
SELECT enable_chunk_skipping('temperature_forecasts', 'forecast_time');
SELECT enable_chunk_skipping('temperature_forecasts', 'parameter_id');
SELECT enable_chunk_skipping('temperature_forecasts', 'location_id');

-- Update statistics
ANALYZE temperature_forecasts;

-- Test 1a: Very selective query - Small dataset WITH chunk skipping
SELECT 'Small dataset, selective query WITH chunk skipping:';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    location_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '15 days') AND (CURRENT_DATE - INTERVAL '14 days')
    AND parameter_id = 1
    AND location_id = 1
GROUP BY day, parameter_id, location_id
ORDER BY day, parameter_id, location_id;

-- Test 1b: Very selective query - Small dataset WITHOUT chunk skipping
SELECT 'Small dataset, selective query WITHOUT chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'off';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    location_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '15 days') AND (CURRENT_DATE - INTERVAL '14 days')
    AND parameter_id = 1
    AND location_id = 1
GROUP BY day, parameter_id, location_id
ORDER BY day, parameter_id, location_id;

-- Test 1c: Medium selective query - Small dataset WITH chunk skipping
SELECT 'Small dataset, medium-selective query WITH chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'on';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '20 days') AND (CURRENT_DATE - INTERVAL '15 days')
    AND parameter_id IN (1, 2)
    AND location_id <= 3
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Test 1d: Medium selective query - Small dataset WITHOUT chunk skipping
SELECT 'Small dataset, medium-selective query WITHOUT chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'off';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '20 days') AND (CURRENT_DATE - INTERVAL '15 days')
    AND parameter_id IN (1, 2)
    AND location_id <= 3
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Test 2: Medium dataset (3 months, 20 locations, 5 parameters)
TRUNCATE temperature_forecasts;
SELECT 'Generating medium dataset (3 months, 20 locations, 5 parameters)...';
SELECT generate_test_data(
    CURRENT_DATE - INTERVAL '90 days',
    CURRENT_DATE,
    20,
    5
);

SELECT count(*) AS row_count FROM temperature_forecasts;

-- Compress chunks older than 7 days
DO $$
DECLARE
    chunk_name TEXT;
BEGIN
    FOR chunk_name IN SELECT show_chunks('temperature_forecasts', older_than => INTERVAL '7 days')
    LOOP
        EXECUTE format('SELECT compress_chunk(%L)', chunk_name);
    END LOOP;
END $$;

-- Number of chunks created
SELECT count(*) AS total_chunks FROM show_chunks('temperature_forecasts');
SELECT count(*) AS compressed_chunks 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'temperature_forecasts' AND is_compressed = true;

-- Update statistics
ANALYZE temperature_forecasts;

-- Test 2a: Very selective query - Medium dataset WITH chunk skipping
SELECT 'Medium dataset, selective query WITH chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'on';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    location_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '45 days') AND (CURRENT_DATE - INTERVAL '44 days')
    AND parameter_id = 2
    AND location_id = 5
GROUP BY day, parameter_id, location_id
ORDER BY day, parameter_id, location_id;

-- Test 2b: Very selective query - Medium dataset WITHOUT chunk skipping
SELECT 'Medium dataset, selective query WITHOUT chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'off';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    location_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '45 days') AND (CURRENT_DATE - INTERVAL '44 days')
    AND parameter_id = 2
    AND location_id = 5
GROUP BY day, parameter_id, location_id
ORDER BY day, parameter_id, location_id;

-- Test 2c: Medium selective query - Medium dataset WITH chunk skipping
SELECT 'Medium dataset, medium-selective query WITH chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'on';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '60 days') AND (CURRENT_DATE - INTERVAL '50 days')
    AND parameter_id IN (1, 2, 3)
    AND location_id <= 10
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Test 2d: Medium selective query - Medium dataset WITHOUT chunk skipping
SELECT 'Medium dataset, medium-selective query WITHOUT chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'off';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '60 days') AND (CURRENT_DATE - INTERVAL '50 days')
    AND parameter_id IN (1, 2, 3)
    AND location_id <= 10
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Test 3: Large dataset (6 months, 50 locations, 10 parameters)
TRUNCATE temperature_forecasts;
SELECT 'Generating large dataset (6 months, 50 locations, 10 parameters)...';
SELECT generate_test_data(
    CURRENT_DATE - INTERVAL '180 days',
    CURRENT_DATE,
    50,
    10
);

SELECT count(*) AS row_count FROM temperature_forecasts;

-- Compress chunks older than 7 days
DO $$
DECLARE
    chunk_name TEXT;
BEGIN
    FOR chunk_name IN SELECT show_chunks('temperature_forecasts', older_than => INTERVAL '7 days')
    LOOP
        EXECUTE format('SELECT compress_chunk(%L)', chunk_name);
    END LOOP;
END $$;

-- Number of chunks created
SELECT count(*) AS total_chunks FROM show_chunks('temperature_forecasts');
SELECT count(*) AS compressed_chunks 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'temperature_forecasts' AND is_compressed = true;

-- Update statistics
ANALYZE temperature_forecasts;

-- Test 3a: Very selective query - Large dataset WITH chunk skipping
SELECT 'Large dataset, selective query WITH chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'on';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    location_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '90 days') AND (CURRENT_DATE - INTERVAL '89 days')
    AND parameter_id = 3
    AND location_id = 10
GROUP BY day, parameter_id, location_id
ORDER BY day, parameter_id, location_id;

-- Test 3b: Very selective query - Large dataset WITHOUT chunk skipping
SELECT 'Large dataset, selective query WITHOUT chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'off';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    location_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '90 days') AND (CURRENT_DATE - INTERVAL '89 days')
    AND parameter_id = 3
    AND location_id = 10
GROUP BY day, parameter_id, location_id
ORDER BY day, parameter_id, location_id;

-- Test 3c: Medium selective query - Large dataset WITH chunk skipping
SELECT 'Large dataset, medium-selective query WITH chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'on';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '120 days') AND (CURRENT_DATE - INTERVAL '90 days')
    AND parameter_id IN (1, 2, 3, 4, 5)
    AND location_id <= 25
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Test 3d: Medium selective query - Large dataset WITHOUT chunk skipping
SELECT 'Large dataset, medium-selective query WITHOUT chunk skipping:';
SET timescaledb.enable_chunk_skipping = 'off';
EXPLAIN (ANALYZE, VERBOSE)
SELECT 
    date_trunc('day', forecast_time) AS day,
    parameter_id,
    avg(value) AS avg_value,
    count(*) AS count
FROM temperature_forecasts
WHERE 
    forecast_time BETWEEN (CURRENT_DATE - INTERVAL '120 days') AND (CURRENT_DATE - INTERVAL '90 days')
    AND parameter_id IN (1, 2, 3, 4, 5)
    AND location_id <= 25
GROUP BY day, parameter_id
ORDER BY day, parameter_id;

-- Reset chunk skipping to default
SET timescaledb.enable_chunk_skipping = 'on';

-- Summary
-- Clean up
DROP FUNCTION IF EXISTS generate_test_data;

\timing off