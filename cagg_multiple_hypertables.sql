-- First, clean up any existing jobs
SELECT delete_job(job_id) 
FROM timescaledb_information.jobs 
WHERE proc_name IN ('sync_instrument1_data', 'sync_instrument2_data');

-- Drop existing objects
DROP VIEW IF EXISTS merged_data_view;
DROP TABLE IF EXISTS merged_measurements;
DROP TABLE IF EXISTS instrument1_data;
DROP TABLE IF EXISTS instrument2_data;
DROP FUNCTION IF EXISTS generate_instrument1_data(TIMESTAMPTZ, INT);
DROP FUNCTION IF EXISTS generate_instrument2_data(TIMESTAMPTZ, INT);
DROP FUNCTION IF EXISTS circular_avg(DOUBLE PRECISION[]);
DROP PROCEDURE IF EXISTS sync_instrument1_data(TIMESTAMPTZ, TIMESTAMPTZ);
DROP PROCEDURE IF EXISTS sync_instrument2_data(TIMESTAMPTZ, TIMESTAMPTZ);

-- Enable TimescaleDB extension if not already enabled
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create sample source hypertables (representing your different instruments)
CREATE TABLE instrument1_data (
    timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    direction DOUBLE PRECISION  -- 0-360 degrees
);

CREATE TABLE instrument2_data (
    timestamp TIMESTAMPTZ NOT NULL,
    speed DOUBLE PRECISION,
    heading DOUBLE PRECISION    -- 0-360 degrees
);

-- Convert these to hypertables
SELECT create_hypertable('instrument1_data', by_range('timestamp', INTERVAL '1 week'));
SELECT create_hypertable('instrument2_data', by_range('timestamp', INTERVAL '1 week'));

-- Create the merged data hypertable that will store synchronized data
CREATE TABLE merged_measurements (
    timestamp TIMESTAMPTZ NOT NULL PRIMARY KEY,
    avg_temperature DOUBLE PRECISION,
    avg_speed DOUBLE PRECISION,
    avg_direction DOUBLE PRECISION,
    avg_heading DOUBLE PRECISION,
    samples_count INTEGER
);

-- Convert to hypertable with 5-second chunks
SELECT create_hypertable('merged_measurements', by_range('timestamp',  INTERVAL '1 day'));

-- Create function to handle circular average (for directions/headings)
CREATE OR REPLACE FUNCTION circular_avg(angles DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
DECLARE
    x_sum DOUBLE PRECISION := 0;
    y_sum DOUBLE PRECISION := 0;
    result DOUBLE PRECISION;
BEGIN
    -- Sum of cosines and sines
    FOR i IN 1..array_length(angles, 1) LOOP
        x_sum := x_sum + cos(radians(angles[i]));
        y_sum := y_sum + sin(radians(angles[i]));
    END LOOP;
    
    -- Calculate average angle
    result := degrees(atan2(y_sum, x_sum));
    
    -- Convert to 0-360 range
    RETURN CASE 
        WHEN result < 0 THEN result + 360
        ELSE result
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create user-defined action for instrument1 data
CREATE OR REPLACE PROCEDURE sync_instrument1_data(start_time TIMESTAMPTZ, end_time TIMESTAMPTZ)
LANGUAGE plpgsql AS $$
BEGIN
    -- Insert aggregated data from instrument1 into merged measurements
    INSERT INTO merged_measurements (
        timestamp,
        avg_temperature,
        avg_direction,
        samples_count
    )
    SELECT 
        time_bucket('5 seconds', timestamp) AS bucket_time,
        avg(temperature) AS avg_temperature,
        circular_avg(array_agg(direction)) AS avg_direction,
        count(*) AS samples
    FROM instrument1_data
    WHERE timestamp >= start_time 
    AND timestamp < end_time
    GROUP BY bucket_time
    ON CONFLICT (timestamp) 
    DO UPDATE SET
        avg_temperature = EXCLUDED.avg_temperature,
        avg_direction = EXCLUDED.avg_direction,
        samples_count = merged_measurements.samples_count + EXCLUDED.samples_count;
END;
$$;

-- Create user-defined action for instrument2 data
CREATE OR REPLACE PROCEDURE sync_instrument2_data(start_time TIMESTAMPTZ, end_time TIMESTAMPTZ)
LANGUAGE plpgsql AS $$
BEGIN
    -- Insert aggregated data from instrument2 into merged measurements
    INSERT INTO merged_measurements (
        timestamp,
        avg_speed,
        avg_heading,
        samples_count
    )
    SELECT 
        time_bucket('5 seconds', timestamp) AS bucket_time,
        avg(speed) AS avg_speed,
        circular_avg(array_agg(heading)) AS avg_heading,
        count(*) AS samples
    FROM instrument2_data
    WHERE timestamp >= start_time 
    AND timestamp < end_time
    GROUP BY bucket_time
    ON CONFLICT (timestamp) 
    DO UPDATE SET
        avg_speed = EXCLUDED.avg_speed,
        avg_heading = EXCLUDED.avg_heading,
        samples_count = merged_measurements.samples_count + EXCLUDED.samples_count;
END;
$$;


-- Optional: Create a view for easier querying of complete records
CREATE VIEW merged_data_view AS
SELECT 
    timestamp,
    avg_temperature,
    avg_speed,
    avg_direction,
    avg_heading,
    samples_count
FROM merged_measurements
WHERE avg_temperature IS NOT NULL 
  AND avg_speed IS NOT NULL
  AND avg_direction IS NOT NULL
  AND avg_heading IS NOT NULL;

-- Function to generate test data for instrument 1
CREATE OR REPLACE FUNCTION generate_instrument1_data(
    start_time TIMESTAMPTZ,
    duration_minutes INT
) RETURNS void AS $$
DECLARE
    curr_ts TIMESTAMPTZ;
    temp_base DOUBLE PRECISION := 20.0;
    dir_base DOUBLE PRECISION := 0.0;
BEGIN
    curr_ts := start_time;
    WHILE curr_ts < start_time + (duration_minutes || ' minutes')::interval LOOP
        INSERT INTO instrument1_data (timestamp, temperature, direction)
        VALUES (
            curr_ts,
            temp_base + 2 * sin(extract(epoch from curr_ts)/3600.0) + random() - 0.5,
            -- Use mathematical modulo for floating point
            (dir_base + extract(epoch from curr_ts - start_time)/10.0 + random()*5) - 
            360.0 * floor((dir_base + extract(epoch from curr_ts - start_time)/10.0 + random()*5) / 360.0)
        );
        curr_ts := curr_ts + '1 second'::interval;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_instrument2_data(
    start_time TIMESTAMPTZ,
    duration_minutes INT
) RETURNS void AS $$
DECLARE
    curr_ts TIMESTAMPTZ;
    speed_base DOUBLE PRECISION := 10.0;
    heading_base DOUBLE PRECISION := 180.0;
BEGIN
    curr_ts := start_time;
    WHILE curr_ts < start_time + (duration_minutes || ' minutes')::interval LOOP
        INSERT INTO instrument2_data (timestamp, speed, heading)
        VALUES (
            curr_ts,
            speed_base + 2 * cos(extract(epoch from curr_ts)/1800.0) + random() - 0.5,
            heading_base + 30 * sin(extract(epoch from curr_ts)/900.0) + random()*5
        );
        curr_ts := curr_ts + '1 second'::interval;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
-- Function to generate and sync test data
CREATE OR REPLACE FUNCTION generate_test_dataset(
    minutes_of_data INT DEFAULT 2
) RETURNS TABLE (
    instrument1_rows BIGINT,
    instrument2_rows BIGINT,
    merged_rows BIGINT
) AS $$
DECLARE
    start_ts TIMESTAMPTZ := date_trunc('minute', now());
BEGIN
    -- Clean existing data
    TRUNCATE TABLE instrument1_data;
    TRUNCATE TABLE instrument2_data;
    TRUNCATE TABLE merged_measurements;
    
    -- Generate test data for both instruments
    PERFORM generate_instrument1_data(start_ts, minutes_of_data);
    PERFORM generate_instrument2_data(start_ts, minutes_of_data);
    
    -- Run initial sync
    CALL sync_instrument1_data(start_ts, start_ts + (minutes_of_data || ' minutes')::interval);
    CALL sync_instrument2_data(start_ts, start_ts + (minutes_of_data || ' minutes')::interval);
    
    -- Return counts
    RETURN QUERY
    SELECT 
        (SELECT count(*) FROM instrument1_data)::bigint,
        (SELECT count(*) FROM instrument2_data)::bigint,
        (SELECT count(*) FROM merged_measurements)::bigint;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
SELECT * FROM generate_test_dataset(2);  -- Generate 2 minutes of data
SELECT * FROM merged_data_view ORDER BY timestamp LIMIT 5;  -- View results

-- 1. Check raw data counts
SELECT 
    (SELECT count(*) FROM instrument1_data) as instrument1_rows,
    (SELECT count(*) FROM instrument2_data) as instrument2_rows,
    (SELECT count(*) FROM merged_measurements) as merged_rows;

-- 3. Check for gaps in merged data
SELECT 
    timestamp as gap_start,
    timestamp + '5 seconds'::interval as gap_end
FROM (
    SELECT timestamp,
           lead(timestamp) OVER (ORDER BY timestamp) as next_timestamp
    FROM merged_measurements
) sub
WHERE next_timestamp - timestamp > '5 seconds'::interval;

table instrument1_data order by timestamp limit 5;
table instrument2_data order by timestamp limit 5;
-- 4. Verify circular averaging is working
SELECT 
    timestamp,
    avg_direction,
    avg_heading,
    samples_count
FROM merged_measurements
ORDER BY timestamp
LIMIT 5;


-- Example of scheduling the sync procedures (adjust intervals as needed)
-- SELECT add_job( 'sync_instrument1_data', '5 seconds', config => '{"start_offset": "5 seconds"}');
-- SELECT add_job( 'sync_instrument2_data', '5 seconds', config => '{"start_offset": "5 seconds"}');

