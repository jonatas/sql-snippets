-- Example by Dustin Soresen - TimescaleDB community.
-- Aggregate an array of 'any' jsonb values
CREATE OR REPLACE FUNCTION aggregate_jsonb_array(jsonb[])
RETURNS jsonb AS $$
DECLARE
  jsonb_value jsonb;
  agg_mode text;
  agg_avg numeric;
BEGIN
  -- Use mode for non numeric values
  FOREACH jsonb_value IN ARRAY $1
  LOOP
      -- If there is even one non-numeric value then treat the whole array as non-numeric
      IF NOT jsonb_typeof(jsonb_value) IN ('number') THEN
        -- Convert array to table in order to pass into mode() as an aggregated argument
        SELECT
            mode() WITHIN GROUP (ORDER BY value) FROM (SELECT trim('"' FROM value::text) AS value FROM unnest($1) AS value) AS values
        INTO agg_mode;
        RETURN to_jsonb(agg_mode);
      END IF;
  END LOOP;
  -- Use average for numeric values
  -- Convert array to table in order to pass into avg() as an aggregated argument
  SELECT
      avg(value::numeric) FROM (SELECT value FROM unnest($1) AS value) AS values
  INTO agg_avg;
  RETURN to_jsonb(agg_avg);
END;
$$
STRICT
IMMUTABLE
LANGUAGE plpgsql;

-- Custom aggregate for 'any' values stored as jsonb
CREATE OR REPLACE AGGREGATE aggregate_all_types_jsonb(jsonb) (
  sfunc = array_append,
  stype = jsonb[],
  combinefunc = array_cat,
  finalfunc = aggregate_jsonb_array,
  initcond = '{}'
);

-- Step 1: Create a table for storing metrics
CREATE TABLE control_system_metrics (
    time TIMESTAMPTZ NOT NULL,
    gateway TEXT NOT NULL,
    channel TEXT NOT NULL,
    data JSONB NOT NULL
);

-- Step 2: Convert the table into a hypertable for better performance with TimescaleDB
SELECT create_hypertable('control_system_metrics', 'time');

-- Step 3: Insert example data into the control_system_metrics table
INSERT INTO control_system_metrics (time, gateway, channel, data) VALUES
('2024-03-15 12:00:00', 'gateway_1', 'channel_1', '{"v": [10, 20, 30]}'),
('2024-03-15 12:05:00', 'gateway_2', 'channel_1', '{"v": [15, 25, 35]}'),
('2024-03-15 12:10:00', 'gateway_1', 'channel_2', '{"v": "sensor_error"}');

-- Custom functions for aggregating JSONB values provided in the previous response

-- Step 4: Create the materialized view for aggregating sensor data
CREATE MATERIALIZED VIEW IF NOT EXISTS five_minute_aggregate
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT
  time_bucket('5 minutes', time) AS time_bucket,
  gateway,
  channel,
  aggregate_all_types_jsonb(data -> 'v') AS value
FROM control_system_metrics
GROUP BY time_bucket, gateway, channel
WITH NO DATA;

-- Refresh the materialized view to see the aggregated results
CALL refresh_continuous_aggregate('five_minute_aggregate', NULL, NULL);

-- Query the materialized view to verify the aggregated results
SELECT * FROM five_minute_aggregate;
