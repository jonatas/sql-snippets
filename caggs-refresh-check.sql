DROP TABLE metrics CASCADE;
-- Create the 'metrics' table
CREATE TABLE metrics(
    time timestamptz NOT NULL, 
    value float
);

-- Convert 'metrics' table into a hypertable
SELECT create_hypertable('metrics', 'time');

-- Create a continuous aggregate materialized view for daily data
CREATE MATERIALIZED VIEW metrics_by_day
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS 
SELECT time_bucket('1 day', time) AS bucket, avg(value) as average_cagg
FROM metrics
GROUP BY 1 ORDER BY 1 WITH NO DATA;

-- Insert data into 'metrics' table
INSERT INTO metrics VALUES
(now(), 3),
(now() - interval '1 hour', 4),
(now() - interval '2 hour', 4);

INSERT INTO metrics VALUES (now() - interval '1 day', 3);
INSERT INTO metrics VALUES (now() + interval '1 day', 2);

table metrics_by_day;

select 'metrics table';
-- Aggregate data by day
SELECT time_bucket('1 day', time), avg(value) as avg_from_raw_data
FROM metrics
GROUP BY 1 ORDER BY 1;

table metrics_by_day;

select 'refreshing materialized view';
-- Refresh the continuous aggregate
CALL refresh_continuous_aggregate('metrics_by_day', NULL, NULL);

-- Retrieve data from materialized views
SELECT * FROM metrics_by_day;

