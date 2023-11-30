-- DROP TABLE metrics CASCADE;
CREATE TABLE metrics(time timestamptz NOT NULL, device_id int, value float);
SELECT create_hypertable('metrics', 'time');

CREATE MATERIALIZED VIEW metrics_by_hour WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, count(*) FROM metrics GROUP BY 1;

CREATE MATERIALIZED VIEW metrics_by_day WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket) AS bucket, sum(count) AS count FROM metrics_by_hour GROUP BY 1;

CREATE MATERIALIZED VIEW metrics_by_week WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', bucket) AS bucket, sum(count) AS count FROM metrics_by_day GROUP BY 1;

CREATE OR REPLACE PROCEDURE refresh_all_caggs(job_id int, config jsonb)
LANGUAGE PLPGSQL AS $$
DECLARE
    _cagg RECORD;
BEGIN
    FOR _cagg IN
        WITH RECURSIVE caggs AS (
            SELECT mat_hypertable_id, parent_mat_hypertable_id, user_view_name
            FROM _timescaledb_catalog.continuous_agg
            WHERE user_view_name = 'metrics_by_week'
            UNION ALL
            SELECT continuous_agg.mat_hypertable_id, continuous_agg.parent_mat_hypertable_id, continuous_agg.user_view_name
            FROM _timescaledb_catalog.continuous_agg
            JOIN caggs ON caggs.parent_mat_hypertable_id = continuous_agg.mat_hypertable_id
        )
        SELECT * FROM caggs ORDER BY mat_hypertable_id
    LOOP
        EXECUTE format('CALL refresh_continuous_aggregate(%L, NULL, NULL)', _cagg.user_view_name);
        COMMIT;
    END LOOP;
END;
$$;

SELECT add_job('refresh_all_caggs', '5 seconds');
-- Let's insert some data
CREATE OR REPLACE FUNCTION insert_random_metrics(job_id int, config jsonb)
RETURNS VOID LANGUAGE PLPGSQL AS $$
DECLARE
    last_time timestamptz;
    interval_value interval DEFAULT '1 minute'; -- default interval
BEGIN
    -- Attempt to fetch the most recent timestamp from the metrics table
    SELECT INTO last_time MAX(time) FROM metrics;

    -- If no data is found, default to one week ago
    IF last_time IS NULL THEN
        last_time := now() - interval '1 week';
    END IF;

    -- Check if an interval is provided in the config and use it if available
    IF config ? 'interval' THEN
        interval_value := (config ->> 'interval')::interval;
    END IF;

    -- Insert new data starting from the determined timestamp
    INSERT INTO metrics (time, device_id, value)
    VALUES (last_time + interval_value, trunc(random() * 100)::int, random() * 100);
END;
$$;

-- After creating the function, you can schedule it to run at regular intervals using TimescaleDB's job scheduling system.
SELECT add_job('insert_random_metrics', '1 second', '{"interval": "35 minutes"}');


SELECT 
    mbh.bucket AS hour_bucket,
    mbd.bucket AS day_bucket,
    mbw.bucket AS week_bucket,
    mbh.count AS count_hour,
    mbd.count AS count_day,
    mbw.count AS count_week
FROM 
    (SELECT bucket, count FROM metrics_by_hour ORDER BY bucket DESC LIMIT 1) mbh
JOIN 
    (SELECT bucket, count FROM metrics_by_day ORDER BY bucket DESC LIMIT 1) mbd 
    ON mbd.bucket = time_bucket('1 day',mbh.bucket)
JOIN 
    (SELECT bucket, count FROM metrics_by_week ORDER BY bucket DESC LIMIT 1) mbw 
    ON mbw.bucket = time_bucket('1 week',mbh.bucket)


\watch
