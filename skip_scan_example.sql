-- Create the extension and table
CREATE EXTENSION IF NOT EXISTS timescaledb;

DROP TABLE IF EXISTS metrics cascade;
CREATE TABLE metrics (
    ts timestamp with time zone NOT NULL,
    device_id uuid NOT NULL,
    metric_name text NOT NULL,
    int_val integer,
    double_val double precision,
    string_val text,
    boolean_val boolean
);

-- Create the hypertable
SELECT create_hypertable('metrics', by_range('ts'));

-- Create the index with metric_name first
CREATE INDEX "IX_metrics_metric_name_device_id_ts" ON metrics (metric_name, device_id, ts DESC);

-- Generate sample data with specific focus on the metrics we care about
WITH RECURSIVE devices AS (
    SELECT gen_random_uuid() AS device_id
    FROM generate_series(1, 3)
),
metrics AS (
    SELECT unnest(ARRAY['motor_b', 'alarm_a', 'temperature', 'pressure']) AS metric_name
),
times AS (
    SELECT generate_series(
        '2024-01-01 00:00:00'::timestamptz,
        '2024-01-02 00:00:00'::timestamptz,
        '1 hour'::interval
    ) AS ts
)
INSERT INTO metrics (ts, device_id, metric_name, double_val)
SELECT 
    times.ts,
    devices.device_id,
    metrics.metric_name,
    random() * 100
FROM times
CROSS JOIN devices
CROSS JOIN metrics;

ANALYZE metrics;

-- Your query with IN clause
EXPLAIN (ANALYZE, COSTS OFF)
SELECT DISTINCT ON (metric_name) *
FROM metrics
WHERE device_id = (SELECT device_id FROM metrics LIMIT 1)
  AND ts < '2024-01-02'
  AND metric_name IN ('motor_b', 'alarm_a')
ORDER BY metric_name, ts DESC;

--
-- Unique (actual time=0.042..0.059 rows=2 loops=1)
--   InitPlan 1 (returns $0)
--     ->  Limit (actual time=0.009..0.010 rows=1 loops=1)
--           ->  Seq Scan on _hyper_393_8594_chunk _hyper_393_8594_chunk_1 (actual time=0.009..0.009 rows=1 loops=1)
--   ->  Custom Scan (ChunkAppend) on metrics (actual time=0.029..0.043 rows=2 loops=1)
--         Order: metrics.metric_name, metrics.ts DESC
--         Hypertables excluded during runtime: 0
--         ->  Custom Scan (SkipScan) on _hyper_393_8594_chunk (actual time=0.023..0.037 rows=2 loops=1)
--               ->  Index Scan using "_hyper_393_8594_chunk_IX_metrics_metric_name_device_id_ts" on _hyper_393_8594_chunk (actual time=0.022..0.032 rows=2 loops=1)
--                     Index Cond: ((metric_name = ANY ('{moto_hours,alarm_exhaust_louver_2_error}'::text[])) AND (device_id = $0) AND (ts < '2024-01-02 00:00:00+00'::timestamp with time zone))
-- Planning Time: 0.683 ms
-- Execution Time: 0.098 ms
--(12 rows)
--
