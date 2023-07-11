\timing
CREATE TABLE
  src (
    timestamp TIMESTAMP NOT NULL,
    a SMALLINT NOT NULL,
    b SMALLINT NOT NULL,
    c SMALLINT NOT NULL,
    d SMALLINT NOT NULL,
    value SMALLINT NOT NULL
  );

SELECT
  create_hypertable(
    'src', 'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 hour',
    create_default_indexes => FALSE
  );

INSERT INTO src
SELECT timestamp, a, b, c, d, random()*100 as value FROM generate_series(
    '2023-01-01 00:00:00'::timestamp,
    '2023-01-01 00:01:00'::timestamp,
    INTERVAL '1 second'
  ) as timestamp,
generate_series(1,4) a,
generate_series(1,4) b,
generate_series(1,2) c,
generate_series(1,2) d;

CREATE MATERIALIZED VIEW agg_1m
WITH (timescaledb.continuous, timescaledb.create_group_indexes=false, timescaledb.materialized_only=false) AS
SELECT time_bucket(INTERVAL '1 minute', timestamp) as bucket, a, b, c, d, avg(value) as value
FROM src GROUP BY 1, a, b, c, d WITH NO DATA;

CREATE MATERIALIZED VIEW agg_5m
WITH (timescaledb.continuous, timescaledb.create_group_indexes=false, timescaledb.materialized_only=false) AS
SELECT time_bucket(INTERVAL '5 minute', bucket) as bucket, a, b, c, d, avg(value) as value
FROM agg_1m GROUP BY 1, a, b, c, d WITH NO DATA;

CREATE MATERIALIZED VIEW agg_15m
WITH (timescaledb.continuous, timescaledb.create_group_indexes=false, timescaledb.materialized_only=false) AS
SELECT time_bucket(INTERVAL '15 minute', bucket) as bucket, a, b, c, d, avg(value) as value
FROM agg_5m GROUP BY 1, a, b, c, d WITH NO DATA;

CREATE INDEX ON src (a, b, c, d, timestamp DESC);
CREATE INDEX ON agg_1m (a, b, c, d, bucket DESC);
CREATE INDEX ON agg_5m (a, b, c, d, bucket DESC);
CREATE INDEX ON agg_15m (a, b, c, d, bucket DESC);

CALL refresh_continuous_aggregate('agg_1m', '2023-01-01 00:00:00', '2023-01-01 01:55:00');
CALL refresh_continuous_aggregate('agg_5m', '2023-01-01 00:00:00', '2023-01-01 01:55:00');
CALL refresh_continuous_aggregate('agg_15m', '2023-01-01 00:00:00', '2023-01-01 01:55:00');

-- RUN against agg_1m: this is very fast
explain (ANALYZE, COSTS, VERBOSE, BUFFERS) select bucket, a, b, c, d, value from agg_1m where a = 1 and b = 2 and c = 3 and d = 4 and bucket > '2023-01-01 00:00:00';

-- RUN against agg_15m: this is very slow
explain (ANALYZE, COSTS, VERBOSE, BUFFERS) select bucket, a, b, c, d, value from agg_15m where a = 1 and b = 2 and c = 3 and d = 4 and bucket > '2023-01-01 00:00:00';
