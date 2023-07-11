
DROP TABLE "ticks" CASCADE;
CREATE TABLE "ticks" ("time" timestamp with time zone, "symbol" character varying, "price" decimal, "volume" float);
SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '1 week');
ALTER TABLE ticks SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time',
  timescaledb.compress_segmentby = 'symbol'
);

SELECT add_compression_policy('ticks', INTERVAL '1 month');
CREATE MATERIALIZED VIEW candlestick_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', time), "ticks"."symbol", toolkit_experimental.candlestick_agg(time, price, volume) as candlestick FROM "ticks" GROUP BY 1, 2 ORDER BY 1
WITH NO DATA;

SELECT add_continuous_aggregate_policy('candlestick_1m',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute');

CREATE MATERIALIZED VIEW candlestick_1h
WITH (timescaledb.continuous) AS
SELECT symbol, time_bucket('1 hour', "time_bucket"),
            toolkit_experimental.rollup(candlestick) as candlestick FROM "candlestick_1m" GROUP BY "candlestick_1m"."symbol", time_bucket('1 hour', "time_bucket")
WITH NO DATA;

SELECT add_continuous_aggregate_policy('candlestick_1h',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW candlestick_1d
WITH (timescaledb.continuous) AS
SELECT symbol, time_bucket('1 day', "time_bucket"),
            toolkit_experimental.rollup(candlestick) as candlestick FROM "candlestick_1h" GROUP BY "candlestick_1h"."symbol", time_bucket('1 day', "time_bucket")
WITH NO DATA;

