\echo on
select delete_job(job_id) from timescaledb_information.jobs where job_id > 1000;

DROP MATERIALIZED VIEW candlestick_10s cascade;
DROP TABLE "ticks" CASCADE;

CREATE TABLE "ticks" ("time" timestamp with time zone not null, "symbol" text, "price" decimal, "volume" float);

SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '1 day');

ALTER TABLE ticks SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time',
  timescaledb.compress_segmentby = 'symbol'
);
CREATE MATERIALIZED VIEW candlestick_10s
WITH (timescaledb.continuous) AS
SELECT time_bucket('10s', time),
       "ticks"."symbol",
       toolkit_experimental.candlestick_agg(time, price, volume) as candlestick
FROM "ticks"
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

CREATE MATERIALIZED VIEW candlestick_1m
WITH (timescaledb.continuous ) AS
SELECT time_bucket('1m', "time_bucket"),
       symbol,
       toolkit_experimental.rollup(candlestick) as candlestick 
FROM "candlestick_10s"
GROUP BY 1, 2
ORDER BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW candlestick_5m
WITH (timescaledb.continuous) AS
SELECT time_bucket('5m', "time_bucket"),
       symbol,
       toolkit_experimental.rollup(candlestick) as candlestick
FROM "candlestick_1m"
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

CREATE OR REPLACE PROCEDURE notify_new_candlestick(job_id int, config jsonb)
LANGUAGE PLPGSQL AS $$
DECLARE
  view_name TEXT := config ->> 'view_name';
  last_row RECORD;
BEGIN
  EXECUTE format('SELECT * FROM %I ORDER BY time_bucket DESC LIMIT 1', view_name)
  INTO last_row;
  PERFORM pg_notify(view_name, row_to_json(last_row)::text);
END
$$;

SELECT add_job('notify_new_candlestick', '10s', config => '{"view_name": "candlestick_10s"}', fixed_schedule => true);
SELECT add_job('notify_new_candlestick', '1m', config => '{"view_name": "candlestick_1m"}', fixed_schedule => true);
SELECT add_job('notify_new_candlestick', '5m', config => '{"view_name": "candlestick_5m"}', fixed_schedule => true);

LISTEN candlestick_10s;
LISTEN candlestick_1m;
LISTEN candlestick_5m;

CREATE OR REPLACE PROCEDURE simulate_tick(job_id int, config jsonb)
LANGUAGE PLPGSQL AS $$
DECLARE
  candlestick record := null;
BEGIN
  EXECUTE 'INSERT INTO ticks
    SELECT now(), $1,
      (random()*30)::int,
      100*(random()*10)::int'
    USING config->>'symbol';
END
$$;

SELECT add_job('simulate_tick', '1s', config => '{"symbol": "APPL"}', fixed_schedule => true);
SELECT add_job('simulate_tick', '0.5s', config => '{"symbol": "GOOG"}', fixed_schedule => true);

