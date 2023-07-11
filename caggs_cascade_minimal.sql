\echo
 drop materialized view candlestick_1d cascade;
 drop materialized view candlestick_1h cascade;
 drop materialized view candlestick_1m cascade;

DROP TABLE "ticks" CASCADE;

CREATE TABLE "ticks" ("time" timestamp with time zone not null, "symbol" text, "price" decimal, "volume" float);

SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '1 day');

ALTER TABLE ticks SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time',
  timescaledb.compress_segmentby = 'symbol'
);
CREATE MATERIALIZED VIEW candlestick_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', time),
       "ticks"."symbol",
       toolkit_experimental.candlestick_agg(time, price, volume) as candlestick
FROM "ticks"
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

CREATE MATERIALIZED VIEW candlestick_1h
WITH (timescaledb.continuous ) AS
SELECT time_bucket('1 hour', "time_bucket"),
       symbol,
       toolkit_experimental.rollup(candlestick) as candlestick 
FROM "candlestick_1m"
GROUP BY 1, 2
ORDER BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW candlestick_1d
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', "time_bucket"),
       symbol,
       toolkit_experimental.rollup(candlestick) as candlestick
FROM "candlestick_1h"
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

CREATE OR REPLACE FUNCTION notify_new_row() RETURNS TRIGGER AS $$
  DECLARE
  threshold INTERVAL := INTERVAL '0 seconds';
  h1 candlestick_1h := null;
BEGIN
  select * from candlestick_1h order by time_bucket desc limit 1 into h1;
  raise notice ' ---- NEW %  >>> H1 % ----', NEW, h1;
  raise notice '% > %', NEW.time - h1.time_bucket, threshold;
  IF (NEW.time - h1.time_bucket) >= threshold THEN
    PERFORM pg_notify('h1', row_to_json(h1)::text);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER notify_new_row_trigger
AFTER INSERT ON ticks
FOR EACH ROW
EXECUTE FUNCTION notify_new_row();


\echo last tick ;
table ticks order by time desc limit 1  ;
\echo last candlestick_1h;

table candlestick_1h order by time_bucket desc limit 1;

INSERT INTO ticks
SELECT time, 'SYMBOL', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-01 01:11:00',
             INTERVAL '10 minutes') AS time;

-- SELECT time_bucket, symbol, toolkit_experimental.open(candlestick), toolkit_experimental.high(candlestick), toolkit_experimental.low(candlestick), toolkit_experimental.close(candlestick), toolkit_experimental.volume(candlestick) FROM candlestick_1d WHERE time_bucket BETWEEN '2022-01-01' and '2022-01-07' ;

