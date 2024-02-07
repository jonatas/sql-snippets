\echo
 drop materialized view ohlcv_1d cascade;
 drop materialized view ohlcv_1h cascade;
 drop materialized view ohlcv_1m cascade;

DROP TABLE "ticks" CASCADE;

CREATE TABLE "ticks" (
  "time" timestamp with time zone not null,
  "symbol" text,
  "source" text,
  "price" decimal,
  "volume" float);

SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '4 hours');

ALTER TABLE ticks SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time',
  timescaledb.compress_segmentby = 'source,symbol'
);

select add_retention_policy('ticks', drop_after => interval '1 day');

CREATE MATERIALIZED VIEW ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', time),
       "ticks"."symbol",
       "ticks"."source",
       candlestick_agg(time, price, volume) as ohlcv
FROM "ticks"
GROUP BY 1, 2, 3
ORDER BY 1
WITH DATA;

CREATE MATERIALIZED VIEW ohlcv_1h
WITH (timescaledb.continuous ) AS
SELECT time_bucket('1 hour', "time_bucket"),
       symbol,
       source,
       rollup(ohlcv) as ohlcv 
FROM "ohlcv_1m"
GROUP BY 1, 2, 3
ORDER BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW ohlcv_1d
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', "time_bucket"),
       symbol,
       source,
       rollup(ohlcv) as ohlcv
FROM "ohlcv_1h"
GROUP BY 1, 2, 3
ORDER BY 1
WITH DATA;

INSERT INTO ticks
SELECT time, 'STOCK0', 'A', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-01 01:11:00',
             INTERVAL '10 minutes') AS time;

INSERT INTO ticks
SELECT time, 'STOCK1', 'B', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-01 01:11:00',
             INTERVAL '10 minutes') AS time;

CREATE VIEW ohlcv_1m_b as
SELECT * FROM ohlcv_1m
WHERE source = 'B';

CREATE VIEW ohlcv_1m_a as
SELECT * FROM ohlcv_1m
WHERE source = 'A';

CREATE VIEW ohlcv_1m_a_stock_1 AS
SELECT * FROM ohlcv_1m_a
WHERE symbol = 'STOCK1';

CREATE VIEW ohlcv_1m_b_stock_0 as
SELECT * FROM ohlcv_1m_b
WHERE symbol = 'STOCK0';

SELECT time_bucket, symbol, source,
open(ohlcv), high(ohlcv),
low(ohlcv), close(ohlcv), volume(ohlcv) FROM ohlcv_1d ;

