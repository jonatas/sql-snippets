DROP TABLE if exists ticks CASCADE;
DROP materialized view if exists ohlc CASCADE;
CREATE TABLE ticks
( time TIMESTAMPTZ NOT NULL,
    symbol varchar,
    price double precision,
    volume int);

SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '1 day');

CREATE MATERIALIZED VIEW _ohlcv_1m
WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 minute'::interval, time),
      symbol,
      toolkit_experimental.ohlc(time, price),
      sum(volume) as volume
    FROM ticks
    GROUP BY 1,2 WITH DATA;

CREATE VIEW _ohlcv_1h AS
    SELECT time_bucket('1 hour'::interval, time_bucket),
      symbol,
      toolkit_experimental.rollup(ohlc) as ohlc,
      sum(volume) as volume
    FROM _ohlcv_1m
    GROUP BY 1,2;

CREATE VIEW ohlcv_1m as
SELECT time_bucket,
  symbol,
  toolkit_experimental.open(ohlc),
  toolkit_experimental.open_time(ohlc),
  toolkit_experimental.high(ohlc),
  toolkit_experimental.high_time(ohlc),
  toolkit_experimental.low(ohlc),
  toolkit_experimental.low_time(ohlc),
  toolkit_experimental.close(ohlc),
  toolkit_experimental.close_time(ohlc),
  volume
FROM _ohlcv_1m;

CREATE VIEW ohlcv_1h as
SELECT time_bucket,
  symbol,
  toolkit_experimental.open(ohlc),
  toolkit_experimental.open_time(ohlc),
  toolkit_experimental.high(ohlc),
  toolkit_experimental.high_time(ohlc),
  toolkit_experimental.low(ohlc),
  toolkit_experimental.low_time(ohlc),
  toolkit_experimental.close(ohlc),
  toolkit_experimental.close_time(ohlc),
  volume
FROM _ohlcv_1h;


INSERT INTO ticks
SELECT time, 'SYMBOL', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-02 00:00:00' + INTERVAL '1 hour',
             INTERVAL '1 second') AS time;

INSERT INTO ticks
SELECT time, 'SYMBOL2', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-02 00:00:00' + INTERVAL '1 hour',
             INTERVAL '1 second') AS time;

select time_bucket, open, high, low, close, volume
from ohlcv_1m
ORDER BY time_bucket DESC LIMIT 10;

select array_agg(extract( epoch from  time_bucket)) as t,
array_agg(open) as o,
array_agg(high) as h,
array_agg(low) as l,
array_agg(close) as c,
array_agg(volume) as v from ohlcv_1m

where symbol = 'SYMBOL2'
group BY time_bucket('3 m', time_bucket) limit 1 ;

