DROP TABLE if exists ticks CASCADE;
DROP view if exists ohlc_1m CASCADE;
CREATE TABLE ticks
( time TIMESTAMP NOT NULL,
    symbol varchar,
    price decimal,
    volume int);
SELECT create_hypertable('ticks', 'time');

CREATE MATERIALIZED VIEW ohlc_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', time) as bucket,
          symbol,
          FIRST(price, time) as open,
          MAX(price) as high,
          MIN(price) as low,
          LAST(price, time) as close,
          SUM(volume) as volume FROM ticks
        GROUP BY 1, 2
WITH DATA;

SELECT add_continuous_aggregate_policy('ohlc_1m',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

INSERT INTO ticks
SELECT time, 'SYMBOL', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '50 seconds',
             INTERVAL '1 second') AS time;

TABLE ohlc_1m ORDER BY bucket;

INSERT INTO ticks
SELECT time, 'SYMBOL', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:01:00',
                 TIMESTAMP '2000-01-01 00:01:00' + INTERVAL '5 minutes',
             INTERVAL '1 second') AS time;

TABLE ohlc_1m ORDER BY bucket;

