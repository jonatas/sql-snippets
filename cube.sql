DROP TABLE if exists ticks CASCADE;
CREATE TABLE ticks
( time TIMESTAMP NOT NULL,
    symbol varchar,
    price decimal,
    volume int);
SELECT create_hypertable('ticks', 'time');



INSERT INTO ticks
SELECT time, 'SYMBOL', (random()*30)::int, 1 + (100*(random()*10))::int
FROM generate_series(TIMESTAMP '2000-01-01 00:01:00',
                 TIMESTAMP '2000-01-01 00:01:00' + INTERVAL '1 hour',
             INTERVAL '1 second') AS time;
INSERT INTO ticks
SELECT time, 'OTHER', (random()*30)::int, 1 + (100*(random()*10))::int
FROM generate_series(TIMESTAMP '2000-01-01 00:01:00',
                 TIMESTAMP '2000-01-01 00:01:00' + INTERVAL '1 hour',
             INTERVAL '1 second') AS time;


SELECT time_bucket('15m', time) as bucket,
       symbol,
        FIRST(price, time) as open,
          MAX(price) as high,
          MIN(price) as low,
          LAST(price, time) as close,
          SUM(volume) as volume FROM ticks
        GROUP BY CUBE ( 1, 2) order by 1, 2;

SELECT time_bucket('1h', time) as bucket,
       symbol,
        FIRST(price, time) as open,
          MAX(price) as high,
          MIN(price) as low,
          LAST(price, time) as close,
          SUM(volume) as volume FROM ticks
        GROUP BY CUBE ( 1, 2) order by 1, 2;
