DROP TABLE ticks CASCADE;
CREATE TABLE ticks ( time TIMESTAMP NOT NULL, symbol varchar, price decimal, volume int);
SELECT create_hypertable('ticks', 'time');


INSERT INTO ticks VALUES 
('2021-08-26 10:09:00.01'::timestamp, 'SYMBOL', 10.1, 100),
('2021-08-26 10:09:00.08'::timestamp, 'SYMBOL', 10.0, 100),
('2021-08-26 10:09:00.23'::timestamp, 'SYMBOL', 10.2, 100),
('2021-08-26 10:09:00.40'::timestamp, 'SYMBOL', 10.3, 100);


DROP TABLE ohlc_1s CASCADE;
CREATE TABLE ohlc_1s ( time TIMESTAMP NOT NULL, symbol varchar, o decimal, h decimal, l decimal, c decimal, v int);
SELECT create_hypertable('ohlc_1s', 'time');
CREATE OR REPLACE FUNCTION feed_ohlc_1s() RETURNS trigger AS
$BODY$
DECLARE
    last_time timestamp;
BEGIN
   SELECT time_bucket('1 second', time) INTO last_time
   FROM ticks WHERE symbol = NEW.symbol
   ORDER BY time DESC LIMIT 1;

   -- When turn next second
   IF NEW.time - last_time >= INTERVAL '1 second' THEN
      INSERT INTO ohlc_1s (time, symbol, o, h, l, c, v)
        SELECT time_bucket('1 second', time) as time,
          symbol,
          FIRST(price, time) as open,
          MAX(price) as high,
          MIN(price) as low,
          LAST(price, time) as close,
          SUM(volume) as volume FROM ticks
        GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 1;
  END IF;
  RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER feed_ohlc_every_new_second
               BEFORE INSERT
               ON ticks
               FOR EACH ROW
               EXECUTE PROCEDURE feed_ohlc_1s();
table ticks;
table ohlc_1s;
INSERT INTO ticks VALUES 
('2021-08-26 10:09:01.02'::timestamp, 'SYMBOL', 10.0, 100),
('2021-08-26 10:09:01.04'::timestamp, 'SYMBOL', 14.0, 200),
('2021-08-26 10:09:01.42'::timestamp, 'SYMBOL', 12.3, 200),
('2021-08-26 10:09:01.62'::timestamp, 'SYMBOL', 8.3, 200),
('2021-08-26 10:09:02.80'::timestamp, 'SYMBOL', 9.0, 500);
table ticks;
table ohlc_1s;
