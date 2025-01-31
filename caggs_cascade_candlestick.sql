
drop view ny_1h;
drop view berlin_1h;
drop MATERIALIZED view if exists candlestick_1d_tz_europe_berlin;
drop MATERIALIZED view if exists candlestick_1h_tz_america_new_york;
drop MATERIALIZED view if exists candlestick_1h_tz_europe_berlin;
drop MATERIALIZED view if exists candlestick_1m cascade;

DROP TABLE "ticks" CASCADE;

CREATE TABLE "ticks" ("time" timestamptz, "symbol" text, "price" decimal, "volume" float);
SELECT create_hypertable('ticks', by_range('time', INTERVAL '1 week'));

insert into ticks (time, symbol, price, volume)
values ('2025-01-31 10:00:00', 'BTC', 10000, 1),
       ('2025-01-31 10:10:00', 'BTC', 10001, 2),
       ('2025-01-31 10:20:00', 'BTC', 10002, 3),
       ('2025-01-31 10:30:00', 'BTC', 10003, 4),
       ('2025-01-31 10:40:00', 'BTC', 10004, 5),
       ('2025-01-31 10:50:00', 'BTC', 10005, 6),
       ('2025-01-31 11:00:00', 'BTC', 10006, 7),
       ('2025-01-31 11:10:00', 'BTC', 10007, 8),
       ('2025-01-31 11:20:00', 'BTC', 10008, 9),
       ('2025-01-31 11:30:00', 'BTC', 10009, 10),
       ('2025-01-31 11:40:00', 'BTC', 10010, 11),
       ('2025-01-31 11:50:00', 'BTC', 10011, 12),
       ('2025-01-31 12:00:00', 'BTC', 10012, 13),
       ('2025-01-31 12:10:00', 'BTC', 10013, 14),
       ('2025-01-31 12:20:00', 'BTC', 10014, 15),
       ('2025-01-31 12:30:00', 'BTC', 10015, 16),
       ('2025-01-31 12:40:00', 'BTC', 10016, 17),
       ('2025-01-31 12:50:00', 'BTC', 10017, 18),
       ('2025-01-31 13:00:00', 'BTC', 10018, 19),
       ('2025-01-31 13:10:00', 'BTC', 10019, 20),
       ('2025-01-31 13:20:00', 'BTC', 10020, 21),
       ('2025-01-31 13:30:00', 'BTC', 10021, 22),
       ('2025-01-31 13:40:00', 'BTC', 10022, 23),
       ('2025-01-31 13:50:00', 'BTC', 10023, 24),
       ('2025-01-31 14:00:00', 'BTC', 10024, 25),
       ('2025-01-31 14:10:00', 'BTC', 10025, 26);


create materialized view candlestick_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', time) as time,
  symbol,
  candlestick_agg(time, price, volume) as candlestick
FROM ticks
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

CREATE MATERIALIZED VIEW candlestick_1h_tz_europe_berlin
WITH (timescaledb.continuous) AS
SELECT symbol, time_bucket('1 hour', "time", 'Europe/Berlin') as time,
            rollup(candlestick) as candlestick
FROM candlestick_1m
GROUP BY 1, 2
WITH DATA;

CREATE MATERIALIZED VIEW candlestick_1h_tz_america_new_york
WITH (timescaledb.continuous) AS
SELECT symbol, time_bucket('1 hour', "time", 'America/New_York') as time,
            rollup(candlestick) as candlestick
FROM candlestick_1m
GROUP BY 1, 2
WITH DATA;

CREATE MATERIALIZED VIEW candlestick_1d_tz_europe_berlin
WITH (timescaledb.continuous) AS
SELECT symbol, time_bucket('1 day', "time") as time,
    rollup(candlestick) as candlestick
FROM candlestick_1h_tz_europe_berlin
GROUP BY 1, 2
WITH DATA;

create view ny_1h as 
select symbol, time,
  open(candlestick),
  high(candlestick),
  low(candlestick),
  close(candlestick) from candlestick_1h_tz_america_new_york;

create view berlin_1h as
select symbol, time,
  open(candlestick),
  high(candlestick),
  low(candlestick),
  close(candlestick) from candlestick_1h_tz_europe_berlin;

table ny_1h;
table berlin_1h;

