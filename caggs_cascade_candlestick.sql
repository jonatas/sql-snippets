-- Drop all existing views with CASCADE to handle dependencies
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1m_tz_america_new_york CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1m_tz_europe_berlin CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1d_tz_america_new_york CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1d_tz_europe_berlin CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1m CASCADE;
DROP VIEW IF EXISTS timezone_comparison_daily CASCADE;
DROP VIEW IF EXISTS timezone_comparison_hourly CASCADE;
DROP VIEW IF EXISTS timezone_comparison_ny CASCADE;
DROP VIEW IF EXISTS timezone_comparison_all CASCADE;
DROP VIEW IF EXISTS timezone_comparison_prices_transposed CASCADE;
DROP VIEW IF EXISTS timezone_comparison_berlin CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1h_tz_ny CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1h_tz_europe_berlin CASCADE;
-- First drop everything with CASCADE to handle all dependencies
DROP VIEW IF EXISTS daily_comparison CASCADE;
DROP VIEW IF EXISTS ny_daily CASCADE;
DROP VIEW IF EXISTS berlin_daily CASCADE;
DROP VIEW IF EXISTS candlestick_comparison CASCADE;
DROP VIEW IF EXISTS ny_1h CASCADE;
DROP VIEW IF EXISTS berlin_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS candlestick_1d_tz_america_new_york CASCADE;
DROP MATERIALIZED VIEW IF EXISTS candlestick_1d_tz_europe_berlin CASCADE;
DROP MATERIALIZED VIEW IF EXISTS candlestick_1h_tz_america_new_york CASCADE;
DROP MATERIALIZED VIEW IF EXISTS candlestick_1h_tz_europe_berlin CASCADE;
DROP MATERIALIZED VIEW IF EXISTS candlestick_1m CASCADE;
DROP TABLE IF EXISTS "ticks" CASCADE;

-- Create base table
CREATE TABLE "ticks" ("time" timestamptz, "symbol" text, "price" decimal, "volume" float);
SELECT create_hypertable('ticks', by_range('time', INTERVAL '1 week'));

-- Insert test data with values that clearly show daily boundaries
insert into ticks (time, symbol, price, volume)
values 
       -- NY Day 1 / Berlin Day 2 (around midnight Berlin)
       ('2025-01-31 22:45:00', 'BTC', 1000, 1),  -- NY 17:45, Berlin 23:45 (Day 1)
       ('2025-01-31 23:15:00', 'BTC', 1100, 2),  -- NY 18:15, Berlin 00:15 (Day 2)
       
       -- UTC Day boundary
       ('2025-01-31 23:45:00', 'BTC', 1200, 3),  -- NY 18:45, Berlin 00:45
       ('2025-02-01 00:15:00', 'BTC', 1300, 4),  -- NY 19:15, Berlin 01:15
       
       -- NY Day boundary
       ('2025-02-01 04:45:00', 'BTC', 1400, 5),  -- NY 23:45 (Day 1), Berlin 05:45
       ('2025-02-01 05:15:00', 'BTC', 1500, 6),  -- NY 00:15 (Day 2), Berlin 06:15
       
       -- Additional data points for Day 2
       ('2025-02-01 15:00:00', 'BTC', 1600, 7),  -- NY 10:00, Berlin 16:00
       ('2025-02-01 20:00:00', 'BTC', 1700, 8);  -- NY 15:00, Berlin 21:00

-- Create minute-level continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS candlestick_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', time) as time,
  symbol,
  candlestick_agg(time, price, volume) as candlestick
FROM ticks
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

-- Create hourly continuous aggregates with timezone support
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

-- Fix the daily continuous aggregate by using the time column directly
CREATE MATERIALIZED VIEW candlestick_1d_tz_europe_berlin
WITH (timescaledb.continuous) AS
SELECT symbol, 
       time_bucket('1 day', "time", 'Europe/Berlin') as time,
       rollup(candlestick) as candlestick
FROM candlestick_1m
GROUP BY 1, 2
WITH DATA;

-- Add the missing daily continuous aggregate for NY timezone
CREATE MATERIALIZED VIEW candlestick_1d_tz_america_new_york
WITH (timescaledb.continuous) AS
SELECT symbol, 
       time_bucket('1 day', "time", 'America/New_York') as time,
       rollup(candlestick) as candlestick
FROM candlestick_1m
GROUP BY 1, 2
WITH DATA;

-- Create readable views
create view ny_1h as 
select symbol, time,
  open(candlestick),
  high(candlestick),
  low(candlestick),
  close(candlestick) 
from candlestick_1h_tz_america_new_york;

create view berlin_1h as
select symbol, time,
  open(candlestick),
  high(candlestick),
  low(candlestick),
  close(candlestick) 
from candlestick_1h_tz_europe_berlin;

-- Fix the comparison view to properly reference candlesticks
create view candlestick_comparison as
SELECT 
    ny.time AT TIME ZONE 'UTC' as time_utc,
    ny.time AT TIME ZONE 'America/New_York' as time_ny,
    berlin.time AT TIME ZONE 'Europe/Berlin' as time_berlin,
    ny.symbol,
    open(ny.candlestick) as ny_open,
    high(ny.candlestick) as ny_high,
    low(ny.candlestick) as ny_low,
    close(ny.candlestick) as ny_close,
    volume(ny.candlestick) as ny_volume,
    open(berlin.candlestick) as berlin_open,
    high(berlin.candlestick) as berlin_high,
    low(berlin.candlestick) as berlin_low,
    close(berlin.candlestick) as berlin_close,
    volume(berlin.candlestick) as berlin_volume
FROM candlestick_1h_tz_america_new_york ny
FULL OUTER JOIN candlestick_1h_tz_europe_berlin berlin 
    ON ny.time AT TIME ZONE 'UTC' = berlin.time AT TIME ZONE 'UTC'
    AND ny.symbol = berlin.symbol
ORDER BY time_utc;

-- Show the results
table ny_1h;
table berlin_1h;

-- Fix the final SELECT query to reference the correct column names
SELECT 
    to_char(time_utc, 'YYYY-MM-DD HH24:MI') as time_utc,
    to_char(time_ny, 'YYYY-MM-DD HH24:MI') as time_ny,
    to_char(time_berlin, 'YYYY-MM-DD HH24:MI') as time_berlin,
    symbol,
    ny_open as open,
    ny_high as high,
    ny_low as low,
    ny_close as close,
    ny_volume as volume
FROM candlestick_comparison
ORDER BY time_utc;

-- Fix daily views to use correct source tables
create view ny_daily as 
select symbol, time,
  open(candlestick) as open,
  high(candlestick) as high,
  low(candlestick) as low,
  close(candlestick) as close,
  volume(candlestick) as volume
from candlestick_1d_tz_america_new_york;

create view berlin_daily as
select symbol, time,
  open(candlestick) as open,
  high(candlestick) as high,
  low(candlestick) as low,
  close(candlestick) as close,
  volume(candlestick) as volume
from candlestick_1d_tz_europe_berlin;

-- Update the daily comparison view to properly handle timezone conversions
create or replace view daily_comparison as
SELECT 
    ny.time AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC' as time_utc,
    ny.time AT TIME ZONE 'America/New_York' as time_ny,
    berlin.time AT TIME ZONE 'Europe/Berlin' as time_berlin,
    ny.symbol,
    ny.open as ny_open,
    ny.high as ny_high,
    ny.low as ny_low,
    ny.close as ny_close,
    ny.volume as ny_volume,
    berlin.open as berlin_open,
    berlin.high as berlin_high,
    berlin.low as berlin_low,
    berlin.close as berlin_close,
    berlin.volume as berlin_volume
FROM ny_daily ny
FULL OUTER JOIN berlin_daily berlin 
    ON ny.time AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC' = 
       berlin.time AT TIME ZONE 'Europe/Berlin' AT TIME ZONE 'UTC'
    AND ny.symbol = berlin.symbol
ORDER BY time_utc;

-- Show the results
SELECT 'Hourly New York' as view_type;
table ny_1h;

SELECT 'Hourly Berlin' as view_type;
table berlin_1h;

SELECT 'Daily Comparison' as view_type;
SELECT 
    to_char(time_utc, 'YYYY-MM-DD') as date_utc,
    to_char(time_ny, 'YYYY-MM-DD') as date_ny,
    to_char(time_berlin, 'YYYY-MM-DD') as date_berlin,
    symbol,
    ny_open, ny_high, ny_low, ny_close, ny_volume,
    berlin_open, berlin_high, berlin_low, berlin_close, berlin_volume
FROM daily_comparison
ORDER BY time_utc;

