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

-- Drop and recreate the base table
DROP TABLE IF EXISTS "ticks" CASCADE;
CREATE TABLE "ticks" ("time" timestamptz, "symbol" text, "price" decimal, "volume" float);
SELECT create_hypertable('ticks', by_range('time', INTERVAL '1 week'));

-- Insert test data with consistent time-based pricing
insert into ticks (time, symbol, price, volume)
values 
       -- Group 1: NY Day 1 / UTC Day 1 / Berlin Day 2 (around midnight Berlin)
       ('2025-01-31 22:45:00', 'BTC', 2245, 1),  -- NY 17:45, Berlin 23:45
       ('2025-01-31 22:50:00', 'BTC', 2250, 2),
       ('2025-01-31 22:55:00', 'BTC', 2255, 3),
       ('2025-01-31 23:00:00', 'BTC', 2300, 4),  -- NY 18:00, Berlin 00:00
       ('2025-01-31 23:05:00', 'BTC', 2305, 5),
       ('2025-01-31 23:10:00', 'BTC', 2310, 6),
       ('2025-01-31 23:15:00', 'BTC', 2315, 7),

       -- Group 2: NY Day 1 / UTC Day 2 / Berlin Day 2 (around midnight UTC)
       ('2025-01-31 23:45:00', 'BTC', 2345, 8),  -- NY 18:45, Berlin 00:45
       ('2025-01-31 23:50:00', 'BTC', 2350, 9),
       ('2025-01-31 23:55:00', 'BTC', 2355, 10),
       ('2025-02-01 00:00:00', 'BTC', 0, 11),    -- NY 19:00, Berlin 01:00
       ('2025-02-01 00:05:00', 'BTC', 5, 12),
       ('2025-02-01 00:10:00', 'BTC', 10, 13),
       ('2025-02-01 00:15:00', 'BTC', 15, 14),

       -- Group 3: NY Day 1-2 / UTC Day 2 / Berlin Day 2 (around midnight NY)
       ('2025-02-01 04:45:00', 'BTC', 445, 15),  -- NY 23:45, Berlin 05:45
       ('2025-02-01 04:50:00', 'BTC', 450, 16),
       ('2025-02-01 04:55:00', 'BTC', 455, 17),
       ('2025-02-01 05:00:00', 'BTC', 500, 18),  -- NY 00:00, Berlin 06:00
       ('2025-02-01 05:05:00', 'BTC', 505, 19),
       ('2025-02-01 05:10:00', 'BTC', 510, 20),
       ('2025-02-01 05:15:00', 'BTC', 515, 21);

-- Create hourly view as intermediate
create materialized view ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) as time,
  symbol,
  first(price, time) as open,
  max(price) as high,
  min(price) as low,
  last(price, time) as close,
  sum(volume) as volume
FROM ticks
GROUP BY 1, 2
ORDER BY 1
WITH DATA;

-- Create daily view for Berlin timezone using hourly data
CREATE MATERIALIZED VIEW ohlcv_1d_tz_europe_berlin
WITH (timescaledb.continuous) AS
SELECT symbol,
   time_bucket('1 day', "time", 'Europe/Berlin') as time,
  first(open, time_bucket('1 day', "time", 'Europe/Berlin') at time zone 'Europe/Berlin') as open,
  max(high) as high,
  min(low) as low,
  last(close, time_bucket('1 day', "time", 'Europe/Berlin') at time zone 'Europe/Berlin') as close,
  sum(volume) as volume
FROM ohlcv_1h
GROUP BY 2, 1
WITH DATA;

-- Create daily view for NY timezone using hourly data
CREATE MATERIALIZED VIEW ohlcv_1d_tz_ny 
WITH (timescaledb.continuous) AS
SELECT symbol,
   time_bucket('1 day', "time", 'America/New_York') as time,
  first(open, time_bucket('1 day', "time", 'America/New_York') at time zone 'America/New_York') as open,
  max(high) as high,
  min(low) as low,
  last(close, time_bucket('1 day', "time", 'America/New_York') at time zone 'America/New_York') as close,
  sum(volume) as volume
FROM ohlcv_1h
GROUP BY 1, 2
WITH DATA;

-- Create comparison view
CREATE OR REPLACE VIEW timezone_comparison_daily AS
SELECT 
    t1.time AT TIME ZONE 'UTC' as time_utc,
    t1.time AT TIME ZONE 'America/New_York' as time_ny,
    t2.time AT TIME ZONE 'Europe/Berlin' as time_berlin,
    t1.symbol,
    t1.open as open_ny,
    t1.high as high_ny,
    t1.low as low_ny,
    t1.close as close_ny,
    t2.open as open_berlin,
    t2.high as high_berlin,
    t2.low as low_berlin,
    t2.close as close_berlin
FROM ohlcv_1d_tz_ny t1
JOIN ohlcv_1d_tz_europe_berlin t2 
    ON date(t1.time AT TIME ZONE 'UTC') = date(t2.time AT TIME ZONE 'UTC')
    AND t1.symbol = t2.symbol
ORDER BY t1.time;

-- Add a more detailed comparison view to show hourly breakdowns
CREATE OR REPLACE VIEW timezone_comparison_hourly AS
SELECT 
    time AT TIME ZONE 'UTC' as time_utc,
    time AT TIME ZONE 'America/New_York' as time_ny,
    time AT TIME ZONE 'Europe/Berlin' as time_berlin,
    symbol,
    open,
    high,
    low,
    close,
    volume
FROM ohlcv_1h
ORDER BY time;

-- Show the results with additional formatting for clarity
SELECT 
    to_char(time_utc, 'YYYY-MM-DD HH24:MI') as time_utc,
    to_char(time_ny, 'YYYY-MM-DD HH24:MI') as time_ny,
    to_char(time_berlin, 'YYYY-MM-DD HH24:MI') as time_berlin,
    symbol,
    open,
    high,
    low,
    close,
    volume
FROM timezone_comparison_hourly
ORDER BY time_utc;

-- Show daily views
SELECT * FROM ohlcv_1d_tz_ny;
SELECT * FROM ohlcv_1d_tz_europe_berlin;
SELECT * FROM timezone_comparison_daily;