-- Drop existing functions
DROP FUNCTION IF EXISTS gregorian_to_persian(DATE);
DROP FUNCTION IF EXISTS get_persian_week_start(DATE);

CREATE OR REPLACE FUNCTION gregorian_to_persian(g_date DATE) 
RETURNS DATE AS $$
DECLARE
    g_year INT := EXTRACT(YEAR FROM g_date);
    g_month INT := EXTRACT(MONTH FROM g_date);
    g_day INT := EXTRACT(DAY FROM g_date);
    jd INT;
    p_year INT;
    p_month INT;
    p_day INT;
    p_day_no INT;
BEGIN
    jd := (1461 * (g_year + 4800 + (g_month - 14) / 12)) / 4 +
          (367 * (g_month - 2 - 12 * ((g_month - 14) / 12))) / 12 -
          (3 * ((g_year + 4900 + (g_month - 14) / 12) / 100)) / 4 +
          g_day - 32075;

    p_day_no := jd - 2121445;  -- Adjusted base date
    p_year := 474 + 33 * (p_day_no / 12053);
    p_day_no := p_day_no % 12053;

    p_year := p_year + 4 * (p_day_no / 1461);
    p_day_no := p_day_no % 1461;

    IF p_day_no >= 366 THEN
        p_year := p_year + (p_day_no - 1) / 365;
        p_day_no := (p_day_no - 1) % 365;
    END IF;

    IF p_day_no < 186 THEN
        p_month := 1 + p_day_no / 31;
        p_day := 1 + (p_day_no % 31);
    ELSE
        p_month := 7 + (p_day_no - 186) / 30;
        p_day := 1 + ((p_day_no - 186) % 30);
    END IF;

    RETURN make_date(p_year, p_month, p_day);
END;
$$ LANGUAGE plpgsql;

-- Function to get Persian week start
CREATE OR REPLACE FUNCTION get_persian_week_start(p_date DATE) 
RETURNS DATE AS $$
BEGIN
    -- In Persian calendar, weeks start on Saturday
    RETURN p_date - EXTRACT(DOW FROM p_date)::INT;
END;
$$ LANGUAGE plpgsql;

-- Test the conversion
SELECT 
    d::date AS gregorian_date,
    gregorian_to_persian(d::date) AS persian_date
FROM generate_series('2024-03-20'::date, '2024-03-25'::date, '1 day'::interval) d;


-- Enable TimescaleDB extension if not already enabled
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing objects if they exist
DROP TABLE IF EXISTS trades;

-- Create the trades table
CREATE TABLE trades (
    time TIMESTAMPTZ NOT NULL,
    persian_date DATE NOT NULL,
    persian_week_start DATE NOT NULL,
    persian_month_start DATE NOT NULL,
    persian_year_start DATE NOT NULL,
    price NUMERIC NOT NULL,
    volume NUMERIC NOT NULL
);

-- Convert trades table to a hypertable
SELECT create_hypertable('trades', 'time');

-- Insert seed data
INSERT INTO trades (time, persian_date, persian_week_start, persian_month_start, persian_year_start, price, volume)
SELECT
    generate_series,
    gregorian_to_persian(generate_series::date),
    get_persian_week_start(gregorian_to_persian(generate_series::date)),
    DATE_TRUNC('MONTH', gregorian_to_persian(generate_series::date)),
    DATE_TRUNC('YEAR', gregorian_to_persian(generate_series::date)),
    10000 + random() * 1000,
    100 + random() * 50
FROM generate_series(
    '2024-03-20 00:00:00'::timestamp,
    '2024-03-21 23:59:59'::timestamp,
    '5 minutes'
);

-- Check inserted data
SELECT COUNT(*) FROM trades;

-- Check Persian dates
SELECT DISTINCT time::date, persian_date, persian_week_start, persian_month_start, persian_year_start 
FROM trades 
ORDER BY time::date;

-- Drop existing materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS yearly_ohlc;
DROP MATERIALIZED VIEW IF EXISTS monthly_ohlc;
DROP MATERIALIZED VIEW IF EXISTS weekly_ohlc;
DROP MATERIALIZED VIEW IF EXISTS daily_ohlc;
DROP MATERIALIZED VIEW IF EXISTS hourly_ohlc;

-- Hourly OHLC Materialized View (base view)
CREATE MATERIALIZED VIEW hourly_ohlc AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    persian_date,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(volume) AS volume
FROM trades
GROUP BY bucket, persian_date
WITH NO DATA;

-- Daily OHLC Materialized View (built on hourly)
CREATE MATERIALIZED VIEW daily_ohlc AS
SELECT
    persian_date,
    first(open, bucket) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, bucket) AS close,
    sum(volume) AS volume
FROM hourly_ohlc
GROUP BY persian_date
WITH NO DATA;

-- Weekly OHLC Materialized View (built on daily)
CREATE MATERIALIZED VIEW weekly_ohlc AS
SELECT
    date_trunc('week', persian_date) AS week_start,
    first(open, persian_date) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, persian_date) AS close,
    sum(volume) AS volume
FROM daily_ohlc
GROUP BY week_start
WITH NO DATA;

-- Monthly OHLC Materialized View (built on daily)
CREATE MATERIALIZED VIEW monthly_ohlc AS
SELECT
    date_trunc('month', persian_date) AS month_start,
    first(open, persian_date) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, persian_date) AS close,
    sum(volume) AS volume
FROM daily_ohlc
GROUP BY month_start
WITH NO DATA;

-- Yearly OHLC Materialized View (built on monthly)
CREATE MATERIALIZED VIEW yearly_ohlc AS
SELECT
    date_trunc('year', month_start) AS year_start,
    first(open, month_start) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, month_start) AS close,
    sum(volume) AS volume
FROM monthly_ohlc
GROUP BY year_start
WITH NO DATA;

-- Function to refresh all OHLC materialized views
CREATE OR REPLACE FUNCTION refresh_ohlc_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW hourly_ohlc;
    REFRESH MATERIALIZED VIEW daily_ohlc;
    REFRESH MATERIALIZED VIEW weekly_ohlc;
    REFRESH MATERIALIZED VIEW monthly_ohlc;
    REFRESH MATERIALIZED VIEW yearly_ohlc;
END;
$$ LANGUAGE plpgsql;

select refresh_ohlc_views();

table hourly_ohlc;
table daily_ohlc;
table weekly_ohlc;
table monthly_ohlc;

