-- Clean up previous objects for idempotency
DROP VIEW IF EXISTS all_vendors_hourly_qualification_rollup;
DROP VIEW IF EXISTS all_vendor_data;
DROP MATERIALIZED VIEW IF EXISTS vendor_a_hourly_qualification_rollup;
DROP MATERIALIZED VIEW IF EXISTS vendor_b_hourly_qualification_rollup;
DROP MATERIALIZED VIEW IF EXISTS vendor_a_hourly_ok;
DROP MATERIALIZED VIEW IF EXISTS vendor_b_hourly_ok;
DROP TABLE IF EXISTS vendor_a_data;
DROP TABLE IF EXISTS vendor_b_data;
DROP FUNCTION IF EXISTS qualify(vendor_a_data);
DROP FUNCTION IF EXISTS qualify(vendor_b_data);
DROP FUNCTION IF EXISTS set_qualification_vendor_a();
DROP FUNCTION IF EXISTS set_qualification_vendor_b();
DROP TYPE IF EXISTS qualified;

-- Custom enum type for qualification
CREATE TYPE qualified AS ENUM ('Discard', 'OK');

-- Vendor A table with qualification column
CREATE TABLE vendor_a_data (
    time timestamptz NOT NULL,
    id serial NOT NULL,
    value numeric,
    status text,
    qualification qualified,
    PRIMARY KEY (time, id, qualification)
);

-- Vendor B table with qualification column
CREATE TABLE vendor_b_data (
    time timestamptz NOT NULL,
    id serial NOT NULL,
    reading numeric,
    flag boolean,
    qualification qualified,
    PRIMARY KEY (time, id, qualification)
);

-- Per-table qualify() function for vendor_a_data
CREATE OR REPLACE FUNCTION qualify(vendor_a_data)
RETURNS qualified AS $$
DECLARE
    row ALIAS FOR $1;
BEGIN
    IF row.status = 'bad' OR row.value < 0 THEN
        RETURN 'Discard';
    ELSE
        RETURN 'OK';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Per-table qualify() function for vendor_b_data
CREATE OR REPLACE FUNCTION qualify(vendor_b_data)
RETURNS qualified AS $$
DECLARE
    row ALIAS FOR $1;
BEGIN
    IF NOT row.flag OR row.reading IS NULL THEN
        RETURN 'Discard';
    ELSE
        RETURN 'OK';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Trigger function for vendor_a_data
CREATE OR REPLACE FUNCTION set_qualification_vendor_a()
RETURNS TRIGGER AS $$
BEGIN
    NEW.qualification := qualify(NEW.*);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for vendor_b_data
CREATE OR REPLACE FUNCTION set_qualification_vendor_b()
RETURNS TRIGGER AS $$
BEGIN
    NEW.qualification := qualify(NEW.*);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
CREATE TRIGGER trigger_set_qualification_vendor_a
    BEFORE INSERT OR UPDATE ON vendor_a_data
    FOR EACH ROW
    EXECUTE FUNCTION set_qualification_vendor_a();

CREATE TRIGGER trigger_set_qualification_vendor_b
    BEFORE INSERT OR UPDATE ON vendor_b_data
    FOR EACH ROW
    EXECUTE FUNCTION set_qualification_vendor_b();

-- Insert sample data
INSERT INTO vendor_a_data (time, value, status) VALUES
('2024-06-01 10:00:00', 10, 'good'),
('2024-06-01 10:05:00', -5, 'good'),
('2024-06-01 10:10:00', 20, 'bad');

INSERT INTO vendor_b_data (time, reading, flag) VALUES
('2024-06-01 10:00:00', 100, true),
('2024-06-01 10:05:00', NULL, true),
('2024-06-01 10:10:00', 200, false);

-- Convert tables to hypertables (TimescaleDB) with space partitioning on qualification
SELECT create_hypertable('vendor_a_data', 'time', partitioning_column => 'qualification', number_partitions => 2, migrate_data => true, if_not_exists => TRUE);
SELECT create_hypertable('vendor_b_data', 'time', partitioning_column => 'qualification', number_partitions => 2, migrate_data => true, if_not_exists => TRUE);

-- Example: Continuous aggregate for vendor_a_data, counting only qualified rows
CREATE MATERIALIZED VIEW vendor_a_hourly_ok
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  count(*) AS ok_count
FROM vendor_a_data
WHERE qualification = 'OK'
GROUP BY bucket
WITH NO DATA;

-- Example: Continuous aggregate for vendor_b_data, counting only qualified rows
CREATE MATERIALIZED VIEW vendor_b_hourly_ok
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  count(*) AS ok_count
FROM vendor_b_data
WHERE qualification = 'OK'
GROUP BY bucket
WITH NO DATA;

-- Refresh continuous aggregates to populate them
CALL refresh_continuous_aggregate('vendor_a_hourly_ok', NULL, NULL);
CALL refresh_continuous_aggregate('vendor_b_hourly_ok', NULL, NULL);

-- Query the aggregates
SELECT * FROM vendor_a_hourly_ok;
SELECT * FROM vendor_b_hourly_ok;

-- Unified view for both vendors (optional)
CREATE OR REPLACE VIEW all_vendor_data AS
SELECT time, id, value AS reading, qualification, 'A' AS vendor
FROM vendor_a_data
UNION ALL
SELECT time, id, reading, qualification, 'B' AS vendor
FROM vendor_b_data;

-- Query unified view for OK rows
SELECT * FROM all_vendor_data WHERE qualification = 'OK';

-- Rollup continuous aggregate for vendor_a_data by qualification
CREATE MATERIALIZED VIEW vendor_a_hourly_qualification_rollup
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  qualification,
  count(*) AS count,
  avg(value) AS avg_value
FROM vendor_a_data
GROUP BY bucket, qualification
WITH NO DATA;

-- Rollup continuous aggregate for vendor_b_data by qualification
CREATE MATERIALIZED VIEW vendor_b_hourly_qualification_rollup
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  qualification,
  count(*) AS count,
  avg(reading) AS avg_reading
FROM vendor_b_data
GROUP BY bucket, qualification
WITH NO DATA;

-- Refresh the new rollup caggs
CALL refresh_continuous_aggregate('vendor_a_hourly_qualification_rollup', NULL, NULL);
CALL refresh_continuous_aggregate('vendor_b_hourly_qualification_rollup', NULL, NULL);

-- Query the new rollup caggs
SELECT * FROM vendor_a_hourly_qualification_rollup;
SELECT * FROM vendor_b_hourly_qualification_rollup;

-- Unified rollup view for both vendors
CREATE OR REPLACE VIEW all_vendors_hourly_qualification_rollup AS
SELECT bucket, qualification, count, avg_value AS avg, 'A' AS vendor
FROM vendor_a_hourly_qualification_rollup
UNION ALL
SELECT bucket, qualification, count, avg_reading AS avg, 'B' AS vendor
FROM vendor_b_hourly_qualification_rollup;

-- Query the unified rollup view
SELECT * FROM all_vendors_hourly_qualification_rollup;
