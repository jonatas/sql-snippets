drop materialized view if exists network_data_agg_1min cascade;
DROP TABLE IF EXISTS network_device_data cascade;
-- Create the raw data table for storing network device counters
CREATE TABLE IF NOT EXISTS network_device_data
(
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    device INTEGER NOT NULL,
    id INTEGER NOT NULL,
    counter32bit BIGINT,
    counter64bit BIGINT
);

-- Convert the table into a hypertable for efficient time-series data storage
-- This enhances performance and allows for partitioning based on time
SELECT create_hypertable('network_device_data', 'time');

-- Insert example data into the network_device_data table
-- Note: The counter32bit resets after the 6th minute, and
-- there's a missing entry for the 3rd minute to simulate imperfect data ingestion
INSERT INTO network_device_data (time, device, id, counter32bit, counter64bit) VALUES
('2024-03-22 12:39:00+00', 7, 10000008, 4294667296, 4294667296),
('2024-03-22 12:40:00+00', 7, 10000008, 4294727296, 4294727296),
-- Missing data point for '2024-03-22 12:41:00+00'
('2024-03-22 12:42:00+00', 7, 10000008, 4294847296, 4294847296),
('2024-03-22 12:43:00+00', 7, 10000008, 4294907296, 4294907296),
('2024-03-22 12:44:00+00', 7, 10000008, 1, 4294967296), -- Counter32bit reset
('2024-03-22 12:45:00+00', 7, 10000008, 60001, 4295027296),
('2024-03-22 12:46:00+00', 7, 10000008, 120001, 4295087296),
('2024-03-22 12:47:00+00', 7, 10000008, 180001, 4295147296),
('2024-03-22 12:48:00+00', 7, 10000008, 240001, 4295207296);

-- Create a continuous aggregate to aggregate raw data by minute
CREATE MATERIALIZED VIEW network_data_agg_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    device,
    id,
    counter_agg(time, counter32bit) AS counter32bit_agg,
    counter_agg(time, counter64bit) AS counter64bit_agg
FROM network_device_data
GROUP BY bucket, device, id
WITH DATA; -- Initially create the view without data


-- Create a final view that applies gap filling and rate calculation
CREATE OR REPLACE VIEW network_data_final AS
SELECT
    id,
    bucket,
    -- Calculate interpolated rate for counter32bit
    interpolated_rate(
        counter32bit_agg,
        bucket,
        '1 minute'::interval,
        LAG(counter32bit_agg) OVER (PARTITION BY id ORDER BY bucket),
        LEAD(counter32bit_agg) OVER (PARTITION BY id ORDER BY bucket)
    ) AS counter32bitrate,
    -- Calculate interpolated rate for counter64bit
    interpolated_rate(
        counter64bit_agg,
        bucket,
        '1 minute'::interval,
        LAG(counter64bit_agg) OVER (PARTITION BY id ORDER BY bucket),
        LEAD(counter64bit_agg) OVER (PARTITION BY id ORDER BY bucket)
    ) AS counter64bitrate
FROM network_data_agg_1min
ORDER BY id, bucket;

table network_data_final;
