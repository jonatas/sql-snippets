CREATE TABLE "sensor_data" ("time" timestamp with time zone not null, "device" text, "power_usage" decimal);

SELECT create_hypertable('sensor_data', 'time', chunk_time_interval => INTERVAL '1 day');


