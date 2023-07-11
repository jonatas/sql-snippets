CREATE OR REPLACE FUNCTION generate_temperature_series(
    start_date TIMESTAMPTZ DEFAULT now() - INTERVAL '1 month',
    end_date TIMESTAMPTZ DEFAULT now(),
    avg_temperature NUMERIC,
    temperature_fluctuation NUMERIC,
    time_interval INTERVAL DEFAULT INTERVAL '1 hour'
)
RETURNS TABLE (
    day DATE,
    temperature FLOAT
)
LANGUAGE plpgsql
AS $$
DECLARE
    cur_date DATE := start_date;
BEGIN
    WHILE cur_date <= end_date LOOP
        RETURN QUERY
        SELECT
            cur_date,
            avg_temperature + temperature_fluctuation * (random() * 2 - 1) AS temperature;
        cur_date := cur_date + time_interval;
    END LOOP;
END;
$$;

select generate_temperature_series(now() - INTERVAL '1 year', now(), 20.4, 0.1);
 DROP TABLE sensor_data;
 CREATE TABLE sensor_data (
  time TIMESTAMPTZ NOT NULL,
  temperature DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time');

INSERT INTO sensor_data (time, temperature)
SELECT
  time,
  temperature
FROM
  generate_temperature_series( '2000-01-01', '2023-01-31', 25.0, 5.0) AS g1(time, temperature);
