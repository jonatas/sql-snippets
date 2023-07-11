CREATE OR REPLACE FUNCTION generate_temperature_series(
    start_date DATE,
    end_date DATE,
    avg_temperature NUMERIC,
    temperature_fluctuation NUMERIC,
    time_interval INTERVAL DEFAULT INTERVAL '1 day'
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
