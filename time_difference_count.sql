
DROP TABLE time_differences ;
DROP TABLE events ;
CREATE TABLE events (
    time TIMESTAMPTZ NOT NULL,
    key TEXT NOT NULL,
    value INT NOT NULL
);
SELECT create_hypertable('events', 'time');

CREATE OR REPLACE FUNCTION min_time_diff(a_time TIMESTAMPTZ, b_time TIMESTAMPTZ)
RETURNS TIMESTAMPTZ AS $$
DECLARE
    min_diff TIMESTAMPTZ;
BEGIN
    IF a_time IS NULL OR b_time IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT MIN(b.time - a.time) INTO min_diff
    FROM (SELECT time FROM events WHERE key = 'A' AND value = 1 AND time <= b_time) a,
         (SELECT time FROM events WHERE key = 'B' AND value = 1 AND time >= a_time) b
    WHERE a.time < b.time;

    RETURN min_diff;
END;
$$ LANGUAGE plpgsql;
CREATE TABLE time_differences (
    time_bucket TIMESTAMPTZ NOT NULL UNIQUE,
    min_time_difference INTERVAL
);
CREATE OR REPLACE FUNCTION update_time_differences()
RETURNS TRIGGER AS $$
DECLARE
    a_time TIMESTAMPTZ;
    b_time TIMESTAMPTZ;
    min_diff INTERVAL;
BEGIN
    IF NEW.key = 'B' AND NEW.value = 1 THEN
        SELECT time INTO a_time
        FROM events
        WHERE key = 'A' AND value = 1 AND time < NEW.time
        ORDER BY time DESC
        LIMIT 1;

        IF a_time IS NOT NULL THEN
            min_diff := NEW.time - a_time;

            INSERT INTO time_differences (time_bucket, min_time_difference)
            VALUES (time_bucket(INTERVAL '1 minute', NEW.time), min_diff)
            ON CONFLICT (time_bucket) DO UPDATE
            SET min_time_difference = LEAST(time_differences.min_time_difference, EXCLUDED.min_time_difference);
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER update_time_differences_trigger
AFTER INSERT ON events
FOR EACH ROW
EXECUTE FUNCTION update_time_differences();

INSERT INTO events (time, key, value) VALUES
('2023-04-28 08:00:00', 'A', 1),
('2023-04-28 08:10:00', 'A', 0),
('2023-04-28 08:15:00', 'B', 1),
('2023-04-28 08:25:00', 'B', 0);


WITH a_events AS (
    SELECT time
    FROM events
    WHERE key = 'A' AND value = 1
),
b_events AS (
    SELECT time
    FROM events
    WHERE key = 'B' AND value = 1
)
SELECT a.time AS a_time, b.time AS b_time, b.time - a.time AS time_difference
FROM a_events a, b_events b
WHERE a.time < b.time
ORDER BY a.time, b.time;


table time_differences;
