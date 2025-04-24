-- Create the hypertables
CREATE TABLE consumption (
    time TIMESTAMPTZ NOT NULL,
    kwh DOUBLE PRECISION NOT NULL
);

CREATE TABLE type_changes (
    time TIMESTAMPTZ NOT NULL,
    type_id INTEGER NOT NULL
);

-- Convert to hypertables
SELECT create_hypertable('consumption', 'time');
SELECT create_hypertable('type_changes', 'time');

-- Insert sample data
-- Consumption data (every 15 minutes)
INSERT INTO consumption VALUES
    ('2024-01-01 05:00:00', 1.1),
    ('2024-01-01 05:15:00', 1.2),
    ('2024-01-01 05:30:00', 1.3),
    ('2024-01-01 05:45:00', 1.4),
    ('2024-01-01 06:00:00', 0.6),
    ('2024-01-01 06:15:00', 0.7),
    ('2024-01-01 06:30:00', 0.8),
    ('2024-01-01 06:45:00', 0.9),
    ('2024-01-01 07:00:00', 2.1),
    ('2024-01-01 07:15:00', 2.3),
    ('2024-01-01 07:30:00', 2.4),
    ('2024-01-01 07:45:00', 2.5);

-- Type changes (less frequent)
INSERT INTO type_changes VALUES
    ('2024-01-01 05:00:00', 1),
    ('2024-01-01 05:30:00', 2),
    ('2024-01-01 06:15:00', 1),
    ('2024-01-01 07:00:00', 2);

-- Query to get hourly consumption by type
WITH time_ranges AS (
    SELECT 
        time_bucket('1 hour', c.time) AS hour,
        c.time,
        c.kwh,
        last_value(tc.type_id) OVER (
            ORDER BY tc.time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS type_id
    FROM consumption c
    LEFT JOIN LATERAL (
        SELECT type_id, time 
        FROM type_changes 
        WHERE time <= c.time
        ORDER BY time DESC 
        LIMIT 1
    ) tc ON true
)
SELECT 
    hour,
    sum(kwh) as total_kwh,
    type_id
FROM time_ranges
GROUP BY hour, type_id
ORDER BY hour, type_id;
