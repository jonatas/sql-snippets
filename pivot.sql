WITH pivoted_data AS (
  SELECT
    time,
    MAX(CASE WHEN sensor_name = 'speed' THEN sensor_value END) AS speed,
    MAX(CASE WHEN sensor_name = 'cons' THEN sensor_value END) AS cons
  FROM sensor_data
  GROUP BY time
),
forward_filled AS (
  SELECT
    time,
    COALESCE(
      speed,
      (
        SELECT speed
        FROM pivoted_data pd2
        WHERE pd2.time <= pd1.time AND pd2.speed IS NOT NULL
        ORDER BY pd2.time DESC
        LIMIT 1
      )
    ) AS speed,
    COALESCE(
      cons,
      (
        SELECT cons
        FROM pivoted_data pd2
        WHERE pd2.time <= pd1.time AND pd2.cons IS NOT NULL
        ORDER BY pd2.time DESC
        LIMIT 1
      )
    ) AS cons
  FROM pivoted_data pd1
)
SELECT time, cons, speed
FROM forward_filled
WHERE speed > 20.2;
