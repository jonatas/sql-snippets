CREATE MATERIALIZED VIEW monthly_weather
WITH (timescaledb.continuous)
AS SELECT time_bucket('1 month'::interval, time) AS bucket,
    city_name,
    percentile_agg(temp_c)
FROM weather_metrics
GROUP BY 1, 2;


WITH month_baseline as (
  SELECT date_part('month', bucket) as mon,
    city_name,
    rollup(percentile_agg) as pct
  FROM monthly_weather
  WHERE city_name = 'New York'
  GROUP BY 1, 2),
ranks as (
  SELECT time, w.city_name, temp_c, approx_percentile_rank(temp_c, pct)
  FROM weather_metrics w
    INNER JOIN month_baseline mb
      ON date_part('month', w.time) = mb.mon
      AND w.city_name = mb.city_name
  WHERE w.city_name = 'New York'
),
ranked as (
  SELECT time_bucket('1 month', time),
    city_name,
    count(*) as total,
    count(*) FILTER (WHERE approx_percentile_rank > 0.95)
  FROM ranks
  GROUP BY 1,2
  ORDER BY 2, 1
)
select *, count::decimal / total from ranked order by 4 desc;
