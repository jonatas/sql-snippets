WITH city_names AS (
        SELECT DISTINCT city_name as name
        FROM weather_metrics
),
pairs as (
SELECT a.name as first, b.name as second
FROM city_names a
JOIN city_names b ON a.name != b.name
),
summary AS (
    SELECT time_bucket('1 month', time),
        city_name,
        avg(temp_c)
    FROM weather_metrics
    WHERE time BETWEEN '2010-01-01' AND '2021-01-01'
    GROUP BY 1,2)
SELECT
    a.city_name as first, b.city_name as second,
    covariance(stats_agg (a.avg, b.avg)),
    corr(stats_agg(a.avg, b.avg))
FROM pairs
JOIN summary a ON (pairs.first = a .city_name)
JOIN summary b ON (pairs.second = b.city_name AND a.time_bucket = b.time_bucket)
WHERE b.city_name = 'New York' and a.city_name = 'Nairobi'
GROUP BY 1,2
 order by abs(corr(stats_agg(a.avg, b.avg)));
