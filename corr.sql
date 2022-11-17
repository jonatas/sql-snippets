SELECT * FROM crosstab($$
        WITH city_names AS (
                SELECT DISTINCT city_name as name
                FROM weather_metrics order by 1
        ),
pairs as (
SELECT a.name as first, b.name as second
FROM city_names a
JOIN city_names b ON true --# a.name != b.name
),
summary AS (
    SELECT time_bucket('1 h', time), city_name,
        avg(temp_c)
    FROM weather_metrics
    WHERE time BETWEEN '2010-01-01' AND '2021-01-01'
    GROUP BY 1,2
ORDER BY 1,2
    )
SELECT
    a.city_name as first, b.city_name as second,
    corr(stats_agg(a.avg, b.avg))
FROM pairs
JOIN summary a ON (pairs.first = a .city_name)
JOIN summary b ON (pairs.second = b.city_name AND a.time_bucket = b.time_bucket)
--WHERE b.city_name = 'New York' and a.city_name = 'Nairobi'
GROUP BY 1,2
 order by 1, 2
$$::text,
'select distinct city_name from weather_metrics order by 1'::text
) as ct(city_name text,
  "Austin" double precision, "Lisbon" double precision, "Nairobi" double precision, "New York" double precision, "Pietermaritzburg" double precision, "Princeton" double precision, "San Francisco" double precision, "Stockholm" double precision, "Toronto" double precision, "Vienna" double precision);
