create table sensor_1 (
    time timestamp not null,
    value float not null
);

create table sensor_2 (
    time timestamp not null,
    value float not null
);

create table sensor_3 (
    time timestamp not null,
    value float not null
);

select create_hypertable('sensor_1', 'time'),
 create_hypertable('sensor_2', 'time'), 
 create_hypertable('sensor_3', 'time');

insert into sensor_1 (time, value) values (now(), 1.0), (now() - interval '1 day', 2.0), (now() - interval '2 day', 3.0);
insert into sensor_2 (time, value) values (now(), 4.0), (now() - interval '1 day', 5.0), (now() - interval '2 day', 6.0);
insert into sensor_3 (time, value) values (now(), 7.0), (now() - interval '1 day', 8.0), (now() - interval '2 day', 9.0);

-- Query to combine all sensor readings, showing NULLs when a sensor doesn't have a reading
SELECT 
    COALESCE(s1.time, s2.time, s3.time) AS tstamp,
    s1.value AS sensor_1,
    s2.value AS sensor_2,
    s3.value AS sensor_3
FROM 
    sensor_1 s1
    FULL OUTER JOIN sensor_2 s2 ON s1.time = s2.time
    FULL OUTER JOIN sensor_3 s3 ON COALESCE(s1.time, s2.time) = s3.time
ORDER BY 
    tstamp;

-- Example for combining data from separate sensor tables (using existing schema)
-- Our tables are structured as: sensor_X(time timestamp, value float)

SELECT 
    COALESCE(s1.time, s2.time, s3.time) AS tstamp,
    s1.value AS Sensor_01,
    s2.value AS Sensor_02,
    s3.value AS Sensor_03
FROM 
    sensor_1 s1
    FULL OUTER JOIN sensor_2 s2 ON s1.time = s2.time
    FULL OUTER JOIN sensor_3 s3 ON COALESCE(s1.time, s2.time) = s3.time
ORDER BY 
    tstamp;

-- Using time_bucket to group readings into 5-second intervals
-- This query groups timestamp data into buckets

-- A cleaner approach using FULL OUTER JOIN with time_bucket
WITH 
    sensor_1_bucketed AS (
        SELECT time_bucket('5 seconds', time) AS bucket_time, value
        FROM sensor_1
    ),
    sensor_2_bucketed AS (
        SELECT time_bucket('5 seconds', time) AS bucket_time, value
        FROM sensor_2
    ),
    sensor_3_bucketed AS (
        SELECT time_bucket('5 seconds', time) AS bucket_time, value
        FROM sensor_3
    )
SELECT 
    COALESCE(s1.bucket_time, s2.bucket_time, s3.bucket_time) AS tstamp,
    s1.value AS Sensor_01,
    s2.value AS Sensor_02,
    s3.value AS Sensor_03
FROM 
    sensor_1_bucketed s1
    FULL OUTER JOIN sensor_2_bucketed s2 ON s1.bucket_time = s2.bucket_time
    FULL OUTER JOIN sensor_3_bucketed s3 ON COALESCE(s1.bucket_time, s2.bucket_time) = s3.bucket_time
ORDER BY 
    tstamp;
