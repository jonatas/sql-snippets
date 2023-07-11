create table test_table (
	device_id bigint not null,
	reading bigint not null,
	time_ts timestamp with time zone not null
);

create index test_table_time_ts_idx on test_table(time_ts desc);

--> single node hypertable
select create_hypertable('test_table', 'time_ts', chunk_time_interval => INTERVAL '1 day');

Insert into test_table(device_id, time_ts, reading)
values 
(1, '2023-04-05T00:00:00+00:00', 18.8),
(1, '2023-04-05T01:00:00+00:00', 35.542),
(1, '2023-04-05T02:00:00+00:00', 45.943),
(1, '2023-04-05T03:00:00+00:00', 60.829),
(1, '2023-04-05T04:00:00+00:00', 73.726),
(1, '2023-04-05T05:00:00+00:00', 88.522);

-- Query
SELECT
    device_id,
    bucket,
    interpolated_delta(
        summary,
        bucket,
        '1 hour',
        LAG(summary) OVER (PARTITION BY device_id ORDER by bucket),
        LEAD(summary) OVER (PARTITION BY device_id ORDER by bucket)
    )
FROM (
    SELECT
        device_id,
        time_bucket('1 hour'::interval, time_ts) AS bucket,
        counter_agg(time_ts, reading) AS summary
    FROM test_table
    WHERE device_id = 1 and time_ts > '2023-04-05T00:00:00+00:00' and time_ts < '2023-04-05T04:00:00+00:00'
    GROUP BY device_id, bucket
) t;
