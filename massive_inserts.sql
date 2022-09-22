select delete_job(job_id) from timescaledb_information.jobs where job_id >=1000;
drop table conditions cascade;
CREATE TABLE conditions (
      time TIMESTAMPTZ NOT NULL,
      device INTEGER NOT NULL,
      temperature FLOAT NOT NULL
);
SELECT * FROM create_hypertable('conditions', 'time');

INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-21 00:00:00',
             INTERVAL '1 second') AS time;

SELECT device, min(time), max(time), count(*) from conditions group by 1;

WITH c AS (
SELECT time, device, ROW_NUMBER() OVER(PARTITION BY device ORDER BY time DESC) AS rank
FROM conditions )
SELECT * FROM c where c.rank = 10000;

CREATE OR REPLACE PROCEDURE limit_devices_data(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE NOTICE 'DELETING in the job % with config %', job_id, config;
   WITH summary AS (
    SELECT time,
           device,
           ROW_NUMBER() OVER(PARTITION BY device
                                 ORDER BY time DESC ) AS rank
      FROM conditions )
 DELETE FROM conditions USING summary
   WHERE summary.rank = 10000 and conditions.time < summary.time and summary.device = conditions.device;
  COMMIT;
END
$$;

SELECT add_job('limit_devices_data','5 seconds', initial_start => now() + INTERVAL '5 seconds');
SELECT alter_job(job_id, max_runtime =>  INTERVAL '1 minute')
FROM timescaledb_information.jobs
WHERE proc_name = 'limit_devices_data';

select pg_sleep(10);
SELECT * FROM timescaledb_information.job_stats;
select pg_sleep(10);
SELECT * FROM timescaledb_information.job_stats;

SELECT device, min(time), max(time), count(*) from conditions group by 1;
select * from timescaledb_information.jobs where proc_name = 'limit_devices_data';
