select delete_job(job_id) from timescaledb_information.jobs where job_id >=1000;
drop table conditions cascade;



CREATE TABLE conditions (
      time TIMESTAMPTZ NOT NULL,
      device INTEGER NOT NULL,
      temperature FLOAT NOT NULL
);
SELECT * FROM create_hypertable('conditions', 'time');


ALTER TABLE conditions SET (timescaledb.compress, timescaledb.compress_orderby='time');

CREATE MATERIALIZED VIEW conditions_hourly(time, device, low, high, average )
WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', time) as time,
  device,
  min(temperature) as low,
  max(temperature) as high, 
  AVG(temperature) as average
    FROM conditions
    GROUP BY 1,2;

SELECT add_retention_policy('conditions', INTERVAL '1 day');
SELECT add_continuous_aggregate_policy('conditions_hourly', 
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 minute');


INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series(
  now()::timestamp - INTERVAL '1 day',
  now()::timestamp + INTERVAL '1 day',
  INTERVAL '10 seconds') AS time;

select count(distinct time) from conditions_hourly ;
