select delete_job(job_id) from timescaledb_information.jobs where job_id >=1000;
drop table conditions cascade;

CREATE TABLE conditions (
      time TIMESTAMPTZ NOT NULL,
      device INTEGER NOT NULL,
      temperature FLOAT NOT NULL
);
SELECT * FROM create_hypertable('conditions', 'time');


ALTER TABLE conditions SET (timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time');

SELECT add_compression_policy('conditions', INTERVAL '1 day');

INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series(
  now()::timestamp - INTERVAL '1 day',
  now()::timestamp + INTERVAL '1 day',
  INTERVAL '10 seconds') AS time;

