select delete_job(job_id) from timescaledb_information.jobs where job_id >=1000;
drop table conditions cascade;
CREATE TABLE conditions (
      time TIMESTAMPTZ NOT NULL,
      device INTEGER NOT NULL,
      temperature FLOAT NOT NULL
);
SELECT * FROM create_hypertable('conditions', 'time', 'device');
ALTER TABLE conditions SET (timescaledb.compress, timescaledb.compress_orderby='time');

INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1 day',
             INTERVAL '1 second') AS time;

CREATE OR REPLACE PROCEDURE insert_massive_data(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE NOTICE 'Inserting in the job % with config %', job_id, config;
      INSERT INTO conditions
      WITH latest AS materialized (SELECT time FROM conditions ORDER BY time DESC LIMIT 1 )
      SELECT a.time, a.device, random()*80 - 40 AS temperature
      FROM latest LEFT JOIN lateral (
        SELECT * FROM
        generate_series(latest.time + INTERVAL '1 second',
          latest.time + INTERVAL '2 hours', INTERVAL '1 second') AS g1(time),
        generate_series(1, 300) AS g2(device)
      ) a ON true;
  --  END;
  --  COMMIT;
END
$$;

SELECT add_job('insert_massive_data','5 seconds', initial_start => now() + INTERVAL '10 seconds');

