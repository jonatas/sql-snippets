
CREATE OR REPLACE PROCEDURE insert_more_data(job_id int, config jsonb) LANGUAGE PLPGSQL AS
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
        generate_series(301, 600) AS g2(device)
      ) a ON true;
  --  END;
  --  COMMIT;
END
$$;

SELECT add_job('insert_more_data','5 seconds', initial_start => now() + INTERVAL '10 seconds');

