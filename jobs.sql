CREATE TABLE logs (time timestamp, message text);

CREATE OR REPLACE PROCEDURE do_in_background(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE NOTICE 'Executing job % with config %', job_id, config;
  INSERT INTO logs (time, message) values (now(), 'Running '|| job_id::varchar);
END
$$;

SELECT add_job('do_in_background','5 seconds', initial_start => now() +' 5 seconds');

