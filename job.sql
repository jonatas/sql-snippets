
CREATE OR REPLACE PROCEDURE do_it(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE NOTICE 'Do it (job_id: %, config: %)', job_id, config;
END
$$;


SELECT add_job('do_it','1s', fixed_schedule => true, config => '{"hello": "friend", "tag": 123}');



