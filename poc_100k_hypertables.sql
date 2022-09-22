-- First set the numbers of hypertables you'd like to test
\set hypertables_count 100
-- Hypertable configuration with the chunk time interval for every hypertable
\set chunk_time_interval '''1 hour'''
-- How much data you'd like to append for every append_data call
\set append_interval '''1 day'''
-- How many devices would you like to simulate in parallel
\set number_of_devices '''1'''
-- When the data starts
\set start_date '''2000-01-01'''
-- Interval between each record
\set interval_between_records '''1 second'''

SELECT FORMAT('CREATE TABLE conditions_%s ( time TIMESTAMPTZ NOT NULL, device INTEGER NOT NULL, temperature FLOAT NOT NULL);', i) FROM generate_series(1,:hypertables_count,1) i;
\gexec

SELECT FORMAT($$SELECT create_hypertable('conditions_%s', 'time', chunk_time_interval => INTERVAL '%s' );$$, i, :chunk_time_interval) FROM generate_series(1,:hypertables_count,1) i;
\gexec

CREATE OR REPLACE PROCEDURE append_data(
  table_name varchar,
  start_date varchar,
  interval_between_records varchar,
  append_interval varchar,
  chunk_time_interval varchar,
  number_of_devices varchar
) AS $func$
BEGIN
  EXECUTE FORMAT($sql$
    INSERT INTO %s
      WITH latest AS materialized (
        SELECT '%s'::timestamp  as time
        UNION ALL
        SELECT time FROM %s ORDER BY time DESC LIMIT 1 )
      SELECT a.time, a.device, random()*80 - 40 AS temperature
      FROM latest LEFT JOIN lateral (
        SELECT * FROM
        generate_series(
          latest.time + INTERVAL '%s',
          latest.time + INTERVAL '%s', INTERVAL '%s') AS g1(time),
        generate_series(1, %s) AS g2(device)
      ) a ON true;
      $sql$,  table_name, start_date, table_name, interval_between_records, append_interval, interval_between_records, number_of_devices);
END;
$func$ language plpgsql;

SELECT FORMAT(
  $$call append_data('conditions_%s', '%s'::varchar, '%s'::varchar, '%s'::varchar, '%s'::varchar, '%s'::varchar );$$, i,
  :start_date, :interval_between_records, :append_interval, :chunk_time_interval, :number_of_devices) FROM generate_series(1,:hypertables_count,1) i;
\gexec

select hypertable_name, count(1) as chunks from timescaledb_information.chunks group by 1 ;
select count(1) as total_chunks from timescaledb_information.chunks ;
