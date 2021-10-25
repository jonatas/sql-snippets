drop table if exists table1;
create table if not exists table1(time timestamp, category varchar, value numeric(10,2));
SELECT create_distributed_hypertable('table1','time','category',3, chunk_time_interval => Interval '3 hours');
--SELECT add_dimension('table1', 'col1', number_partitions => 3);

INSERT INTO table1
SELECT time, 'category-'||((random()*3)::int), (random()*100)::numeric(10,2)
FROM generate_series(TIMESTAMP '2000-01-01 00:01:00',
                 TIMESTAMP '2000-01-01 00:01:00' + INTERVAL '5 minutes',
             INTERVAL '1 second') AS time;

SELECT * FROM chunks_detailed_size('table1') ORDER BY chunk_name, node_name;

