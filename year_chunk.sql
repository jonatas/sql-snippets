drop schema test cascade;

create schema test;
CREATE TABLE test.year_partition_hypertable(id INTEGER, create_date TIMESTAMPTZ);

SELECT create_hypertable('test.year_partition_hypertable', 'create_date', chunk_time_interval => INTERVAL '1 year');

INSERT INTO test.year_partition_hypertable
SELECT gs, TIMESTAMPTZ '2010-01-01' + (gs || ' month')::INTERVAL
FROM pg_catalog.generate_series(0, 25, 1) gs
;

SELECT 'SELECT * FROM ' || a || ';' FROM show_chunks('test.year_partition_hypertable') AS a;
\gexec


