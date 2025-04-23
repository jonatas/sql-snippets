drop table if exists metrics cascade;
drop table if exists metric_readings cascade;
-- Enable pgcrypto extension for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create table

create table metrics (
  uuid uuid primary key default gen_random_uuid(),
  name text not null
);

CREATE TABLE metric_readings (
  time TIMESTAMPTZ NOT NULL,
  metric_uuid uuid references metrics(uuid),
  value DOUBLE PRECISION
);

-- Convert to hypertable
SELECT create_hypertable('metric_readings', by_range('time', INTERVAL '1 day'));


-- Create secondary indexes
CREATE INDEX metric_readings_metric_uuid_hash_idx ON metric_readings USING hash (metric_uuid);
CREATE UNIQUE INDEX metric_readings_metric_uuid_time_idx ON metric_readings (metric_uuid, time);

alter table metric_readings set access method hypercore;

alter table metric_readings
set (
  timescaledb.orderby = 'time',
  timescaledb.segmentby = 'metric_uuid'
);

alter table metric_readings
set (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'metric_uuid',
  timescaledb.compress_orderby = 'time'
);

-- Create compression policy
SELECT add_compression_policy('metric_readings', INTERVAL '3 days', if_not_exists => true);
-- Create columnstore policy
CALL add_columnstore_policy( 'metric_readings', interval '1 day', if_not_exists => true);

insert into metrics (name) values ('temperature'),('humidity'),('pressure');

with temperature_uuid as (select uuid from metrics where name = 'temperature'),
humidity_uuid as (select uuid from metrics where name = 'humidity'),
pressure_uuid as (select uuid from metrics where name = 'pressure') 

INSERT INTO metric_readings (time, metric_uuid, value)
VALUES 
(now(), (select uuid from temperature_uuid), 100), 
(now() - interval '1 day', (select uuid from temperature_uuid), 22.0),
(now() - interval '2 day', (select uuid from temperature_uuid), 22.3),
(now() - interval '3 day', (select uuid from temperature_uuid), 22.7),
(now() - interval '4 day', (select uuid from temperature_uuid), 22.0),
(now() - interval '5 day', (select uuid from temperature_uuid), 21.0),
(now() - interval '6 day', (select uuid from temperature_uuid), 20.0),
(now() - interval '7 day', (select uuid from temperature_uuid), 19.0),
(now(), (select uuid from humidity_uuid), 100), 
(now() - interval '1 day', (select uuid from humidity_uuid), 50.0),
(now() - interval '2 day', (select uuid from humidity_uuid), 55.0),
(now() - interval '3 day', (select uuid from humidity_uuid), 60.0),
(now() - interval '4 day', (select uuid from humidity_uuid), 65.0),
(now() - interval '5 day', (select uuid from humidity_uuid), 70.0),
(now() - interval '6 day', (select uuid from humidity_uuid), 75.0),
(now() - interval '7 day', (select uuid from humidity_uuid), 80.0),
(now(), (select uuid from pressure_uuid), 1000),
(now() - interval '1 day', (select uuid from pressure_uuid), 1001),
(now() - interval '2 day', (select uuid from pressure_uuid), 1002),
(now() - interval '3 day', (select uuid from pressure_uuid), 1003),
(now() - interval '4 day', (select uuid from pressure_uuid), 1004),
(now() - interval '5 day', (select uuid from pressure_uuid), 1005),
(now() - interval '6 day', (select uuid from pressure_uuid), 1006),
(now() - interval '7 day', (select uuid from pressure_uuid), 1007)
 ON CONFLICT (metric_uuid, time) DO UPDATE SET value = EXCLUDED.value;

SELECT * FROM metric_readings;