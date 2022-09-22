DROP TABLE if exists measurements cascade;
CREATE TABLE "measurements" ("device_id" integer not null, "val" decimal not null, "ts" timestamp not null);
SELECT create_hypertable('measurements', 'ts', chunk_time_interval => INTERVAL '1 day');

INSERT INTO measurements (ts, device_id, val)
SELECT ts, device_id, random()*80
FROM generate_series(TIMESTAMP '2022-01-01 00:00:00',
                   TIMESTAMP '2022-02-01 00:00:00',
             INTERVAL '5 minutes') AS g1(ts),
      generate_series(0, 5) AS g2(device_id);

set search_path to toolkit_experimental, public;

-- Validating if timevector is generating a null value
WITH a as (
  SELECT device_id, (timevector(ts, val) -> unnest()).*
  FROM "measurements"
  GROUP BY device_id)
select a.* FROM a WHERE a.time is null or a.value is null;

-- Validating if sort is generating some null value
WITH a as (
  SELECT device_id, (timevector(ts, val) -> sort() -> unnest()).*
  FROM "measurements"
  GROUP BY device_id)
select a.* FROM a WHERE a.time is null or a.value is null;

-- Trying only to compute delta
--SELECT device_id, timevector(ts, val) -> sort() -> delta()
--FROM "measurements"
--GROUP BY device_id ;

SELECT device_id, timevector(ts, val) -> sort() -> delta() -> abs() -> sum() as volatility
FROM "measurements"
GROUP BY device_id;
