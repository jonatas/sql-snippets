DROP MATERIALIZED VIEW IF EXISTS connect_hourly CASCADE;
DROP TABLE IF EXISTS connect_events CASCADE;

CREATE TABLE IF NOT EXISTS connect_events(server_id integer NOT NULL, status bigint NOT NULL, occurred_at timestamptz NOT NULL);
CREATE UNIQUE INDEX idx_connet_events ON connect_events (server_id, occurred_at); 


CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
SELECT * from create_hypertable('connect_events', 'occurred_at');

CREATE MATERIALIZED VIEW connect_hourly
  WITH (timescaledb.continuous) AS
    SELECT server_id, time_bucket(INTERVAL '1 hour', occurred_at) AS hourly_bucket,
    state_agg(occurred_at, status) as hourly
      FROM connect_events
      GROUP BY server_id, hourly_bucket
      ORDER BY server_id, hourly_bucket
    WITH DATA;

SELECT remove_continuous_aggregate_policy('connect_hourly');
SELECT add_continuous_aggregate_policy('connect_hourly',
  start_offset => INTERVAL '1 day',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

truncate connect_events;
-- insert into connect_events (server_id, status, occurred_at) values
--   (1, 'connected',    '2023-08-01 00:00:00'),
--   (1, 'disconnected', '2023-08-22 12:00:00'),
--   (1, 'connected',    '2023-08-22 12:05:00'),
--   (1, 'disconnected', '2023-08-23 09:00:00'),
--   (1, 'connected',    '2023-08-23 09:10:00'),
--   (1, 'disconnected', '2023-08-24 12:00:00'),
--   (1, 'connected',    '2023-08-24 14:00:00');

insert into connect_events (server_id, status, occurred_at) values
  (1, 1,    '2023-08-01 00:00:00'),
  (1, 0, '2023-08-22 12:00:00'),
  (1, 1,    '2023-08-22 12:05:00'),
  (1, 0, '2023-08-23 09:00:00'),
  (1, 1,    '2023-08-23 09:10:00'),
  (1, 0, '2023-08-24 12:00:00'),
  (1, 1,    '2023-08-24 14:00:00');

CALL refresh_continuous_aggregate('connect_hourly', '2023-01-01', '2024-01-01');


select occurred_at, status from connect_events order by 1;

SELECT
	date(hourly_bucket),
	interpolated_duration_in( hourly, 1, hourly_bucket,
    LAG(hourly) OVER (ORDER BY hourly_bucket),
    LEAD(hourly) OVER (ORDER BY hourly_bucket)
 	) as connected,
	interpolated_duration_in( hourly, 0, hourly_bucket,
    LAG(hourly) OVER (ORDER BY hourly_bucket),
    LEAD(hourly) OVER (ORDER BY hourly_bucket)
 	) as disconnected
FROM connect_hourly;
