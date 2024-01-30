create table events (
  who text,
  time timestamp,
  status text
);

select create_hypertable('events', 'time');

insert into events (who, time, status) values
  ('alice', now() - INTERVAL '1 day', 'joined'),
  ('alice', now() - INTERVAL '1 hour', 'log in'),
  ('alice', now(), 'log out');

WITH states AS (
  SELECT 
    (state_timeline(state_agg(time, status))).*,
    who
  FROM events
  GROUP BY who
)
SELECT 
  who,
  state, 
  start_time, 
  end_time - start_time AS total_time 
FROM states
ORDER BY who, start_time
