set search_path to public, toolkit_experimental, timescaledb_experimental ;

DROP TABLE if exists ticks CASCADE;

CREATE TABLE ticks ( time TIMESTAMP NOT NULL, symbol varchar, price decimal, volume int);

SELECT create_hypertable('ticks', 'time');
CREATE MATERIALIZED VIEW tv_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) as bucket,
        symbol,
        timevector(time AT TIME ZONE 'GMT', price) as tv_price,
        timevector(time AT TIME ZONE 'GMT', volume) as tv_volume
        FROM ticks
        GROUP BY 1, 2
WITH DATA;

SELECT add_continuous_aggregate_policy('tv_1m', start_offset => INTERVAL '1 month', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');
\timing on

INSERT INTO ticks
SELECT time, 'SYMBOL', (random()*30)::int, 100*(random()*10)::int
FROM generate_series(TIMESTAMP '2000-01-01 00:00:00',
                 TIMESTAMP '2000-01-01 23:59:59',
             INTERVAL '1 second') AS time;



CREATE OR REPLACE FUNCTION volatility()
RETURNS pipelinethensum IMMUTABLE PARALLEL SAFE LANGUAGE SQL AS $$
SELECT ( sort() -> delta() -> abs() -> sum()) ;
$$;


SELECT time_bucket('15 min', bucket),
  sum(tv_price -> volatility()) as price_volatility,
  sum(tv_volume -> sum()) as traded_volume
FROM tv_1m group by 1 order by 1;

