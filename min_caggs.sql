DROP TABLE if exists raw_data CASCADE;
DROP view if exists downsampled CASCADE;
CREATE TABLE raw_data
( time TIMESTAMP NOT NULL,
     tag_id varchar,
    value decimal);
SELECT create_hypertable('raw_data', 'time');

CREATE MATERIALIZED VIEW  downsampled
WITH (timescaledb.continuous) AS
    select time_bucket('1h', time) as time,
           tag_id,
    first(time, time) as real_time,
          avg(value) as value
          FROM raw_data 
          GROUP BY 1,2
WITH DATA;

INSERT INTO raw_data ( tag_id, time, value)
VALUES
( 'tag_1', '2000-01-01 12:45:00', random()),
( 'tag_1', '2000-01-01 12:50:00', random()),
( 'tag_2', '2000-01-01 12:32:00', random()),
( 'tag_2', '2000-01-01 12:45:00', random()),
( 'tag_1', '2000-01-01 13:05:00', random()),
( 'tag_1', '2000-01-01 13:50:00', random()),
( 'tag_2', '2000-01-01 14:02:00', random()),
( 'tag_2', '2000-01-01 14:03:00', random());

TABLE downsampled ORDER BY time;


