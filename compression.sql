drop table data cascade;
CREATE TABLE data (
    time TIMESTAMP with time zone NOT NULL,
    value FLOAT NOT NULL
);

SELECT create_hypertable('data', 'time');
INSERT INTO data (time, value) VALUES
('2023-03-28 12:00:00', 1.0),
('2023-03-28 12:01:00', 2.0),
('2023-03-28 12:02:00', 3.0),
('2023-03-28 12:03:00', 4.0),
('2023-03-28 12:04:00', 5.0),
('2023-03-28 12:05:00', 6.0);

ALTER TABLE data SET (
    timescaledb.compress = true
);

SELECT compress_chunk(i) FROM show_chunks('data') i;

ALTER TABLE data ADD COLUMN new_column INTEGER;

SELECT * FROM data;
