DROP TABLE my_table;
CREATE TABLE my_table ( time TIMESTAMP NOT NULL,  value varchar);
SELECT create_hypertable('my_table', 'time');


INSERT INTO my_table (time, value) VALUES
('2021-08-26 10:09:00.01', '1012311'),
('2021-08-26 10:09:00.08',  '1022220'),
('2021-08-26 10:09:00.40',  '103333000');
ALTER TABLE my_table ALTER COLUMN value TYPE bigint USING value::bigint;
