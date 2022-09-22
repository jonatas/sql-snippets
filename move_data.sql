CREATE TABLE old_table ( id bigserial, time_string text NOT NULL, price decimal);
CREATE TABLE new_table ( time TIMESTAMP NOT NULL,  price decimal);
SELECT create_hypertable('new_table', 'time');


INSERT INTO old_table (time_string, price) VALUES
('2021-08-26 10:09:00.01', 10.1),
('2021-08-26 10:09:00.08',  10.0),
('2021-08-26 10:09:00.23',  10.2),
('2021-08-26 10:09:00.40',  10.3);
INSERT INTO new_table SELECT time_string::timestamp as time, price from old_table;
