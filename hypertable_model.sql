DROP TABLE model CASCADE;
DROP view mother CASCADE;
CREATE TABLE model ( time TIMESTAMP NOT NULL, identifier text, value decimal);
CREATE view mother AS select '' as table_name, null::timestamp as time, null::text as identifier, null::decimal as value;

CREATE OR REPLACE FUNCTION feed_child_table()
RETURNS trigger AS
$BODY$
DECLARE
  table_exists boolean;
  create_table text;
  insert_data text;
BEGIN
   SELECT true INTO table_exists
   FROM timescaledb_information.hypertables
   WHERE hypertable_name = NEW.table_name
   LIMIT 1;

   IF table_exists IS NULL THEN
     create_table := 'CREATE TABLE IF NOT EXISTS ' || NEW.table_name || '( like model )';
     EXECUTE create_table;
   END IF;

   insert_data := 'INSERT INTO ' || NEW.table_name || ' (time, identifier, value) VALUES ($1, $2, $3)';
   EXECUTE insert_data USING NEW.time, NEW.identifier, NEW.value;

  RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER feed_child_table_trigger
INSTEAD OF INSERT ON mother FOR EACH ROW
EXECUTE PROCEDURE feed_child_table();

INSERT INTO mother (table_name, time, identifier, value) VALUES
('a', '2021-08-26 10:09:00.01'::timestamp, 'id1', 10.1),
('a', '2021-08-26 10:09:00.08'::timestamp, 'id2', 10.0),
('b', '2021-08-26 10:09:00.23'::timestamp, 'id3', 10.2),
('b', '2021-08-26 10:09:00.40'::timestamp, 'id4', 10.3);


