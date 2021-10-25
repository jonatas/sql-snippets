drop table batteries cascade;
CREATE TABLE batteries (t timestamp not null, batt_uid varchar, charge int);
SELECT create_hypertable('batteries', 't');

DROP FUNCTION IF EXISTS watch_charge;
CREATE OR REPLACE FUNCTION watch_charge(INOUT t timestamp, INOUT batt_uid varchar, INOUT charge int) AS
$BODY$
BEGIN
  IF charge > 100 then
    raise notice 'Battery % charge is too high: %', batt_uid, charge;
  END IF;
END;
$BODY$
LANGUAGE plpgsql;

\timing

INSERT into batteries VALUES (now()::timestamp, 'battery-1', 90);
INSERT into batteries VALUES (now()::timestamp, 'battery-1', 91);
INSERT into batteries VALUES (now()::timestamp, 'battery-1', 98);
INSERT into batteries VALUES (now()::timestamp, 'battery-1', 99);
INSERT into batteries VALUES (now()::timestamp, 'battery-1', 100);
INSERT into batteries VALUES (now()::timestamp, 'battery-1', 101);
Insert into batteries VALUES (now()::timestamp, 'battery-2', 90);
INSERT into batteries VALUES (now()::timestamp, 'battery-2', 91);


INSERT into batteries SELECT * FROM watch_charge(now()::timestamp, 'battery-1', 90);
INSERT into batteries SELECT * FROM watch_charge(now()::timestamp, 'battery-1', 91);
INSERT into batteries SELECT * FROM watch_charge(now()::timestamp, 'battery-1', 98);
INSERT into batteries SELECT * FROM watch_charge(now()::timestamp, 'battery-1', 99);
INSERT into batteries SELECT * FROM watch_charge(now()::timestamp, 'battery-1', 100);
INSERT into batteries SELECT * FROM watch_charge(now()::timestamp, 'battery-1', 101);

