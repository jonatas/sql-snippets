drop table batteries cascade;
CREATE TABLE batteries ( time timestamp not null, batt_uid varchar, charge int, delta int);
SELECT create_hypertable('batteries', 'time');

CREATE OR REPLACE FUNCTION update_delta() RETURNS trigger AS
$BODY$
DECLARE
    previous_charge integer; 
BEGIN
   select charge
   into previous_charge
   from batteries where batt_uid = NEW.batt_uid
   order by time desc limit 1;

  IF NEW.charge IS NOT NULL THEN
    IF previous_charge IS NOT NULL THEN
       NEW.delta = NEW.charge - previous_charge;
    ELSE
      NEW.delta = 0;

    END IF;
  END IF;

  RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER update_delta_on_insert
               BEFORE INSERT
               ON batteries
               FOR EACH ROW
               EXECUTE PROCEDURE update_delta();

INSERT INTO batteries VALUES 
('2021-08-26 10:09:00'::timestamp, 'battery-1', 32),
('2021-08-26 10:09:01'::timestamp, 'battery-1', 34),
('2021-08-26 10:09:02'::timestamp, 'battery-1', 38);

INSERT INTO batteries VALUES 
'2021-08-26 10:09:00'::timestamp, 'battery-2', 0),
('2021-08-26 10:09:01'::timestamp, 'battery-2', 4),
('2021-08-26 10:09:02'::timestamp, 'battery-2', 28),
('2021-08-26 10:09:03'::timestamp, 'battery-2', 32),
('2021-08-26 10:09:04'::timestamp, 'battery-2', 28);

SELECT * FROM batteries;
