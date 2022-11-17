DROP TABLE if exists a ;
DROP TABLE if exists b ;
CREATE TABLE "a" ("t" timestamp not null, "v" decimal not null);
CREATE TABLE "b" ("t" timestamp not null, "v" decimal not null);
SELECT create_hypertable('a', 't', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('b', 't', chunk_time_interval => INTERVAL '1 day');

insert into a VALUES ('2022-10-10 10:10:10'::timestamp, 2);
insert into a VALUES ('2022-10-10 10:20:21'::timestamp, 3);
insert into a VALUES ('2022-10-10 10:30:01'::timestamp, 4);
insert into b VALUES ('2022-10-10 10:10:15'::timestamp, 5);
insert into b VALUES ('2022-10-10 10:20:22'::timestamp, 6);
insert into b VALUES ('2022-10-10 10:30:03'::timestamp, 7);

select a.t, a.v * b.v
FROM a join b on time_bucket('10 s',a.t)= time_bucket('10s', b.t);
