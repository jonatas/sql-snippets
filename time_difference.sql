drop table if exists sensor_data ;
create table sensors_data (id bigint, uid text, server_time timestamp);
insert into sensors_data  values
(1, 'a', '2021-07-29 11:36:15'),
(2, 'b', '2021-07-29 12:36:15'),
(3, 'a', '2021-07-29 12:36:15'),
(4, 'b', '2021-07-29 11:39:15'),
(5, 'a', '2021-07-29 13:36:15'),
(6, 'a', '2021-07-29 13:45:51'),
(7, 'b', '2021-07-29 13:45:51'),
(8, 'a', '2021-07-29 13:54:51');


with f as (
        select uid, min(server_time)
        from sensors_data
        group by 1, time_bucket('1h', server_time)
) select f.uid, f.min - sensors_data.server_time
from f
left join sensors_data 
on f.uid = sensors_data.uid
--and time_bucket('1d', f.min) = time_bucket('1d', sensors_data.server_time)
group by 1, 2 having f.min - sensors_data.server_time > interval '1 hour'
order by 2;
