drop table if exists test ;
create table test (t timestamp, v integer);
insert into test (t, v) select i, (random() > 0.5)::int from generate_series(now(), now()+ interval '1 minute', interval '5 seconds') i;


table test;
select toolkit_experimental.state_agg(t::timestamptz, v::text) from test;
