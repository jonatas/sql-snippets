with base as (
        select time
        from pages
        order by 1 desc limit 1 offset 1000)
select min(pages.time), max(pages.time) -- OR select count(1)
from pages join base on pages.time > base.time;
