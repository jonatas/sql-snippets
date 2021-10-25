select histogram(n,1,10,2) from unnest(array[1,2,3,4,5]) n;
select histogram(n,1,10,2) from unnest(array[1,2,3,4,5,6,7,8]) n;
