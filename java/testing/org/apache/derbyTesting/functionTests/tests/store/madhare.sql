--
-- this test shows the basic functionality
-- that does work in the language system for mad hare
--

-- this was the simple mad hare challenge

create table t(i int);
insert into t (i) values (1956);

select i from t;

-- we can also have multiple columns
create table s (i int, n int, t int, e int, g int, r int);
-- and reorder the columns on the insert
insert into s (i,r,t,n,g,e) values (1,6,3,2,5,4);
-- or not list the columns at all
-- (not to mention inserting more than one row into the table)
insert into s values (10,11,12,13,14,15);

-- and we can select some of the columns
select i from s;

-- and in funny orders
select n,e,r,i,t,g from s;

-- and with constants instead
select 20,n,22,e,24,r from s;

-- we do have prepare and execute support
prepare stmt as 'select i,n,t,e,g,r from s';
execute stmt;
-- execute can be done multiple times
execute stmt;

-- and, we also have smallint!
create table r (s smallint, i int);
insert into r values (23,2);
select s,i from r;

-- cleanup
drop table r;
drop table s;
drop table t;
