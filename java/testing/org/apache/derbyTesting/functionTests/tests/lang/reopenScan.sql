--
-- Test reopening scans.  A in subquery generates
-- a lot of reopen requests to the underlying scan.
-- We are used to having to reopen something like a
-- base table scan, but we need to be careful reopening
-- things like join nodes.  This test is to ensure
-- that we don't leave around any state when reopening
-- various complex nodes.


drop table x;
drop table y;
drop table z;
create table x (x int);
create table y (x int);
create table z (x int);

insert into x values 1,2,3;
insert into y values 1,2,3;
insert into z values 3,2,3,2;

select x from y where x in (select x from x);
select x from z where x in (1,2,3);

--
-- nested loop
--

-- one row right side
select x from z where x in (select x from y where x in (select x from x));

-- not one row right side
select x from z where x in (select x.x from x,y where x.x=y.x);

--
-- hash join
--
select x from z where x in (select x.x from x,y where x.x=y.x);

--
-- outer join
--
select x from z where x in (select x.x from x left outer join y on (y.x=x.x));

delete from y;
insert into y values 0,1,5,2,2;
select x.x from x left outer join y on (y.x=x.x);
select x from z where x in (select x.x from x left outer join y on (y.x=x.x));

delete from x;
insert into x values 0,1,5,2,2;
delete from y;
insert into y values 1,2,3;
select x.x from x left outer join y on (y.x=x.x);
select x from z where x in (select x.x from x left outer join y on (y.x=x.x));

insert into z values 1,5;
select x from z where x in (select x.x from x left outer join y on (y.x=x.x));


--
-- aggregate result set
--
delete from x;
delete from y;
delete from z;

insert into x values 1,2,3;
insert into y values 1,2,3;
insert into z values 3,2,666,3,2,null,2;

select x from z where x in (select x from x group by x);
select x from z where x in (select max(x) from x group by x);
select x from z where x in (select max(x) from x);
select x from z where x in (select sum(distinct x) from x group by x);
insert into x values 1,1,2,2,2,5,5,null,6;
select x from z where x in (select sum(distinct x) from x group by x);

--
-- union
--
delete from x;
delete from y;
delete from z;

insert into x values null,2,3;
insert into y values 1,2,null;
insert into z values 3,2,666,3,2,null,2;

select x from z where x in (select x from x union select x from y);

--
-- normalize
--
delete from x;
delete from y;
delete from z;

create table n (x smallint);
insert into n values 1,2,3;
insert into x values 1,2,3;

select * from x where x in (select x from n);



