
-- ** insert aggregatesPositive.sql
autocommit on;
-- General aggregate tests.  Aggregate
-- specifics are tested in specific test (e.g. sum.jsql).
--
-- Note that this test does NOT test multiple datatypes,
-- that is exercised in the specific aggregate tests.
-- INSERT SELECT is also in the specific aggregate tests.
-- 
-- need to add: objects

create table t1 (c1 int, c2 int);
create table t2 (c1 int, c2 int);
create table oneRow (c1 int, c2 int);
insert into oneRow values(1,1);
create table empty (c1 int, c2 int);
create table emptyNull (c1 int, c2 int);
insert into emptyNull values (null, null);
insert into t1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10);
insert into t2 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10);
select * from t1;

--------------------------------------
-- Expressions within an aggregate
--------------------------------------
select max(c1+10) from t1;
select max(c1+10) from t1 group by c2;

select max(2*10) from t1;
select max(2*10) from t1 group by c2;

-- conditional operator within aggregate
select max(case when c1 <> 1 then 666 else 999 end) from oneRow;
select max(case when c1 = 1 then 666 else c2 end) from oneRow;
select max(case when c1 = 1 then 666 else c1 end) from oneRow;

-- subquery in aggregate
select max((select c1 from empty)) from t1;

-- cast to string in aggregate
select max(cast (c1 as char(1))) from oneRow;

-- cast to string in aggregate and concatenate with another
select max(cast(c1 as char(1)) || cast (c2 as char(1))) from oneRow;

-- unary
select max(-c1) from t1;

-- count
select count(c1) from t1;

-- cast
select count(cast (null as int)) from t1;

-- avg
-- DB2 returns error 22003
-- CS returns no error!
select avg(2147483647) from t1;

--------------------------------------
-- Expressions on an aggregates/with aggregates
--------------------------------------
select 10+sum(c1) from t1;
select 10+sum(c1+10) from t1; 

-- conditional operator on aggregate
select (case when max(c1) = 1 then 666 else 1 end) from t1;
select (case when max(c1) = 1 then 666 else c1 end) from t1 group by c1;

-- method call on aggregate, cannot use nulls
select cast (max(c1) as char(1)) from oneRow;
select cast (max(c1) as char(1)) from oneRow group by c1;
select (cast(c1 as char(1)) || (cast (max(c2) as char(1)))) from oneRow group by c1;

-- subquery on aggregate
select (select max(c1) from t2)from t1;
select (select max(c1) from oneRow group by c2)from t1;

-- unary
select -max(c1) from t1; 
select -max(c1) from t1 group by c1;

-- cast
select cast (null as int), count(c1) from t1 group by c1;
select count(cast (null as int)) from t1 group by c1;

-- binary list operator
-- beetle 5571 - transient boolean type not allowed in DB2
select (1 in (1,2)), count(c1) from t1 group by c1;
select count((1 in (1,2))) from t1 group by c1;

-- some group by specific tests
select c2, 10+sum(c1), c2 from t1 group by c2;
select c2, 10+sum(c1+10), c2*2 from t1 group by c2;
select c2+sum(c1)+c2 from t1 group by c2;
select (c2+sum(c1)+c2)+10, c1, c2 from t1 group by c1, c2;
select c1+10, c2, c1*1, c1, c2*5 from t1 group by c1, c2;

--------------------------------------
-- Distincts
--------------------------------------
select sum(c1) from t1;
select sum(distinct c1) from t1;
select sum(distinct c1), sum(c1) from t1;
select sum(distinct c1), sum(c1) from oneRow;
select max(c1), sum(distinct c1), sum(c1) from t1;
select sum(distinct c1) from empty;
select sum(distinct c1) from emptyNull;

select sum(c1) from t1 group by c2;
select sum(distinct c1) from t1 group by c2;
select sum(distinct c1), sum(c1) from t1 group by c2;
select sum(distinct c1), sum(c1) from oneRow group by c2;
select max(c1), sum(distinct c1), sum(c1) from t1 group by c2;
select c2, max(c1), c2+1, sum(distinct c1), c2+2, sum(c1) from t1 group by c2;
select sum(distinct c1) from empty group by c2;
select sum(distinct c1) from emptyNull group by c2;

--------------------------------------
-- Subqueries in where clause
--------------------------------------
-- subqueries that might return more than 1 row
select c1 from t1 where c1 not in (select sum(c1) from t2);
select c1 from t1 where c1 not in (select sum(distinct c1) from t2);
select c1 from t1 where c1 not in (select sum(distinct c1)+10 from t2);

select c1 from t1 where c1 in (select max(c1) from t2 group by c2);
select c1 from t1 where c1 in (select max(distinct c1) from t2 group by c2);
select c1 from t1 where c1 in (select max(distinct c1)+10 from t2 group by c2);

-- subqueries that return 1 row
select c1 from t1 where c1 = (select max(c1) from t2);
select c1 from t1 where c1 = (select max(distinct c1) from t2);
select c1 from t1 where c1 = (select max(distinct c1)+10 from t2);

select c1 from t1 where c1 = (select max(c1) from oneRow group by c2);
select c1 from t1 where c1 = (select max(distinct c1) from oneRow group by c2);
select c1 from t1 where c1 = (select max(distinct c1)+10 from oneRow group by c2);

--------------------------------------
-- From Subqueries (aka table expressions)
--------------------------------------
select tmpC1 from 
	(select max(c1+10) from t1) as tmp (tmpC1);
select max(tmpC1) from 
	(select max(c1+10) from t1) as tmp (tmpC1);
select tmpC1 from 
	(select max(c1+10) from t1 group by c2) as tmp (tmpC1);
select max(tmpC1) from 
	(select max(c1+10) from t1 group by c2) as tmp (tmpC1);

select max(tmpC1), tmpC2 from 
	(select max(c1+10), c2 from t1 group by c2) as tmp (tmpC1, tmpC2)
group by tmpC2;

--------------------------------------
-- Cartesian product on from subquery: forces
-- multiple opens/closes on the sort
-- result set (bug 447)
--------------------------------------
select * from t1, (select max(c1) from t1) as mytab(c1);
select * from t1, (select max(c1) from t1 group by c1) as mytab(c1);

--------------------------------------
-- Union
--------------------------------------
select max(c1) from t1
union all
select max(c1) from t2;

--------------------------------------
-- Joins
--------------------------------------
select max(t1.c1), max(t2.c2) 
from t1, t2
where t1.c1 = t2.c1;

select max(t1.c1), max(t2.c2) 
from t1, t2
where t1.c1 = t2.c1
group by t1.c1;


--------------------------------------
-- Having
--------------------------------------

-- having with agg on a join
select max(t1.c1), max(t2.c2) 
from t1, t2
where t1.c1 = t2.c1
group by t1.c1
having count(*) > 0;

-- having with subqueries and aggs, agg on grouping col
select c1 from t1
group by c1
having max(c2) in (select c1 from t2);

-- agg not on grouping column
select c1 from t1
group by c1
having max(c2) in (select c1 from t2);

-- having with a subquery that returns a single value
select c1 from t1
group by c1
having avg(c2) in (select max(t2.c1) from t2);

-- similar to above
select c1 from t1
group by c1
having (select max(t2.c1) from t2) = avg(c2);

-- various and sundry column references in the having clause
select c1 from t1
group by c1
having max(c2) > (select avg(t2.c1 + t1.c1)-20 from t2);

-- multiple subqueries
select c1 from t1
group by c1
having (max(c2) in (select c1 from t2)) OR
		(max(c1) in (select c2-999 from t2)) OR
		(count(*) > 0)
;

-- non-correlated subquery w/o aggregate in aggreate select list
select max(c1), (select c1 from oneRow) from t1;
select max(c1), (select c1 from oneRow) from t1 group by c1;


--- tests of exact numeric results

create table bd (i decimal(31,30));
insert into bd values(0.1);
insert into bd values(0.2);
select * from bd;

-- should be the same
select avg(i), sum(i)/count(i) from bd;
drop table bd;

create table it (i int);
insert into it values (1);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (0);
insert into it values (200001);

-- should be the same
select avg(i), sum(i)/count(i), sum(i), count(i) from it;
drop table it;

--- test avg cases where the sum will overflow
create table ovf_int (i int);
insert into ovf_int values (2147483647);
insert into ovf_int values (2147483647 - 1);
insert into ovf_int values (2147483647 - 2);
select avg(i), 2147483647 - 1 from ovf_int;
drop table ovf_int;

create table ovf_small (i smallint);
insert into ovf_small values (32767);
insert into ovf_small values (32767 - 1);
insert into ovf_small values (32767 - 2);
select avg(i), 32767 - 1 from ovf_small;
drop table ovf_small;

create table ovf_long (i bigint);
insert into ovf_long values (9223372036854775807);
insert into ovf_long values (9223372036854775807 - 1);
insert into ovf_long values (9223372036854775807 - 2);
-- beetle 5571 - transient boolean type not allowed in DB2 UDB
select avg(i), 9223372036854775807 - 1 from ovf_long;
select avg(i), 9223372036854775807 from ovf_long;
-- operands are allowed in DB2 UDB
select avg(i) from ovf_long;
select avg(i) - 1  from ovf_long;
drop table ovf_long;

-- Test that AVG is not limited by columns type precision
-- using DB2 MAX REAL VALUES
create table ovf_real (i real);
insert into ovf_real values (+3.402E+38);
insert into ovf_real values (+3.402E+38 - 1);
insert into ovf_real values (+3.402E+38 - 2);
select avg(i) from ovf_real;
drop table ovf_real;

-- Test that AVG is not limited by columns type precision
-- using DB2 MAX DOUBLE VALUES
create table ovf_double (i double precision);
insert into ovf_double values (+1.79769E+308);
insert into ovf_double values (+1.79769E+308 - 1);
insert into ovf_double values (+1.79769E+308 - 2);
select avg(i) from ovf_double;
drop table ovf_double;

--------------------------------------
-- CLEAN UP
--------------------------------------
drop table t1;
drop table t2;
drop table oneRow;
drop table empty;
drop table emptyNull;

-- ** insert aggregateNegative.sql
-- For aggregates.  General issues
autocommit on;
create table t (i int, l bigint);
create table t1 (c1 int);
create table t2 (c1 int);
--------------------------------------
-- NEGATIVE TESTS
--------------------------------------

-- only a single distinct is supported
select sum(distinct i), sum(distinct l) from t;

-- parameters in aggregate
prepare p1 as 'select max(?) from t';

-- aggregates in aggregates
select max(max(i)) from t;
select max(1+1+1+max(i)) from t;

-- TEMPORARY RESTRICTION, aggregates in the select list
-- of a subquery on an aggregated result set
select max(c1), (select max(c1) from t2) from t1;
select max(c1), (select max(t1.c1) from t2) from t1;
select max(c1), max(c1), (select max(c1) from t1) from t1;

-- cursor with aggregate is not updatable
get cursor c1 as 'select max(i) from t group by i for update';

-- max over a join on a column with an index -- Beetle 4423
create table t3(a int);
insert into t3 values(1),(2),(3),(4),(5);
create table t4(a int);
insert into t4 select a from t3;
create index tindex on t3(a);
select max(t3.a)
from t3, t4
where t3.a = t4.a
and t3.a = 1;

drop table t;
drop table t1;
drop table t2;
drop table t3;
drop table t4;

-- beetle 5122, aggregate on JoinNode

CREATE TABLE DOCUMENT_VERSION
   (
      DOCUMENT_ID INT,
      DOCUMENT_STATUS_ID INT
   )
;

insert into DOCUMENT_VERSION values (2,2),(9,9),(5,5),(1,3),(10,5),(1,6),(10,8),(1,10);

CREATE VIEW MAX_DOCUMENT_VERSION
   AS SELECT  DOCUMENT_ID  FROM DOCUMENT_VERSION
;


CREATE VIEW MAX_DOCUMENT_VERSION_AND_STATUS_ID
   AS SELECT  MAX(DV.DOCUMENT_STATUS_ID) AS MAX_DOCUMENT_STATUS_ID
   FROM DOCUMENT_VERSION AS DV , MAX_DOCUMENT_VERSION 
   WHERE DV.DOCUMENT_ID = 1;


CREATE VIEW LATEST_DOC_VERSION
   AS SELECT DOCUMENT_ID 
   FROM DOCUMENT_VERSION AS DV, MAX_DOCUMENT_VERSION_AND_STATUS_ID AS MDVASID
   WHERE DV.DOCUMENT_ID = MDVASID.MAX_DOCUMENT_STATUS_ID;

select * from LATEST_DOC_VERSION;

drop view LATEST_DOC_VERSION;
drop view MAX_DOCUMENT_VERSION_AND_STATUS_ID;
drop view  MAX_DOCUMENT_VERSION;
drop table DOCUMENT_VERSION;

-- Defect 5737. Prevent aggregates being used in VALUES clause or WHERE clause.
create table tmax(i int);
values sum(1);
values max(3);

select * from tmax where sum(i)=1;
select i from tmax where substr('abc', sum(1), 3) = 'abc';

drop table tmax;

