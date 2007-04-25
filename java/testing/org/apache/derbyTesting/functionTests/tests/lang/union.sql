--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
--
-- this test shows union functionality
--

-- create the tables
create table t1 (i int, s smallint, d double precision, r real, c10 char(10),
				c30 char(30), vc10 varchar(10), vc30 varchar(30));
create table t2 (i int, s smallint, d double precision, r real, c10 char(10),
				c30 char(30), vc10 varchar(10), vc30 varchar(30));
create table dups (i int, s smallint, d double precision, r real, c10 char(10),
				c30 char(30), vc10 varchar(10), vc30 varchar(30));

-- populate the tables
insert into t1 values (null, null, null, null, null, null, null, null);
insert into t1 values (1, 1, 1e1, 1e1, '11111', '11111     11', '11111',
	'11111      11');
insert into t1 values (2, 2, 2e1, 2e1, '22222', '22222     22', '22222',
	'22222      22');

insert into t2 values (null, null, null, null, null, null, null, null);
insert into t2 values (3, 3, 3e1, 3e1, '33333', '33333     33', '33333',
	'33333      33');
insert into t2 values (4, 4, 4e1, 4e1, '44444', '44444     44', '44444',
	'44444      44');
insert into dups select * from t1 union all select * from t2;

-- simple cases
values (1, 2, 3, 4) union values (5, 6, 7, 8);
values (1, 2, 3, 4) union values (1, 2, 3, 4);
values (1, 2, 3, 4) union distinct values (5, 6, 7, 8);
values (1, 2, 3, 4) union distinct values (1, 2, 3, 4);

values (1, 2, 3, 4) union values (5, 6, 7, 8) union values (9, 10, 11, 12);
values (1, 2, 3, 4) union values (1, 2, 3, 4) union values (1, 2, 3, 4);

select * from t1 union select * from t2;
select * from t1 union select * from t1;
select * from t1 union select * from t2 union select * from dups;

select * from t1 union select i, s, d, r, c10, c30, vc10, vc30 from t2;
select * from t1 union select i, s, d, r, c10, c30, vc10, vc30 from t2
		union select * from dups;

-- derived tables
select * from (values (1, 2, 3, 4) union values (5, 6, 7, 8)) a;
select * from (values (1, 2, 3, 4) union values (5, 6, 7, 8) union
			   values (1, 2, 3, 4)) a;

-- mix unions and union alls
select i from t1 union select i from t2 union all select i from dups;
(select i from t1 union select i from t2) union all select i from dups;
select i from t1 union (select i from t2 union all select i from dups);
select i from t1 union all select i from t2 union select i from dups;
(select i from t1 union all select i from t2) union select i from dups;
select i from t1 union all (select i from t2 union select i from dups);


-- joins
select a.i, b.i from t1 a, t2 b union select b.i, a.i from t1 a, t2 b;
values (9, 10) union 
	select a.i, b.i from t1 a, t2 b union select b.i, a.i from t1 a, t2 b;
select a.i, b.i from t1 a, t2 b union 
	select b.i, a.i from t1 a, t2 b union values (9, 10);

-- non-correlated subqueries

-- positive tests
select i from t1 where i = (values 1 union values 1);
select i from t1 where i = (values 1 union values 1 union values 1);

-- expression subquery
select i from t1 where i = (select 1 from t2 union values 1);

-- in subquery
select i from t1 where i in (select i from t2 union values 1 union values 2);
select i from t1 where i in 
		(select a from (select i from t2 union values 1 union values 2) a (a));

-- not in subquery
select i from t1 where i not in (select i from t2 union values 1 union values 2);
select i from t1 where i not in (select i from t2 where i is not null union 
								 values 1 union values 22);
select i from t1 where i not in 
		(select a from (select i from t2 where i is not null union 
						values 111 union values 2) a (a));

-- correlated union subquery
select i from t1 a where i in (select i from t2 where 1 = 0 union 
							   select a.i from t2 where a.i < i);
select i from t1 a where i in (select a.i from t2 where a.i < i union 
							   select i from t2 where 1 < 0);

-- exists subquery
select i from t1 where exists (select * from t2 union select * from t2);
select i from t1 where exists (select 1 from t2 union select 2 from t2);
select i from t1 where exists (select 1 from t2 where 1 = 0 union 
							   select 2 from t2 where t1.i < i);
select i from t1 where exists (select i from t2 where t1.i < i union 
							    select i from t2 where 1 = 0 union 
							   select i from t2 where t1.i < i union 
							    select i from t2 where 1 = 0);

-- These next two should fail because left/right children do not have
-- the same number of result columns.
select i from t1 where exists (select 1 from t2 where 1 = 0 union 
							   select * from t2 where t1.i < i);
select i from t1 where exists (select i from t2 where t1.i < i union 
							    select * from t2 where 1 = 0 union 
							   select * from t2 where t1.i < i union 
							    select i from t2 where 1 = 0);

-- order by tests
select i from t1 union select i from dups order by i desc;
select i, s from t1 union select s as i, 1 as s from dups order by s desc, i;

-- insert tests
create table insert_test (i int, s smallint, d double precision, r real,
	c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30));

-- simple tests
insert into insert_test select * from t1 union select * from dups;
select * from insert_test;
delete from insert_test;

insert into insert_test (s, i) values (2, 1) union values (4, 3);
select * from insert_test;
delete from insert_test;

-- test type dominance/length/nullability
insert into insert_test (vc30) select vc10 from t1 union select c30 from t2;
select * from insert_test;
delete from insert_test;
insert into insert_test (c30)
	select vc10 from t1
	union
	select c30 from t2
	union
	select c10 from t1;
select * from insert_test;
delete from insert_test;

-- test NormalizeResultSet generation
select i, d from t1 union select d, i from t2;
select vc10, c30 from t1 union select c30, vc10 from t2;

create table insert_test2 (s smallint not null, vc30 varchar(30) not null);
-- the following should fail due to null constraint
insert into insert_test2 select s, c10 from t1 union select s, c30 from t2;
select * from insert_test2;

-- negative tests
-- ? in select list of union
select ? from insert_test union select vc30 from insert_test;
select vc30 from insert_test union select ? from insert_test;
-- DB2 requires matching target and result column for insert
insert into insert_test values (1, 2) union values (3, 4);

-- try some unions of different types.  
-- types should be ok if comparable.
values (1) union values (1.1);
values (1) union values (1.1e1);
values (1.1) union values (1);
values (1.1e1) union values (1);

-- negative cases
values (x'aa') union values (1);

-- drop the tables
drop table t1;
drop table t2;
drop table dups;
drop table insert_test;
drop table insert_test2;
--
-- this test shows the current supported union all functionality
--
-- RESOLVE - whats not tested
--	type compatability
--  nullability of result
--  type dominance
--  correlated subqueries
--  table constructors

-- create the tables
create table t1 (i int, s smallint, d double precision, r real, c10 char(10),
				 c30 char(30), vc10 varchar(10), vc30 varchar(30));
create table t2 (i int, s smallint, d double precision, r real, c10 char(10),
				 c30 char(30), vc10 varchar(10), vc30 varchar(30));

-- populate the tables
insert into t1 values (null, null, null, null, null, null, null, null);
insert into t1 values (1, 1, 1e1, 1e1, '11111', '11111     11', '11111',
	'11111      11');
insert into t1 values (2, 2, 2e1, 2e1, '22222', '22222     22', '22222',
	'22222      22');

insert into t2 values (null, null, null, null, null, null, null, null);
insert into t2 values (3, 3, 3e1, 3e1, '33333', '33333     33', '33333',
	'33333      33');
insert into t2 values (4, 4, 4e1, 4e1, '44444', '44444     44', '44444',
	'44444      44');

-- negative tests

-- non matching number of columns
select * from t1 union all select * from t1, t2;
select * from t1 union all values (1, 2, 3, 4);
values (1, 2, 3, 4) union all select * from t1;

-- simple cases
values (1, 2, 3, 4) union all values (5, 6, 7, 8);
values (1, 2, 3, 4) union all values (5, 6, 7, 8) union all values (9, 10, 11, 12);
select * from t1 union all select * from t2;
select * from t1 union all select i, s, d, r, c10, c30, vc10, vc30 from t2;

-- derived tables
select * from (values (1, 2, 3, 4) union all values (5, 6, 7, 8)) a;
select * from (values (1, 2, 3, 4) union all values (5, 6, 7, 8)) a (a, b, c, d);
select b, d from (values (1, 2, 3, 4) union all values (5, 6, 7, 8)) a (a, b, c, d);
select * from (select i, s, c10, vc10 from t1 union all select i, s, c10, vc10 from t2) a;
select * from (select i, s, c10, vc10 from t1 union all 
			   select i, s, c10, vc10 from t2) a (j, k, l, m), 
			   (select i, s, c10, vc10 from t1 union all 
			   select i, s, c10, vc10 from t2) b (j, k, l, m)
where a.j = b.j;

-- joins
select a.i, b.i from t1 a, t2 b union all select b.i, a.i from t1 a, t2 b;
values (9, 10) union all 
	select a.i, b.i from t1 a, t2 b union all select b.i, a.i from t1 a, t2 b;
select a.i, b.i from t1 a, t2 b union all 
	select b.i, a.i from t1 a, t2 b union all values (9, 10);

-- incompatible types
select date('9999-11-11') from t1 union all select time('11:11:11') from t2;

-- non-correlated subqueries

-- negative tests

-- select * in subquery
select i from t1 where i = (select * from t2 union all select 1 from t1);
select i from t1 where i = (select 1 from t2 union all select * from t1);

-- too many columns
select i from t1 where i = (values (1, 2, 3) union all values (1, 2, 3));
select i from t1 where i = (select i, s from t2 union all select i, s from t1);

-- cardinality violation
select i from t1 where i = (values 1 union all values 1);

-- both sides of union have same type, which is incompatible with LHS
select i from t1 where i in (select date('1999-02-04') from t2 union all select date('1999-03-08') from t2);

-- positive tests

-- expression subquery
select i from t1 where i = (select i from t2 where 1 = 0 union all values 1);

-- in subquery
select i from t1 where i in (select i from t2 union all values 1 union all values 2);
select i from t1 where i in 
		(select a from (select i from t2 union all values 1 union all values 2) a (a));

-- not in subquery
select i from t1 where i not in (select i from t2 union all values 1 union all values 2);
select i from t1 where i not in (select i from t2 where i is not null union all 
								 values 1 union all values 22);
select i from t1 where i not in 
		(select a from (select i from t2 where i is not null union all 
						values 111 union all values 2) a (a));

-- correlated union subquery
select i from t1 a where i in (select i from t2 where 1 = 0 union all
							   select a.i from t2 where a.i < i);
select i from t1 a where i in (select a.i from t2 where a.i < i union all
							   select i from t2 where 1 < 0);

-- exists subquery
select i from t1 where exists (select * from t2 union all select * from t2);
select i from t1 where exists (select 1 from t2 union all select 2 from t2);
select i from t1 where exists (select 1 from t2 where 1 = 0 union all
							   select 2 from t2 where t1.i < i);
select i from t1 where exists (select i from t2 where t1.i < i union all
							    select i from t2 where 1 = 0 union all
							   select i from t2 where t1.i < i union all
							    select i from t2 where 1 = 0);

-- These next two should fail because left/right children do not have
-- the same number of result columns.
select i from t1 where exists (select 1 from t2 where 1 = 0 union all
							   select * from t2 where t1.i < i);
select i from t1 where exists (select i from t2 where t1.i < i union all
							    select * from t2 where 1 = 0 union all
							   select * from t2 where t1.i < i union all
							    select i from t2 where 1 = 0);

-- insert tests
create table insert_test (i int, s smallint, d double precision, r real, c10 char(10),
				 c30 char(30), vc10 varchar(10), vc30 varchar(30));

-- simple tests
insert into insert_test select * from t1 union all select * from t2;
select * from insert_test;
delete from insert_test;

insert into insert_test (s, i) values (2, 1) union all values (4, 3);
select * from insert_test;
delete from insert_test;

-- type conversions between union all and target table
insert into insert_test select s, i, r, d, vc10, vc30, c10, c30 from t1 union all
						select s, i, r, d, vc10, vc30, c10, vc30 from t2;
select * from insert_test;
delete from insert_test;

-- test type dominance/length/nullability
select vc10 from t1 union all select c30 from t2;
insert into insert_test (vc30) select vc10 from t1 union all select c30 from t2;
select * from insert_test;
delete from insert_test;
insert into insert_test (c30)
	select vc10 from t1
	union all
	select c30 from t2
	union all
	select c10 from t1;
select * from insert_test;
delete from insert_test;

-- test NormalizeResultSet generation
select i, d from t1 union all select d, i from t2;
select vc10, c30 from t1 union all select c30,  vc10 from t2;

create table insert_test2 (s smallint not null, vc30 varchar(30) not null);
-- the following should fail due to null constraint
insert into insert_test2 select s, c10 from t1 union all select s, c30 from t2;
select * from insert_test2;

-- negative tests
-- ? in select list of union
select ? from insert_test union all select vc30 from insert_test;
select vc30 from insert_test union all select ? from insert_test;
-- DB2 requires matching target and result columns
insert into insert_test values (1, 2) union all values (3, 4);

-- Beetle 4454 - test multiple union alls in a subquery
select vc10 from (select vc10 from t1 union all
select vc10 from t1 union all
select vc10 from t1 union all
select vc10 from t1 union all
select vc10 from t1 union all
select vc10 from t1 union all
select vc10 from t1) t;
-- force union all on right side
select vc10 from (select vc10 from t1 union all (select vc10 from t1 union all
select vc10 from t1)) t;

-- drop the tables
drop table t1;
drop table t2;
drop table insert_test;
drop table insert_test2;

-- DERBY-1967
-- NULLIF with UNION throws SQLSTATE 23502.

create table a (f1 varchar(10));
create table b (f2 varchar(10));
insert into b values('test');
-- this used to throw 23502
select nullif('x','x') as f0, f1 from a
   union all
   select nullif('x','x') as f0, nullif('x','x') as f1 from b; 
drop table a;
drop table b;
create table a (f1 int);
create table b (f2 int);
insert into b values(1);
-- ok
select nullif('x','x') as f0, f1 from a
   union all
   select nullif('x','x') as f0, nullif(1,1) as f1 from b; 
drop table a;
drop table b;

-- DERBY-681. Check union with group by/having
create table o (name varchar(20), ord int);
create table a (ord int, amount int);

create view v1 (vx, vy) 
as select name, sum(ord) from o where ord > 0 group by name, ord
    having ord <= ANY (select ord from a);

select vx, vy from v1
     union select vx, sum(vy) from v1 group by vx, vy having (vy / 2) > 15;
drop view v1;
drop table o;
drop table a;

-- DERBY-1852: Incorrect results when a UNION U1 (with no "ALL") appears
-- in the FROM list of a SELECT query, AND there are duplicate rows
-- across the left and/or right result sets of U1, AND U1 is the left or
-- right child of another set operator.

create table t1 (i int, j int);
create table t2 (i int, j int);
insert into t1 values (1, 2), (2, 4), (3, 6), (4, 8), (5, 10);
insert into t2 values (1, 2), (2, -4), (3, 6), (4, -8), (5, 10);
insert into t2 values (3, 6), (4, 8), (3, -6), (4, -8);

-- U1 is left child of another UNION; top-level query.
select * from t1 union select * from t2 union select * from t1;

-- U1 is left child of another UNION; subquery in FROM list.
select * from
  (select * from t1 union select * from t2 union select * from t1) x;

-- Same kind of thing, but in the form of a view (which is a
-- more likely use-ccase).
create view uv as
  select * from t1 union select * from t2 union select * from t1;
select * from uv;
drop view uv;

-- U1 is left child of a UNION ALL; top-level query.
select * from t1 union select * from t2 union all select * from t1;

-- U1 is left child of a UNION ALL; subquery in FROM list.
select * from
  (select * from t1 union select * from t2 union all select * from t1) x;

-- U1 is left child of an EXCEPT; top-level query.
select * from t1 union select * from t2 except select * from t1;

-- U1 is left child of an EXCEPT; subquery in FROM list.
select * from
  (select * from t1 union select * from t2 except select * from t1) x;

-- U1 is left child of an EXCEPT ALL; top-level query.
select * from t1 union select * from t2 except all select * from t1;

-- U1 is left child of an EXCEPT ALL; subquery in FROM list.
select * from
  (select * from t1 union select * from t2 except all select * from t1) x;

-- U1 is left child of an INTERSECT; top-level query.
-- Note: intersect has higher precedence than union so we have to use
-- quotes to force the UNION to be a child of the intersect.
(select * from t1 union select * from t2) intersect select * from t2;

-- U1 is left child of an INTERSECT; subquery in FROM list.
create view iv as
  (select * from t1 union select * from t2) intersect select * from t2;
select * from iv;
drop view iv;

-- U1 is left child of an INTERSECT ALL; top-level query.
(select * from t1 union select * from t2) intersect all select * from t2;

-- U1 is left child of an INTERSECT ALL; subquery in FROM list.
create view iv as
  (select * from t1 union select * from t2) intersect all select * from t2;
select * from iv;
drop view iv;

-- Just as a sanity check, make sure things work if U1 is a child of
-- an explicit JoinNode (since JoinNode is an instanceof TableOperatorNode
-- and TableOperatorNode is where the bug for DERBY-1852 was fixed).
select * from
  (select * from t1 union select * from t2) x2 left join t2 on x2.i = t2.i;

-- cleanup.
drop table t1;
drop table t2;
