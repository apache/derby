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
-- subquery tests
--
autocommit off;
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker'
LANGUAGE JAVA PARAMETER STYLE JAVA;
autocommit off;

-- create the all type tables
create table s (i int, s smallint, c char(30), vc char(30), b bigint);
create table t (i int, s smallint, c char(30), vc char(30), b bigint);
create table tt (ii int, ss smallint, cc char(30), vcvc char(30), b bigint);
create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30));

-- populate the tables
insert into s values (null, null, null, null, null);
insert into s values (0, 0, '0', '0', 0);
insert into s values (1, 1, '1', '1', 1);

insert into t values (null, null, null, null, null);
insert into t values (0, 0, '0', '0', 0);
insert into t values (1, 1, '1', '1', 1);
insert into t values (1, 1, '1', '1', 1);
insert into t values (2, 2, '2', '2', 1);

insert into tt values (null, null, null, null, null);
insert into tt values (0, 0, '0', '0', 0);
insert into tt values (1, 1, '1', '1', 1);
insert into tt values (1, 1, '1', '1', 1);
insert into tt values (2, 2, '2', '2', 1);

insert into ttt values (null, null, null, null);
insert into ttt values (11, 11, '11', '11');
insert into ttt values (11, 11, '11', '11');
insert into ttt values (22, 22, '22', '22');

commit;

-- exists 
-- non-correlated

-- negative tests
-- "mis"qualified all
select * from s where exists (select tt.* from t);
select * from s where exists (select t.* from t tt);
-- too many columns in select list
select * from s where exists (select i, s from t);
-- invalid column reference in select list
select * from s where exists (select nosuchcolumn from t);
-- multiple matches at subquery level
select * from s where exists (select i from s, t);
-- ? parameter in select list of exists subquery
select * from s where exists (select ? from s);

-- positive tests

-- qualified *
select * from s where exists (select s.* from t);
select * from s t where exists (select t.* from t);
select * from s u where exists (select u.* from t);

-- column reference in select list
select * from s where exists (select i from t);
select * from s where exists (select t.i from t);

-- subquery returns empty result set
select * from s where exists (select * from t where i = -1);

-- test semantics of AnyResultSet
select * from s where exists (select t.* from t);
select * from s where exists (select 0 from t);

-- subquery in derived table
select * from 
(select * from s where exists (select * from t) and i = 0) a;

-- exists under an OR
select * from s where 0=1 or exists (select * from t);
select * from s where 1=1 or exists (select * from t where 0=1);
select * from s where exists (select * from t where 0=1) or
					  exists (select * from t);
select * from s where exists 
		(select * from t where exists (select * from t where 0=1) or
							   exists (select * from t));

-- (exists empty set) is null
select * from s where (exists (select * from t where 0=1)) is null;
-- not exists
select * from s where not exists (select * from t);
select * from s where not exists (select * from t where i = -1);

-- expression subqueries
-- non-correlated

-- negative tests
-- all node
select * from s where i = (select * from t);
-- too many columns in select list
select * from s where i = (select i, s from t);
-- no conversions
select * from s where i = (select 1 from t);
select * from s where i = (select b from t);
-- ? parameter in select list of expression subquery
select * from s where i = (select ? from t);

-- do consistency check on scans, etc.
values ConsistencyChecker();
-- cardinality violation
select * from s where i = (select i from t);

-- do consistency check on scans, etc.
values ConsistencyChecker();
select * from s where s = (select s from t where s = 1);

-- do consistency check on scans, etc.
values ConsistencyChecker();
update s set b = (select max(b) from t)
where vc <> (select vc from t where vc = '1');

-- do consistency check on scans, etc.
values ConsistencyChecker();
delete from s where c = (select c from t where c = '1');

-- do consistency check on scans, etc.
values ConsistencyChecker();

-- positive tests
select * from s;
select * from t;

-- simple subquery for each data type
select * from s where i = (select i from t where i = 0);
select * from s where s = (select s from t where s = 0);
select * from s where c = (select c from t where c = '0');
select * from s where vc = (select vc from t where vc = '0');
select * from s where b = (select max(b) from t where b = 0);
select * from s where b = (select max(b) from t where i = 2);

-- ? parameter on left hand side of expression subquery
prepare subq1 as 'select * from s where ? = (select i from t where i = 0)';
execute subq1 using 'values (0)';
remove subq1;

-- conversions
select * from s where i = (select s from t where s = 0);
select * from s where s = (select i from t where i = 0);
select * from s where c = (select vc from t where vc = '0');
select * from s where vc = (select c from t where c = '0');

-- (select nullable_column ...) is null
-- On of each data type to test clone()
select * from s where (select s from s where i is null) is null;
select * from s where (select i from s where i is null) is null;
select * from s where (select c from s where i is null) is null;
select * from s where (select vc from s where i is null) is null;
select * from s where (select b from s where i is null) is null;
select * from s where
	(select 1 from t where exists (select * from t where 1 = 0) and s = -1) is null;

-- subquery = subquery
select * from s where
(select i from t where i = 0) = (select s from t where s = 0);

-- multiple subqueries at the same level
select * from s
where i = (select s from t where s = 0) and
	  s = (select i from t where i = 2);
select * from s
where i = (select s from t where s = 0) and
	  s = (select i from t where i = 0);

-- nested subqueries
select * from s
where i = (select i from t where s = (select i from t where s = 2));
select * from s
where i = (select i - 1 from t where s = (select i from t where s = 2));

-- expression subqueries in select list
select (select i from t where 0=1) from s;
select (select i from t where i = 2) * (select s from t where i = 2) from s
where i > (select i from t where i = 0) - (select i from t where i = 0);

-- in subqueries

-- negative tests
-- select * subquery
select * from s where s in (select * from s);
-- incompatable types
select * from s where s in (select b from t);

-- positive tests

-- constants on left side of subquery
select * from s where 1 in (select s from t);
select * from s where -1 in (select i from t);
select * from s where '1' in (select vc from t);
select * from s where 0 in (select b from t);

-- constants in subquery select list
select * from s where i in (select 1 from t);
select * from s where i in (select -1 from t);
select * from s where c in (select '1' from t);
select * from s where b in (select 0 from t);

-- constants on both sides
select * from s where 1=1 in (select 0 from t);
select * from s where 0 in (select 0 from t);

-- compatable types
select * from s where c in (select vc from t);
select * from s where vc in (select c from t);
select * from s where i in (select s from t);
select * from s where s in (select i from t);

-- empty subquery result set
select * from s where i in (select i from t where 1 = 0);
select * from s where (i in (select i from t where i = 0)) is null;

-- select list
select ( i in (select i from t) ) a from s order by a;
select ( i in (select i from t where 1 = 0) ) a from s order by a;
select ( (i in (select i from t where 1 = 0)) is null ) a from s order by a;

-- subquery under an or
select i from s where i = -1 or i in (select i from t);
select i from s where i = 0 or i in (select i from t where i = -1);
select i from s where i = -1 or i in (select i from t where i = -1 or i = 1);

-- distinct elimination
select i from s where i in (select i from s);
select i from s where i in (select distinct i from s);
select i from s ss where i in (select i from s where s.i = ss.i);
select i from s ss where i in (select distinct i from s where s.i = ss.i);

-- do consistency check on scans, etc.
values ConsistencyChecker();

-- correlated subqueries

-- negative tests

-- multiple matches at parent level
select * from s, t where exists (select i from tt);
-- match is against base table, but not derived column list
select * from s ss (c1, c2, c3, c4, c5) where exists (select i from tt);
select * from s ss (c1, c2, c3, c4, c5) where exists (select ss.i from tt);
-- correlation name exists at both levels, but only column match is at
-- parent level
select * from s where exists (select s.i from tt s);
-- only match is at peer level
select * from s where exists (select * from tt) and exists (select ii from t);
select * from s where exists (select * from tt) and exists (select tt.ii from t);
-- correlated column in a derived table
select * from s, (select * from tt where i = ii) a;
select * from s, (select * from tt where s.i = ii) a;

-- positive tests

-- simple correlated subqueries
select (select i from tt where ii = i and ii <> 1) from s;
select (select s.i from tt where ii = s.i and ii <> 1) from s;
select (select s.i from ttt where iii = i) from s;
select * from s where exists (select * from tt where i = ii and ii <> 1);
select * from s where exists (select * from tt where s.i = ii and ii <> 1);
select * from s where exists (select * from ttt where i = iii);

-- 1 case where we get a cardinality violation after a few rows
select (select i from tt where ii = i) from s;

-- skip levels to find match
select * from s where exists (select * from ttt where iii = 
								(select 11 from tt where ii = i and ii <> 1)); 

-- join in subquery
select * from s where i in (select i from t, tt where s.i <> i and i = ii);
select * from s where i in (select i from t, ttt where s.i < iii and s.i = t.i);
-- join in outer query block
select s.i, t.i from s, t where exists (select * from ttt where iii = 1);
select s.i, t.i from s, t where exists (select * from ttt where iii = 11);
-- joins in both query blocks
select s.i, t.i from s, t where t.i = (select iii from ttt, tt where iii = t.i);
select s.i, t.i from s, t 
where t.i = (select ii from ttt, tt where s.i = t.i and t.i = tt.ii and iii = 22 and ii <> 1);

-- Beetle 5382 proper caching of subqueries in prepared statements
prepare pstmt as 'select s.i from s where s.i in (select s.i from s, t where s.i = t.i and t.s = ?)';
execute pstmt using 'values(0)';
execute pstmt using 'values(1)';
remove pstmt;
commit;
prepare pstmt2 as 'select s.i from s where s.i in (select s.i from s, t where s.i = t.i and t.s = 3)';
execute pstmt2;
insert into t(i,s) values(1,3);
execute pstmt2;
remove pstmt2;
rollback;

-- correlated subquery in select list of a derived table
select * from 
(select (select iii from ttt where sss > i and sss = iii and iii <> 11) from s) a;

-- bigint and subqueries
create table li(i int, s smallint, l bigint);
insert into li values (null, null, null);
insert into li values (1, 1, 1);
insert into li values (2, 2, 2);

select l from li o where l = (select i from li i where o.l = i.i);
select l from li o where l = (select s from li i where o.l = i.s);
select l from li o where l = (select l from li i where o.l = i.l);
select l from li where l in (select i from li);
select l from li where l in (select s from li);
select l from li where l in (select l from li);

----------------------------------
-- Some extra tests for subquery flattening
-- on table expressions (remapColumnReferencesToExpressions()

-- binary list node
select i in (1,2) from (select i from s) as tmp(i);

-- conditional expression
select i = 1 ? 1 : i from (select i from s) as tmp(i);

-- more tests for correlated column resolution
select * from s where i = (values i);
select t.* from s, t where t.i = (values s.i);
select * from s where i in (values i);
select t.* from s, t where t.i in (values s.i);

-- tests for not needing to do cardinality check
select * from s where i = (select min(i) from s where i is not null);
select * from s where i = (select min(i) from s group by i);

-- tests for distinct expression subquery
create table dist1 (c1 int);
create table dist2 (c1 int);
insert into dist1 values null, 1, 2;
insert into dist2 values null, null;
-- no match, no violation
select * from dist1 where c1 = (select distinct c1 from dist2);
-- violation
insert into dist2 values 1;
select * from dist1 where c1 = (select distinct c1 from dist2);
-- match, no violation
update dist2 set c1 = 2;
select * from dist1 where c1 = (select distinct c1 from dist2);
drop table dist1;
drop table dist2;


----------------------------------
-- update
create table u (i int, s smallint, c char(30), vc char(30), b bigint);
insert into u select * from s;
select * from u;

update u set b = exists (select b from t)
where vc <> (select vc from s where vc = '1');
select * from u;

delete from u;
insert into u select * from s;

-- delete
delete from u where c < (select c from t where c = '2');
select * from u;

-- restore u
delete from u;
insert into u select * from s;

-- check clean up when errors occur in subqueries

-- insert
insert into u select * from s s_outer
where i = (select s_inner.i/(s_inner.i-1) from s s_inner where s_outer.i = s_inner.i);
select * from u;

-- delete
delete from u 
where i = (select i/(i-1) from s where u.i = s.i);
select * from u;

-- update
update u  set i = (select i from s where u.i = s.i)
where i = (select i/(i-1) from s where u.i = s.i);
update u  set i = (select i/i-1 from s where u.i = s.i)
where i = (select i from s where u.i = s.i);
select * from u;

-- error in nested subquery
select (select (select (select i from s) from s) from s) from s;

-- do consistency check on scans, etc.
values ConsistencyChecker();


-- reset autocommit
autocommit on;

-- subquery with groupby and having clause
select distinct vc, i from t as myt1
      where s <= (select max(myt1.s) from t as myt2
          where myt1.vc = myt2.vc and myt1.s <= myt2.s
          group by s
          having count(distinct s) <= 3); 

-- subquery with having clause but no groupby
select distinct vc, i from t as myt1
      where s <= (select max(myt1.s) from t as myt2
          where myt1.vc = myt2.vc and myt1.s <= myt2.s
          having count(distinct s) <= 3); 

-- drop the tables
drop table li;
drop table s;
drop table t;
drop table tt;
drop table ttt;
drop table u;

-- DERBY-1007: Optimizer for subqueries can return incorrect cost estimates
-- leading to sub-optimal join orders for the outer query.  Before the patch
-- for that isssue, the following query plan will show T3 first and then T1--
-- but that's determined by the optimizer to be the "bad" join order.  After
-- the fix, the join order will show T1 first, then T3, which is correct
-- (based on the optimizer's estimates).

create table t1 (i int, j int);
insert into T1 values (1,1), (2,2), (3,3), (4,4), (5,5);
create table t3 (a int, b int);
insert into T3 values (1,1), (2,2), (3,3), (4,4);
insert into t3 values (6, 24), (7, 28), (8, 32), (9, 36), (10, 40);

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 20000;

select x1.j, x2.b from
  (select distinct i,j from t1) x1,
  (select distinct a,b from t3) x2
where x1.i = x2.a
order by x1.j, x2.b;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- clean up.
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);
drop table t1;
drop table t3;

-- DERBY-781: Materialize subqueries where possible to avoid creating
-- invariant result sets many times.  This test case executes a query
-- that has subqueries twice: the first time the tables have only a
-- few rows in them; the second time they have hundreds of rows in
-- them.

create table t1 (i int, j int);
create table t2 (i int, j int);

insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

create table t3 (a int, b int);
create table t4 (a int, b int);

insert into t3 values (2, 2), (4, 4), (5, 5);
insert into t4 values (2, 2), (4, 4), (5, 5);

-- Use of the term "DISTINCT" makes it so that we don't flatten
-- the subqueries.
create view V1 as select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i;
create view V2 as select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a;

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 20000;

-- Run the test query the first time, with only a small number
-- of rows in each table. Before the patch for DERBY-781
-- the optimizer would have chosen a nested loop join, which 
-- means that we would generate the result set for the inner
-- view multiple times.  After DERBY-781 the optimizer will
-- choose to do a hash join and thereby materialize the inner
-- result set, thus improving performance.  Should see a
-- Hash join as the top-level join with a HashTableResult as
-- the right child of the outermost join. 
select * from V1, V2 where V1.j = V2.b and V1.i in (1,2,3,4,5);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Now add more data to the tables.

insert into t1 select * from t2;
insert into t2 select * from t1;
insert into t2 select * from t1;
insert into t1 select * from t2;
insert into t2 select * from t1;
insert into t1 select * from t2;
insert into t2 select * from t1;
insert into t1 select * from t2;
insert into t2 select * from t1;
insert into t1 select * from t2;

insert into t3 select * from t4;
insert into t4 select * from t3;
insert into t3 select * from t4;
insert into t4 select * from t3;
insert into t3 select * from t4;
insert into t4 select * from t3;
insert into t3 select * from t4;
insert into t4 select * from t3;
insert into t3 select * from t4;
insert into t4 select * from t3;
insert into t3 select * from t4;

-- Drop the views and recreate them with slightly different
-- names.  The reason we use different names is to ensure that
-- the query will be "different" from the last time and thus we'll
-- we'll go through optimization again (instead of just using
-- the cached plan from last time).
drop view v1;
drop view v2;

-- Use of the term "DISTINCT" makes it so that we don't flatten
-- the subqueries.
create view VV1 as select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i;
create view VV2 as select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a;

-- Now execute the query again using the larger tables.
select * from VV1, VV2 where VV1.j = VV2.b and VV1.i in (1,2,3,4,5);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- clean up.
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);
drop view vv1;
drop view vv2;
drop table t1;
drop table t2;
drop table t3;
drop table t4;

-- DERBY-1574: Subquery in COALESCE gives NPE due
-- to preprocess not implemented for that node type
create table t1 (id int);
create table t2 (i integer primary key, j int);

insert into t1 values 1,2,3,4,5;
insert into t2 values (1,1),(2,4),(3,9),(4,16);
 
update t1 set id = coalesce((select j from t2 where t2.i=t1.id), 0);
select * from t1;

drop table t1;
drop table t2;

-- DERBY-2218
create table t1 (i int);
-- ok
select * from t1 where i in (1, 2, (values cast(null as integer)));
-- expect error, this used to throw NPE
select * from t1 where i in (1, 2, (values null));
select * from t1 where i in (select i from t1 where i in (1, 2, (values null)));
-- expect error
select * from t1 where exists (values null);
select * from t1 where exists (select * from t1 where exists(values null));
select i from t1 where exists (select i from t1 where exists(values null));
select * from (values null) as t2;
select * from t1 where exists (select 1 from (values null) as t2);
select * from t1 where exists (select * from (values null) as t2);
drop table t1;
