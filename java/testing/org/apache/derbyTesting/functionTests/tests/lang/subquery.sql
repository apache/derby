--
-- subquery tests
--
autocommit off;
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ConsistencyChecker.runConsistencyChecker'
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
select i in (select i from t) from s;
select i in (select i from t where 1 = 0) from s;
select (i in (select i from t where 1 = 0)) is null from s;

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

-- drop the tables
drop table li;
drop table s;
drop table t;
drop table tt;
drop table ttt;
drop table u;

