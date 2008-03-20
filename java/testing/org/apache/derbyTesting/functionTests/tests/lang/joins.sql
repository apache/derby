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
-- this test shows the current supported join functionality
--

-- create some tables
create table t1 (t1_c1 int, t1_c2 char(10));
create table t2 (t2_c1 int, t2_c2 char(10));
create table t3 (t3_c1 int, t3_c2 char(10));
create table t4 (t4_c1 int, t4_c2 char(10));

-- populate the tables
insert into t1 values (1, 't1-row1');
insert into t1 values (2, 't1-row2');
insert into t2 values (1, 't2-row1');
insert into t2 values (2, 't2-row2');
insert into t3 values (1, 't3-row1');
insert into t3 values (2, 't3-row2');
insert into t4 values (1, 't4-row1');
insert into t4 values (2, 't4-row2');

-- negative test, same exposed name
select * from t1, t1;

-- cartesian products
-- full projection
select * from t1, t2;
select * from t1 a, t2 b, t3 cc, t4 d order by 1,2,3,4,5,6;
-- reorder columns
select t2.*, t1.* from t1, t2;
select t2_c2, t1_c2, t1_c1, t2_c1 from t1, t2;

-- project out columns
select t2_c2, t1_c1 from t1, t2;
select a.t1_c1, cc.t1_c1, e.t1_c1, g.t1_c1, i.t1_c1 from t1 a, t1 cc, t1 e, t1 g, t1 i;

-- project/restricts
select a.t1_c1, b.t1_c1, cc.t1_c1, d.t1_c1, e.t1_c1, f.t1_c1, g.t1_c1, h.t1_c1, i.t1_c1, j.t1_c1
from t1 a, t1 b, t1 cc, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j
where a.t1_c2 = b.t1_c2 and b.t1_c2 = cc.t1_c2 and cc.t1_c2 = d.t1_c2 and
      d.t1_c2 = e.t1_c2 and e.t1_c2 = f.t1_c2 and f.t1_c2 = g.t1_c2 and
      g.t1_c2 = h.t1_c2 and h.t1_c2 = i.t1_c2 and i.t1_c2 = j.t1_c2;

select a.t1_c1, b.t1_c1, cc.t1_c1, d.t1_c1, e.t1_c1, f.t1_c1, g.t1_c1, h.t1_c1, i.t1_c1, j.t1_c1
from t1 a, t1 b, t1 cc, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j
where a.t1_c1 = 1 and b.t1_c1 = 1 and cc.t1_c1 = 1 and d.t1_c1 = 1 and e.t1_c1 = 1 and
	  f.t1_c1 = 1 and g.t1_c1 = 1 and h.t1_c1 = 1 and i.t1_c1 = 1 and
	  a.t1_c2 = b.t1_c2 and b.t1_c2 = cc.t1_c2 and cc.t1_c2 = d.t1_c2 and
      d.t1_c2 = e.t1_c2 and e.t1_c2 = f.t1_c2 and f.t1_c2 = g.t1_c2 and
      g.t1_c2 = h.t1_c2 and h.t1_c2 = i.t1_c2 and i.t1_c2 = j.t1_c2;


-- project out entire tables
select 1, 2 from t1, t2;
select 1, t1.t1_c1 from t1, t2;
select t2.t2_c2,1 from t1, t2;

-- bug #306
select c.t1_c1 from (select a.t1_c1 from t1 a, t1 b) c, t1 d where c.t1_c1 = d.t1_c1;

-- create a table for testing inserts
create table instab (instab_c1 int, instab_c2 char(10), instab_c3 int,
		     instab_c4 char(10));

-- insert select with joins
-- cartesian product
insert into instab select * from t1, t2;
select * from instab;
delete from instab;

insert into instab (instab_c1, instab_c2, instab_c3, instab_c4)
	select * from t1, t2;
select * from instab;
delete from instab;

insert into instab (instab_c1, instab_c2, instab_c3, instab_c4)
	select t2_c1, t2_c2, t1_c1, t1_c2 from t1, t2;
select * from instab;
delete from instab;

insert into instab (instab_c3, instab_c1, instab_c2, instab_c4)
	select t2_c1, t1_c1, t1_c2, t2_c2 from t1, t2;
select * from instab;
delete from instab;

-- projection
insert into instab (instab_c1, instab_c3)
	select t1_c1, t2_c1 from t1, t2;
select * from instab;
delete from instab;

-- project out 1 or more tables from join
insert into instab select 1, '2', 3, '4' from t1, t2;
select * from instab;
delete from instab;

insert into instab select 1, t1.t1_c2, 3, t1.t1_c2 from t1, t2;
select * from instab;
delete from instab;

insert into instab select t2.t2_c1, '2', t2.t2_c1, '4' from t1, t2;
select * from instab;
delete from instab;

------------------------------------------
-- test optimizations where we push around
-- predicates (remapColumnReferences)
------------------------------------------
-- case
select t1_c1 from t1, t2 where (case when t1_c1 = 1 then t2_c2 end) = t2_c2;

-- CHAR built-in function
select t1_c1 from t1, t2 where CHAR(t1_c1) = t2_c2;

-- logical operator OR
select t1_c1 from t1, t2 where t1_c1 = 1 or t2_c1 = 2;

-- logical operator AND
select t1_c1 from t1, t2 where t1_c1 = 2147483647 and 2147483647 = t2_c1;

-- beetle 5421
-- INT built-in function
select t1_c1 from t1, t2 where INT(t1_c1) = t2_c1;
select t1_c1 from t1, t2 where t1_c1 = INT(2147483647) and INT(2147483647) = t2_c1;

-- transitive closure - verify join condition doesn't get dropped
create table x(c1 int);
create table y(c1 int);
insert into x values 1, 2, null;
insert into y values 1, 2, null;

select * from x,y where x.c1 = y.c1 and x.c1 = 1 and y.c1 = 2;
select * from x,y where x.c1 = y.c1 and x.c1 is null;
select * from x,y where x.c1 = y.c1 and x.c1 is null and y.c1 = 2;
select * from x,y where x.c1 = y.c1 and x.c1 is null and y.c1 is null;

-- DERBY-2526: join node flattening leads to incorrect transitive closure,
-- which in turn results in incorrect results.

-- Ex. 1: As posted to DERBY-2526:

create table b2 (c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int);
create table b4 (c7 int, c4 int, c6 int);
create table b3 (c8 int, c9 int, c5 int, c6 int);
create table b (c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int);

create view bvw (c5, c1 ,c2 ,c3 ,c4) as
          select c5, c1 ,c2 ,c3 ,c4 from b2 union
          select c5, c1 ,c2 ,c3 ,c4 from b;

create view bvw2 (c1 ,c2 ,c3 ,c4 ,c5) as
           select c1 ,c2 ,c3 ,c4 ,c5 from b2 union
           select c1 ,c2 ,c3 ,c4 ,c5 from b;

insert into b4 (c7,c4,c6) values (4, 42, 31);
insert into b2 (c5,c1,c3,c4,c6) values (3,4, 'F',43,23);
insert into b3 (c5,c8,c9,c6) values (2,3,19,28);

-- Should see 1 row for *both* of these queries.
select b3.* from b3 join bvw on (b3.c8 = bvw.c5) join b4 on (bvw.c1 = b4.c7) where b4.c4 = 42;
select b3.* from b3 join bvw2 on (b3.c8 = bvw2.c5) join b4 on (bvw2.c1 = b4.c7) where b4.c4 = 42;

-- Cleanup.
drop view bvw;
drop view bvw2;
drop table b;
drop table b2;
drop table b3;
drop table b4;

-- Ex. 2: Simplified repro.

  create table b1 (c0 int);
  create table xx (c1 int, c2 int);
  create table b2 (c3 int, c4 int);

  insert into b1 values 1;
  insert into xx values (0, 1);
  insert into b2 values (0, 2);

-- Following should return 1 row.
select b1.* from
    b1 JOIN (select * from xx) VW(c1,c2) on (b1.c0 = vw.c2)
       JOIN b2 on (vw.c1 = b2.c3);

-- Try out various correlation name combinations to make sure that
-- correct column remapping occurs regardless of correlation name.

select b1.* from
    b1 JOIN (select * from xx) VW(ccx1,ccx2) on (b1.c0 = vw.ccx2)
       JOIN b2 on (vw.ccx1 = b2.c3);

select b1.* from
    b1 JOIN (select c1 as ccx1, c2 as ccx2 from xx) VW(ccx1,ccx2) on (b1.c0 = vw.ccx2)
       JOIN b2 on (vw.ccx1 = b2.c3);

select b1.* from
    b1 JOIN (select c1 as ccx1, c2 as ccx2 from xx) VW(x1,x2) on (b1.c0 = vw.x2)
       JOIN b2 on (vw.x1 = b2.c3);

select b1.* from
    b1 JOIN (select c1 as ccx1, c2 as ccx2 from xx) VW(c1,c2) on (b1.c0 = vw.c2)
       JOIN b2 on (vw.c1 = b2.c3);

-- Cleanup.
drop table b1;
drop table b2;
drop table xx;

-- DERBY-3023: join node flattening leads to incorrect search transitive
-- closure, which in turn leads to incorrect results.

CREATE TABLE d3023_t1 (A INTEGER, B INTEGER);
insert into d3023_t1 values (1, 1), (-2, 2), (3, 3);

CREATE TABLE d3023_t2 (C INTEGER, D INTEGER);
insert into d3023_t2 values (1, -1), (2, -2), (3, -3);

CREATE TABLE d3023_t3 (I INTEGER, J INTEGER);
insert into d3023_t3 values (-2, 1), (-3, -2);

CREATE TABLE d3023_t4 (X INTEGER, Y INTEGER);
insert into d3023_t4 values (1, 1), (2, 2), (3, 3);

-- Incremental queries building up to the query in question...

select distinct * from
  d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d;

select distinct * from
  d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d
 where d3023_t1.a = -2;

select distinct * from
  d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d
  inner join d3023_t3 on d3023_t1.a = d3023_t3.j;

select distinct * from
  d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d
  inner join d3023_t3 on d3023_t1.a = d3023_t3.j
 where d3023_t1.a = -2;

-- This query only returns a single row, even without the
-- explicit search predicate.
select distinct * from
  d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d
  inner join d3023_t3 on d3023_t1.a = d3023_t3.j
  inner join d3023_t4 on d3023_t2.c = d3023_t4.x;

-- Slight variation of the same query.  Add a search predicate
-- enforcing "d3023_t1.a = -2" to the join condition.  Since the
-- row we saw in the previous query satisifies that predicate,
-- we should see the same row again.
select distinct * from
  d3023_t1 left outer join d3023_t2
    on d3023_t1.a = d3023_t2.d AND d3023_t1.a = -2
  inner join d3023_t3 on d3023_t1.a = d3023_t3.j
  inner join d3023_t4 on d3023_t2.c = d3023_t4.x;

-- Same query as above, but with the predicate "d3023_t1.a = -2"
-- sitting at the top-most (outer) SELECT.  That makes the predicate
-- available for inclusion in the "search transitive closure" logic
-- for the outer SELECT. That said, prior to the fix for DERBY-3023,
-- search transitive closure was incorrectly adding a new predicate,
-- d3023_t4.x = -2, to the query.  This was because two different
-- column references were incorrectly mapped to the same column
-- position w.r.t. the outer join: i.e. "d3023_t1.a" in the search
-- predicate "d3023_t1.a = -2" AND "d3023_t2.c" in the join predicate
-- "d3023_t2.c = d3023_t4.x" were BOTH referencing the first column
-- in the HalfOuterJoinNode.  As a result, the search transitive
-- closure logic thought that there was transitive equality between
-- the two predicates, which was incorrect.  That in turn caused the
-- query to return incorrect results (no rows).  With the fix for
-- DERBY-3023, this query should now return a single row.

select distinct * from
  d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d
  inner join d3023_t3 on d3023_t1.a = d3023_t3.j
  inner join d3023_t4 on d3023_t2.c = d3023_t4.x
 where d3023_t1.a = -2;

-- Cleanup.
drop table d3023_t1;
drop table d3023_t2;
drop table d3023_t3;
drop table d3023_t4;

-- Beetle task 5000. Bug found by Websphere. Should not return any rows.
select t1_c1, t1_c2, t2_c1, t2_c2
  from t1, t2
  where t1_c1 = t2_c1
    and t1_c1 = 1
    and t2_c1 <> 1;

-- Beetle task 4736
create table a (a1 int not null primary key, a2 int, a3 int, a4 int, a5 int, a6 int);
create table b (b1 int not null primary key, b2 int, b3 int, b4 int, b5 int, b6 int);
create table c (c1 int not null, c2 int, c3 int not null, c4 int, c5 int, c6 int);
create table d (d1 int not null, d2 int, d3 int not null, d4 int, d5 int, d6 int);

alter table c add primary key (c1,c3);
alter table d add primary key (d1,d3);

insert into a values (1,1,3,6,NULL,2),(2,3,2,4,2,2),(3,4,2,NULL,NULL,NULL),
                     (4,NULL,4,2,5,2),(5,2,3,5,7,4),(7,1,4,2,3,4),
                     (8,8,8,8,8,8),(6,7,3,2,3,4);

insert into b values (6,7,2,3,NULL,1),(4,5,9,6,3,2),(1,4,2,NULL,NULL,NULL),
                     (5,NULL,2,2,5,2),(3,2,3,3,1,4),(7,3,3,3,3,3),(9,3,3,3,3,3);

insert into c values (3,7,7,3,NULL,1),(8,3,9,1,3,2),(1,4,1,NULL,NULL,NULL),
                     (3,NULL,1,2,4,2),(2,2,5,3,2,4),(1,7,2,3,1,1),(3,8,4,2,4,6);

insert into d values (1,7,2,3,NULL,3),(2,3,9,1,1,2),(2,2,2,NULL,3,2),
                     (1,NULL,3,2,2,1),(2,2,5,3,2,3),(2,5,6,3,7,2);

select a1,b1,c1,c3,d1,d3 
  from D join (A left outer join (B join C on b2=c2) on a1=b1) 
    on d3=b3 and d1=a2;

select a1,b1,c1,c3,d1,d3 
  from D join ((B join C on b2=c2) right outer join A on a1=b1) 
    on d3=b3 and d1=a2;

-- JIRA 1089: demonstrate that a table with an identity column generated
-- always can be used as the target of an insert-as-select join:
create table j1089_source (source_id int);
insert into j1089_source values (0);
create table j1089_dest (
    dest_id int not null primary key generated always as identity,
    source_id_1 int not null,
    source_id_2 int not null);

insert into j1089_dest (source_id_1, source_id_2)
    select s1.source_id, s2.source_id
        from j1089_source as s1
            join j1089_source as s2 on 1 = 1;
select * from j1089_dest;


-- DERBY-3538 NullPointerException during execution for query with LEFT
-- OUTER JOIN whose inner table selects all constants.
create table t3538 (i int, j int);
insert into t3538 values (-1, -2), (-2, -4), (-3, -9);

select * from
t3538 left outer join
    (select -1 a, 1 b from t3538) x0 --DERBY-PROPERTIES joinStrategy=NESTEDLOOP
   on x0.a = t3538.i; 

-----------------------------------
-- clean up
----------------------------------
drop table a;
drop table b;
drop table c;
drop table d;
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table instab;
drop table x;
drop table y;
drop table j1089_source;
drop table j1089_dest;
drop table t3538;
