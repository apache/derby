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
-- test elimination of sort for order by
set isolation to rr;

-- test combining of sorts for distinct and order by

-- create some tables
create table t1(c1 int, c2 int, c3 int, c4 int);

insert into t1 values (1, 2, 3, 4);
insert into t1 values (2, 3, 4, 5);
insert into t1 values (-1, -2, -3, -4);
insert into t1 values (-2, -3, -4, -5);
insert into t1 values (1, 2, 4, 3);
insert into t1 values (1, 3, 2, 4);
insert into t1 values (1, 3, 4, 2);
insert into t1 values (1, 4, 2, 3);
insert into t1 values (1, 4, 3, 2);
insert into t1 values (2, 1, 4, 3);

maximumdisplaywidth 7000;
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);

-- no index on t1
-- full match
select distinct c1, c2, c3, c4 from t1 order by 1, 2, 3, 4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c1, c2, c3, c4 from t1 order by c1, c2, c3, c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- in order prefix
select distinct c3, c4 from t1 order by 1, 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c4 from t1 order by c3, c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- no prefix
select distinct c3, c4 from t1 order by 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c4 from t1 order by c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- expression
select distinct c3, 1 from t1 order by 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, 1 from t1 order by 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify that a sort is still done when a unique index 
-- exists
create unique index i1 on t1(c1, c2, c3, c4);
select distinct c4, c3 from t1 where c1 = 1 and c2 = 2 order by c4, c3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c4 from t1 where c1 = 1 and c2 = 2 order by c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- order by and union
select c1 from t1 union select c2 from t1 order by 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select c1 from t1 union select c2 as c1 from t1 order by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- RESOLVE: next 2 will do 2 sorts (bug 58)
select c3, c4 from t1 union select c2, c1 from t1 order by 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select c3, c4 from t1 union select c2, c1 as c4 from t1 order by c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- DERBY-2887: investigate affect of NULLS FIRST/LAST on sorting

insert into t1 values (1, null, 14, null);

-- should NOT do a sort:
select c1,c2,c3 from t1 where c1 = 1 order by c1,c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- Needs to do a sort to get the NULLS FIRST:
select c1,c2,c3 from t1 where c1 = 1 order by c1,c2 nulls first;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- test recognition of single row tables
-- even when scanning heap
create table u1(c1 int, c2 int);
create table u2(c2 int, c3 int);
create table u3(c3 int, c4 int);
insert into u1 values (1, 1), (2, 2);
insert into u2 values (1, 1), (2, 2);
insert into u3 values (1, 1), (2, 2);
create unique index u1_i1 on u1(c1);
create unique index u2_i1 on u2(c2);
create unique index u3_i1 on u3(c3);

select * from
u1,
u2,
u3
where u1.c1 = 1 and u1.c1 = u2.c2
order by u3.c3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- clean up
drop table t1;
drop table u1;
drop table u2;
drop table u3;

-- DERBY-3997: Elimination of ORDER BY clause because all the columns
-- to order by were known to be constant, made extra columns appear in
-- the result.
create table d3997(x int, y int, z int);
-- These queries used to have two result columns, but should only have one
select 1 from d3997 where x=1 order by x;
select y from d3997 where x=1 order by x;
-- Used to have three columns, should only have two
select y,z from d3997 where x=1 order by x;
-- Used to have three columns, should only have one
select x from d3997 where y=1 and z=1 order by y,z;
-- Dynamic parameters are also constants (expect one column)
execute 'select x from d3997 where y=? order by y' using 'values 1';
-- Order by columns should not be removed from the result here
select * from d3997 where x=1 order by x;
select x,y,z from d3997 where x=1 order by x;
select x,y,z from d3997 where x=1 and y=1 order by x,y;
-- Order by should not be eliminated here (not constant values). Insert some
-- data in reverse order to verify that the results are sorted.
insert into d3997 values (9,8,7),(6,5,4),(3,2,1);
select * from d3997 where y<>2 order by y;
select z from d3997 where y>2 order by y;
drop table d3997;
