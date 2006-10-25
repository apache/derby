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
-- test the optimizer overrides
autocommit off;

-- change display width in anticipation of runtimestatistics
maximumdisplaywidth 5000;
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);

-- create the tables
create table t1 (c1 int, c2 int, c3 int, constraint cons1 primary key(c1, c2));
create table t2 (c1 int not null, c2 int not null, c3 int, constraint cons2 unique(c1, c2));

-- populate the tables
insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);
insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);

-- create some indexes
create index t1_c1c2c3 on t1(c1, c2, c3);
create index t1_c3c2c1 on t1(c3, c2, c1);
create index t1_c1 on t1(c1);
create index t1_c2 on t1(c2);
create index t1_c3 on t1(c3);
create index "t1_c2c1" on t1(c2, c1);
create index t2_c1c2c3 on t2(c1, c2, c3);
create index t2_c3c2c1 on t2(c3, c2, c1);
create index t2_c1 on t2(c1);
create index t2_c2 on t2(c2);
create index t2_c3 on t2(c3);

-- create some views
create view v1 as select * from t1 --derby-properties index = t1_c1
;
create view v2 as select t1.* from t1, t2;
create view v3 as select * from v1;
create view neg_v1 as select * from t1 --derby-properties asdf = fdsa
;

-- negative tests
select 
-- derby-properties index = t1_c1
* from t1;
select * -- derby-properties index = t1_c1
from t1;
select 
-- derby-properties
* from t1;
-- optimizer override did not specify propertyname=value pairs
select * from t1 --derby-properties
;

-- invalid property
select * from t1 --derby-properties asdf = i1
;
select * from t1 exposedname --derby-properties asdf = i1
;

-- non-existent index
select * from t1 --derby-properties index = t1_notexists
;
select * from t1 exposedname --derby-properties index = t1_notexists
;

-- non-existent constraint
select * from t1 --derby-properties constraint = t1_notexists
;
select * from t1 exposedname --derby-properties constraint = t1_notexists
;

-- make sure following get treated as comments
--d 
-- de
-- der
--derb
--derby comment
-- derby another comment
--derby-
--derby-p
--derby-pr
--derby-pro
--derby-prop
--derby-prope
--derby-proper
-- derby-propert
-- derby-properti
-- derby-propertie
-- derby-propertiex

-- both index and constraint
select * from t1 --derby-properties index = t1_c1, constraint = cons1
;
select * from t1 exposedname --derby-properties index = t1_c1, constraint = cons1
;

-- index which includes columns in for update of list
select * from t1 --derby-properties index = t1_c1 
for update;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 exposedname --derby-properties index = t1_c1 
for update;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 --derby-properties index = t1_c1 
for update of c2, c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 exposedname --derby-properties index = t1_c1 
for update of c2, c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 --derby-properties constraint = null
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- constraint which includes columns in for update of list
select * from t1 --derby-properties constraint = cons1 
for update;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 exposedname --derby-properties constraint = cons1 
for update;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 --derby-properties constraint = cons1 
for update of c2, c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from t1 exposedname --derby-properties constraint = cons1 
for update of c2, c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select from view with bad derby-properties list
select * from neg_v1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- bad derby-properties tests on outer joins
select * from t1 --derby-properties i = a 
left outer join t2 on 1=1;
select * from t1 left outer join t2 --derby-properties i = t1_c1 
on 1=1;
select * from t1 left outer join t2 --derby-properties index = t1_c1 
on 1=1;
select * from t1 right outer join t2 --derby-properties index = t1_c1 
on 1=1;

-- invalid joinStrategy
select * from t1 a, t1 b --derby-properties joinStrategy = asdf
;

-- positive tests

-- verify that statements are dependent on specified index or constraint
commit;

-- dependent on index
prepare p1 as 'select * from t1 --derby-properties index = t1_c1
';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
execute p1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
drop index t1_c1;
execute p1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
remove p1;
rollback;

-- dependent on constraint
prepare p2 as 'select * from t1 --derby-properties constraint = cons1
';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
execute p2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
alter table t1 drop constraint cons1;
execute p2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
remove p2;
rollback;

-- the token derby-properties is case insensitive. Few tests for that
select * from t1 --DeRbY-pRoPeRtIeS index = t1_c1 
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- misspell derby-properties and make sure that it gets treated as a regular comment rather than optimizer override
select * from t1 --DeRbY-pRoPeRtIeAAA index = t1_c1 
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- force index, delimited identifier
select * from t1 --derby-properties index = "t1_c2c1"
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- force table scan
select * from t1 --derby-properties index = null
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- force index in create view
select * from v1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- cursor updateability test
select * from t1 --derby-properties index = t1_c1 
for update of c2, c3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- joins
select 1 from t1 a --derby-properties index = t1_c1
, t2 b --derby-properties index = t2_c2
;values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select 1 from --derby-PROPERTIES joinOrder=fixed
t1, t2 where t1.c1 = t2.c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- comparisons that can't get pushed down
select * from t1 --derby-properties index = t1_c1 
where c1 = c1;
select * from t1 --derby-properties index = t1_c1 
where c1 = c2;
select * from t1 --derby-properties index = t1_c1 
where c1 + 1 = 1 + c1;

-- outer joins
select * from t1 --derby-properties index = t1_c1 
left outer join t2 --derby-properties index = t2_c2 
on t1.c1 = t2.c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify nestedloop joinStrategy
select * from t1 a, t1 b --derby-properties joinStrategy = nestedloop
where a.c1 = b.c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

--negative test. insertModeValue is not avaible to a user and hence will
--give a syntax error. There are some undocumented properties which are
--allowed within Derby engine only and insertModeValue is one of them.
create table temp1 (c1 int, c2 int, c3 int, constraint temp1cons1 primary key(c1, c2));
insert into temp1 (c1,c2,c3) -- derby-properties insertModeValue=replace
select * from t1;

-- clean up
drop view neg_v1;
drop view v3;
drop view v2;
drop view v1;
drop table t2;
drop table t1;
drop table temp1;
commit;

