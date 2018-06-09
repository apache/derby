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
-- tests for synonym support
-- When we decide to convert this test to junit test, the converted tests can 
-- go in existing SynonymTest.java

set schema APP;
-- negative tests
-- Create a synonym to itself. Error.
create synonym syn for syn;
create synonym syn for APP.syn;
create synonym APP.syn for syn;
create synonym APP.syn for APP.syn;

-- Create a simple synonym loop. Error.
create synonym synonym1 for synonym;
create synonym synonym for synonym1;
drop synonym synonym1;

-- Create a larger synonym loop.
create synonym ts1 for ts;
create synonym ts2 for ts1;
create synonym ts3 for ts2;
create synonym ts4 for ts3;
create synonym ts5 for ts4;
create synonym ts6 for ts5;
create synonym ts for ts6;
drop synonym App.ts1;
drop synonym "APP".ts2;
drop synonym TS3;
drop synonym ts4;
drop synonym ts5;
drop synonym app.ts6;

-- Synonyms and table/view share same namespace. Negative tests for this.
create table table1 (i int, j int);
insert into table1 values (1,1), (2,2);
create view view1 as select i, j from table1;

create synonym table1 for t1;
create synonym APP.Table1 for t1;
create synonym app.TABLE1 for "APP"."T";

create synonym APP.VIEW1 for v1;
create synonym "APP"."VIEW1" for app.v;

-- Synonyms can't be created on temporary tables
declare global temporary table session.t1 (c1 int) not logged;
create synonym synForTemp for session.t1;
create synonym synForTemp for session."T1";

-- Synonyms can't be created in session schemas
create synonym session.table1 for APP.table1;

-- Creating a table or a view when a synonym of that name is present. Error.
create synonym myTable for table1;

create table myTable(i int, j int);

create view myTable as select * from table1;


-- Positive test cases

-- Using synonym in DML
select * from myTable;
select * from table1;
insert into myTable values (3,3), (4,4);

select * from mytable;

update myTable set i=3 where j=4;

select * from mytable;
select * from table1;

delete from myTable where i> 2;

select * from "APP"."MYTABLE";
select * from APP.table1;

-- Try some cursors
get cursor c1 as 'select * from myTable';

next c1;
next c1;

close c1;

-- Try updatable cursors

autocommit off;
get cursor c2 as 'select * from myTable for update';

next c2;
update myTable set i=5 where current of c2;
close c2;

autocommit on;

select * from table1;

-- Try updatable cursors, with synonym at the top, base table inside.
autocommit off;
get cursor c2 as 'select * from app.table1 for update';

next c2;
update myTable set i=6 where current of c2;
close c2;

autocommit on;

select * from table1;

-- trigger tests
create table table2 (i int, j int);

-- Should fail
create trigger tins after insert on myTable referencing new_table as new for each statement insert into table2 select i,j from table1;

-- Should pass
create trigger tins after insert on table1 referencing new_table as new for each statement insert into table2 select i,j from table1;

drop trigger tins;

create trigger triggerins after insert on table2 referencing new_table as new for each statement insert into myTable select i,j from new;

select * from myTable;
insert into table2 values (5, 5);
select * from myTable;

drop table table2;

-- Try referential constraints. Synonyms should not be allowed there.

create table primaryTab (i int not null primary key, j int, c char(10));

create synonym synPrimary for primaryTab;

-- Should fail
create table foreignTab(i int, j int CONSTRAINT SYNPK_F references synPrimary(i));

create table foreignTab(i int, j int references primaryTab(i));

drop table foreignTab;
drop table primaryTab;
drop synonym synPrimary;

-- Tests with non existant schemas
-- Implicitly creates junkSchema
create synonym junkSchema.syn1 for table2;
select * from junkSchema.syn1;
set schema junkSchema;
create table table2(c char(10));
select * from syn1;
set schema APP;

-- Should resolve to junkSchema.table2
select * from junkSchema.syn1;
drop table junkSchema.table2;

-- Should fail. Need to drop synonym first
drop schema junkSchema restrict;
drop synonym junkSchema.syn1;
drop schema junkSchema restrict;

-- Test with target schema missing
create synonym mySynonym for notPresent.t1;
select * from mySynonym;
create table notPresent.t1(j int, c char(10));
insert into notPresent.t1 values (100, 'satheesh');
-- Should resolve now
select * from mySynonym;

drop table notPresent.t1;
drop synonym mySynonym;

-- Positive test case with three levels of synonym chaining

create schema synonymSchema;

create synonym synonymSchema.mySynonym1 for APP.table1;
create synonym APP.mySynonym2 for "SYNONYMSCHEMA"."MYSYNONYM1";
create synonym mySynonym for mySynonym2;


select * from table1;
select * from mySynonym;

insert into mySynonym values (6,6);
insert into mySynonym select * from mySynonym where i<2;

select * from mySynonym;

update mySynonym set j=5;

update mySynonym set j=4 where i=5;

delete from mySynonym where j=6;

select * from mySynonym;
select * from table1;

-- cursor on mySynonym
get cursor c1 as 'select * from mySynonym';

next c1;
next c1;
next c1;

close c1;

-- More negative tests to check dependencies
select * from mySynonym;
drop synonym mySynonym;

-- Previously compiled cached statement should get invalidated
select * from mySynonym;

-- drop and recreate schema test
create schema testSchema;

create synonym multiSchema for testSchema.testtab;

select * from multiSchema;

create table testSchema.testtab(i int, c char(10));
insert into testSchema.testtab values (1, 'synonym');

select * from multiSchema;

drop table testSchema.testtab;
drop schema testSchema restrict;

create schema testSchema;

create table testSchema.testtab(j int, c1 char(10), c2 char(20));
insert into testSchema.testtab values (1, 'synonym', 'test');

select * from multiSchema;

drop synonym multiSchema;
drop table testSchema.testtab;

drop view view1;
drop table table1;

-- DERBY-1784
create schema test1;
create schema test2;
create table test1.t1 ( id bigint not null );
insert into test1.t1 values 1;
create synonym test2.t1 for test1.t1;
set schema test1;
select t1.id from t1;
set schema test2;
select id from t1;
select id from test2.t1;
select t1.id from t1;
select t1.id from test2.t1;
-- DERBY-1894 
-- ORDER BY clause with column qualifed by a synonym name where it is declared in 
-- a different schema than the underlying table.
select t1.id from t1 order by id;
select t1.id from t1 order by t1.id;
select t1.id as c1 from t1 order by c1;
select t1.id from t1 where t1.id > 0 order by t1.id;
select t1.id from t1 where t1.id > 0 group by t1.id;
select t1.id from t1 where t1.id > 0 group by t1.id having t1.id > 0 order by t1.id;
select test2.t1.id from t1;
select test2.t1.id from test2.t1;
select test2.t1.id from test2.t1 where t1.id > 0;
select test2.t1.id from test2.t1 where t1.id > 0 order by t1.id;
select test2.t1.id from test2.t1 order by id;
select test2.t1.id from test2.t1 order by t1.id;
select test2.t1.id from test2.t1 where t1.id > 0 order by test2.t1.id;
select test2.t1.id from test2.t1 where t1.id > 0 group by test2.t1.id;
select test2.t1.id from test2.t1 where t1.id > 0 group by test2.t1.id having test2.t1.id > 0 order by test2.t1.id;
select w1.id from t1 w1 order by id;
select w1.id from t1 w1 order by w1.id;
select t1.id as idcolumn1, t1.id as idcolumn2 from t1 order by idcolumn1, idcolumn2;
select t1.id as idcolumn1, t1.id as idcolumn2 from t1 order by t1.idcolumn1, t1.idcolumn2;
select t1.id from (select t1.id from t1) t1 order by t1.id;
select t1.id from (select t1.id from t1 a, t1 b where a.id=b.id) t1 order by t1.id;

create table t2 (id bigint not null, name varchar(20));
create synonym s1 for test2.t1;
create synonym s2 for test2.t2;
insert into s2 values (1, 'John');
insert into s2 values (2, 'Yip');
insert into s2 values (3, 'Jane');
select s1.id, s2.name from s1, s2 where s1.id=s2.id order by s1.id, s2.name;
select s2.name from s2 where s2.id in (select s1.id from s1) order by s2.id;
select s2.name from s2 where exists (select s1.id from s1) order by s2.id;
select s2.name from s2 where exists (select s1.id from s1 where s1.id=s2.id) order by s2.id;

-- should fail
select w1.id from t1 w1 order by test2.w1.id;
select w1.id from t1 w1 order by test1.w1.id;
select t1.id as idcolumn1, t1.id as idcolumn2 from t1 group by idcolumn1, idcolumn2 order by idcolumn1, idcolumn2;
select t1.id as idcolumn1, t1.id as idcolumn2 from t1 group by t1.idcolumn1, t1.idcolumn2 order by t1.idcolumn1, t1.idcolumn2;
select t1.id as c1 from t1 where c1 > 0 order by c1;

drop synonym s1;
drop synonym s2;
drop synonym t1;
drop table test2.t2;
drop table test1.t1;

set schema app;
create table A (id integer);
insert into A values 29;
create synonym B for A;
select a.id from a;
select b.id from b;
select b.id from b as b;
select b.id from (select b.id from b) as b;
select b.id from (select b.id from b as b) as b;
drop synonym B;
drop table A;

create table t1 (i int, j int);
create view v1 as select * from t1;
insert into t1 values (1, 10);
create synonym s1 for t1;
create synonym sv1 for v1;
-- should fail
select t1.i from s1;
select v1.i from sv1;
select sv1.i from sv1 as w1;
select s1.j from s1 where s1.k = 1;
select s1.j from s1 where w1.i = 1;
select * from s1 where w1.i = 1;
select s1.j from s1 as w1 where w1.i = 1;
select w1.j from s1 as w1 where s1.i = 1;
select s1.j from s1 where t1.i = 1;
select s1.j from s1 group by t1.j;
select s1.j from s1 group by s1.j having t1.j > 0;
insert into s1 (t1.i) values 100;
update s1 set t1.i=1;
delete from s1 where t1.i=100;

-- ok
select s1.i from s1;
select s1.i from s1 as s1;
select s1.i from s1 where i = 1;
select s1.i from s1 where s1.i = 1;
select s1.i from s1 as s1 where i = 1;
select w1.i from s1 as w1 where w1.i = 1;
select sv1.i from sv1;
select sv1.i from sv1 as sv1;
select sv1.i from sv1 where i = 1;
select sv1.i from sv1 where sv1.i = 1;
select sv1.i from sv1 as sv1 where i = 1;
select wv1.i from sv1 as wv1 where wv1.i = 1;

select s1.i, s1.i from s1;
select sv1.i, sv1.i from sv1;
select * from s1;
select * from s1 where i = 1;
select * from s1 where s1.i = 1;
select * from s1 as s1;
select * from s1 as w1;
select * from sv1;
select * from sv1 as sv1;
select * from sv1 as w1;
select * from sv1 where i = 1;
select * from sv1 where sv1.i = 1;
select s1.i from (select s1.i from s1) as s1;
select sv1.i from (select sv1.i from sv1) as sv1;

create table t2 (i int, j int);
insert into t2 values (1, 100), (1, 100), (2, 200);
create view v2 as select * from t2;
create synonym s2 for t2;
create synonym sv2 for v2;
select s2.j from s2 group by s2.j order by s2.j;
select s2.j from s2 group by s2.j having s2.j > 100 order by s2.j;
select s1.i, s1.j from (select s1.i, s2.j from s1,s2 where s1.i=s2.i) as s1;
select sv2.j from sv2 group by sv2.j order by sv2.j;
select sv2.j from sv2 group by sv2.j having sv2.j > 100 order by sv2.j;
select sv1.i, sv1.j from (select sv1.i, sv2.j from sv1,sv2 where sv1.i=sv2.i) as sv1;
select max(s2.i) from s2;
select max(sv2.i) from sv2;
select * from s1 inner join s2 on (s1.i = s2.i);
select * from sv1 inner join sv2 on (sv1.i = sv2.i);
select s1.* from s1;
select sv1.* from sv1;

create table t3 (i int, j int);
insert into t3 values (10, 0), (11, 0), (12, 0);
create synonym s3 for t3;
insert into s1 (s1.i, s1.j) values (2, 20);
insert into app.s1 (s1.i, s1.j) values (3, 30);
insert into app.s1 (app.s1.i, s1.j) values (4, 40);
insert into app.s1 (app.s1.i, app.s1.j) values (5, 50);
update s1 set s1.j = 1;
update app.s1 set s1.j = 2;
update app.s1 set app.s1.j = 3;
update s1 set s1.j = 4 where s1.i = 3;
update app.s1 set app.s1.j = 5 where app.s1.i = 4;
delete from s1 where s1.i = 4;
delete from app.s1 where app.s1.i = 5;
update app.s1 set s1.j = s1.i, s1.i = s1.j;
select * from s1;
update app.s1 set s1.j = s1.i, s1.i = s1.j;
select * from s1;
delete from s1;

-- should fail
insert into s1 (s1.i) select s1.i from s3;

-- ok
insert into s1 (s1.i) select s3.i from s3;
insert into s1 select * from s3;
select * from s1;

-- clean up  
drop synonym s3;
drop synonym sv2;
drop synonym s2;
drop synonym s1;
drop synonym sv1;
drop view v2;
drop view v1;
drop table t3;
drop table t2;
drop table t1;

