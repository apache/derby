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
-- alter table tests
-- add column
-- (add constraint & drop constraint to be added)

-- create some database objects
create table t0(c1 int not null constraint p1 primary key);
create table t0_1(c1 int);
create table t0_2(c1 int);
create table t0_3(c1 int);
create table t1(c1 int);
create table t1_1(c1 int);
create table t2(c1 int);
create table t3(c1 int);
create table t4(c1 int not null);
create view v1 as select * from t2;
create view v2 as select c1 from t2;
create index i0_1 on t0_1(c1);
create index i0_2 on t0_2(c1);

-- do some population
insert into t1 values 1;
insert into t1_1 values 1;
insert into t2 values 1;
insert into t2 values 2;
insert into t3 values 1;
insert into t3 values 2;
insert into t3 values 3;
insert into t4 values 1, 2, 3, 1;

autocommit off;

-- add column

-- negative tests

-- alter a non-existing table
alter table notexists add column c1 int;

-- add a column that already exists
alter table t0 add column c1 int;

-- alter a system table
alter table sys.systables add column c1 int;

-- alter table on a view
alter table v2 add column c2 int;

-- add a primary key column to a table which already has one
-- this will produce an error
alter table t0 add column c2 int not null default 0 primary key;

-- add a unique column constraint to a table with > 1 row
alter table t3 add column c2 int not null default 0 unique;

-- cannot alter a table when there is an open cursor on it
get cursor c1 as 'select * from t1';
alter table t1 add column c2 int;
close c1;

-- positive tests

-- add a non-nullable column to a non-empty table
alter table t1 add column c2 int not null default 0;

-- add a primary key column to a non-empty table
alter table t1 add column c3 int not null default 0 primary key;

-- add a column with a check constraint to a non-empty column
alter table t1 add column c4 int check(c4 = 1);


select * from v1;
prepare p1 as 'select * from t2';
execute p1;

alter table t2 add column c2 int;

-- select * views don't see added columns after alter table
select * from v1;

-- select * prepared statements do see added columns after alter table
execute p1;

-- rollback and recheck
rollback;
select * from v1;
execute p1;
remove p1;

-- add non-nullable column to 0 row table and verify
alter table t0 add column c2 int not null default 0;
insert into t0 values (1, default);
select * from t0;
drop table t0;
rollback;
select  * from t0;

-- add primary key to 0 row table and verify
alter table t0_1 add column c2 int not null primary key default 0;
insert into t0_1 values (1, 1);
insert into t0_1 values (1, 1);
select * from t0_1;
rollback;

-- add unique constraint to 0 and 1 row tables and verify 
 alter table t0_1 add column c2 int not null unique default 0;
 insert into t0_1 values (1, default);
 insert into t0_1 values (2, default);
 insert into t0_1 values (3, 1);
 delete from t1;
 alter table t1 add column c2 int not null unique default 0;
 insert into t1 values (2, 2);
 insert into t1 values (3, 1);

-- verify the consistency of the indexes on the user tables
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', tablename)
from sys.systables where tabletype = 'T';

 rollback;

create function countopens() returns varchar(128)
language java parameter style java
external name 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.countOpens';
commit;

-- do consistency check on scans, etc.
values countopens();


-- some typical data

create table tab1 (c1 int, c2 int not null constraint tab1pk primary key, c3 double, c4 int);
create index i11 on tab1 (c1);
create unique index i12 on tab1 (c1);
create index i13 on tab1 (c3, c1, c4);
create unique index i14 on tab1 (c3, c1);
insert into tab1 values (6, 5, 4.5, 90);
insert into tab1 values (10, 3, 8.9, -5);
insert into tab1 values (100, 15, 4.5, 9);
insert into tab1 values (2, 8, 4.4, 8);
insert into tab1 values (11, 9, 2.5, 88);
insert into tab1 values(null,10, 3.5, 99);

create view vw1 (col_sum, col_diff) as select c1+c4, c1-c4 from tab1;
create view vw2 (c1) as select c3 from tab1;

create table tab2 (c1 int not null unique, c2 double, c3 int, c4 int not null constraint c4_PK primary key, c5 int, constraint t2ck check (c2+c3<100.0));
create table tab3 (c1 int, c2 int, c3 int, c4 int, constraint t3fk foreign key (c2) references tab2(c1), constraint t3ck check (c2-c3<80));

create view vw3 (c1, c2) as select c5, tab3.c4 from tab2, tab3 where tab3.c1 > 0;
create view vw4 (c1) as select c4 from tab3 where c2 > 8;

create table tab4 (c1 int, c2 int, c3 int, c4 int);
create table tab5 (c1 int);
insert into tab4 values (1,2,3,4);
create trigger tr1 after update of c2, c3, c4 on tab4 for each row insert into tab5 values (1);
create trigger tr2 after update of c3, c4 on tab4 for each row insert into tab5 values (2);

-- tr1 is dropped, tr2 still OK
drop trigger tr1;

select * from tab5;
-- fire tr2 only
update tab4 set c3 = 33;
update tab4 set c4 = 44;
select * from tab5;

-- drop tr2
drop trigger tr2;

update tab4 set c4 = 444;
select * from tab2;

drop view vw2;
create view vw2 (c1) as select c3 from tab1;

-- vw1 should be dropped
drop view vw1;
select * from vw1;

-- do the indexes still exist?
-- the create index statements should fail
create index i13 on tab1 (c3, c1, c4);
create unique index i14 on tab1 (c3, c1);
create unique index i12 on tab1 (c1);
select c2, c3, c4 from tab1 order by c3;
drop index i12;
drop index i13;
drop index i14;

-- more data
insert into tab1 (c2, c3, c4) values (22, 8.9, 5);
insert into tab1 (c2, c3, c4) values (11, 4.5, 67);
select c2 from tab1;

-- add a new column
alter table tab1 add column c5 double;

-- drop view vw2 so can create a new one, with where clause
drop view vw2;
create view vw2 (c1) as select c5 from tab1 where c2 > 5;

-- drop vw2 as well
drop view vw2;
alter table tab1 drop constraint tab1pk;

-- any surviving index? 
-- creating the index should not fail
select c4 from tab1 order by 1;
create index i13 on tab1 (c3, c1, c4);

-- should drop t2ck
alter table tab2 drop constraint t2ck;

-- this should drop t3fk, unique constraint and backing index
alter table tab3 drop constraint t3fk;
alter table tab2 drop constraint c4_PK;
insert into tab3 values (1,2,3,4);

-- drop view vw3
drop view vw3;

-- violates t3ck
insert into tab3 (c1, c2, c3) values (81, 1, 2);
insert into tab3 (c1, c2, c3) values (81, 2, 2);

-- this should drop t3ck, vw4
alter table tab3 drop constraint t3ck;
drop view vw4;
insert into tab3 (c2, c3) values (-82, 9);
create view vw4 (c1) as select c3 from tab3 where c3+5>c4;

-- drop view vw4
drop view vw4;

rollback;

-- check that dropping a column will drop backing index on referencing
-- table
create table tt1(a int, b int not null constraint tt1uc unique);
create table reftt1(a int constraint reftt1rc references tt1(b));
-- count should be 2
select count(*) 
from sys.sysconglomerates c, sys.systables t 
where t.tableid = c.tableid
and t.tablename = 'REFTT1';

alter table reftt1 drop constraint reftt1rc;
alter table tt1 drop constraint tt1uc;

-- count should be 1
select count(*) 
from sys.sysconglomerates c, sys.systables t 
where t.tableid = c.tableid
and t.tablename = 'REFTT1';
rollback;

-- add constraint

-- negative tests

-- add primary key to table which already has one
alter table t0 add column c3 int;
alter table t0 add constraint cons1 primary key(c3);
alter table t0 add primary key(c3);

-- add constraint references non-existant column

alter table t4 add constraint t4pk primary key("c1");
alter table t4 add constraint t4uq unique("c1");
alter table t4 add constraint t4fk foreign key ("c1") references t0;
alter table t4 add constraint t4ck check ("c1" <> 4);

-- add primary key to non-empty table with duplicates
alter table t4 add primary key(c1);

-- positive tests

-- add primary key to 0 row table and verify
alter table t0_1 add column c2 int not null constraint p2 primary key default 0;
insert into t0_1 values (1, 1);
insert into t0_1 values (1, 1);
select * from t0_1;
rollback;

-- add check constraint to 0 row table and verify
alter table t0_1 add column c2 int check(c2 != 3);
insert into t0_1 values (1, 1);
insert into t0_1 values (1, 3);
insert into t0_1 values (1, 1);
select * from t0_1;
rollback;

-- add check constraint to table with rows that are ok
alter table t0_1 add column c2 int;
insert into t0_1 values (1, 1);
insert into t0_1 values (2, 2);
alter table t0_1 add constraint ck1 check(c2 = c1);
select * from t0_1;

-- verify constraint has been added, the following should fail
insert into t0_1 values (1, 3);
rollback;


-- add check constraint to table with rows w/ 3 failures
alter table t0_1 add column c2 int;
insert into t0_1 values (1, 1);
insert into t0_1 values (2, 2);
insert into t0_1 values (2, 2);
insert into t0_1 values (666, 2);
insert into t0_1 values (2, 2);
insert into t0_1 values (3, 3);
insert into t0_1 values (666, 3);
insert into t0_1 values (666, 3);
insert into t0_1 values (3, 3);
alter table t0_1 add constraint ck1 check(c2 = c1);

-- verify constraint has NOT been added, the following should succeed
insert into t0_1 values (1, 3);
select * from t0_1;
rollback;


-- check and primary key constraints on same table and enforced
alter table t0_1 add column c2 int not null constraint p2 primary key default 0;
alter table t0_1 add check(c2 = c1);
insert into t0_1 values (1, 1);
insert into t0_1 values (1, 2);
insert into t0_1 values (1, 1);
insert into t0_1 values (2, 1);
insert into t0_1 values (2, 2);
select * from t0_1;
rollback;

-- add primary key constraint to table with > 1 row
alter table t3 add column c3 int;
alter table t3 add unique(c3);

-- add unique constraint to 0 and 1 row tables and verify 
alter table t0_2 add column c2 int not null unique default 0;
insert into t0_2 values (1, default);
insert into t0_2 values (1, 1);
delete from t1_1;
alter table t1_1 add column c2 int not null unique default 0;
insert into t1_1 values (1, 2);
insert into t1_1 values (1, 2);
insert into t1_1 values (1, 1);

-- add unique constraint to table with > 1 row
alter table t3 add unique(c1);

-- verify prepared alter table dependent on underlying table
prepare p1 as 'alter table xxx add check(c2 = 1)';
create table xxx(c1 int, c2 int);
prepare p1 as 'alter table xxx add check(c2 = 1)';
execute p1;
drop table xxx;
create table xxx(c1 int);
execute p1;
alter table xxx add column c2 int;
execute p1;
drop table xxx;

-- verify the consistency of the indexes on the user tables
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', tablename)
from sys.systables where tabletype = 'T';


-- drop constraint

-- negative tests

-- drop non-existent constraint
alter table t0 drop constraint notexists;

-- constraint/table mismatch
alter table t1 drop constraint p1;


-- In DB2 compatibility mode, we cann't add a nullable primary key
alter table t0_1 add constraint p2 primary key(c1);
alter table t0_1 drop constraint p2;

-- positive tests
-- verify that we can add/drop/add/drop/... constraints
alter table t0_1 add column c2 int not null constraint p2 primary key default 0;
delete from t0_1;
alter table t0_1 drop constraint p2;
alter table t0_1 add constraint p2 primary key(c2);
alter table t0_1 drop constraint p2;
alter table t0_1 add constraint p2 primary key(c2);


-- verify that constraint is still enforced
insert into t0_1 values (1,1);
insert into t0_1 values (1,1);

-- verify the consistency of the indexes on the user tables
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', tablename)
from sys.systables where tabletype = 'T' and tablename = 'T0_1';


-- verify that alter table works after drop/recreate of table
prepare p1 as 'alter table t0_1 drop constraint p2';
execute p1;
drop table t0_1;
create table t0_1 (c1 int, c2 int not null constraint p2 primary key);
execute p1;

-- do consistency check on scans, etc.
-- values (org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker::countOpens());

-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

-- verify the consistency of the indexes on the user tables
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', tablename)
from sys.systables where tabletype = 'T';

-- bugs 793
create table b793 (pn1 int not null constraint named_primary primary key, 
				   pn2 int constraint named_pn2 check (pn2 > 3));
alter table b793 drop constraint named_primary;
drop table b793;

-- test that drop constraint removes backing indexes
drop table t1;
create table t1(a int not null constraint t1_pri primary key);
create table reft1(a int constraint t1_ref references t1(a));
-- count should be 2
select count(*) 
from sys.sysconglomerates c, sys.systables t
where c.tableid = t.tableid and
t.tablename = 'REFT1';
alter table reft1 drop constraint t1_ref;
alter table t1 drop constraint t1_pri;
-- count should be 1
select count(*) 
from sys.sysconglomerates c, sys.systables t
where c.tableid = t.tableid and
t.tablename = 'REFT1';
drop table reft1;


-- clean up
drop view v2;
drop view v1;
drop table t0;
drop table t0_1;
drop table t0_2;
drop table t0_3;
drop table t1;
drop table t1_1;
drop table t3;
drop table t4;

------------------------------------------------------
--
-- special funky schema tests
--
------------------------------------------------------

create schema newschema;
drop table x;
create table x (x int not null, y int not null);
alter table x add constraint newcons primary key (x);

-- schemaname should be app
select schemaname, constraintname from sys.sysconstraints c, sys.sysschemas s where s.schemaid = c.schemaid order by 1;
insert into x values (1,1),(1,1);
alter table x drop constraint app.newcons;
alter table x add constraint newcons primary key (x);

-- schemaname should be app
select schemaname, constraintname from sys.sysconstraints c, sys.sysschemas s where s.schemaid = c.schemaid order by 1;

-- fail
alter table x drop constraint badschema.newcons;

-- fail
alter table x drop constraint newschema.newcons;

-- ok
alter table x drop constraint app.newcons;

-- bad schema name
alter table x add constraint badschema.newcons primary key (x);

-- two constriants, same name, different schema (second will fail)
drop table x;
create table x (x int not null, y int not null);
alter table x add constraint con check (x > 1);
alter table x add constraint newschema.con check (x > 1);
select schemaname, constraintname from sys.sysconstraints c, sys.sysschemas s where s.schemaid = c.schemaid order by 1;

create schema emptyschema;
set schema emptyschema;

-- fail, cannot find emptyschema.conn
alter table app.x drop constraint emptyschema.con;
select schemaname, constraintname from sys.sysconstraints c, sys.sysschemas s where s.schemaid = c.schemaid order by 1;

set schema newschema;

-- add constraint, default to table schema
alter table app.x add constraint con2 check (x > 1);

-- added constraint in APP (defaults to table's schema)
select schemaname, constraintname from sys.sysconstraints c, sys.sysschemas s where s.schemaid = c.schemaid order by 1,2;

drop table app.x;
drop schema newschema restrict;


-- some temporary table tests
-- declare temp table with no explicit on commit behavior.
declare global temporary table session.t1 (c11 int) not logged;
declare global temporary table session.t2 (c21 int) on commit delete rows not logged;
declare global temporary table session.t3 (c31 int) on commit preserve rows not logged;
drop table session.t1;
drop table session.t2;
drop table session.t3;

drop table t1;
create table t1(c1 int, c2 int not null primary key);
insert into t1 values (1, 1);
insert into t1 values (1, 1);
alter table t1 drop primary key;
insert into t1 values (1, 1);
select * from t1;
alter table t1 drop primary key;

alter table t1 drop constraint emptyschema.C1;
alter table t1 drop constraint nosuchschema.C2;

alter table t1 add constraint emptyschema.C1_PLUS_C2 check ((c1 + c2) < 100);
alter table t1 add constraint C1_PLUS_C2 check ((c1 + c2) < 100);

prepare alplus as 'alter table t1 drop constraint C1_PLUS_C2';
alter table APP.t1 drop constraint APP.C1_PLUS_C2;
execute alplus;
remove alplus;

drop table t1;

-- bug 5817 - make LOGGED non-reserved keyword. following test cases for that
create table LOGGED(c11 int);
drop table LOGGED;
create table logged(logged int);
drop table logged;
declare global temporary table session.logged(logged int) on commit delete rows not logged;

-- tests for ALTER TABLE ALTER COLUMN [NOT] NULL
create table atmcn_1 (a integer, b integer not null);
-- should fail because b cannot be null
insert into atmcn_1 (a) values (1);
insert into atmcn_1 values (1,1);
select * from atmcn_1;
alter table atmcn_1 alter column a not null;
-- should fail because a cannot be null
insert into atmcn_1 (b) values (2);
insert into atmcn_1 values (2,2);
select * from atmcn_1;
alter table atmcn_1 alter column b null;
insert into atmcn_1 (a) values (1);
select * from atmcn_1;
-- Now that B has a null value, trying to modify it to NOT NULL should fail
alter table atmcn_1 alter column b not null;
-- show that a column which is part of the PRIMARY KEY cannot be modified NULL
create table atmcn_2 (a integer not null primary key, b integer not null);
alter table atmcn_2 alter column a null;
create table atmcn_3 (a integer not null, b integer not null);
alter table atmcn_3 add constraint atmcn_3_pk primary key(a, b);
alter table atmcn_3 alter column b null;
-- verify that the keyword "column" in the ALTER TABLE ... ALTER COLUMN ...
-- statement is optional:
create table atmcn_4 (a integer not null, b integer);
alter table atmcn_4 alter a null;
--set column, part of unique constraint, to null
create table atmcn_5 (a integer not null, b integer not null unique);
alter table atmcn_5 alter column b null;

-- tests for ALTER TABLE ALTER COLUMN DEFAULT
create table atmod_1 (a integer, b varchar(10));
insert into atmod_1 values (1, 'one');
alter table atmod_1 alter column a default -1;
insert into atmod_1 values (default, 'minus one');
insert into atmod_1 (b) values ('b');
select * from atmod_1;
alter table atmod_1 alter a default 42;
insert into atmod_1 values(3, 'three');
insert into atmod_1 values (default, 'forty two');
select * from atmod_1;

-- Tests for renaming a column. These tests are in altertable.sql because
-- renaming a column is closely linked, conseptually, to other table
-- alterations. However, the actual syntax is:
--    RENAME COLUMN t.c1 TO c2

create table renc_1 (a int, b varchar(10), c timestamp, d double);
-- table doesn't exist, should fail:
rename column renc_no_such.a to b;
-- table exists, but column doesn't exist
rename column renc_1.no_such to e;
-- new column name already exists in table:
rename column renc_1.a to c;
-- can't rename a column to itself:
rename column renc_1.b to b;
-- new column name is a reserved word:
rename column renc_1.a to select;
-- attempt to rename a column in a system table. Should fali:
rename column sys.sysconglomerates.isindex to is_an_index;
-- attempt to rename a column in a view, should fail:
create view renc_vw_1 (v1, v2) as select b, d from renc_1;
rename column renc_vw_1.v2 to v3;
describe renc_vw_1;
-- attempt to rename a column in an index, should fail:
create index renc_idx_1 on renc_1 (c, d);
show indexes from renc_1;
rename column renc_idx_1.d to d_new;
show indexes from renc_1;
-- A few syntax errors in the statement, to check for reasonable messages:
rename column renc_1 to b;
rename column renc_1 rename a to b;
rename column renc_1.a;
rename column renc_1.a b;
rename column renc_1.a to;
rename column renc_1.a to b, c;
rename column renc_1.a to b and c to d;
-- Rename a column which is the primary key of the table:
create table renc_2(c1 int not null constraint renc_2_p1 primary key);
rename column renc_2.c1 to c2;
describe renc_2;
show indexes from renc_2;
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t 
    where t.tableid = c.tableid and t.tablename = 'RENC_2';
create table renc_3 (a integer not null, b integer not null, c int,
            constraint renc_3_pk primary key(a, b));
rename column renc_3.b to newbie;
describe renc_3;
show indexes from renc_3;
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t 
    where t.tableid = c.tableid and t.tablename = 'RENC_3';
create table renc_4 (c1 int not null unique, c2 double, c3 int,
    c4 int not null constraint renc_4_c4_PK primary key, c5 int, c6 int,
    constraint renc_4_t2ck check (c2+c3<100.0));
create table renc_5 (c1 int, c2 int, c3 int, c4 int, c5 int not null, c6 int,
    constraint renc_5_t3fk foreign key (c2) references renc_4(c4),
    constraint renc_5_unq unique(c5),
    constraint renc_5_t3ck check (c2-c3<80));
-- Attempt to rename a column referenced by a foreign key constraint 
-- should fail:
rename column renc_4.c4 to another_c4;
-- Rename a column with a unique constraint should work:
rename column renc_4.c1 to unq_c1;
rename column renc_5.c5 to unq_c5;
show indexes from renc_4;
show indexes from renc_5;
-- Attempt to rename a column used in a check constraint should fail:
rename column renc_4.c2 to some_other_name;
-- Attempt to rename a column used in a trigger should fail:
create trigger renc_5_tr1 after update of c2, c3, c6 on renc_4
    for each row mode db2sql insert into renc_5 (c6) values (1);
-- This fails, because the tigger is dependent on it:
rename column renc_4.c6 to some_name;
-- This succeeds, because the trigger is not dependent on renc_5.c6. 
-- DERBY-2041 requests that triggers should be marked as dependent on
-- tables and columns in their body. If that improvement is made, this
-- test will need to be changed, as the next rename would fail, and the
-- insert after it would then succeed.
rename column renc_5.c6 to new_name;
-- The update statement will fail, because column c6 no longer exists.
-- See DERBY-2041 for a discussion of this topic.
insert into renc_4 values(1, 2, 3, 4, 5, 6);
update renc_4 set c6 = 92;
select * from renc_5;
-- Rename a column which has a granted privilege, show that the grant is
-- properly processed and now applies to the new column:
create table renc_6 (a int, b int, c int);
grant select (a, b) on renc_6 to bryan;
select p.grantee,p.type, p.columns from sys.syscolperms p, sys.systables t
    where t.tableid=p.tableid and t.tablename='RENC_6';
rename column renc_6.b to bb_gun;
select p.grantee,p.type, p.columns from sys.syscolperms p, sys.systables t
    where t.tableid=p.tableid and t.tablename='RENC_6';
-- Attempt to rename a column should fail when there is an open cursor on it:
get cursor renc_c1 as 'select * from renc_6';
rename column renc_6.bb_gun to water_pistol;
close renc_c1;
-- Attempt to rename a column when there is an open prepared statement on it.
-- The rename of the column will be successful; the open statement will get
-- errors when it tries to re-execute.
autocommit off;
prepare renc_p1 as 'select * from renc_6 where a = ?';
execute renc_p1 using 'values (30)';
rename column renc_6.a to abcdef;
execute renc_p1 using 'values (30)';
autocommit on;

-- Demonstrate that you cannot rename a column in a synonym, and demonstrate
-- that renaming a column in the underlying table correctly renames it
-- in the synonym too
create table renc_7 (c1 varchar(50), c2 int);
create synonym renc_7_syn for renc_7;
insert into renc_7 values ('one', 1);
rename column renc_7_syn.c2 to c2_syn;
describe renc_7;
rename column renc_7.c1 to c1_renamed;
select c1_renamed from renc_7_syn;

-- demonstrate that you can rename a column in a table in a different schema
create schema renc_schema_1;
create schema renc_schema_2;
set schema renc_schema_2;
create table renc_8 (a int, b int, c int);
set schema renc_schema_1;
-- This should fail, as there is no table renc_8 in schema 1:
rename column renc_8.b to bbb;
-- But this should work, and should find the table in the other schema
rename column renc_schema_2.renc_8.b to b2;
describe renc_schema_2.renc_8;

-- alter table tests for ALTER TABLE DROP COLUMN.

-- The overall syntax is:
--    ALTER TABLE tablename DROP [ COLUMN ] columnname [ CASCADE | RESTRICT ]
-- 
create table atdc_0 (a integer);
create table atdc_1 (a integer, b integer);
insert into atdc_1 values (1, 1);
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
alter table atdc_1 drop column b;
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
alter table atdc_1 add column b varchar (20);
insert into atdc_1 values (1, 'new val');
insert into atdc_1 (a, b) values (2, 'two val');
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
alter table atdc_1 add column c integer;
insert into atdc_1 values (3, null, 3);
select * from atdc_1;
alter table atdc_1 drop b;
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
-- Demonstrate that we can drop a column which is the primary key. Also
-- demonstrate that when we drop a column which is the primary key, that
-- cascade processing will drop the corresponding foreign key constraint
create table atdc_1_01 (a int, b int, c int not null primary key);
alter table atdc_1_01 drop column c cascade;
create table atdc_1_02 (a int not null primary key, b int);
create table atdc_1_03 (a03 int, 
   constraint a03_fk foreign key (a03) references atdc_1_02(a));
alter table atdc_1_02 drop column a cascade;
-- drop column restrict should fail because column is used in a constraint:
alter table atdc_1 add constraint atdc_constraint_1 check (a > 0);
select * from sys.sysconstraints where tableid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
select sc.* from sys.syschecks sc,sys.sysconstraints con, sys.systables st
		where sc.constraintid = con.constraintid and con.tableid = st.tableid
              and st.tablename = 'ATDC_1';
alter table atdc_1 drop column a restrict;
-- drop column cascade should also drop the check constraint:
alter table atdc_1 drop column a cascade;
select * from sys.sysconstraints where tableid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
-- Verify the behavior of the various constraint types:
-- check, primary key, foreign key, unique, not null
create table atdc_1_constraints (a int not null primary key,
   b int not null,
   c int constraint atdc_1_c_chk check (c is not null),
   d int not null unique,
   e int,
   f int,
   constraint atdc_1_e_fk foreign key (e) references atdc_1_constraints(a));
-- In restrict mode, none of the columns a, c, d, or e should be droppable,
-- but in cascade mode each of them should be droppable, and at the end
-- we should have only column f
-- column b is droppable because an unnamed NOT NULL constraint doesn't
-- prevent DROP COLUMN, only an explicit CHECK constraint does.
describe atdc_1_constraints;
alter table atdc_1_constraints drop column a restrict;
alter table atdc_1_constraints drop column b restrict;
alter table atdc_1_constraints drop column c restrict;
alter table atdc_1_constraints drop column d restrict;
alter table atdc_1_constraints drop column e restrict;
describe atdc_1_constraints;
alter table atdc_1_constraints drop column a cascade;
alter table atdc_1_constraints drop column c cascade;
alter table atdc_1_constraints drop column d cascade;
alter table atdc_1_constraints drop column e cascade;
describe atdc_1_constraints;
-- Some negative testing of ALTER TABLE DROP COLUMN
-- Table does not exist:
alter table atdc_nosuch drop column a;
-- Table exists, but column does not exist:
create table atdc_2 (a integer);
alter table atdc_2 drop column b;
alter table atdc_2 drop b;
-- Column name is spelled incorrectly (wrong case)
alter table atdc_2 drop column 'a';
-- Some special reserved words to cause parser errors
alter table atdc_2 drop column column;
alter table atdc_2 drop column;
alter table atdc_2 drop column constraint;
alter table atdc_2 drop column primary;
alter table atdc_2 drop column foreign;
alter table atdc_2 drop column check;
create table atdc_3 (a integer);
create index atdc_3_idx_1 on atdc_3 (a);
-- This fails because a is the only column in the table.
alter table atdc_3 drop column a restrict;
drop index atdc_3_idx_1;
-- cascade/restrict processing doesn't currently consider indexes.
-- The column being dropped is automatically dropped from all indexes
-- as well. If that was the only (last) column in the index, then the
-- index is dropped, too.
create table atdc_4 (a int, b int, c int, d int, e int);
insert into atdc_4 values (1,2,3,4,5);
create index atdc_4_idx_1 on atdc_4 (a);
create index atdc_4_idx_2 on atdc_4 (b, c, d);
create index atdc_4_idx_3 on atdc_4 (c, a);
select conglomeratename,isindex from sys.sysconglomerates where tableid in
    (select tableid from sys.systables where tablename = 'ATDC_4');
show indexes from atdc_4;
-- This succeeds, because cascade/restrict doesn't matter for indexes. The
-- effect of dropping column a is that:
--    index atdc_4_idx_1 is entirely dropped
--    index atdc_4_idx_2 is left alone but the column positions are fixed up
--    index atdc_4_idx_3 is modified to refer only to column c
alter table atdc_4 drop column a restrict;
select conglomeratename,isindex from sys.sysconglomerates where tableid in
    (select tableid from sys.systables where tablename = 'ATDC_4');
show indexes from atdc_4;
describe atdc_4;
-- The effect of dropping column c is that:
--    index atdc_4_idx_2 is modified to refer to columns b and d
--    index atdc_4_idx_3 is entirely dropped
alter table atdc_4 drop column c restrict;
show indexes from atdc_4;
select * from atdc_4 where c = 3;
select count(*) from sys.sysconglomerates where conglomeratename='ATDC_4_IDX_2';
select conglomeratename, isindex from sys.sysconglomerates
     where conglomeratename like 'ATDC_4%';
drop index atdc_4_idx_2;
-- drop column restrict should fail becuase column is used in a view:
create table atdc_5 (a int, b int);
create view atdc_vw_1 (vw_b) as select b from atdc_5;
alter table atdc_5 drop column b restrict;
select * from atdc_vw_1;
-- drop column cascade drops the column, and also drops the dependent view:
alter table atdc_5 drop column b cascade;
select * from atdc_vw_1;
-- cascade processing should transitively drop a view dependent on a view
-- dependent in turn on the column being dropped:
create table atdc_5a (a int, b int, c int);
create view atdc_vw_5a_1 (vw_5a_b, vw_5a_c) as select b,c from atdc_5a;
create view atdc_vw_5a_2 (vw_5a_c_2) as select vw_5a_c from atdc_vw_5a_1;
alter table atdc_5a drop column b cascade;
select * from atdc_vw_5a_1;
select * from atdc_vw_5a_2;
-- drop column restrict should fail because column is used in a trigger:
create table atdc_6 (a integer, b integer);
create trigger atdc_6_trigger_1 after update of b on atdc_6
	for each row values current_date;
alter table atdc_6 drop column b restrict;
select triggername from sys.systriggers where triggername='ATDC_6_TRIGGER_1';
alter table atdc_6 drop column b cascade;
select triggername from sys.systriggers where triggername='ATDC_6_TRIGGER_1';
create table atdc_7 (a int, b int, c int, primary key (a));
alter table atdc_7 drop column a restrict;
alter table atdc_7 drop column a cascade;
create table atdc_8 (a int, b int, c int, primary key (b, c));
alter table atdc_8 drop column c restrict;
alter table atdc_8 drop column c cascade;
create table atdc_9 (a int not null, b int);
alter table atdc_9 drop column a restrict;
-- Verify that a GRANTED privilege fails a drop column in RESTRICT mode,
-- and verify that the privilege is dropped in CASCADE mode:
create table atdc_10 (a int, b int, c int);
grant select(a, b, c) on atdc_10 to bryan;
select * from sys.syscolperms;
alter table atdc_10 drop column b restrict;
select * from sys.syscolperms;
alter table atdc_10 drop column b cascade;
select * from sys.syscolperms;
-- Include the test from the DERBY-1909 report:
drop table d1909;
create table d1909 (a int, b int, c int);
grant select (a) on d1909 to user1;
grant select (a,b) on d1909 to user2;
grant update(c) on d1909 to super_user;
select c.grantee, c.type, c.columns from sys.syscolperms c, sys.systables t
	where c.tableid = t.tableid and t.tablename='D1909';
alter table d1909 drop column a;
select c.grantee, c.type, c.columns from sys.syscolperms c, sys.systables t
	where c.tableid = t.tableid and t.tablename='D1909';
grant update(b) on d1909 to user1;
grant select(c) on d1909 to user1;
grant select(c) on d1909 to user2;
select c.grantee, c.type, c.columns from sys.syscolperms c, sys.systables t
	where c.tableid = t.tableid and t.tablename='D1909';

-- JIRA 3175: Null Pointer Exception or SanityManager ASSERT because
-- autoincrement properties of generated column are not maintained properly
-- when a column before it in the table is dropped:

create table d3175 (x varchar(12), y varchar(12),
                    id int primary key generated by default as identity);
select * from sys.syscolumns c,sys.systables t where c.referenceid = t.tableid and t.tablename='D3175';
insert into d3175(x) values 'b';
alter table d3175 drop column y;
insert into d3175(x) values 'a';
select * from sys.syscolumns c,sys.systables t where c.referenceid = t.tableid and t.tablename='D3175';

-- JIRA 3177 appears to be aduplicate of JIRA 3175, but the reproduction
-- test script is different. In the interests of additional testing, we
-- include the JIRA 3177 test script, as it has a number of additional
-- examples of interesting ALTER TABLE statements
--
-- In the original JIRA 3177 bug, by the time we get to the end of the
-- ALTER TABLE processing, the select from SYS.SYSCOLUMNS retrieves NULL
-- for the autoinc columns, instead of the correct value (1).

create table d3177_SchemaVersion ( version INTEGER NOT NULL  );
insert into d3177_SchemaVersion (version) values (0);
create table d3177_BinaryData ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  CRC32 BIGINT NOT NULL , 
  data BLOB NOT NULL , 
  CONSTRAINT d3177_BinaryData_id_pk PRIMARY KEY(id) 
);
create table d3177_MailServers ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  port INTEGER NOT NULL , 
  username varchar(80) NOT NULL , 
  protocol varchar(80) NOT NULL , 
  SSLProtocol varchar(10), 
  emailAddress varchar(80) NOT NULL , 
  server varchar(80) NOT NULL , 
  password varchar(80) NOT NULL , 
  CONSTRAINT d3177_MailServers_id_pk PRIMARY KEY(id) 
);
create table d3177_Mailboxes ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  port INTEGER NOT NULL , 
  folder varchar(80) NOT NULL , 
  username varchar(80) NOT NULL , 
  SSLProtocol varchar(10), 
  hostname varchar(80) NOT NULL , 
  storeType varchar(80) NOT NULL , 
  password varchar(80) NOT NULL , 
  timeout INTEGER NOT NULL , 
  MailServerID INTEGER NOT NULL , 
  CONSTRAINT d3177_Mailboxes_id_pk PRIMARY KEY(id) 
);
create table d3177_MESSAGES ( 
  Message_From varchar(1000), 
  Message_Cc varchar(1000), 
  Message_Subject varchar(1000), 
  Message_ID varchar(256) NOT NULL , 
  Message_Bcc varchar(1000), 
  Message_Date TIMESTAMP, 
  Content_Type varchar(256), 
  MailboxID INTEGER NOT NULL , 
  Search_Text CLOB NOT NULL , 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  Message_To varchar(1000), 
  Display_Text CLOB NOT NULL , 
  Message_Data_ID INTEGER NOT NULL , 
  CONSTRAINT d3177_MESSAGES_id_pk PRIMARY KEY(id) 
);
 select * from sys.syscolumns c,sys.systables t where c.referenceid = t.tableid and c.columnname='ID' and t.tablename='D3177_MESSAGES';
create table D3177_ATTACHMENTS ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  Inline INTEGER, 
  CRC32 BIGINT NOT NULL , 
  Attachment_Name varchar(256) NOT NULL , 
  Attachment_File varchar(512) NOT NULL , 
  Message_ID INTEGER NOT NULL , 
  Content_Type varchar(256) NOT NULL , 
  CONSTRAINT D3177_ATTACHMENTS_id_pk PRIMARY KEY(id) 
);
alter table D3177_ATTACHMENTS ADD CONSTRAINT ATTACHMENTS_Message_ID_MESSAGES_ID FOREIGN KEY ( Message_ID ) REFERENCES D3177_MESSAGES ( ID );
alter table D3177_MESSAGES ADD CONSTRAINT MESSAGES_MailboxID_Mailboxes_ID FOREIGN KEY ( MailboxID ) REFERENCES d3177_Mailboxes ( ID );
alter table D3177_MESSAGES ADD CONSTRAINT MESSAGES_Message_Data_ID_d3177_BinaryData_ID FOREIGN KEY ( Message_Data_ID ) REFERENCES d3177_BinaryData ( ID );
alter table d3177_Mailboxes ADD CONSTRAINT Mailboxes_MailServerID_MailServers_ID FOREIGN KEY ( MailServerID ) REFERENCES d3177_MailServers ( ID );

update d3177_SchemaVersion set version=1;

alter table D3177_MESSAGES alter Message_To SET DATA TYPE varchar(10000);
alter table D3177_MESSAGES alter Message_From SET DATA TYPE varchar(10000);
alter table D3177_MESSAGES alter Message_Cc SET DATA TYPE varchar(10000);
alter table D3177_MESSAGES alter Message_Bcc SET DATA TYPE varchar(10000);

 select * from sys.syscolumns c,sys.systables t where c.referenceid = t.tableid and c.columnname='ID' and t.tablename='D3177_MESSAGES';
update d3177_SchemaVersion set version=2;

create table D3177_MailStatistics ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  ProcessedCount INTEGER DEFAULT 0 NOT NULL , 
  HourOfDay INTEGER NOT NULL , 
  LastModified TIMESTAMP NOT NULL , 
  RejectedMailCount INTEGER DEFAULT 0 NOT NULL , 
  DayOfWeek INTEGER NOT NULL , 
  CONSTRAINT D3177_MailStatistics_id_pk PRIMARY KEY(id) 
);
CREATE INDEX D3177_MailStatistics_HourOfDay_idx ON D3177_MailStatistics(HourOfDay);
CREATE INDEX D3177_MailStatistics_DayOfWeek_idx ON D3177_MailStatistics(DayOfWeek);
alter table D3177_MESSAGES alter CONTENT_TYPE SET DATA TYPE varchar(256);

update d3177_SchemaVersion set version=3;

alter table D3177_messages alter column Message_ID NULL;

CREATE INDEX D3177_MESSAGES_Message_ID_idx ON D3177_MESSAGES(Message_ID);

update d3177_SchemaVersion set version=4;

alter table D3177_MESSAGES add filename varchar(256);
alter table D3177_MESSAGES add CRC32 BIGINT;
select id,crc32,data from d3177_BinaryData;

 select * from sys.syscolumns c,sys.systables t where c.referenceid = t.tableid and c.columnname='ID' and t.tablename='D3177_MESSAGES';
alter table D3177_messages alter column filename NOT NULL;
alter table D3177_messages alter column crc32 NOT NULL;
alter table D3177_messages alter column mailboxid NULL;
ALTER TABLE D3177_MESSAGES DROP CONSTRAINT MESSAGES_message_data_id_BinaryData_id;
alter table D3177_messages drop column message_data_id;
drop table d3177_BinaryData;

update d3177_SchemaVersion set version=6;

create table D3177_EmailAddresses ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  address varchar(256) NOT NULL , 
  CONSTRAINT D3177_EmailAddresses_id_pk PRIMARY KEY(id), 
  CONSTRAINT D3177_EmailAddresses_address_uq UNIQUE(address) 
);

CREATE UNIQUE INDEX D3177_EmailAddresses_address_idx ON D3177_EmailAddresses(address);

create table D3177_EmailAddressesToMessages ( 
  MessageID INTEGER NOT NULL , 
  EmailAddressID INTEGER NOT NULL  
);
alter table D3177_EmailAddressesToMessages ADD CONSTRAINT EmailAddressesToMessages_MessageID_Messages_ID FOREIGN KEY ( MessageID ) REFERENCES D3177_Messages ( ID );
alter table D3177_EmailAddressesToMessages ADD CONSTRAINT EmailAddressesToMessages_EmailAddressID_EmailAddresses_ID FOREIGN KEY ( EmailAddressID ) REFERENCES D3177_EmailAddresses ( ID );

create table AuthenticationServers ( 
  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), 
  port INTEGER NOT NULL , 
  protocol varchar(20) NOT NULL , 
  hostname varchar(40) NOT NULL , 
  CONSTRAINT AuthenticationServers_id_pk PRIMARY KEY(id) 
);
alter table d3177_Mailboxes add AuthenticationServerID INTEGER;
select id,filename from D3177_messages;

alter table D3177_MESSAGES drop column message_to;
alter table D3177_MESSAGES drop column message_cc;
alter table D3177_MESSAGES drop column message_from;

 select * from sys.syscolumns c,sys.systables t where c.referenceid = t.tableid and c.columnname='ID' and t.tablename='D3177_MESSAGES';

update d3177_SchemaVersion set version=7;
-- JIRA 2371: ensure that a non-numeric, non-autogenerated column can
-- have its default value modified:
create table t2371 ( a varchar(10));
describe t2371;
alter table t2371 alter column a default 'my val';
describe t2371;
insert into t2371 (a) values ('hi');
insert into t2371 (a) values (default);
alter table t2371 alter column a default 'another';
describe t2371;
insert into t2371 (a) values (default);
select * from t2371;

-- DERBY-3355: Exercise ALTER TABLE ... NOT NULL with table and column
-- names which are in mixed case. This is important because
-- AlterTableConstantAction.validateNotNullConstraint generates and
-- executes some SQL on-the-fly, and it's important that it properly
-- delimits the table and column names in that SQL. We also include a few
-- other "unusual" table and column names.

create table d3355 ( c1 varchar(10), "c2" varchar(10), c3 varchar(10));
create table "d3355_a" ( c1 varchar(10), "c2" varchar(10), c3 varchar(10));
create table d3355_qt_col ("""c""4" int, """""C5" int, "c 6" int);
create table "d3355_qt_""tab" ( c4 int, c5 int, c6 int); 
insert into d3355 values ('a', 'b', 'c');
insert into "d3355_a" values ('d', 'e', 'f');
insert into d3355_qt_col values (4, 5, 6);
insert into "d3355_qt_""tab" values (4, 5, 6);
-- All of these ALTER TABLE statements should succeed.
alter table d3355 alter column c1 not null;
alter table d3355 alter column "c2" not null;
alter table d3355 alter column "C3" not null;
alter table "d3355_a" alter column c1 not null;
alter table "d3355_a" alter column "c2" not null;
alter table "d3355_a" alter column "C3" not null;
alter table d3355_qt_col alter column """""C5" not null;
alter table d3355_qt_col alter column "c 6" not null;
alter table "d3355_qt_""tab" alter column c5 not null;
-- These ALTER TABLE statements should fail, with no-such-column and/or
-- no-such-table errors:
alter table d3355 alter column "c1" not null;
alter table d3355 alter column c2 not null;
alter table d3355_a alter column c1 not null;
alter table "d3355_a" alter column "c1" not null;

