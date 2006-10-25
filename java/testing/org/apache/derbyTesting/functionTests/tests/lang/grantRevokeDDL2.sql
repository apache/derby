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
-- ------------------------------------------------------------------- 
-- GRANT and REVOKE test Part 2
-- -------------------------------------------------------------------
connect 'grantRevokeDDL2;create=true' user 'user1' as user1;
connect 'grantRevokeDDL2' user 'user2' as user2;
connect 'grantRevokeDDL2' user 'user3' as user3;
connect 'grantRevokeDDL2' user 'user4' as user4;
connect 'grantRevokeDDL2' user 'user5' as user5;
 
-- DERBY-1729
-- test grant and revoke in Java stored procedure with triggers.
-- Java stored procedure that contains grant or revoke statement 
-- requires MODIFIES SQL DATA to execute.
-- Since only 2 of the 8 Java stored procedures(which contains
-- grant or revoke statement) are declared with MODIFIES SQL DATA, 
-- the rest are expected to fail in this test.
 
-- setup the environment
set connection user1;

-- table used in the procedures
drop table t1;
create table t1 (i int primary key, b char(15));
insert into t1 values (1, 'XYZ');
insert into t1 values (2, 'XYZ');
insert into t1 values (3, 'XYZ');
insert into t1 values (4, 'XYZ');
insert into t1 values (5, 'XYZ');
insert into t1 values (6, 'XYZ');
insert into t1 values (7, 'XYZ');
insert into t1 values (8, 'XYZ');

-- table used in this test
drop table t2;
create table t2 (x integer, y integer);

create procedure grant_select_proc1() 
       parameter style java
       dynamic result sets 0 language java 
       NO SQL
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure grant_select_proc2() 
       parameter style java
       dynamic result sets 0 language java 
       CONTAINS SQL
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure grant_select_proc3() 
       parameter style java
       dynamic result sets 0 language java 
       READS SQL DATA 
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure grant_select_proc4() 
       parameter style java
       dynamic result sets 0 language java 
       MODIFIES SQL DATA  
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure revoke_select_proc1() 
       parameter style java
       dynamic result sets 0 language java 
       NO SQL  
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

create procedure revoke_select_proc2() 
       parameter style java
       dynamic result sets 0 language java 
       CONTAINS SQL  
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

create procedure revoke_select_proc3() 
       parameter style java
       dynamic result sets 0 language java 
       READS SQL DATA 
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

create procedure revoke_select_proc4() 
       parameter style java
       dynamic result sets 0 language java 
       MODIFIES SQL DATA 
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

-- tests

create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc1();
-- should fail
delete from t1 where i = 1;
-- check delete failed
select * from t1 where i = 1;
drop trigger grant_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc2();
-- should fail
delete from t1 where i = 2;
-- check delete failed
select * from t1 where i = 2; 
drop trigger grant_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc3();
-- should fail
delete from t1 where i = 3;
-- check delete failed
select * from t1 where i = 3; 
drop trigger grant_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc4();
-- ok
delete from t1 where i = 4;
-- check delete
select * from t1 where i = 4;
drop trigger grant_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc1();
-- should fail
delete from t1 where i = 5;
-- check delete failed
select * from t1 where i = 5;
drop trigger revoke_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc2();
-- should fail
delete from t1 where i = 6;
-- check delete failed
select * from t1 where i = 6;
drop trigger revoke_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc3();
-- should fail
delete from t1 where i = 7;
-- check delete failed
select * from t1 where i = 7; 
drop trigger revoke_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc4();
-- ok
delete from t1 where i = 8;
-- check delete 
select * from t1 where i = 8; 
drop trigger revoke_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
drop table t2;
drop table t1;
 
-- -------------------------------------------------------------------
-- table privileges (tp)
-- -------------------------------------------------------------------
set connection user1;
create table t1 (c1 int primary key not null, c2 varchar(10));
create table t2 (c1 int primary key not null, c2 varchar(10), c3 int);
create index idx1 on t1(c2);
insert into t1 values (1, 'a'), (2, 'b'), (3, 'c');
insert into t2 values (1, 'Yip', 10);
select * from t1;
CREATE FUNCTION F_ABS1(P1 INT)
	RETURNS INT NO SQL
	RETURNS NULL ON NULL INPUT
	EXTERNAL NAME 'java.lang.Math.abs'
	LANGUAGE JAVA PARAMETER STYLE JAVA;
values f_abs1(-5);
-- grant on a non-existing table, expect error
grant select on table t0 to user2;
-- revoke on a non-existing table, expect error
revoke select on table t0 from user2;
-- grant more than one table, expect error
grant select on t0, t1 to user2; 
-- revoke more than one table, expect error
revoke select on t0, t1 from user2;
-- revoking privilege that has not been granted, expect warning
revoke select,insert,update,delete,trigger,references on t1 from user2;
-- syntax errors, expect errors
grant select on t1 from user2;
revoke select on t1 to user2;
-- redundant but ok
grant select, select on t1 to user2;
revoke select, select on t1 from user2;

-- switch to user2
set connection user2;
-- test SELECT privilege, expect error
select * from user1.t1;
-- test INSERT privilege, expect error
insert into user1.t1(c1) values 4;
-- test UPDATE privilege, expect error
update user1.t1 set c1=10;
-- test DELETE privilege, expect error
delete from user1.t1;
-- test REFERENCES privilege, expect error
create table t2 (c1 int primary key not null, c2 int references user1.t1);
-- test TRIGGER privilege, expect error
create trigger trigger1 after update on user1.t1 for each statement mode db2sql values integer('123');
-- try to DROP user1.idx1 index, expect error
drop index user1.idx1;
-- try to DROP user1.t1 table, expect error
drop table user1.t1;
-- non privileged user try to grant privileges on user1.t1, expect error
grant select,insert,delete,update,references,trigger on user1.t1 to user2;
-- try to grant privileges for public on user1.t1, expect error
grant select,insert,delete,update,references,trigger on user1.t1 to public;
-- try to grant all privileges for user2 on user1.t1, expect error
grant ALL PRIVILEGES on user1.t1 to user2;
-- try to grant all privileges on user1.t1 to public, expect error
grant ALL PRIVILEGES on user1.t1 to public;
-- try to revoke user1 from table user1.t1, expect error
revoke select,insert,delete,update,references,trigger on user1.t1 from user1;
-- try to revoke all privileges from user1 on table user1.t1, expect error
revoke ALL PRIVILEGES on user1.t1 from user1;
-- try to revoke execute on a non-existing function on user1.t1, expect error
revoke execute on function user1.f1 from user1 restrict;

create table t2 (c1 int);
-- try revoking yourself from user2.t2, expect error
revoke select on t2 from user2;
-- try granting yourself again on user2.t2, expect error. Why?
grant select on t2 to user2;
-- try granting yourself multiple times, expect error.  Why?
grant insert on t2 to user2,user2,user2;

-- try to execute user1.F_ABS1, expect error
values user1.F_ABS1(-9);

set connection user1;
select * from sys.systableperms;
select * from sys.syscolperms;
select * from sys.sysroutineperms; 

grant select,update on table t1 to user2, user3;
grant execute on function F_ABS1 to user2; 
select * from sys.systableperms;
select * from sys.syscolperms;
select * from sys.sysroutineperms;

set connection user2;
-- try to select from t1, ok
select * from user1.t1;
-- try to insert from t1, expect error
insert into user1.t1 values (5, 'e');
-- ok
values user1.F_ABS1(-8);
-- ok
update user1.t1 set c2 = 'user2';

set connection user1;
-- add a column to t1, user2 should still be able to select
alter table t1 add column c3 varchar(10);
set connection user2;
-- ok
select * from user1.t1;
-- error
insert into user1.t1 values (2, 'abc', 'ABC');
-- ok
update user1.t1 set c3 = 'XYZ';

set connection user3;
-- try to select from t1, ok
select * from user1.t1;
-- user3 does not have permission to execute, expect error
values user1.F_ABS1(-8);
-- ok
update user1.t1 set c2 = 'user3';

set connection user1;
-- expect warnings
revoke update(c2) on t1 from user3;
revoke select(c2) on t1 from user3;

set connection user2;
-- ok
update user1.t1 set c2 = 'user2';

set connection user3;
-- revoking part of table privilege raises warning, so ok
update user1.t1 set c2 = 'user3';
-- same as above
select * from user1.t1;
-- same as above
select c2 from user1.t1;

set connection user1;
grant select, update on t1 to PUBLIC;
set connection user3;
-- ok, use PUBLIC 
select * from user1.t1;
-- ok, use PUBLIC 
update user1.t1 set c2 = 'user3';

set connection user1;
grant select on t1 to user3;
-- revoke select from PUBLIC
revoke select on t1 from PUBLIC;
set connection user3;
-- ok, privileged
select * from user1.t1;
-- ok, use PUBLIC 
update user1.t1 set c2 = 'user3';
set connection user1;
revoke select, update on t1 from user3;
revoke update on t1 from PUBLIC;
set connection user3;
-- expect error
select * from user1.t1;
-- expect error 
update user1.t1 set c2 = 'user3';

set connection user1;
declare global temporary table SESSION.t1(c1 int) not logged;
-- expect error
grant select on session.t1 to user2;
revoke select on session.t1 from user2;

-- -------------------------------------------------------------------
-- column privileges 
-- -------------------------------------------------------------------
set connection user1;
create table t3 (c1 int, c2 varchar(10), c3 int);
create table t4 (c1 int, c2 varchar(10), c3 int);
-- grant table select privilege then revoke partially 
grant select, update on t3 to user2;
-- expect warning
revoke select(c1) on t3 from user2;
revoke update(c2) on t3 from user2;
set connection user2;
select * from user1.t3;

set connection user1;
grant select (c2, c3), update (c2), insert on t4 to user2;
set connection user2;
-- expect error
select * from user1.t4;
-- expect error
select c1 from user1.t4;
-- ok
select c2, c3 from user1.t4;
-- expect error
update user1.t4 set c1=10, c3=100;
-- ok
update user1.t4 set c2='XYZ';
set connection user1;

-- DERBY-1847
-- alter table t4 add column c4 int;
-- set connection user2;
-- expect error
-- select c4 from user1.t4;
-- ok
-- select c2 from user1.t4;

set connection user1;
-- revoke all columns
revoke select, update on t4 from user2;
set connection user2;
-- expect error
select c2 from user1.t4;
-- expect error
update user1.t4 set c2='ABC';

-- -------------------------------------------------------------------
-- schemas
-- -------------------------------------------------------------------
set connection user2;
-- expect error
create table myschema.t5 (i int);
-- ok
create table user2.t5 (i int);

-- expect error
CREATE SCHEMA w3 AUTHORIZATION user2;
create table w3.t1 (i int);

-- expect error, already exists
CREATE SCHEMA AUTHORIZATION user2;
   
-- expect error
CREATE SCHEMA myschema;

-- expect error
CREATE SCHEMA user2;

set connection user1;
-- ok
CREATE SCHEMA w3 AUTHORIZATION user2;
CREATE SCHEMA AUTHORIZATION user6;
CREATE SCHEMA myschema;

-- DERBY-1858
set connection user5;
-- expect error
DROP SCHEMA w3 RESTRICT;

-- -------------------------------------------------------------------
-- views
-- -------------------------------------------------------------------
set connection user1;
create view sv1 as select * from sys.systables;
set connection user2;
-- expect error
select tablename from user1.sv1;
set connection user1;
grant select on sv1 to user2;
set connection user2;
-- ok
select tablename from user1.sv1;

set connection user1;
create table ta (i int);
insert into ta values 1,2,3;
create view sva as select * from ta;
create table tb (j int);
insert into tb values 2,3,4;
create view svb as select * from tb;
grant select on sva to user2;
set connection user2;
-- expect error
create view svc (i) as select * from user1.sva union select * from user1.svb;
set connection user1;
grant select on svb to user2;
set connection user2;
-- ok
create view svc (i) as select * from user1.sva union select * from user1.svb;
select * from svc;

-- DERBY-1715, DERBY-1631
--set connection user1;
--create table t01 (i int);
--insert into t01 values 1;
--grant select on t01 to user2;
--set connection user2;
--select * from user1.t01;
--create view v01 as select * from user1.t01;
--create view v02 as select * from user2.v01;
--create view v03 as select * from user2.v02;
--set connection user1;
--revoke select on t01 from user2;
--set connection user2;
--select * from user1.t01;
--select * from user2.v01;
--select * from user2.v02;
--select * from user2.v03;
--drop view user2.v01;
--drop view user2.v02;
--drop view user3.v03;

-- grant all privileges then create the view
set connection user1;
create table t01ap (i int);
insert into t01ap values 1;
grant all privileges on t01ap to user2;
set connection user2;
-- ok
create view v02ap as select * from user1.t01ap;
-- ok
select * from v02ap;

-- expect error, don't have with grant option
grant select on user2.v02ap to user3;
set connection user3;
-- expect error
create view v03ap as select * from user2.v02ap;
select * from v03ap;
-- expect error
grant all privileges on v03ap to user4;
set connection user4;
-- expect error
create view v04ap as select * from user3.v03ap;
select * from v04ap;
-- expect error
grant select on v04ap to user2;

set connection user2;
select * from user4.v04ap;

set connection user4;
-- expect error
revoke select on v04ap from user2;

set connection user2;
-- expect error
select * from user4.v04ap;

-- -------------------------------------------------------------------
-- references and constraints
-- -------------------------------------------------------------------
set connection user1;
drop table user1.rt1;
drop table user2.rt2;
create table rt1 (c1 int not null primary key, c2 int not null);
insert into rt1 values (1, 10);
insert into rt1 values (2, 20);
set connection user2;
-- expect error
create table rt2 (c1 int primary key not null, c2 int not null, c3 int not null, constraint rt2fk foreign key(c1) references user1.rt1);
set connection user1;
grant references on rt1 to user2;
set connection user2;
-- ok
create table rt2 (c1 int primary key not null, c2 int not null, c3 int not null, constraint rt2fk foreign key(c2) references user1.rt1);
insert into rt2 values (1,1,1);
-- expect error
insert into rt2 values (3,3,3);
set connection user1;
revoke references on rt1 from user2;
set connection user2;
-- ok, fk constraint got dropped by revoke
insert into rt2 values (3,3,3);
select * from rt2;
-- expect errors
create table rt3 (c1 int primary key not null, c2 int not null, c3 int not null, constraint rt3fk foreign key(c1) references user1.rt1);

-- test PUBLIC
-- DERBY-1857
--set connection user1;
--drop table user3.rt3;
--drop table user2.rt2;
--drop table user1.rt1;
--create table rt1 (c1 int primary key not null, c2 int not null unique, c3 int not null);
--insert into rt1 values (1,1,1);
--insert into rt1 values (2,2,2);
--insert into rt1 values (3,3,3);
--grant references(c2, c1) on rt1 to PUBLIC;
--set connection user2;
--create table rt2 (c1 int primary key not null, constraint rt2fk foreign key(c1) references user1.rt1(c2) );
--insert into rt2 values (1), (2);
--set connection user3;
--create table rt3 (c1 int primary key not null, constraint rt3fk foreign key(c1) references user1.rt1(c2) );
--insert into rt3 values (1), (2);
--set connection user1;
--revoke references(c1) on rt1 from PUBLIC;
--set connection user2;
-- expect constraint error
--insert into rt2 values (4);
--set connection user3;
-- expect constraint error
--insert into rt3 values (4);

-- test user privilege and PUBLIC
set connection user1;
drop table user3.rt3;
drop table user2.rt2;
drop table user1.rt1;
create table rt1 (c1 int primary key not null, c2 int);
insert into rt1 values (1,1), (2,2);
grant references on rt1 to PUBLIC, user2, user3;
set connection user2;
create table rt2 (c1 int primary key not null, constraint rt2fk foreign key(c1) references user1.rt1);
insert into rt2 values (1), (2);
set connection user3;
create table rt3 (c1 int primary key not null, constraint rt3fk foreign key(c1) references user1.rt1);
insert into rt3 values (1), (2);
set connection user1;
-- ok, use the privilege granted to user2
revoke references on rt1 from PUBLIC;
-- ok, user3 got no privileges, so rt3fk should get dropped.  
revoke references on rt1 from user3;
set connection user2;
-- expect error, FK enforced.
insert into rt2 values (3);
set connection user3;
-- ok
insert into rt3 values (3);

-- test multiple FKs
-- DERBY-1589?
--set connection user1;
--drop table user3.rt3;
--drop table user2.rt2;
--drop table user1.rt1;
--create table rt1 (c1 int primary key not null, c2 int);
--insert into rt1 values (1,1), (2,2);
--grant references on rt1 to PUBLIC, user2, user3;
--set connection user2;
-- XJ001 occurred at create table rt2...
--create table rt2 (c1 int primary key not null, constraint rt2fk foreign key(c1) references user1.rt1);
--insert into rt2 values (1), (2);
--grant references on rt2 to PUBLIC, user3;
--set connection user3;
--create table rt3 (c1 int primary key not null, constraint rt3fk1 foreign key(c1) references user1.rt1, 
--	constraint rt3fk2 foreign key(c1) references user1.rt2);
--insert into rt3 values (1), (2);

--set connection user1;
-- rt3fk1 should get dropped.
--revoke references on rt1 from PUBLIC;
--revoke references on rt1 from user3;

--set connection user2;
--revoke references on rt2 from PUBLIC;

-- expect error
--insert into rt2 values (3);
--set connection user3;
-- expect error, use user3 references privilege, rt3fk2 still in effect
--insert into rt3 values (3);
--set connection user2;
--revoke references on rt2 from user3;
--set connection user3;
-- ok, rt3fk2 should be dropped.
--insert into rt3 values (3);

-- -------------------------------------------------------------------
-- routines and standard builtins
-- -------------------------------------------------------------------
set connection user1;
CREATE FUNCTION F_ABS2(P1 INT)
	RETURNS INT NO SQL
	RETURNS NULL ON NULL INPUT
	EXTERNAL NAME 'java.lang.Math.abs'
	LANGUAGE JAVA PARAMETER STYLE JAVA;
	
-- syntax error
grant execute on F_ABS2 to user2;
-- F_ABS2 is not a procedure, expect errors
grant execute on procedure F_ABS2 to user2;

set connection user2;
-- expect errors
values user1.F_ABS1(10) + user1.F_ABS2(-10);
set connection user1;
-- ok
grant execute on function F_ABS2 to user2;
set connection user2;
-- ok
values user1.F_ABS1(10) + user1.F_ABS2(-10);

-- expect errors
revoke execute on function ABS from user2 restrict;
revoke execute on function AVG from user2 restrict;
revoke execute on function LENGTH from user2 restrict;

set connection user1;
-- ok
revoke execute on function F_ABS2 from user2 restrict;
revoke execute on function F_ABS1 from user2 restrict;

set connection user2;
-- expect error
values user1.F_ABS1(10) + user1.F_ABS2(-10);

set connection user1;
-- ok
grant execute on function F_ABS1 to PUBLIC;
grant execute on function F_ABS2 to PUBLIC;

set connection user2;
-- ok
values user1.F_ABS1(10) + user1.F_ABS2(-10);

-- -------------------------------------------------------------------
-- system tables
-- -------------------------------------------------------------------
set connection user1;
-- not allowed. expect errors, sanity check
grant ALL PRIVILEGES on sys.sysaliases to user2;
grant ALL PRIVILEGES on sys.syschecks to user2;
grant ALL PRIVILEGES on sys.syscolperms to user2;
grant ALL PRIVILEGES on sys.syscolumns to user2;
grant ALL PRIVILEGES on sys.sysconglomerates to user2;
grant ALL PRIVILEGES on sys.sysconstraints to user2;
grant ALL PRIVILEGES on sys.sysdepends to user2;
grant ALL PRIVILEGES on sys.sysfiles to user2;
grant ALL PRIVILEGES on sys.sysforeignkeys to user2;
grant ALL PRIVILEGES on sys.syskeys to user2;
grant ALL PRIVILEGES on sys.sysroutineperms to user2;
grant ALL PRIVILEGES on sys.sysschemas to user2;
grant ALL PRIVILEGES on sys.sysstatistics to user2;
grant ALL PRIVILEGES on sys.sysstatements to user2;
grant ALL PRIVILEGES on sys.systableperms to user2;
grant ALL PRIVILEGES on sys.systables to user2;
grant ALL PRIVILEGES on sys.systriggers to user2;
grant ALL PRIVILEGES on sys.sysviews to user2;
grant ALL PRIVILEGES on syscs_diag.lock_table to user2;

grant select on sys.sysaliases to user2, public;
grant select on sys.syschecks to user2, public;
grant select on sys.syscolperms to user2, public;
grant select on sys.syscolumns to user2, public;
grant select on sys.sysconglomerates to user2, public;
grant select on sys.sysconstraints to user2, public;
grant select on sys.sysdepends to user2, public;
grant select on sys.sysfiles to user2, public;
grant select on sys.sysforeignkeys to user2, public;
grant select on sys.syskeys to user2, public;
grant select on sys.sysroutineperms to user2, public;
grant select on sys.sysschemas to user2, public;
grant select on sys.sysstatistics to user2, public;
grant select on sys.sysstatements to user2, public;
grant select on sys.systableperms to user2, public;
grant select on sys.systables to user2, public;
grant select on sys.systriggers to user2, public;
grant select on sys.sysviews to user2, public;
grant select on syscs_diag.lock_table to user2, public;

revoke ALL PRIVILEGES on sys.sysaliases from user2;
revoke ALL PRIVILEGES on sys.syschecks from user2;
revoke ALL PRIVILEGES on sys.syscolperms from user2;
revoke ALL PRIVILEGES on sys.syscolumns from user2;
revoke ALL PRIVILEGES on sys.sysconglomerates from user2;
revoke ALL PRIVILEGES on sys.sysconstraints from user2;
revoke ALL PRIVILEGES on sys.sysdepends from user2;
revoke ALL PRIVILEGES on sys.sysfiles from user2;
revoke ALL PRIVILEGES on sys.sysforeignkeys from user2;
revoke ALL PRIVILEGES on sys.syskeys from user2;
revoke ALL PRIVILEGES on sys.sysroutineperms from user2;
revoke ALL PRIVILEGES on sys.sysschemas from user2;
revoke ALL PRIVILEGES on sys.sysstatistics from user2;
revoke ALL PRIVILEGES on sys.sysstatements from user2;
revoke ALL PRIVILEGES on sys.systableperms from user2;
revoke ALL PRIVILEGES on sys.systables from user2;
revoke ALL PRIVILEGES on sys.systriggers from user2;
revoke ALL PRIVILEGES on sys.sysviews from user2;
revoke ALL PRIVILEGES on syscs_diag.lock_table from user2;

revoke select on sys.sysaliases from user2, public;
revoke select on sys.syschecks from user2, public;
revoke select on sys.syscolperms from user2, public;
revoke select on sys.syscolumns from user2, public;
revoke select on sys.sysconglomerates from user2, public;
revoke select on sys.sysconstraints from user2, public;
revoke select on sys.sysdepends from user2, public;
revoke select on sys.sysfiles from user2, public;
revoke select on sys.sysforeignkeys from user2, public;
revoke select on sys.syskeys from user2, public;
revoke select on sys.sysroutineperms from user2, public;
revoke select on sys.sysschemas from user2, public;
revoke select on sys.sysstatistics from user2, public;
revoke select on sys.sysstatements from user2, public;
revoke select on sys.systableperms from user2, public;
revoke select on sys.systables from user2, public;
revoke select on sys.systriggers from user2, public;
revoke select on sys.sysviews from user2, public;
revoke select on syscs_diag.lock_table from user2, public;

-- -------------------------------------------------------------------
-- built-in functions and procedures and routines
-- -------------------------------------------------------------------

set connection user3;
-- test sqlj, only db owner have privileges by default
-- expect errors
CALL SQLJ.INSTALL_JAR ('bogus.jar','user2.bogus',0);
CALL SQLJ.REPLACE_JAR ('bogus1.jar', 'user2.bogus');
CALL SQLJ.REMOVE_JAR  ('user2.bogus', 0);

-- test backup routines, only db owner have privileges by default
-- expect errors
CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE('backup1');
CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE('backup3', 1);
CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT('backup4', 1);

-- test admin routines, only db owner have privileges by default
CALL SYSCS_UTIL.SYSCS_FREEZE_DATABASE();
CALL SYSCS_UTIL.SYSCS_UNFREEZE_DATABASE();
CALL SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();

-- test statistical routines, available for everyone by default
set connection user1;
-- ok
CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);
CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0);

-- ok
set connection user3;
CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);
CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0);

-- test import/export, only db owner have privileges by default
create table TABLEIMP1 (i int);
create table TABLEEXP1 (i int);
insert into TABLEEXP1 values 1,2,3,4,5;
CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE ('USER3', 'TABLEEXP1', 'myfile.del', null, null, null);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE ('USER3', 'TABLEIMP1', 'myfile.del', null, null, null, 0);
CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from user3.TABLEEXP1','myfile.del', null, null, null);
CALL SYSCS_UTIL.SYSCS_IMPORT_DATA ('USER3', 'TABLEIMP1', null, '1,3,4', 'myfile.del', null, null, null,0);

-- test property handling routines, only db owner have privileges by default
-- expect errors
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY ('derby.locks.deadlockTimeout', '10');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.locks.deadlockTimeout');

-- test compress routines, everyone have privilege as long as the user owns the schema
-- ok
CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE('USER3', 'TABLEEXP1', 1);
call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('USER3', 'TABLEEXP1', 1, 1, 1);

-- test check table routines, only db owner have privilege by default
VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('USER3', 'TABLEEXP1');

-- -------------------------------------------------------------------
-- synonyms
-- -------------------------------------------------------------------
set connection user1;
create synonym s1 for user1.t1;
create index ii1 on user1.t1(c2);

-- not supported yet, expect errors
grant select on s1 to user2;
grant insert on s1 to user2;
revoke select on s1 from user2;
revoke insert on s1 from user2;

set connection user2;
-- expect errors
drop synonym user1.s1;
drop index user1.ii1;

-- -------------------------------------------------------------------
-- transactions and lock table stmt
-- -------------------------------------------------------------------
set connection user1;
create table t1000 (i int);
autocommit off;
grant select on t1000 to user2;
set connection user2;
select * from user1.t1000;
set connection user1;
commit;

set connection user2;
-- ok
select * from user1.t1000;
set connection user1;
revoke select on t1000 from user2;
set connection user2;
select * from user1.t1000;
set connection user1;
commit;
set connection user2;
select * from user1.t1000;
autocommit off;
-- should fail
lock table user1.t1000 in share mode;
-- should fail
lock table user1.t1000 in exclusive mode;
commit;
autocommit on;

set connection user1;
grant select on t1000 to user2;
rollback;
set connection user2;
select * from user1.t1000;

set connection user1;
grant select on t1000 to user2;
commit;
revoke select on t1000 from user2;
rollback;
set connection user2;
select * from user1.t1000;
set connection user1;
autocommit on;
drop table t1000;

set connection user1;
create table t1000 (c varchar(1));
insert into t1000 values 'a', 'b', 'c';
grant select on t1000 to user3;
set connection user2;
create table t1001 (i int);
insert into t1001 values 1;
set connection user1;
select * from user2.t1001;
insert into user2.t1001 values 2;
update user2.t1001 set i = 888;
drop table user1.t1000;
drop table user2.t1001;
commit;
autocommit on;

-- -------------------------------------------------------------------
-- cursors
-- -------------------------------------------------------------------
-- DERBY-1716
--set connection user1;
--drop table t1001;
--create table t1001 (c varchar(1));
--insert into t1001 values 'a', 'b', 'c';
--grant select on t1001 to user3;
--set connection user3;
--autocommit off;
--GET CURSOR crs1 AS 'select * from user1.t1001';
--next crs1;
--set connection user1;
-- revoke select privilege while user3 still have an open cursor
--revoke select on t1001 from user3;
--set connection user3;
--next crs1;
--next crs1;
--close crs1;
--autocommit on;
-- -------------------------------------------------------------------
-- rename table 
-- -------------------------------------------------------------------
set connection user1;
drop table user1.rta;
drop table user2.rtb;
create table rta (i int);
grant select on rta to user2;
set connection user2;
select * from user1.rta;
set connection user1;
rename table rta to rtb;
set connection user1;
-- expect error
select * from user1.rta;
-- ok
select * from user1.rtb;
set connection user2;
-- expect error
select * from user1.rta;
-- ok
select * from user1.rtb;
-- -------------------------------------------------------------------
-- DB owner power =)
-- -------------------------------------------------------------------
set connection user2;
create table ttt1 (i int);
insert into ttt1 values 1;
set connection user3;
create table ttt1 (i int);
insert into ttt1 values 10;
set connection user1;
-- the following actions are ok
select * from user2.ttt1;
insert into user2.ttt1 values 2;
update user2.ttt1 set i = 888;
delete from user2.ttt1;
drop table user2.ttt1;
select * from user3.ttt1;
insert into user3.ttt1 values 20;
update user3.ttt1 set i = 999;
delete from user3.ttt1;
drop table user3.ttt1;

set connection user4;
create table ttt1 (i int);
set connection user1;
drop table user4.ttt1;

set connection user2;
-- DERBY-1858
-- expect error
drop schema user4 restrict;

set connection user1;
-- ok
drop schema user4 restrict;

-- -------------------------------------------------------------------
-- Statement preparation
-- -------------------------------------------------------------------
set connection user1;
create table ttt2 (i int);
insert into ttt2 values 8;

set connection user2;
-- prepare statement, ok
prepare p1 as 'select * from user1.ttt2';
-- expect error
execute p1;
remove p1;

set connection user1;
grant select on ttt2 to user2;
set connection user2;
-- prepare statement, ok
prepare p1 as 'select * from user1.ttt2';
-- ok
execute p1;
set connection user1;
revoke select on ttt2 from user2;
set connection user2;
-- expect error
execute p1;
remove p1;

-- -------------------------------------------------------------------
-- Misc 
-- -------------------------------------------------------------------
set connection user2;
create table tshared0 (i int);
-- db owner tries to revoke select access from user2
set connection user1;
-- expect error
revoke select on user2.tshared0 from user2;
set connection user2;
select * from user2.tshared0;

set connection user2;
create table tshared1 (i int);
grant select, insert, delete, update on tshared1 to user3, user4, user5;
set connection user3;
create table tshared1 (i int);
grant select, insert, delete, update on tshared1 to user2, user4, user5;
set connection user2;
insert into user3.tshared1 values 1,2,3;
update user3.tshared1 set i = 888;
select * from user3.tshared1;
delete from user3.tshared1;
insert into user3.tshared1 values 1,2,3;
set connection user3;
insert into user2.tshared1 values 3,2,1;
update user2.tshared1 set i = 999;
select * from user2.tshared1;
delete from user2.tshared1;
insert into user2.tshared1 values 3,2,1;
set connection user1;
update user2.tshared1 set i = 1000;
update user3.tshared1 set i = 1001;
delete from user2.tshared1;
delete from user3.tshared1;
insert into user2.tshared1 values 0,1,2,3;
insert into user3.tshared1 values 4,3,2,1;
set connection user4;
select * from user2.tshared1;
select * from user3.tshared1;
create view vshared1 as select * from user2.tshared1 union select * from user3.tshared1;
create view vshared2 as select * from user2.tshared1 intersect select * from user3.tshared1;
create view vshared3 as select * from user2.tshared1 except select * from user3.tshared1;
create view vshared4(i) as select * from user3.tshared1 union values 0;
insert into user2.tshared1 select * from user3.tshared1;
select * from vshared1;
select * from vshared2;
select * from vshared3;
select * from vshared4;
-- expect errors
grant select on vshared1 to user5;
grant select on vshared2 to user5;
grant select on vshared3 to user5;
grant select on vshared4 to user5;
set connection user5;
select * from user4.vshared1;
select * from user4.vshared2;
select * from user4.vshared3;
select * from user4.vshared4;
set connection user1;

-- -------------------------------------------------------------------
-- triggers
-- -------------------------------------------------------------------
set connection user1;

-- expect error
create trigger tt0a after insert on t1 for each statement mode db2sql grant select on t1 to user2;
-- expect error
create trigger tt0b after insert on t1 for each statement mode db2sql revoke select on t1 from user2;

-- same schema in trigger action
drop table t6;
create table t6 (c1 int not null primary key, c2 int);
grant trigger on t6 to user2;
set connection user2;
drop table t7;
create table t7 (c1 int, c2 int, c3 int);
insert into t7 values (1,1,1);
create trigger tt1 after insert on user1.t6 for each statement mode db2sql update user2.t7 set c2 = 888; 
create trigger tt2 after insert on user1.t6 for each statement mode db2sql insert into user2.t7 values (2,2,2); 

set connection user1;
insert into t6 values (1, 10);
select * from user2.t7;

-- different schema in trigger action
-- this testcase is causing NPE - DERBY-1583
set connection user1;
drop table t8;
drop table t9;
create table t8 (c1 int not null primary key, c2 int);
create table t9 (c1 int, c2 int, c3 int);
insert into user1.t8 values (1,1);
insert into user1.t9 values (10,10,10);
grant trigger on t8 to user2;
grant update(c2, c1), insert on t9 to user2;
set connection user2;
create trigger tt3 after insert on user1.t8 for each statement mode db2sql update user1.t9 set c2 = 888; 
create trigger tt4 after insert on user1.t8 for each statement mode db2sql insert into user1.t9 values (2,2,2); 
set connection user1;
-- expect error
insert into user1.t8 values (1, 10);
-- ok
insert into user1.t8 values (2, 20);
select * from user1.t9;

-- grant all privileges then create trigger, then revoke the trigger privilege
drop table t10;
drop table t11;
create table t10 (i int, j int);
insert into t10 values (1,1), (2,2);
create table t11 (i int);
grant all privileges on t10 to user2;
grant all privileges on t11 to user2;
set connection user2;
-- ok
create trigger tt5 after update on user1.t10 for each statement mode db2sql insert into user1.t11 values 1;
create trigger tt6 after update of i on user1.t10 for each statement mode db2sql insert into user1.t11 values 2;
create trigger tt7 after update of j on user1.t10 for each statement mode db2sql insert into user1.t11 values 3;

update user1.t10 set i=10;
select * from user1.t10;
select * from user1.t11;

set connection user1;
-- triggers get dropped
revoke trigger on t10 from user2;
set connection user2;

update user1.t10 set i=20;
select * from user1.t10;
select * from user1.t11;

set connection user1;
grant trigger on t10 to user2;

set connection user2;
create trigger tt8 after update of j on user1.t10 for each statement mode db2sql delete from user1.t11;

update user1.t10 set j=100;
select * from user1.t10;
select * from user1.t11;
delete from user1.t10;
delete from user1.t11;

-- test trigger, view and function combo
set connection user1;
drop function F_ABS1;
CREATE FUNCTION F_ABS1(P1 INT)
	RETURNS INT NO SQL
	RETURNS NULL ON NULL INPUT
	EXTERNAL NAME 'java.lang.Math.abs'
	LANGUAGE JAVA PARAMETER STYLE JAVA;

grant execute on function F_ABS1 to user5; 
grant trigger,insert,update,delete,select on t10 to user5;	
grant trigger,insert,update,delete,select on t11 to user5;	
drop view v;
create view v(i) as values 888;
grant select on v to user5;

set connection user5;
create trigger tt9 after insert on user1.t10 for each statement mode db2sql insert into user1.t11 values (user1.F_ABS1(-5));
create trigger tt10 after insert on user1.t10 for each statement mode db2sql insert into user1.t11 select * from user1.v;

insert into user1.t10 values (1,1);
select * from user1.t10;
select * from user1.t11;

-- Related to DERBY-1631 
-- cannot revoke execution on F_ABS1 due to X0Y25 (object dependencies)
--set connection user1;
--revoke execute on function F_ABS1 from user5 restrict;

--set connection user5;
--insert into user1.t10 values (2,2);
--select * from user1.t10;
--select * from user1.t11;

--set connection user1;
--revoke select on v from user5;

--set connection user5;
--insert into user1.t10 values (3,3);
--select * from user1.t10;
--select * from user1.t11;
--set connection user1;
--drop view v;
set connection user1;
