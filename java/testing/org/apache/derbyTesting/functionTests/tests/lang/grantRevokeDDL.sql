connect 'grantRevokeDDL;create=true' user 'satheesh' as satConnection;

-- Test table privileges
create table satheesh.tsat(i int not null primary key, j int);
create index tsat_ind on satheesh.tsat(j);
create table satheesh.table1 (a int, b int, c char(10));
grant select on satheesh.tsat to public;
grant insert on satheesh.tsat to foo;
grant delete on satheesh.tsat to foo;
grant update on satheesh.tsat to foo;
grant update(i) on satheesh.tsat to bar;

select * from sys.systableperms;

connect 'grantRevokeDDL' user 'bar' as barConnection;

-- Following revokes should fail. Only owner can revoke permissions
revoke select on satheesh.tsat from public;
revoke insert on satheesh.tsat from foo;
revoke update(i) on satheesh.tsat from foo;
revoke update on satheesh.tsat from foo;
revoke delete on satheesh.tsat from foo;

set connection satConnection;

-- Revoke permissions not granted already
revoke trigger on satheesh.tsat from foo;
revoke references on satheesh.tsat from foo;

-- Following revokes should revoke permissions
revoke update on satheesh.tsat from foo;
revoke delete on satheesh.tsat from foo;

-- Check success by looking at systableperms directly for now
select * from sys.systableperms;

revoke insert on satheesh.tsat from foo;
revoke select on satheesh.tsat from public;

-- Check success by looking at systableperms directly for now
select * from sys.systableperms;

-- Test routine permissions

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT
NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

grant execute on function F_ABS to foo;
grant execute on function F_ABS(int) to bar;

revoke execute on function F_ABS(int) from bar RESTRICT;

drop function f_abs;

-- Tests with views
create view v1 as select * from tsat;

grant select on v1 to bar;
grant insert on v1 to foo;
grant update on v1 to public;

-- Tests for synonym. Not supported currently.
create synonym mySym for satheesh.tsat;

-- Expected to fail
grant select on mySym to bar;
grant insert on mySym to foo;

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

values f_abs(-5);

-- Test for AUTHORIZATION option for create schema
-- GrantRevoke TODO: Need to enforce who can create which schema.
-- More negative test cases need to be added once enforcing is done.

CREATE SCHEMA MYDODO AUTHORIZATION DODO;

CREATE SCHEMA AUTHORIZATION DERBY;

select * from sys.sysschemas where schemaname not like 'SYS%';

-- Now connect as different user and try to do DDLs in schema owned by satheesh
connect 'grantRevokeDDL;user=Swiper' as swiperConnection;

create table swiperTab (i int, j int);
insert into swiperTab values (1,1);

set schema satheesh;

-- All these DDLs should fail.

create table NotMyTable (i int, j int);

drop table tsat;
drop index tsat_ind;

create view myview as select * from satheesh.tsat;

CREATE FUNCTION FuncNotMySchema(P1 INT)
RETURNS INT NO SQL RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

alter table tsat add column k int;

create table swiper.mytab ( i int, j int);

set schema swiper;


-- Some simple DML tests. Should all fail.
select * from satheesh.tsat;
insert into satheesh.tsat values (1, 2);
update satheesh.tsat set i=j;

create table my_tsat (i int not null, c char(10), constraint fk foreign key(i) references satheesh.tsat);

-- Now grant some permissions to swiper

set connection satConnection;

grant select(i), update(j) on tsat to swiper;
grant all privileges on table1 to swiper;

grant references on tsat to swiper;

set connection swiperConnection;

-- Now some of these should pass

select * from satheesh.tsat;

select i from satheesh.tsat;
select i from satheesh.tsat where j=2;
select i from satheesh.tsat where 2 > (select count(i) from satheesh.tsat);
select i from satheesh.tsat where 2 > (select count(j) from satheesh.tsat);
select i from satheesh.tsat where 2 > (select count(*) from satheesh.tsat);

update satheesh.tsat set j=j+1;
update satheesh.tsat set j=2 where i=2;
update satheesh.tsat set j=2 where j=1;

select * from satheesh.table1;
select c from satheesh.table1 t1, satheesh.tsat t2 where t1.a = t2.i;
select b from satheesh.table1 t1, satheesh.tsat t2 where t1.a = t2.j;

select * from satheesh.table1, (select i from satheesh.tsat) table2;
select * from satheesh.table1, (select j from satheesh.tsat) table2;

-- GrantRevoke TODO: This one should pass, but currently fails. Bind update expression in two steps.
update satheesh.tsat set j=i; 

create table my_tsat (i int not null, c char(10), constraint fk foreign key(i) references satheesh.tsat);

-- Some TRIGGER privilege checks. See GrantRevoke.java for more tests
set connection swiperConnection;
-- Should fail
create trigger trig_sat1 after update on satheesh.tsat for each statement mode db2sql values 1;
create trigger trig_sat2 no cascade before delete on satheesh.tsat for each statement mode db2sql values 1;

-- Grant trigger privilege
set connection satConnection;
grant trigger on tsat to swiper;

-- Try now
set connection swiperConnection;
create trigger trig_sat1 after update on satheesh.tsat for each statement mode db2sql values 1;
create trigger trig_sat2 no cascade before delete on satheesh.tsat for each statement mode db2sql values 1;

drop trigger trig_sat1;
drop trigger trig_sat2;

-- Now revoke and try again
set connection satConnection;
revoke trigger on tsat from swiper;

set connection swiperConnection;
create trigger trig_sat1 after update on satheesh.tsat for each statement mode db2sql values 1;
create trigger trig_sat2 no cascade before delete on satheesh.tsat for each statement mode db2sql values 1;

-- Now grant access to public and try again
set connection satConnection;
grant trigger on tsat to public;

set connection swiperConnection;
create trigger trig_sat1 after update on satheesh.tsat for each statement mode db2sql values 1;
create trigger trig_sat2 no cascade before delete on satheesh.tsat for each statement mode db2sql values 1;

drop trigger trig_sat1;
drop trigger trig_sat2;

-- Some simple routine tests. See GrantRevoke.java for more tests
set connection satConnection;

values f_abs(-5);

select f_abs(-4) from sys.systables where tablename like 'SYSTAB%';

-- Same tests should fail
set connection swiperConnection;
set schema satheesh;

values f_abs(-5);

select f_abs(-4) from sys.systables where tablename like 'SYSTAB%';

-- Now grant execute permission and try again

set connection satConnection;

grant execute on function f_abs to swiper;

set connection swiperConnection;

-- Should pass now
values f_abs(-5);

select f_abs(-4) from sys.systables where tablename like 'SYSTAB%';

-- Now revoke permission and try

set connection satConnection;
revoke execute on function f_abs from swiper RESTRICT;

set connection swiperConnection;
values f_abs(-5);
select f_abs(-4) from sys.systables where tablename like 'SYSTAB%';

-- Now try public permission
set connection satConnection;
grant execute on function f_abs to public;
set connection swiperConnection;

-- Should pass again
values f_abs(-5);

select f_abs(-4) from sys.systables where tablename like 'SYSTAB%';

-- Test schema creation authorization checks

set connection swiperConnection;

-- Negative tests. Should all fail
create schema myFriend;
create schema mySchema authorization me;
create schema myschema authorization swiper;

connect 'grantRevokeDDL;user=sam';
create schema sam authorization swiper;

-- Should pass
create schema authorization sam;

connect 'grantRevokeDDL;user=george';
create schema george;

-- Now try as DBA (satheesh)
set connection satConnection;

create schema myFriend;
create schema mySchema authorization me;
create schema authorization testSchema;

select * from sys.sysschemas;

-- Test implicit creation of schemas.. Should fail
set connection swiperConnection;
create table mywork.t1(i int);
create view mywork.v1 as select * from swiper.swiperTab;

-- Implicit schema creation should only work if creating own schema
connect 'grantRevokeDDL;user=monica' as monicaConnection;
create table mywork.t1 ( i int);
create table monica.shouldPass(c char(10));

-- Check if DBA can ignore all privilege checks

set connection swiperConnection;

set schema swiper;

revoke select on swiperTab from satheesh;

revoke insert on swiperTab from satheesh;

set connection satConnection;

-- Should still work, as satheesh is DBA
select * from swiper.swiperTab;
insert into swiper.swiperTab values (2,2);
select * from swiper.swiperTab;

grant select on swiper.swiperTab to sam;
revoke insert on swiper.swiperTab from satheesh;


-- Test system routines. Some don't need explicit grant and others do
-- allowing for only DBA use by default

set connection satConnection;

-- Try granting or revoking from system tables. Should fail

grant select on sys.systables to sam;
grant delete on sys.syscolumns to sam;
grant update(alias) on sys.sysaliases to swiper;
revoke all privileges on sys.systableperms from public;
revoke trigger on sys.sysroutineperms from sam;

-- Try granting or revoking from system routines that is expected fail

grant execute on procedure sysibm.sqlprocedures to sam;
revoke execute on procedure sysibm.sqlcamessage from public restrict;

-- Try positive tests
connect 'grantRevokeDDL;user=sam' as samConnection;

create table samTable(i int);
insert into samTable values 1,2,3,4,5,6,7;

-- Following should pass... PUBLIC should have access to these
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('SAM', 'SAMTABLE', 1);
call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('SAM', 'SAMTABLE', 1, 1, 1);

-- Try compressing tables not owned...
-- INPLACE_COMPRESS currently passes, pending DERBY-1062
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('SWIPER', 'MYTAB', 1);
call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('SWIPER', 'MYTAB', 1, 1, 1);

-- Try other system routines. All should fail

call SYSCS_UTIL.SYSCS_EXPORT_TABLE('SAM', 'SAMTABLE' , 'extinout/table.dat', null, null, null);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.pageSize');

-- Try after DBA grants permissions
set connection satConnection;

grant execute on procedure SYSCS_UTIL.SYSCS_EXPORT_TABLE to public;
grant execute on procedure SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY to sam;
grant execute on function SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY to sam;

-- Now these should pass
call SYSCS_UTIL.SYSCS_EXPORT_TABLE('SAM', 'SAMTABLE' , 'extinout/table.dat', null, null, null);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.pageSize');

-- grant one permission on table to user1 and another permission to user3,
-- then grant another permission on that same table to user1 and 
-- user2(this is the first permission to user2 on the table) and user3 
-- (this user already has the permission being granted). Notice that 
-- the first 2 grant statements created a row in SYSTABLEPERMS for 
-- user1 and user3. Third grant is going to update the pre-existing
-- row for user1. The third grant is going to insert a new row for 
-- user2 in SYSTABLEPERMS and the third grant is going to be a no-op 
-- for user3. 
-- So, basically, this is to test that one single grant statment can
-- update and insert and no-op rows into SYSTABLEPERMS for different users.
connect 'grantRevokeDDL;create=true' user 'mamta1' as mamta1;
create table t11 (c111 int not null primary key);
insert into t11 values(1);
grant select on t11 to mamta2;
grant insert on t11 to mamta3;
grant insert on t11 to mamta2, mamta3, mamta4;
connect 'grantRevokeDDL;create=true' user 'mamta2' as mamta2;
select * from mamta1.t11;
insert into mamta1.t11 values(2);
select * from mamta1.t11;
connect 'grantRevokeDDL;create=true' user 'mamta3' as mamta3;
-- following select will fail because no permissions
select * from mamta1.t11;
insert into mamta1.t11 values(3);
connect 'grantRevokeDDL;create=true' user 'mamta4' as mamta4;
-- following select will fail because no permissions
select * from mamta1.t11;
insert into mamta1.t11 values(4);
set connection mamta1;
revoke all privileges on t11 from PUBLIC;
select * from mamta1.t11;
drop table t11;

-- now test the column level permissions
set connection mamta1;
create table t11 (c111 int not null primary key, c112 int, c113 int, c114 int);
insert into t11 values(1,1,1,1);
grant select(c111) on t11 to mamta2;
grant select(c112) on t11 to mamta2, mamta3;
grant update(c112) on t11 to mamta2, mamta3, mamta4;
grant update on t11 to mamta2;
set connection mamta2;
update mamta1.t11 set c113 = 2 where c111=1;
select c111,c112 from mamta1.t11;
-- following will fail because no select permissions on all the columns
select * from mamta1.t11;
set connection mamta3;
-- following will fail because no update permission on column c113
update mamta1.t11 set c113=3;
select c112 from mamta1.t11;
set connection mamta4;
-- following will fail because no select permission on column c112
select c112 from mamta1.t11;
set connection mamta1;
select * from mamta1.t11;
revoke select on t11 from mamta2, mamta3, mamta4;
revoke update(c111, c112) on t11 from mamta2, mamta3, mamta4;
drop table t11;

-- Testing views to make sure we collect their depedencies on privileges in SYSDEPENDS table
set connection mamta1;
create table t11 (c111 int not null primary key);
insert into t11 values(1);
insert into t11 values(2);
select * from t11;
create table t12 (c121 int, c122 char);
insert into t12 values (1,'1');
select * from t12;
create table t13 (c131 int, c132 char);
insert into t13 values (1,'1');
select * from t13;
grant select on t12 to mamta2;
grant select on t11 to public;

set connection mamta2;
-- both of following will pass because mamt2 has has required privileges because of PUBLIC select access of mamta1.t11.
create view v21 as select t1.c111, t2.c122 from mamta1.t11 as t1, mamta1.t12 as t2;
create view v22 as select * from mamta1.t11;
create view v23 as select * from mamta1.t12;

set connection mamta1;
-- When the create view v23 from mamta2's session is executed in mamta1, there will be only
--    one row in sysdepends for view v23. That row will be for view's dependency on t12.
--    There will be no row for privilege dependency because table t12 is owned by the same
--    user who is creating the view v23 and hence there is no privilege required.
create view v23 as select * from mamta1.t12;

-- satConnection is dba and hence doesn't need explicit privileges to access ojects in any schema within the database
set connection satConnection; 
-- since satConnection is dba, following will not fail even if satConnection has no explicit privilege to mamta2.v22
create view v11 as select * from mamta2.v22;
set connection mamta3;
create table t31(c311 int);
-- since mamta3 is not dba, following will fail because no access to mamta2.v22
create view v31 as select * from mamta2.v22;
-- mamta3 has access to mamta1.t11 since there is PUBLIC select access on that table but there is no access to mamta2.v22
create view v32 as select v22.c111 as a, t11.c111 as b from mamta2.v22 v22, mamta1.t11 t11;
-- Try to create a view with no privilege to more than one object. 
create view v33 as select v22.c111 as a, t11.c111 as b from mamta2.v22 v22, mamta1.t11 t11, mamta2.v21;

-- connect as mamta2 and give select privilege on v22 to mamta3
set connection mamta2;
grant select on v22 to mamta3;
set connection mamta3;
-- mamta3 has the required privileges now, so following should work
create view v31 as select * from mamta2.v22;
-- following will pass because mamta3 has direct access to v22 and public access to t11
create view v32 as select v22.c111 as a, t11.c111 as b from mamta2.v22 v22, mamta1.t11 t11;
-- following will still fail because mamta3 doesn't have access to mamta1.t12.c121
create view v33 as select v22.c111 as a, t12.c121 as b from mamta2.v22 v22, mamta1.t12 t12;

-- connect as mamta2 and give select privilege on v23 to mamta3
set connection mamta2;
grant select on v23 to mamta3;
set connection mamta3;
-- although mamta3 doesn't have direct access to mamta1.t12, it can look at it through view mamta2.v23 since mamta3 has select privilege
-- on mamta2.v23
create view v34 as select * from mamta2.v23;
-- following should work fine because mamta3 has access to all the
-- objects in it's schema
create view v35 as select * from v34;

-- Write some views based on a routine
set connection mamta1;
CREATE FUNCTION F_ABS1(P1 INT)
	RETURNS INT NO SQL
	RETURNS NULL ON NULL INPUT
	EXTERNAL NAME 'java.lang.Math.abs'
	LANGUAGE JAVA PARAMETER STYLE JAVA;
values f_abs1(-5);
create view v11(c111) as values mamta1.f_abs1(-5);
grant select on v11 to mamta2;
select * from v11;
set connection mamta2;
create view v24 as select * from mamta1.v11;
select * from v24;
-- following will fail because no execute permissions on mamta1.f_abs
create view v25(c251) as (values mamta1.f_abs1(-1));
set connection mamta1;
grant execute on function f_abs1 to mamta2;
set connection mamta2;
create view v25(c251) as (values mamta1.f_abs1(-1));
select * from v25;

-- try column level privileges and views
-- In this test, user has permission on one column but not on the other
set connection mamta1;
create table t14(c141 int, c142 int);
insert into t14 values (1,1), (2,2);
grant select(c141) on t14 to mamta2;
set connection mamta2;
-- following will fail because no access on column mamta1.t14.c142
create view v26 as (select * from mamta1.t14 where c142=1);
-- following will fail for the same reason
create view v26 as (select c141 from mamta1.t14 where c142=1);
-- following will pass because view is based on column that it can access
create view v27 as (select c141 from mamta1.t14);
select * from v27;
set connection mamta1;
-- give access to all the columns in t14 to mamta2
grant select on t14 to mamta2;
set connection mamta2;
-- now following will pass
create view v26 as (select c141 from mamta1.t14 where c142=1);
select * from v26;

-- in this column level privilege test, there is a user level permission on one column
--   and a PUBLIC level on the other column. 
set connection mamta1;
create table t15(c151 int, c152 int);
insert into t15 values(1,1),(2,2);
grant select(c151) on t15 to mamta2;
grant select(c152) on t15 to public;
set connection mamta2;
create view v28 as (select c152 from mamta1.t15 where c151=1);

-- create trigger privilege collection
-- TriggerTest
-- first grant one column level privilege at user level and another at public level and then define the trigger
set connection mamta1;
drop table t11TriggerTest;
create table t11TriggerTest (c111 int not null primary key, c112 int);
insert into t11TriggerTest values(1,1);
insert into t11TriggerTest values(2,2);
grant select(c111) on t11TriggerTest to mamta2;
grant select(c112) on t11TriggerTest to public;
set connection mamta2;
drop table t21TriggerTest;
create table t21TriggerTest (c211 int); 
drop table t22TriggerTest;
create table t22TriggerTest (c221 int); 
-- following should pass because all the privileges are in places
create trigger tr21t21TriggerTest after insert on t21TriggerTest for each statement mode db2sql
	insert into t22TriggerTest values (select c111 from mamta1.t11TriggerTest where c112=1);
insert into t21TriggerTest values(1);
select * from t21TriggerTest;
select * from t22TriggerTest;
drop table t21TriggerTest;
drop table t22TriggerTest;

-- grant all the privileges at the table level and then define the trigger
set connection mamta1;
drop table t11TriggerTest;
create table t11TriggerTest (c111 int not null primary key);
insert into t11TriggerTest values(1);
insert into t11TriggerTest values(2);
create table t12RoutineTest (c121 int);
insert into t12RoutineTest values (1),(2);
grant select on t11TriggerTest to mamta2;
grant insert on t12RoutineTest to mamta2;
select * from t11TriggerTest;
select * from t12RoutineTest;
set connection mamta2;
create table t21TriggerTest (c211 int); 
-- following should pass because all the privileges are in places
create trigger tr21t21TriggerTest after insert on t21TriggerTest for each statement mode db2sql
	insert into mamta1.t12RoutineTest values (select c111 from mamta1.t11TriggerTest where c111=1);
-- this insert's trigger will cause a new row in mamta1.t12RoutineTest
insert into t21TriggerTest values(1);
select * from t21TriggerTest;
set connection mamta1;
select * from t11TriggerTest;
select * from t12RoutineTest;
set connection mamta2;
-- following should fail because mamta2 doesn't have trigger permission on mamta1.t11TriggerTest
create trigger tr11t11TriggerTest after insert on mamta1.t11TriggerTest for each statement mode db2sql
        insert into mamta1.t12RoutineTest values (1);
set connection mamta1;
grant trigger on t11TriggerTest to mamta2;
set connection mamta2;
-- following will pass now because mamta2 has trigger permission on mamta1.t11TriggerTest
create trigger tr11t11TriggerTest after insert on mamta1.t11TriggerTest for each statement mode db2sql
        insert into mamta1.t12RoutineTest values (1);
-- following will fail becuae mamta2 has TRIGGER privilege but not INSERT privilege on mamta1.t11TriggerTest
insert into mamta1.t11TriggerTest values(3);
set connection mamta1;
delete from t11TriggerTest;
delete from t12RoutineTest;
insert into mamta1.t11TriggerTest values(3);
select * from t11TriggerTest;
select * from t12RoutineTest;
drop table t11TriggerTest;
drop table t12RoutineTest;

-- Test routine and trigger combination. Thing to note is triggers always
--   run with definer's privileges whereas routines always run with
--   session user's privileges
set connection mamta1;
drop table t12RoutineTest;
create table t12RoutineTest (c121 int);
insert into t12RoutineTest values (1),(2);
drop table t13TriggerTest;
create table t13TriggerTest (c131 int);
insert into t13TriggerTest values (1),(2);
grant select on t12RoutineTest to mamta3;
grant insert on t13TriggerTest to mamta3;
drop function selectFromSpecificSchema;
CREATE FUNCTION selectFromSpecificSchema (P1 INT)
        RETURNS INT 
        RETURNS NULL ON NULL INPUT
        EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectFromSpecificSchema'
        LANGUAGE JAVA PARAMETER STYLE JAVA;
grant execute on function selectFromSpecificSchema to mamta3;
set connection mamta3;
drop table t31TriggerTest;
create table t31TriggerTest(c11 int);
-- following will pass because all the required privileges are in place for mamta3
create trigger tr31t31 after insert on t31TriggerTest for each statement mode db2sql
        insert into mamta1.t13TriggerTest values (values mamta1.selectFromSpecificSchema(1));
-- following insert will cause a row to be inserted into mamta1.t13TriggerTest if the session user
--    has SELECT privilege on mamta1.t12RoutineTest. This shows that although triggers execute
--    with definer privileges, routines always execute with session user's privilege, even when 
--    called by an object which runs with definer's privilege 
insert into t31TriggerTest values(1);
select * from t31TriggerTest;
set connection mamta1;
select * from t12RoutineTest;
select * from t13TriggerTest;
set connection mamta2;
-- will fail because mamta2 doesn't have INSERT privilege on mamta3.t31TriggerTest
insert into mamta3.t31TriggerTest values(1);
set connection mamta3;
grant insert on t31TriggerTest to mamta2;
set connection mamta2;
-- should still fail because trigger on mamta3.t31TriggerTest accesses a routine which
--   accesses a table on which mamta2 doesn't have SELECT privilege on. mamta3 doesn't
--   need execute privilege on routine because it is getting accessed by trigger which runs
--   with the definer privilege. But the routine itself never runs with definer privilege and
--   hence the session user needs access to objects accessed by the routine.
insert into mamta3.t31TriggerTest values(1);
set connection mamta1;
grant select on t12RoutineTest to mamta2;
set connection mamta2;
-- mamta2 got the SELECT privilege on mamta1.t12RoutineTest and hence following insert should pass
insert into mamta3.t31TriggerTest values(1);
set connection mamta3;
select * from t31TriggerTest;
set connection mamta1;
select * from t12RoutineTest;
select * from t13TriggerTest;

-- Test routine and view combination. Thing to note is views always
--   run with definer's privileges whereas routines always run with
--   session user's privileges. So, eventhough a routine might be
--   getting accessed by a view which is running with definer's
--   privileges, during the routine execution, the session user's
--   privileges will get used.
set connection mamta1;
drop table t12RoutineTest;
create table t12RoutineTest (c121 int);
insert into t12RoutineTest values (1),(2);
grant select on t12RoutineTest to mamta3;
drop function selectFromSpecificSchema;
CREATE FUNCTION selectFromSpecificSchema (P1 INT)
        RETURNS INT 
        RETURNS NULL ON NULL INPUT
        EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectFromSpecificSchema'
        LANGUAGE JAVA PARAMETER STYLE JAVA;
grant execute on function selectFromSpecificSchema to mamta3;
set connection mamta3;
drop view v21ViewTest;
-- following will succeed because mamta3 has EXECUTE privileges on the function
create view v21ViewTest(c211) as values mamta1.selectFromSpecificSchema(1);
select * from v21ViewTest; 
grant select on v21ViewTest to mamta2;
set connection mamta2;
-- Although mamta2 has SELECT privileges on mamta3.v21ViewTest, mamta2 doesn't have
--    SELECT privileges on table mamta1.t12RoutineTest accessed by the routine
--    (which is underneath the view) and hence select from view will fail
select * from mamta3.v21ViewTest; 
set connection mamta1;
grant select  on t12RoutineTest to mamta2;
set connection mamta2;
-- now the view select will succeed
select * from mamta3.v21ViewTest; 

-- In this test, the trigger is accessing a view. Any user that has insert privilege
--  on trigger table will be able to make an insert even if that user doesn't have
--  privileges on objects referenced by the trigger.
set connection mamta1;
drop table t11TriggerTest;
create table t11TriggerTest (c111 int not null primary key);
insert into t11TriggerTest values(1);
insert into t11TriggerTest values(2);
grant select on t11TriggerTest to mamta2;
set connection mamta2;
drop view v21ViewTest;
create view v21ViewTest as select * from mamta1.t11TriggerTest;
grant select on v21ViewTest to mamta4;
set connection mamta3;
drop table t31TriggerTest;
create table t31TriggerTest (c311 int);
grant insert on t31TriggerTest to mamta4;
set connection mamta4;
drop table t41TriggerTest;
create table t41TriggerTest (c411 int);
drop trigger tr41t41;
create trigger tr41t41 after insert on t41TriggerTest for each statement mode db2sql
        insert into mamta3.t31TriggerTest (select * from mamta2.v21ViewTest);
insert into t41TriggerTest values(1);
insert into t41TriggerTest values(2);
select * from t41TriggerTest;
set connection mamta1;
select * from t11TriggerTest;
set connection mamta2;
select * from v21ViewTest;
set connection mamta3;
select * from t31TriggerTest;
-- will fail because no permissions on mamta4.t41TriggerTest
insert into mamta4.t41TriggerTest values(1);
-- will fail because no permissions on mamta2.v21ViewTest
select * from mamta2.v21ViewTest;
-- will fail because no permissions on mamta1.t11TriggerTest
select * from mamta1.t11TriggerTest;
set connection mamta4;
grant insert on t41TriggerTest to mamta3;
set connection mamta3;
-- although mamta3 doesn't have access to the objects referenced by the insert trigger
--   following insert will still pass because triggers run with definer's privileges.
insert into mamta4.t41TriggerTest values(1);

-- Test constraints
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null primary key);
insert into t11ConstraintTest values(1);
insert into t11ConstraintTest values(2);
grant references on t11ConstraintTest to mamta3;
set connection mamta2;
drop table t21ConstraintTest;
create table t21ConstraintTest (c111 int not null primary key);
insert into t21ConstraintTest values(1);
insert into t21ConstraintTest values(2);
grant references on t21ConstraintTest to mamta3;
set connection mamta3;
create table t31ConstraintTest (c311 int references mamta1.t11ConstraintTest, c312 int references mamta2.t21ConstraintTest);
drop table t31ConstraintTest;

-- multi-key foreign key constraint and the REFERENCES privilege granted at user level. This should cause only
--   one row in SYSDEPENDS for REFERENCES privilege.
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null, c112 int not null, primary key (c111, c112));
grant references on t11ConstraintTest to mamta3;
set connection mamta3;
drop table t31ConstraintTest;
create table t31ConstraintTest (c311 int, c312 int, foreign key(c311, c312) references mamta1.t11ConstraintTest);
drop table t31ConstraintTest;

-- Same test as above with multi-key foreign key constraint but one column REFERENCES privilege granted at user level
--   and other column REFERENCES privilege granted at PUBLIC level. This should cause two rows in SYSDEPENDS for REFERENCES privilege.
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null, c112 int not null, primary key (c111, c112));
grant references(c111) on t11ConstraintTest to mamta3;
grant references(c112) on t11ConstraintTest to PUBLIC;
--connect 'jdbc:derby:c:/dellater/dbmaintest2;create=true' user 'mamta3' as mamta3;
set connection mamta3;
drop table t31ConstraintTest;
create table t31ConstraintTest (c311 int,  c312 int, foreign key(c311, c312) references mamta1.t11ConstraintTest);
drop table t31ConstraintTest;
-- Same test as above with multi-key foreign key constraint, one column REFERENCES privilege granted at user level
--   and other column REFERENCES privilege granted at PUBLIC level. This should cause two rows in SYSDEPENDS for REFERENCES privilege.
--   But foreign key reference is added using alter table rather than at create table time
create table t31constrainttest(c311 int, c312 int);
alter table t31constrainttest add foreign key (c311, c312) references mamta1.t11constrainttest;
drop table t31ConstraintTest;
-- create the table again, but this time one foreign key constraint on one table with single column primary key and
--   another foreign key constraint on another table with multi-column primary key
create table t31constrainttest(c311 int, c312 int, c313 int references mamta2.t21ConstraintTest);
alter table t31constrainttest add foreign key (c311, c312) references mamta1.t11constrainttest;


-- revoke of TRIGGERS and other privileges should drop dependent triggers
set connection mamta1;
drop table t11TriggerRevokeTest;
create table t11TriggerRevokeTest (c111 int not null primary key);
insert into t11TriggerRevokeTest values(1),(2);
-- mamta2 is later going to create an insert trigger on t11TriggerRevokeTest 
grant TRIGGER on t11TriggerRevokeTest to mamta2;
set connection mamta2;
drop table t21TriggerRevokeTest;
create table t21TriggerRevokeTest (c211 int); 
-- following will pass because mamta2 has trigger permission on mamta1.t11TriggerRevokeTest
create trigger tr11t11 after insert on mamta1.t11TriggerRevokeTest for each statement mode db2sql
        insert into t21TriggerRevokeTest values(99);
-- no data in the table in which trigger is going to insert
select * from t21TriggerRevokeTest;
set connection mamta1;
-- insert trigger will fire
insert into t11TriggerRevokeTest values(3);
set connection mamta2;
-- trigger inserted one row into following table
select * from t21TriggerRevokeTest;
set connection mamta1;
-- this revoke is going to drop dependent trigger
revoke trigger on t11TriggerRevokeTest from mamta2;
-- following insert won't fire an insert trigger because one doesn't exist
insert into t11TriggerRevokeTest values(4);
set connection mamta2;
-- no more rows inserted since last check
select * from t21TriggerRevokeTest;
-- following attempt to create insert trigger again will fail because trigger privilege has been revoked.
create trigger tr11t11 after insert on mamta1.t11TriggerRevokeTest for each statement mode db2sql
        insert into t21TriggerRevokeTest values(99);
set connection mamta1;
grant trigger on t11TriggerRevokeTest to mamta2;
set connection mamta2;
-- following attempt to create insert trigger again will pass because mamta2 has got the necessary trigger privilege.
create trigger tr11t11 after insert on mamta1.t11TriggerRevokeTest for each statement mode db2sql
        insert into t21TriggerRevokeTest values(99);
select * from t21TriggerRevokeTest;
set connection mamta1;
-- insert trigger should get fired
insert into t11TriggerRevokeTest values(5);
set connection mamta2;
-- Should be one more row since last check because insert trigger is back in action
select * from t21TriggerRevokeTest;
drop table t21TriggerRevokeTest;
set connection mamta1;
-- this revoke is going to drop dependent trigger
revoke trigger on t11TriggerRevokeTest from mamta2;
-- following insert won't fire an insert trigger because one doesn't exist
insert into t11TriggerRevokeTest values(6);
-- cleanup
drop table t11TriggerRevokeTest;


-- Define a trigger on a table, then revoke a privilege on the table which trigger doesn't
-- really depend on. The trigger still gets dropped automatically. This will be fixed in
-- subsequent patch
set connection mamta1;
drop table t11TriggerRevokeTest;
create table t11TriggerRevokeTest (c111 int not null primary key);
insert into t11TriggerRevokeTest values(1),(2);
grant SELECT on t11TriggerRevokeTest to mamta2;
-- mamta2 is later going to create an insert trigger on t11TriggerRevokeTest 
grant TRIGGER on t11TriggerRevokeTest to mamta2;
set connection mamta2;
drop table t21TriggerRevokeTest;
create table t21TriggerRevokeTest (c211 int); 
-- following will pass because mamta2 has trigger permission on mamta1.t11TriggerRevokeTest
create trigger tr11t11 after insert on mamta1.t11TriggerRevokeTest for each statement mode db2sql
        insert into t21TriggerRevokeTest values(99);
-- no data in the table in which trigger is going to insert
select * from t21TriggerRevokeTest;
set connection mamta1;
-- insert trigger will fire
insert into t11TriggerRevokeTest values(3);
set connection mamta2;
-- trigger inserted one row into following table
select * from t21TriggerRevokeTest;
set connection mamta1;
-- this revoke is going to drop dependent trigger on the table although dependent trigger does not
-- need this particular permission 
-- WILL FIX THIS IN A SUBSEQUENT PATCH****************************************************************************************
revoke SELECT on t11TriggerRevokeTest from mamta2;
-- following insert won't fire an insert trigger because one doesn't exist
insert into t11TriggerRevokeTest values(4);
set connection mamta2;
-- no more rows inserted since last check
select * from t21TriggerRevokeTest;
-- following attempt to create insert trigger again will pas because TRIGGER privilege was never revoked.
create trigger tr11t11 after insert on mamta1.t11TriggerRevokeTest for each statement mode db2sql
        insert into t21TriggerRevokeTest values(99);
set connection mamta1;
-- insert trigger should get fired
insert into t11TriggerRevokeTest values(5);
set connection mamta2;
-- Should be one more row since last check because insert trigger is back in action
select * from t21TriggerRevokeTest;
drop table t21TriggerRevokeTest;
set connection mamta1;
-- this revoke is going to drop dependent trigger
revoke trigger on t11TriggerRevokeTest from mamta2;
-- following insert won't fire an insert trigger because one doesn't exist
insert into t11TriggerRevokeTest values(6);
-- cleanup
drop table t11TriggerRevokeTest;


--- Test automatic dropping of dependent permission descriptors when objects they refer to is dropped.
--- Dropping of a table, for example, should drop all table and column permission descriptors on it.

create table newTable(i int, j int, k int);

grant select, update(j) on newTable to sammy;

grant references, delete on newTable to user1;

-- Try with a view

create view myView as select * from newTable;

grant select on myView to sammy;

select * from sys.systableperms where grantee='SAMMY' or grantee='USER1';

select * from sys.syscolperms where grantee='SAMMY' or grantee='USER1';

drop view myView;

select * from sys.systableperms where grantee='SAMMY' or grantee='USER1';

drop table newTable;

select * from sys.systableperms where grantee='SAMMY' or grantee='USER1';

select * from sys.syscolperms where grantee='SAMMY' or grantee='USER1';

--- Try droping of a routine with permission descriptors. Should get dropped
CREATE FUNCTION newFunction(P1 INT)
        RETURNS INT 
        RETURNS NULL ON NULL INPUT
        EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectFromSpecificSchema'
        LANGUAGE JAVA PARAMETER STYLE JAVA;

grant execute on function newFunction to sammy;

grant execute on function newFunction(INT) to user3;

select * from sys.sysroutineperms where grantee='SAMMY' or grantee='USER3';

drop function newFunction;

select * from sys.sysroutineperms where grantee='SAMMY' or grantee='USER3';


-- Try the same tests after a permission descriptor is likely to have been cached

create table newTable(i int, j int, k int);

grant select(i,j), delete on newTable to sammy;

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

grant execute on function f_abs to sammy;

select * from sys.sysroutineperms where grantee='SAMMY';

select * from sys.syscolperms where grantee='SAMMY';

select * from sys.systableperms where grantee='SAMMY';

-- Now connect as sammy and access database objects. That should create
-- PermissionsDescriptors and cache them
connect 'grantRevokeDDL' user 'sammy' as sammyConnection;

set schema mamta1;

select i,j from newTable;

values f_abs(-5);

set connection mamta1;

drop table newTable;

drop function f_abs;

-- Confirm rows in catalogs are gone
select * from sys.sysroutineperms where grantee='SAMMY';

select * from sys.syscolperms where grantee='SAMMY';

select * from sys.systableperms where grantee='SAMMY';


-- DERBY-1608: Recognize new SYSFUC routines as system builtin routines
-- Builtin functions don't need any permission checking. They are executable by all

VALUES { fn ACOS(0.0707) }; 

VALUES ACOS(0.0707); 

VALUES PI();

create table SYSFUN_MATH_TEST (d double);
insert into SYSFUN_MATH_TEST values null;
insert into SYSFUN_MATH_TEST values 0.67;
insert into SYSFUN_MATH_TEST values 1.34;

select cast (ATAN(d) as DECIMAL(6,3)) AS ATAN FROM SYSFUN_MATH_TEST;

select cast (COS(d) as DECIMAL(6,3)) AS COS FROM SYSFUN_MATH_TEST;
select cast (SIN(d) as DECIMAL(6,3)) AS SIN FROM SYSFUN_MATH_TEST;
select cast (TAN(d) as DECIMAL(6,3)) AS TAN FROM SYSFUN_MATH_TEST;

select cast (DEGREES(d) as DECIMAL(6,3)) AS DEGREES FROM SYSFUN_MATH_TEST;
select cast (RADIANS(d) as DECIMAL(6,3)) AS RADIANS FROM SYSFUN_MATH_TEST;
