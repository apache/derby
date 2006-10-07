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

-- Revoke table permissions not granted already. This should raise warnings.
revoke trigger on satheesh.tsat from foo;
revoke references on satheesh.tsat from foo;
-- This should raise warnings for bar
revoke insert on satheesh.tsat from foo, bar;
-- This should raise warnings for both foo and bar
revoke insert on satheesh.tsat from foo, bar;
grant insert on satheesh.tsat to foo;

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

-- Revoke routine permission not granted already. This should raise a warning.
revoke execute on function F_ABS(int) from bar RESTRICT;

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
-- should fail
grant select on v22 to mamta3;
set connection mamta3;
-- should fail
create view v31 as select * from mamta2.v22;
-- following will fail because mamta3 has no access to v22
create view v32 as select v22.c111 as a, t11.c111 as b from mamta2.v22 v22, mamta1.t11 t11;
-- following will still fail because mamta3 doesn't have access to mamta1.t12.c121
create view v33 as select v22.c111 as a, t12.c121 as b from mamta2.v22 v22, mamta1.t12 t12;

-- connect as mamta2 and give select privilege on v23 to mamta3
set connection mamta2;
grant select on v23 to mamta3;
set connection mamta3;
-- should fail
create view v34 as select * from mamta2.v23;
-- should fail
create view v35 as select * from v34;

-- Write some views based on a routine
set connection mamta1;
drop function f_abs1;
CREATE FUNCTION F_ABS1(P1 INT)
	RETURNS INT NO SQL
	RETURNS NULL ON NULL INPUT
	EXTERNAL NAME 'java.lang.Math.abs'
	LANGUAGE JAVA PARAMETER STYLE JAVA;
values f_abs1(-5);
drop view v11;
create view v11(c111) as values mamta1.f_abs1(-5);
grant select on v11 to mamta2;
select * from v11;
set connection mamta2;
drop view v24;
create view v24 as select * from mamta1.v11;
select * from v24;
drop view v25;
-- following will fail because no execute permissions on mamta1.f_abs1
create view v25(c251) as (values mamta1.f_abs1(-1));
set connection mamta1;
grant execute on function f_abs1 to mamta2;
set connection mamta2;
-- this view creation will pass now because have execute privileges on the function
create view v25(c251) as (values mamta1.f_abs1(-1));
select * from v25;
set connection mamta1;
-- try revoke execute privilege. Since there are dependent objects, the revoke shold fail
revoke execute on function f_abs1 from mamta2 restrict;
-- drop the dependent objects on the execute privilege and then try to revoke the execute privilege
set connection mamta2;
drop view v25;
set connection mamta1;
-- revoke execute privilege should pass this time because no dependents on that permission.
revoke execute on function f_abs1 from mamta2 restrict;
set connection mamta2;
-- following select should still pass because v24 is not directly dependent on the execute permission.
--   It gets to the routine via view v11 which will be run with definer's privileges and definer of
--   view v11 is also the owner of the routine
select * from v24;
-- cleanup
drop view v24;
set connection mamta1;
drop view v11;
drop function f_abs1;

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

-- write some view based tests and revoke privileges to see if the right thing happens
-- View tests
-- test1 
--  A simple test where a user creates a view based on objects in other schemas and revoke privilege on one of those
--  objects will drop the view
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
insert into t11ViewTest values(2);
drop table t12ViewTest;
create table t12ViewTest (c121 int, c122 char);
insert into t12ViewTest values (1,'1');
-- user mamta2 is going to create a view based on following grants
grant select on t12ViewTest to mamta2;
grant select on t11ViewTest to public;
set connection mamta2;
drop view v21ViewTest;
-- will succeed because all the required privileges are in place
create view v21ViewTest as select t1.c111, t2.c122 from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2;
select * from v21ViewTest;
set connection mamta1;
-- this revoke should drop the dependent view in schema mamta2
revoke select on t11ViewTest from public;
set connection mamta2;
-- the view shouldn't exist anymore because one of the privileges required by it was revoked
select * from v21ViewTest;
set connection mamta1;
-- this revoke should not impact any objects because none depend on it
revoke select on t12ViewTest from mamta2;
set connection mamta2;
select * from v21ViewTest;
-- cleanup
set connection mamta1;
drop table t11ViewTest;
drop table t12ViewTest;

-- View tests
-- test2 
--  Let the dba create a view in schema mamta2 (owned by user mamta2). The view's definition accesses 
--    objects from schema mamta1. The owner of schema mamta2 does not have access to objects in schema mamta1 
--    but the create view by dba does not fail because dba has access to all the objects. 
--  mamta2 will have access to the view created by the dba because mamta2 is owner of the schema "mamta2" and 
--    it has access to all the objects created in it's schema, whether they were created by mamta2 or the dba. 
--  user mamta2 is owner of the schema mamta2 because user mamta2 was the first one to create an object in
--    schema mamta2 earlier in this test.
--  Any other user (except the dba) will need to get explicit select privileges on the view in order to access it
--
set connection mamta1;
-- Note that mamta1 is creating couple tables but has not granted permissions on those tables to anyone
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
insert into t11ViewTest values(2);
drop table t12ViewTest;
create table t12ViewTest (c121 int, c122 char);
insert into t12ViewTest values (1,'1');
-- connect as dba
set connection satConnection;
-- dba is creating a view in schema owned by another user. dba can create objects anywhere and access objects from anywhere
create view mamta2.v21ViewTest as select t1.c111, t2.c122 from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2;
-- dba can do select from that view
select * from mamta2.v21ViewTest;
set connection mamta2;
-- the schema owner can do a select from an object that is part of it's schema even though it was created by the dba
select * from v21ViewTest;
set connection mamta3;
-- mamta3 has not been granted select privileges on mamta2.v21ViewTest
select * from mamta2.v21ViewTest;
set connection mamta2;
-- give select privileges on the view to mamta3, should fail
grant select on v21ViewTest to mamta3;
set connection mamta3;
-- select from mamta2.v21ViewTest will fail for mamta3 because mamta3 has no select privilege on mamta2.v21ViewTest
select * from mamta2.v21ViewTest;
set connection satConnection;
-- have the dba take away select privilege on mamta2.v21ViewTest from mamta3
revoke select on mamta2.v21ViewTest from mamta3;
set connection mamta3;
-- select from mamta2.v21ViewTest will fail this time for mamta3 because dba took away the select privilege on mamta2.v21ViewTest
select * from mamta2.v21ViewTest;
-- cleanup
set connection mamta2;
drop view v21ViewTest;
set connection mamta1;
drop table t12ViewTest;
drop table t11ViewTest;

-- View tests
-- test3 
--  Create a view that relies on table level and column permissions and see that view gets dropped correctly when any of the
--    required privilege is revoked
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
drop table t12ViewTest;
create table t12ViewTest (c121 int, c122 char);
insert into t12ViewTest values (1,'1');
grant select (c111) on t11ViewTest to mamta3;
grant select (c121, c122) on t12ViewTest to public;
set connection mamta2;
drop table t21ViewTest;
create table t21ViewTest (c211 int);
insert into t21ViewTest values(1);
grant select on t21ViewTest to mamta3;
set connection mamta3;
drop view v31ViewTest;
create view v31ViewTest as select t2.c122, t1.*, t3.* from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2, 
	mamta2.t21ViewTest as t3 where t1.c111 = t3.c211;
select * from v31ViewTest;
set connection mamta1;
-- revoke a column level privilege. It should drop the view
revoke select(c122) on t12ViewTest from public;
set connection mamta3;
-- the view got dropped because of revoke issued earlier
select * from v31ViewTest;
-- cleanup
set connection mamta2;
drop table t21ViewTest;
set connection mamta1;
drop table t12ViewTest;
drop table t11ViewTest;

-- View tests
-- test4 
--  Create a view that relies on a user-level table privilege and a user-level column privilege.
--   There also exists a PUBLIC-level column privilege but objects at the creation time always first
--   look for the required privilege at the user level(DERBY-1632). This behavior can be confirmed by the 
--   following test case where when PUBLIC-level column privilege is revoked, it does not impact the
--   view in anyway because the view is relying on user-level column privilege. Confirm that object
--   is relying on user-level privilege by revoking the user-level privilege and that should drop the object
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
drop table t12ViewTest;
create table t12ViewTest (c121 int, c122 char);
insert into t12ViewTest values (1,'1');
grant select (c111) on t11ViewTest to mamta3, public;
grant select (c121, c122) on t12ViewTest to public;
set connection mamta2;
drop table t21ViewTest;
create table t21ViewTest (c211 int);
insert into t21ViewTest values(1);
grant select on t21ViewTest to mamta3, mamta5;
set connection mamta3;
drop view v31ViewTest;
create view v31ViewTest as select t2.c122, t1.*, t3.* from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2, 
	mamta2.t21ViewTest as t3 where t1.c111 = t3.c211;
select * from v31ViewTest;
set connection mamta1;
-- revoke public level privilege. Should not impact the view because user objects always rely on user level privilege.
--   If no user level privilege is found at create object time, then PUBLIC level privilege (if there) is used.
--   If there is no privilege granted at user level or public level at create object time, the create sql will fail
--   DERBY-1632
revoke select(c111) on t11ViewTest from public;
set connection mamta3;
-- still exists because privileges required by it are not revoked
select * from v31ViewTest;
set connection mamta1;
-- this revoke should drop the view mamta3.v31ViewTest
revoke select(c111) on t11ViewTest from mamta3;
set connection mamta3;
-- View shouldn't exist anymore
select * from v31ViewTest;
-- cleanup
set connection mamta2;
drop table t21ViewTest;
set connection mamta1;
drop table t12ViewTest;
drop table t11ViewTest;

-- View tests
-- test5
-- Create a view that relies on a SELECT privilege on only one column of a table. revoke SELECT privilege on 
--  another column in that table and it ends up dropping the view. This is happening because the revoke privilege 
--  work is not completely finished and any dependent object on that permission type for table's columns
--  get dropped when a revoke privilege is issued against any column of that table
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key, c112 int);
insert into t11ViewTest values(1,1);
grant select (c111, c112) on t11ViewTest to mamta2;
set connection mamta2;
drop view v21ViewTest;
create view v21ViewTest as select c111 from mamta1.t11ViewTest;
-- notice that the view above needs SELECT privilege on column c111 of mamta1.t11ViewTest and does not care about column c112
set connection mamta1;
-- the revoke below ends up dropping the view mamta2.v21ViewTest eventhough the view does not depend on column c112
-- This will be fixed in a subsequent patch for revoke privilege
revoke select (c111) on t11ViewTest from mamta2;
set connection mamta2;
select * from v21ViewTest;
-- cleanup
set connection mamta1;
drop table t11ViewTest;

-- View tests
-- test6
--  Create a view that requires a privilege. grant select on the view to another user.
--    Let that user create a trigger based on the granted view. 
--
--    Now if the privilege is revoked from the view owner, the view gets dropped, as 
--    expected. But I had also expected the trigger to fail the next time it gets fired
--    because view used by it doesn't exist anymore. But because of a bug in Derby, 
--    DERBY-1613(A trigger does not get invalidated when the view used by it is dropped),
--    during some runs of this test, the trigger continues to fire successfully and 
--    during other runs of this test, it gives the error that the view does
--    not exist anymore. Seems like this is timing related issue. So, may see 
--    diffs in this particular test until DERBY-1613 is resolved. After the 
--    resolution of DERBY-1613, the insert trigger will always fail after the view
--    gets dropped because of the revoke privilege.
set connection mamta1;
drop table t11TriggerTest;
create table t11TriggerTest (c111 int not null primary key, c112 int);
insert into t11TriggerTest values(1,1);
insert into t11TriggerTest values(2,2);
grant select on t11TriggerTest to mamta2;
set connection mamta2;
create view v21ViewTest as select * from mamta1.t11TriggerTest;
-- should fail
grant select on v21ViewTest to mamta3;
select * from v21ViewTest;
set connection mamta3;
drop table t31TriggerTest;
create table t31TriggerTest (c311 int); 
drop table t32TriggerTest;
create table t32TriggerTest (c321 int); 
-- following should fail because not all the privileges are in place
create trigger tr31t31TriggerTest after insert on t31TriggerTest for each statement mode db2sql
	insert into t32TriggerTest values (select c111 from mamta2.v21ViewTest where c112=1);
insert into t31TriggerTest values(1);
select * from t31TriggerTest;
select * from t32TriggerTest;
set connection mamta1;
-- This will drop the dependent view 
revoke select on t11TriggerTest from mamta2;
set connection mamta2;
select * from v21ViewTest;
set connection mamta3;
-- During some runs of this test, the trigger continues to fire even though the view used by it 
--  has been dropped. (DERBY-1613)
-- During other runs of this test, the trigger gives error as expected about the missing view.
--  After DERBY-1613 is fixed, we should consistently get error from insert below because the
--  insert trigger can't find the view it uses.
insert into t31TriggerTest values(1);
select * from t31TriggerTest;
select * from t32TriggerTest;
-- cleanup
set connection mamta3;
drop table t31TriggerTest;
drop table t32TriggerTest;
set connection mamta1;
drop table t11TriggerTest;

-- View tests
-- test7 - negative test
--  Create a view that relies on a user level table privilege. The view will depend on the user level table privilege. 
--     Later grant the table privilege at the PUBLIC level too. So, there are 2 privileges available and the view
--     relies on one of those privileges. Later, revoke the user level table privilege. This will end up dropping the
--     view although there is another privilege available at PUBLIC level which can cover the view's requirements of
--     privileges. But Derby does not support this automatic switching of privilege reliance on another available
--     privilege when revoke is issued. DERBY-1632
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
insert into t11ViewTest values(2);
grant select on t11ViewTest to mamta2;
set connection mamta2;
drop view v21ViewTest;
create view v21ViewTest as select * from mamta1.t11ViewTest;
select * from v21ViewTest;
set connection mamta1;
-- grant the privilege required by mamta2.v21ViewTest at PUBLIC level
grant select on t11ViewTest to PUBLIC;
-- now revoke the privilege that view is currently dependent on. This will end up dropping the view even though there is 
--   same privilege available at the PUBLIC level
revoke select on t11ViewTest from mamta2;
set connection mamta2;
-- view doesn't exist anymore
select * from v21ViewTest;
-- Issuing the create view again will work because required privilege is available at PUBLIC level
create view v21ViewTest as select * from mamta1.t11ViewTest;
-- view is back in action
select * from v21ViewTest;
set connection mamta1;
-- verify that view above is dependent on PUBLIC level privilege, revoke the PUBLIC level privilege and
--   check if the view got dropped automatically
revoke select on t11ViewTest from PUBLIC;
set connection mamta2;
-- view doesn't exist anymore
select * from v21ViewTest;
--cleanup
set connection mamta1;
drop table t11ViewTest;

-- View tests
-- test8 - negative test
--  This test is similar to test7 above. Create a view that relies on a column level privilege. Later on, grant the
--    same privilege at table level. Now, revoke the column level privilege. The view will get dropped automatically even
--    though there is a covering privilege available at the table level.(DERBY-1632)
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
insert into t11ViewTest values(2);
grant select(c111) on t11ViewTest to mamta2;
set connection mamta2;
drop view v21ViewTest;
create view v21ViewTest as select c111 from mamta1.t11ViewTest;
set connection mamta1;
-- grant the privilege required by mamta2.v21ViewTest at table level
grant select on t11ViewTest to mamta2;
-- now revoke the privilege that view is currently dependent on. This will end up dropping the view even though there is 
--   same privilege available at the table level
revoke select(c111) on t11ViewTest from mamta2;
set connection mamta2;
-- view doesn't exist anymore
select * from v21ViewTest;
-- Issuing the create view again will work because required privilege is available at table level
create view v21ViewTest as select * from mamta1.t11ViewTest;
-- view is back in action
select * from v21ViewTest;
set connection mamta1;
-- verify that view above is dependent on table level privilege, revoke the table level privilege and
--   check if the view got dropped automatically
revoke select on t11ViewTest from mamta2;
set connection mamta2;
-- view doesn't exist anymore
select * from v21ViewTest;
--cleanup
set connection mamta1;
drop table t11ViewTest;

-- View tests
-- test9 - negative test
-- Have SELECT privilege available both at column level and table level. When an object is created which requires the
--  SELECT privilege, Derby is designed to pick up the table level privilege first. Later, when the table level
--  privilege is revoke, the object gets dropped. The object really should start depending on the available column
--  level privilege. DERBY-1632
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
insert into t11ViewTest values(2);
grant select(c111) on t11ViewTest to mamta2;
grant select on t11ViewTest to mamta2;
set connection mamta2;
drop view v21ViewTest;
-- this view will depend on the table level SELECT privilege
create view v21ViewTest as select c111 from mamta1.t11ViewTest;
set connection mamta1;
-- this ends up dropping the view mamta2.v21ViewTest (DERBY-1632). Instead, the view should have started depending on the available 
--  column level SELECT privilege.
revoke select on t11ViewTest from mamta2;
set connection mamta2;
-- view doesn't exist anymore
select * from v21ViewTest;
--cleanup
set connection mamta1;
drop table t11ViewTest;

-- View tests
-- test10 - negative test
--  Create a view that relies on some privileges. Create another view based on that view. A revoke privilege on privilege
--    required by the first view will fail because there is another view dependent on the first view. This is because
--    Derby currently does not support cascade view drop (DERBY-1631)
set connection mamta1;
drop table t11ViewTest;
create table t11ViewTest (c111 int not null primary key);
insert into t11ViewTest values(1);
insert into t11ViewTest values(2);
drop table t12ViewTest;
create table t12ViewTest (c121 int, c122 char);
insert into t12ViewTest values (1,'1');
-- grant permissions to mamta2 so mamta2 can create a view based on these objects
grant select on t11ViewTest to mamta2;
grant select on t12ViewTest to mamta2;
set connection mamta2;
create view v21ViewTest as select t1.c111, t2.c122 from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2;
select * from v21ViewTest;
-- grant permission to mamta3, should fail
grant select on v21ViewTest to mamta3;
set connection mamta3;
create view v31ViewTest as select * from mamta2.v21ViewTest;
select * from v31ViewTest;
set connection mamta1;
-- revoke the privilege from mamta2, should be ok, previous view is not created. 
revoke select on t11ViewTest from mamta2;
set connection mamta2;
-- this view is not created, should fail
select * from v21ViewTest;
set connection mamta3;
-- drop the dependent view
drop view v31ViewTest;
set connection mamta1;
-- revoke privilege will succeed this time and will drop the dependent view on that privilege
revoke select on t11ViewTest from mamta2;
set connection mamta2;
-- view doesn't exist anymore
select * from v21ViewTest;
-- cleanup
set connection mamta1;
drop table t12ViewTest;
drop table t11ViewTest;

-- Constraint test
-- test1
-- Give a constraint privilege at table level to a user. Let user define a foreign key constraint based on that privilege.
--  Later revoke that references privilege and make sure that foreign key constraint gets dropped
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null primary key);
insert into t11ConstraintTest values(1);
insert into t11ConstraintTest values(2);
grant references on t11ConstraintTest to mamta2;
set connection mamta2;
drop table t21ConstraintTest;
create table t21ConstraintTest (c211 int references mamta1.t11ConstraintTest, c212 int);
insert into t21ConstraintTest values(1,1);
-- should fail because the foreign key constraint will fail
insert into t21ConstraintTest values(3,1);
set connection mamta1;
revoke references on t11ConstraintTest from mamta2;
set connection mamta2;
-- will pass because the foreign key constraint got dropped because of revoke statement
insert into t21ConstraintTest values(3,1);
-- cleanup
set connection mamta2;
drop table t21ConstraintTest;
set connection mamta1;
drop table t11ConstraintTest;

-- Constraint test
-- test2
-- Have user mamta1 give a references privilege to mamta3.
-- Have user mamta2 give a references privilege to mamta3.
-- Have mamta3 create a table with 2 foreign key constraints relying on both these granted privileges.
-- Revoke one of those privileges and make sure that the foreign key constraint defined based on that privilege gets dropped.
-- Now revoke the 2nd references privilege and make sure that remaining foreign key constraint gets dropped
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
drop table t31ConstraintTest;
create table t31ConstraintTest (c311 int references mamta1.t11ConstraintTest, c312 int references mamta2.t21ConstraintTest);
select * from t31ConstraintTest;
insert into t31ConstraintTest values(1,1);
-- following should fail because it violates the foreign key reference by column c312
insert into t31ConstraintTest values(1,3);
-- following should fail because it violates the foreign key reference by column c311
insert into t31ConstraintTest values(3,1);
-- following should fail because it violates the foreign key reference by column c311 and c312
insert into t31ConstraintTest values(3,4);
set connection mamta2;
-- the following revoke should drop the foreign key reference by column t31ConstraintTest.c312
revoke references on t21ConstraintTest from mamta3;
set connection mamta3;
-- verify that foreign key reference by column t31ConstraintTest.c312 got dropped by inserting a row.
-- following should pass
insert into t31ConstraintTest values(1,3);
-- following should still fail because foreign key reference by column c311 is still around
insert into t31ConstraintTest values(3,1);
set connection mamta1;
-- now drop the references privilege so that the only foreign key reference on table mamta3.t31ConstraintTest will get dropped
revoke references on t11ConstraintTest from mamta3;
set connection mamta3;
-- verify that foreign key reference by column t31ConstraintTest.c311 got dropped by inserting a row.
-- following should pass
insert into t31ConstraintTest values(3,1);
-- no more foreign key references left and hence following should pass
insert into t31ConstraintTest values(3,3);
-- cleanup
drop table t31ConstraintTest;
set connection mamta2;
drop table t21ConstraintTest;
set connection mamta1;
drop table t11ConstraintTest;

-- Constraint test
-- test3
-- Have mamta1 grant REFERENCES privilege on one of it's tables to mamta2
-- Have mamta2 create a table with primary which references mamta1's granted REFERENCES privilege
-- Have mamta2 grant REFERENCES privilege on that table to user mamta3
-- Have mamta3 create a table which references mamta2's granted REFERENCES privilege
-- Now revoke of granted REFERENCES privilege by mamta1 should drop the foreign key reference 
--  by mamta2's table t21ConstraintTest. It should not impact the foreign key reference by
--  mamta3's table t31ConstraintTest.
-- a)mamta1.t11ConstraintTest (primary key)
-- b)mamta2.t21ConstraintTest (primary key references t11ConstraintTest)
-- c)mamta3.t31ConstraintTest (primary key references t21ConstraintTest)
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null primary key);
insert into t11ConstraintTest values(1);
insert into t11ConstraintTest values(2);
grant references on t11ConstraintTest to mamta2;
set connection mamta2;
drop table t21ConstraintTest;
create table t21ConstraintTest (c111 int not null primary key references mamta1.t11ConstraintTest);
insert into t21ConstraintTest values(1);
insert into t21ConstraintTest values(2);
-- following should fail because of foreign key constraint failure
insert into t21ConstraintTest values(3);
grant references on t21ConstraintTest to mamta3;
set connection mamta3;
drop table t31ConstraintTest;
create table t31ConstraintTest (c311 int references mamta2.t21ConstraintTest);
select * from t31ConstraintTest;
insert into t31ConstraintTest values (1);
-- following should fail because of foreign key constraint failure
insert into t31ConstraintTest values (4);
set connection mamta1;
-- This revoke should drop foreign key constraint on mamta2.t21ConstraintTest
--   This revoke should not impact the foeign key constraint on mamta3.t31ConstraintTest
revoke references on t11ConstraintTest from mamta2;
set connection mamta2;
-- because the foreign key reference got revoked, no constraint violation check will be done
insert into t21ConstraintTest values(3);
set connection mamta3;
-- Make sure the foreign key constraint on t31ConstraintTest is still active
insert into t31ConstraintTest values(3);
-- because the foreign key constraint is still around, following should fail
insert into t31ConstraintTest values(4);
-- cleanup
set connection mamta3;
drop table t31ConstraintTest;
set connection mamta2;
drop table t21ConstraintTest;
set connection mamta1;
drop table t11ConstraintTest;

-- Constraint test
-- test4
-- Grant a REFERENCES permission at public level, create constraint, grant same permission at user level 
--   and take away the public level permission. It ends up dropping the constraint. DERBY-1632
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null primary key);
insert into t11ConstraintTest values(1);
insert into t11ConstraintTest values(2);
grant references on t11ConstraintTest to PUBLIC;
set connection mamta2;
drop table t21ConstraintTest;
create table t21ConstraintTest (c111 int not null primary key, constraint fk foreign key(c111) references mamta1.t11ConstraintTest);
insert into t21ConstraintTest values(1);
insert into t21ConstraintTest values(2);
-- following should fail because of foreign key constraint failure
insert into t21ConstraintTest values(3);
set connection mamta1;
-- grant REFERENCES permission again but this time at user level
grant references on t11ConstraintTest to mamta2;
-- Now, revoke REFERENCES permission which was granted at PUBLIC level, This drops the constraint.
--   DERBY-1632. This should be fixed at some point so that constraint won't get dropped, instead
--   it will start depending on same privilege available at user-level
revoke references on t11ConstraintTest from PUBLIC;
set connection mamta2;
-- because the foreign key reference got revoked, no constraint violation check will be done
insert into t21ConstraintTest values(3);
-- cleanup
set connection mamta2;
drop table t21ConstraintTest;
set connection mamta1;
drop table t11ConstraintTest;

-- Constraint test
-- test5
-- Grant refrences privilege and select privilege on a table. Have a constraint depend on the references
--   privilege. Later, a revoke of select privilege will end up dropping the constraint which shouldn't
--   happen. This will be addressed in a subsequent patch
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null primary key);
insert into t11ConstraintTest values(1);
insert into t11ConstraintTest values(2);
grant references on t11ConstraintTest to PUBLIC;
grant select on t11ConstraintTest to PUBLIC;
set connection mamta2;
drop table t21ConstraintTest;
create table t21ConstraintTest (c111 int not null primary key, constraint fk foreign key(c111)   references mamta1.t11ConstraintTest);
insert into t21ConstraintTest values(1);
insert into t21ConstraintTest values(2);
-- following should fail because of foreign key constraint failure
insert into t21ConstraintTest values(3);
set connection mamta1;
-- revoke of select privilege is going to drop the constraint which is incorrect. Will be handled in a later patch
revoke select on t11ConstraintTest from PUBLIC;
set connection mamta2;
-- following should have failed but it doesn't because foreign key constraint got dropped by revoke select privilege
-- Will be fixed in a subsequent patch
insert into t21ConstraintTest values(3);
-- cleanup
set connection mamta2;
drop table t21ConstraintTest;
set connection mamta1;
drop table t11ConstraintTest;

-- Constraint test
-- test6
-- Have a primary key and a unique key on a table and grant reference on both. Have another table rely on unique
--  key references privilege to create a foreign key constraint. Later, the revoke of primary key reference will end up
--  dropping the foreign key constraint. This will be fixed in a subsequent patch (same as test5)
set connection mamta1;
drop table t11ConstraintTest;
create table t11ConstraintTest (c111 int not null primary key, c112 int not null unique, c113 int);
insert into t11ConstraintTest values(1,1,1);
insert into t11ConstraintTest values(2,2,1);
grant references(c111, c112) on t11ConstraintTest to PUBLIC;
set connection mamta2;
drop table t21ConstraintTest;
create table t21ConstraintTest (c111 int not null primary key, constraint fk foreign key(c111)   references mamta1.t11ConstraintTest(c112));
insert into t21ConstraintTest values(1);
insert into t21ConstraintTest values(2);
-- following should fail because of foreign key constraint failure
insert into t21ConstraintTest values(3);
set connection mamta1;
-- revoke of references privilege on c111 which is not used by foreign key constraint on t21ConstraintTest ends up dropping that
--  foreign key constraint. This Will be handled in a later patch
revoke references(c111) on t11ConstraintTest from PUBLIC;
set connection mamta2;
-- following should have failed but it doesn't because foreign key constraint got dropped by revoke references privilege
-- Will be fixed in a subsequent patch
insert into t21ConstraintTest values(3);
-- cleanup
set connection mamta2;
drop table t21ConstraintTest;
set connection mamta1;
drop table t11ConstraintTest;

-- Miscellaneous test
-- test1
-- Have multiple objects depends on a privilege and make sure they all get dropped when that privilege is revoked.
set connection mamta1;
drop table t11MiscTest;
create table t11MiscTest (c111 int, c112 int, c113 int);
grant select, update, trigger on t11MiscTest to mamta2, mamta3;
drop table t12MiscTest;
create table t12MiscTest (c121 int, c122 int);
grant select on t12MiscTest to mamta2;
set connection mamta2;
drop view v21MiscTest;
create view v21MiscTest as select * from mamta1.t11MiscTest, mamta1.t12MiscTest where c111=c121;
select * from v21MiscTest;
set connection mamta3;
drop view v31MiscTest;
create view v31MiscTest as select c111 from mamta1.t11MiscTest;
select * from v31MiscTest;
set connection mamta1;
-- this should drop both the dependent views
revoke select, update on t11MiscTest from mamta2, mamta3;
set connection mamta2;
-- should fail because it got dropped as part of revoke statement
select * from v21MiscTest;
set connection mamta3;
-- should fail because it got dropped as part of revoke statement
select * from v31MiscTest;
-- cleanup
set connection mamta1;
drop table t11MiscTest;
drop table t12MiscTest;

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
drop table t12RoutineTest;
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
delete from t13TriggerTest;
-- Trying to revoke execute privilege below will fail because mamta3 has created a trigger based on that permission.
-- Derby supports only RESTRICT form of revoke execute. Which means that it can be revoked only if there are no
-- objects relying on that permission
revoke execute on function selectFromSpecificSchema from mamta3 restrict;
-- now try the insert and make sure the insert trigger still fires
set connection mamta2;
insert into mamta3.t31TriggerTest values(1);
set connection mamta1;
-- If number of rows returned by following select is 1, then we know insert trigger did get fire.
-- Insert's trigger's action is to insert into following table. 
select * from t13TriggerTest;
set connection mamta3;
-- drop the trigger manually
drop trigger tr31t31;
set connection mamta1;
-- Now, we should be able to revoke execute permission on routine because there are no dependent objects on that permission
revoke execute on function selectFromSpecificSchema from mamta3 restrict;
set connection mamta3;
-- cleanup
drop table t31TriggerTest;
set connection mamta1;
-- cleanup
drop table t12RoutineTest;
drop table t13TriggerTest;
drop function selectFromSpecificSchema;

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
-- should fail
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
-- Should be one more row since last check because insert trigger got fired
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


-- Define couple triggers on a table relying on privilege on different tables. If a revoke is issued, only the dependent triggers
--   should get dropped, the rest of the triggers should stay active.
set connection mamta1;
drop table t11TriggerRevokeTest;
create table t11TriggerRevokeTest (c111 int);
insert into t11TriggerRevokeTest values(1),(2);
grant INSERT on t11TriggerRevokeTest to mamta2;
drop table t12TriggerRevokeTest;
create table t12TriggerRevokeTest (c121 int);
insert into t12TriggerRevokeTest values(1),(2);
grant INSERT on t12TriggerRevokeTest to mamta2;
set connection mamta2;
drop table t21TriggerRevokeTest;
create table t21TriggerRevokeTest (c211 int); 
insert into t21TriggerRevokeTest values(1);
-- following will pass because mamta2 has required permissions on mamta1.t11TriggerRevokeTest
create trigger tr211t21 after insert on t21TriggerRevokeTest for each statement mode db2sql
        insert into mamta1.t11TriggerRevokeTest values(99);
-- following will pass because mamta2 has required permissions on mamta1.t11TriggerRevokeTest
create trigger tr212t21 after insert on t21TriggerRevokeTest for each statement mode db2sql
        insert into mamta1.t12TriggerRevokeTest values(99);
insert into t21TriggerRevokeTest values(1);
set connection mamta1;
-- there should be 1 new row in each of the tables because of 2 insert triggers
select * from t11TriggerRevokeTest;
select * from t12TriggerRevokeTest;
delete from t11TriggerRevokeTest;
delete from t12TriggerRevokeTest;
-- only one trigger(tr211t21) should get dropped because of following revoke
revoke insert on t11TriggerRevokeTest from mamta2;
set connection mamta2;
insert into t21TriggerRevokeTest values(1);
set connection mamta1;
-- there should be no row in this table
select * from t11TriggerRevokeTest;
-- there should be one new row in mamta1.t12TriggerRevokeTest 
select * from t12TriggerRevokeTest;
-- cleanup
set connection mamta2;
drop table t21TriggerRevokeTest;
set connection mamta1;
drop table t12TriggerRevokeTest;
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

-- DERBY-1538: Disable ability to GRANT or REVOKE from self

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

create table mamta1Table ( i int, j int);

-- Try granting or revoking to mamta1. Should all fail

grant select on mamta1Table to mamta1;
revoke select on mamta1Table from mamta1;

grant execute on function f_abs to mamta1;
revoke execute on function f_abs from mamta1 restrict;

-- Connect as database owner. Even she can not grant to owner or revoke from owner
set connection satConnection;
set schema mamta1;

grant select on mamta1Table to mamta1;
revoke select on mamta1Table from mamta1;

grant execute on function f_abs to mamta1;
revoke execute on function f_abs from mamta1 restrict;

-- But Grant/Revoke to another user should pass
grant select on mamta1Table to randy;
revoke select on mamta1Table from randy;

grant execute on function f_abs to randy;
revoke execute on function f_abs from randy restrict;

set connection mamta1;

drop table mamta1Table;
drop function f_abs;

-- DERBY-1708
-- Test LOCK TABLE statement
connect 'grantRevokeDDL' user 'user1' as user1;
create table t100 (i int);
connect 'grantRevokeDDL' user 'user2' as user2;
autocommit off;
-- expect errors
lock table user1.t100 in exclusive mode;
lock table user1.t100 in share mode;
commit;
set connection user1;
grant select on t100 to user2;
set connection user2;
-- ok
lock table user1.t100 in exclusive mode;
lock table user1.t100 in share mode;
commit;
set connection user1;
revoke select on t100 from user2;
set connection user2;
-- expect errors
lock table user1.t100 in exclusive mode;
lock table user1.t100 in share mode;
commit;
autocommit on;

-- DERBY-1686
set connection user1;
create table t1 (i int);
insert into t1 values 1,2,3;
grant select on t1 to user2;
set connection user2;
create view v1 as select * from user1.t1;
-- attempt to grant this view to others, should fail since user2
-- does not have grant privilege on object user1.t1
grant select on user1.t1 to user3;
-- expect error
grant select on v1 to user3;
-- cleanup
set connection user2;
drop view v1;
set connection user1;
drop table t1;
autocommit on;
set connection user2;
autocommit on;

-- Simple test case for DERBY-1583: column privilege checking should not
-- assume column descriptors have non-null table references.

set connection mamta1;
create table t11TriggerRevokeTest (c111 int not null primary key, c12 int);
insert into t11TriggerRevokeTest values (1, 101), (2, 202), (3, 303);
grant TRIGGER on t11TriggerRevokeTest to mamta2;
create table t12TriggerRevokeTest (c121 int, c122 int, c123 int);
insert into t12TriggerRevokeTest values (10, 1010, 2010),(20,1020,2020);
grant UPDATE(c122, c121) on t12TriggerRevokeTest to mamta2;
set connection mamta2;
create trigger tr11t11 after insert on mamta1.t11TriggerRevokeTest
for each statement mode db2sql
        update mamta1.t12TriggerRevokeTest set c122 = 99; 
set connection mamta1;
select * from t11TriggerRevokeTest;
select * from t12TriggerRevokeTest;
-- This should fire the trigger, changing the c122 values to 99
insert into t11TriggerRevokeTest values(4, 404);
select * from t11TriggerRevokeTest;
select * from t12TriggerRevokeTest;
-- revoking the privilege should drop the trigger
revoke TRIGGER on t11TriggerRevokeTest from mamta2;
update t12TriggerRevokeTest set c122 = 42;
-- now when we insert the trigger should NOT be fired, c122 values should
-- be unchanged and so should be 42
insert into t11TriggerRevokeTest values (5,505);
select * from t11TriggerRevokeTest;
select * from t12TriggerRevokeTest;

-- Simple test case for DERBY-1724, which is a different manifestation
-- of DERBY-1583

set connection mamta1;
create table t1001 (c varchar(1));
insert into t1001 values 'a', 'b', 'c';
autocommit off;
grant select on t1001 to mamta3; 
set connection mamta2;
create table ttt1 (i int);
insert into ttt1 values 1;
grant all privileges on ttt1 to mamta1;
set connection mamta1;
select * from mamta2.ttt1;
insert into mamta2.ttt1 values 2;
update mamta2.ttt1 set i = 888;
commit;
autocommit on;

-- Simple test case for DERBY-1589. The problem here involves dependency
-- management between the FOREIGN KEY clause in the CREATE TABLE statement
-- and the underlying table that the FK refers to. The statement must
-- declare a dependency on the referenced table so that changes to the table
-- cause invalidation of the statement's compiled plan. The test case below
-- sets up such a situation by dropping the referenced table and recreating
-- it and then re-issuing a statement with identical text to one which
-- was issued earlier.

set connection mamta1;
create table d1589t11ConstraintTest (c111 int not null, c112 int not null, primary key (c111, c112));
grant references on d1589t11ConstraintTest to mamta3;
set connection mamta3;
drop table d1589t31ConstraintTest;
create table d1589t31ConstraintTest (c311 int, c312 int, foreign key(c311, c312) references mamta1.d1589t11ConstraintTest);
drop table d1589t31ConstraintTest;
set connection mamta1;
drop table d1589t11ConstraintTest;
create table d1589t11ConstraintTest (c111 int not null, c112 int not null, primary key (c111, c112));
grant references(c111) on d1589t11ConstraintTest to mamta3;
grant references(c112) on d1589t11ConstraintTest to PUBLIC;
set connection mamta3;
create table d1589t31ConstraintTest (c311 int, c312 int, foreign key(c311, c312) references mamta1.d1589t11ConstraintTest); 

-- DERBY-1847 SELECT statement asserts with XJ001 when attempted to select a newly added column
-- Grant access on 2 columns and then add another column to the table. The select on the new column
-- by another user should complain about no permissions granted on that new column.
set connection mamta2;
create table t1Derby1847 (c1 int, c2 int); 
grant select(c1,c2) on t1Derby1847 to mamta3; 
alter table t1Derby1847 add c3 int; 
set connection mamta3;
-- should fail because mamta3 doesn't have any permission on this column in table mamta2.t1Derby1847
select c3 from mamta2.t1Derby1847; 
set connection mamta2;
grant select on t1Derby1847 to mamta3; 
set connection mamta3;
-- should work now because mamta3 got select permission on new column in table mamta2.t1Derby1847 through table level select permission
select c3 from mamta2.t1Derby1847; 
set connection mamta2;
revoke select on t1Derby1847 from mamta3; 
set connection mamta3;
-- should fail because mamta3 lost it's select permission on new column in table mamta2.t1Derby1847
select c3 from mamta2.t1Derby1847; 
set connection mamta2;
grant select(c3) on t1Derby1847 to mamta3; 
set connection mamta3;
-- should work now because mamta3 got select permission on new column in table mamta2.t1Derby1847 through column level select permission
select c3 from mamta2.t1Derby1847; 
set connection mamta2;
drop table t1Derby1847; 
set connection mamta3;
select c3 from mamta2.t1Derby1847; 

-- DERBY-1716
-- Revoking select privilege from a user times out when that user still have
-- a cursor open before the patch.
set connection user1;
drop table t1;
create table t1 (c varchar(1));
insert into t1 values 'a', 'b', 'c';
grant select on t1 to user2;
set connection user2;
autocommit off;
GET CURSOR crs1 AS 'select * from user1.t1';
next crs1;
set connection user1;
-- should succeed without blocking
revoke select on t1 from user2;
set connection user2;
-- still ok to fetch.
next crs1;
next crs1;
close crs1;
commit;
-- should fail since select privilege got revoked
GET CURSOR crs1 AS 'select * from user1.t1';
next crs1;
close crs1;
autocommit on;
-- repeat the scenario
set connection user1;
grant select on t1 to user2;
set connection user2;
autocommit off;
GET CURSOR crs1 AS 'select * from user1.t1';
next crs1;
set connection user1;
-- should succeed without blocking
revoke select on t1 from user2;
set connection user2;
-- still ok to fetch.
next crs1;
next crs1;
close crs1;
commit;
-- should fail since select privilege got revoked
GET CURSOR crs1 AS 'select * from user1.t1';
next crs1;
close crs1;
autocommit on;
set connection user1;
