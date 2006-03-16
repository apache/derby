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

