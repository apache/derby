-- test for CURRENT SCHEMA and optional DB2 compatible SET SCHEMA statement
--
-- test SET SCHEMA syntax variations
-- syntax is SET [CURRENT] SCHEMA [=] (<identifier> | USER | ? | '<string>')
--			 SET CURRENT SQLID [=] (<identifier> | USER | ? | '<string>')
--
values current schema;
set schema sys;
values current schema;
create schema app;
set current schema app;
values current schema;
set schema =  sys;
values current schema;
set current schema = app;
values current schema;
set schema sys;
-- user should use default schema if no user set
set schema user;
values current schema;
-- see what user does when there is a user
create schema judy;
connect 'jdbc:derby:wombat;user=judy' as judy;
set schema app;
values current schema;
set schema user;
values current schema;
disconnect;
set connection connection0;

-- check for default
values current schema;

-- check that current sqlid works as a synonym
values current sqlid;

-- check that sqlid still works as an identifer
create table sqlid(sqlid int);
drop table sqlid;

-- check that set current sqlid works
set current sqlid judy;
values current schema;

-- check that set sqlid doesn't work (not DB2 compatible) - should get error
set sqlid judy;

-- change schema and make sure that the current schema is correct
set schema sys;
values current schema;
set schema app;

-- try using ? outside of a prepared statement
set schema ?;

-- use set schema in a prepared statement
autocommit off;
prepare p1 as 'set schema ?';
-- should get error with no parameters
execute p1;
-- should get error if null is used
create table t1(name varchar(128));
insert into t1 values(null);
execute p1 using 'select name from t1';
-- should get error if schema doesn't exist
execute p1 using 'values(''notthere'')';
-- should error with empty string
execute p1 using 'values('''')';
-- should get error if wrong case used
execute p1 using 'values(''sys'')';
-- should get error if too many parameters
execute p1 using 'values(''sys'',''app'')';
-- USER should return an error as it is interpreted as a string constant not an
-- identifier
execute p1 using 'values(''USER'')';

-- try positive test
execute p1 using 'values(''SYS'')';
values current schema;


rollback;
autocommit on;


-- 
-- try current schema in a number of statements types
set schema app;
create table t1 ( a varchar(128));

-- insert
insert into t1 values (current schema);
select * from t1;
set schema judy;
insert into app.t1 values (current schema);
select * from app.t1;
-- delete where clause
delete from app.t1 where a = current schema;
select * from app.t1;
set current schema app;


-- target list
select current schema from t1;

-- where clause
select * from t1 where a = current schema;

-- update statement
delete from t1;
insert into t1 values ('test');
select * from t1;
update t1 set a = current schema;
select * from t1;
set schema judy;
update app.t1 set a = current schema;
select * from app.t1;
set schema app;

drop table t1;

-- default
set schema APP;
create table t1 ( a int, b varchar(128) default current schema);
insert into t1 (a) values (1);
set schema SYS;
insert into app.t1 (a) values (1);
set schema judy;
insert into app.t1 (a) values (1);
set schema APP;
select * from t1;
drop table t1;

-- check constraint - this should fail
create table t1 ( a varchar(128), check (a = current schema));
create table t1 ( a varchar(128), check (a = current sqlid));

-- try mix case
create schema "MiXCase";
set schema "MiXCase";
values current schema;
set schema app;
values current schema;
set schema 'MiXCase';
values current schema;
-- following should get error - schema not found
set schema 'MIXCASE';
set schema mixcase;

-- try long schema names (maximum schema identifier length has been changed to 30 as part of DB2 compatibility work)
create schema t23456789012345678901234567890;
values current schema;
set schema app;
values current schema;

set schema t23456789012345678901234567890;
values current schema;
set schema app;
values current schema;

set schema 'T23456789012345678901234567890';
values current schema;
set schema app;
values current schema;


autocommit off;
prepare p1 as 'set schema ?';
execute p1 using 'values(''T23456789012345678901234567890'')';

values current schema;

-- the following should fail - 31 length
create schema t234567890123456789012345678901;

set schema t234567890123456789012345678901;

set schema 'T234567890123456789012345678901';

execute p1 using 'values(''T234567890123456789012345678901'')';

rollback;
autocommit on;

-- clean up
drop schema judy restrict;
drop schema t234567890123456789012345678901 restrict;
