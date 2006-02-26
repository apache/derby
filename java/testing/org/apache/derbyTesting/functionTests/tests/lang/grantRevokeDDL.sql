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

-- Test for external security clause
-- Expected to fail
CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
EXTERNAL SECURITY DEFINOR
LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE PROCEDURE AUTH_TEST.addUserUtility(IN userName VARCHAR(50), IN permission VARCHAR(22)) 
LANGUAGE JAVA PARAMETER STYLE JAVA
EXTERNAL SECURITY INVOKER
EXTERNAL NAME 'org.apache.derby.database.UserUtility.add';

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
EXTERNAL SECURITY DEFINER
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

set schema satheesh;

-- All these DDLs should fail.

create table NotMyTable (i int, j int);

drop table tsat;
drop index tsat_ind;

create view myview as select * from satheesh.tsat;

CREATE FUNCTION FuncNotMySchema(P1 INT)
RETURNS INT NO SQL RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
EXTERNAL SECURITY DEFINER
LANGUAGE JAVA PARAMETER STYLE JAVA;

alter table tsat add column k int;

-- Now create own schema
create schema swiper;

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

-- GrantRevoke TODO: This one should pass, but currently fails. Not sure how to handle this yet.
update satheesh.tsat set j=i; 

create table my_tsat (i int not null, c char(10), constraint fk foreign key(i) references satheesh.tsat);

