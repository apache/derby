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
external name 'org.apache.derbyTesting.functionTests.util.ConsistencyChecker.countOpens';
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
create trigger tr1 after update of c2, c3, c4 on tab4 for each row mode db2sql insert into tab5 values (1);
create trigger tr2 after update of c3, c4 on tab4 for each row mode db2sql insert into tab5 values (2);

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
-- values (org.apache.derbyTesting.functionTests.util.ConsistencyChecker::countOpens());

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

