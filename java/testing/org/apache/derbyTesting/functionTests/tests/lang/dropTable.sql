-- tests for drop table
--
autocommit off;
--
-- test simple table - all should work
--
create table t1 ( a int);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 (a int);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 (a int);
drop table t1;
-- t1 shouldn't be found
select * from t1;

--
-- test table with unique constraint - all should work
--
create table t1 (a int not null unique);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 (a int not null unique);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 (a int not null unique);
drop table t1;
-- t1 shouldn't be found
select * from t1;

--
-- test table with primary constraint - all should work
--
create table t1 ( a int not null primary key);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 ( a int not null primary key);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 ( a int not null primary key);
drop table t1;
-- t1 shouldn't be found
select * from t1;

--
-- test table with check constraint - all should work
--
create table t1 ( a int check(a > 0));
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 ( a int check(a > 0));
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 ( a int check(a > 0));
drop table t1;
-- t1 shouldn't be found
select * from t1;

--
-- test table with index - all should work
--
create table t1 ( a int);
create index t1index on t1(a);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 (a int);
create index t1index on t1(a);
drop table t1;
-- t1 shouldn't be found
select * from t1;
create table t1 (a int);
create index t1index on t1(a);
drop table t1;
-- t1 shouldn't be found
select * from t1;

--
-- test table with foreign key references;
--
create table t1(a int not null primary key);
create table t2(a int constraint reft1a references t1(a));
-- this should fail with a dependent constraint error
drop table t1;
-- this should fail with a dependent constraint error
drop table t1;
-- dropping dependent constraint
alter table t2 drop constraint reft1a;
-- this should work since dependent constraint was dropped
drop table t1;
-- t1 shouldn't be found
select * from t1;
-- the following should work since no referential constraint is left
insert into t2 values(1);
drop table t2;

--
-- test table with view
--
create table t1(a int, b int);
create table t2(c int, d int);
create view vt1a as select a from t1;
create view vt1b as select b from t1;
create view vt1t2 as select * from t1, t2;
create view vvt1a as select * from vt1a;
create view vvvt1a as select * from vvt1a;
-- this should fail with view being a dependent object
drop table t1;
-- this should fail with view being a dependent object
drop table t1;
-- dropping dependent views
drop view vvvt1a;
drop view vvt1a;
drop view vt1t2;
drop view vt1b;
drop view vt1a;
-- this should work after dependent views were dropped
drop table t1;
-- this shouldn't find the view
select * from vt1a;
select * from vt1b;
select * from vt1t2;
select * from vvt1a;
select * from vvvt1a;

drop table t2;

--
-- test table with prepared statement
--
create table t1(a int);
prepare t1stmt as 'select * from t1';
-- this should work, statement will be invalidated and will fail when recompiled
drop table t1;
execute t1stmt;
remove t1stmt;

create table t1(a int);
prepare t1stmt as 'select * from t1';
-- this should work, statement will be invalidated and will fail when recompiled
drop table t1;
execute t1stmt;
remove t1stmt;

create table t1(a int);
prepare t1stmt as 'select * from t1';
-- this should work, statement will be invalidated and will fail when recompiled
drop table t1;
execute t1stmt;
remove t1stmt;

--
-- test table with triggers
--
create table t1(a int);
create table t2(a int);
create trigger t1trig after insert on t1 for each row mode db2sql insert into t2 values(1);
-- this should work - trigger should be deleted
drop table t1;
-- t1 shouldn't be found
select * from t1;

create table t1(a int);
create trigger t1trig after insert on t1 for each row mode db2sql insert into t2 values(1);
-- this should work - trigger should be deleted
drop table t1;
-- t1 shouldn't be found
select * from t1;

create table t1(a int);
create trigger t1trig after insert on t1 for each row mode db2sql insert into t2 values(1);
-- this should work - trigger should be deleted
drop table t1;
-- t1 shouldn't be found
select * from t1;
drop table t2;

--
-- test table within the body of a trigger on another table
--
create table t1(a int);
create table t2(a int);
create trigger t2trig after insert on t2 for each row mode db2sql insert into t1 values(1);
-- this should work
drop table t1;
-- the following should get an error when trying to recompile the trigger action
insert into t2 values(1);
drop table t2;

create table t1(a int);
create table t2(a int);
create trigger t2trig after insert on t2 for each row mode db2sql insert into t1 values(1);
-- this should work
drop table t1;
-- the following should get an error when trying to recompile the trigger action
insert into t2 values(1);
drop table t2;

create table t1(a int);
create table t2(a int);
create trigger t2trig after insert on t2 for each row mode db2sql insert into t1 values(1);
-- this should work
drop table t1;
-- the following should get an error when trying to recompile the trigger action
insert into t2 values(1);
drop table t2;

--
-- test drop view
--
create table t1(a int);
create view vt1 as select * from t1;
create view vvt1 as select * from vt1;
-- these should fail
drop view vt1;
drop view vt1 restrict;
drop view vt1 cascade;

-- 
-- make sure that indexes are dropped for drop table
--
create table t2(a int not null primary key);
create table reft2(a int constraint ref1 references t2);
-- count should be 2
select count(*) 
from sys.sysconglomerates c, sys.systables t
where t.tableid = c.tableid and
t.tablename = 'REFT2';
-- drop dependent referential constraint
alter table reft2 drop constraint ref1;
-- should work since dependent constraint was previously dropped
drop table t2;
-- count should be 1
select count(*) 
from sys.sysconglomerates c, sys.systables t
where t.tableid = c.tableid and
t.tablename = 'REFT2';

-- unsuccessful drop table should not affect open cursor
-- beetle 4393
rollback;
create table T1 (i int, c varchar(255), d varchar(255));
insert into T1(i) values(1);
insert into T1(i) values(2);
get cursor X1 as 'select i from t1 for update of c';
prepare U as 'update t1 set c = CHAR(i) where current of X1';
next X1;
drop table T1;
execute U;
select * from T1;

-- pretend all of the above didn't happen
autocommit on;
