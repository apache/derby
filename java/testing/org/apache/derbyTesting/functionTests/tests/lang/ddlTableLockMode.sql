-- test DDL Table Lock mode

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;
CREATE PROCEDURE WAIT_FOR_POST_COMMIT() DYNAMIC RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_Access.waitForPostCommitToFinish' PARAMETER STYLE JAVA;

-- create tables with different lock modes
drop   table default1;
create table default1(c1 int);
drop   table row1;
create table row1(c1 int);
alter table row1 locksize row;
drop   table table1;
create table table1(c1 int);
alter table table1 locksize table;

-- verify that views have table lock mode of 'R' (ignored)
create view v1 as select * from table1;
select tablename, lockgranularity from sys.systables
where tablename = 'V1';
drop view v1;

-- verify that system tables have lock mode of 'R'
select tablename, lockgranularity from sys.systables
where tablename = 'SYSTABLES';

-- READ COMMITTED tests
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

set current isolation = CS;
-- all selects should be row locked except for table1
select * from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all updates should be row locked except for table1
update default1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update default1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- all inserts should be row locked except for table1
insert into default1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into row1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into table1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all deletes should be row locked except for table1
delete from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from default1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();

-- REPEATABLE READ tests
--   repeatable read works the same as serializable when no indexes are involved

-- create tables with different lock modes
drop   table default1;
create table default1(c1 int);
drop   table row1;
create table row1(c1 int);
alter table row1 locksize row;
drop   table table1;
create table table1(c1 int);
alter table table1 locksize table;

set current isolation RS;
-- all selects should be row locked except for table1
select * from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all updates should be row locked except for table1
update default1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update default1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- all inserts should be row locked except for table1
insert into default1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into row1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into table1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all deletes should be row locked except for table1
delete from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from default1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();

-- alter table
-- first set to same value (stupid test)
alter table default1 locksize row;
alter table row1 locksize row;
alter table table1 locksize table;
select tablename, lockGranularity from sys.systables
where tablename in ('DEFAULT1', 'ROW1', 'TABLE1')
order by tablename;

-- set to opposite value 
alter table default1 locksize table;
alter table row1 locksize table;
alter table table1 locksize row;
select tablename, lockGranularity from sys.systables
where tablename in ('DEFAULT1', 'ROW1', 'TABLE1')
order by tablename;

-- READ UNCOMMITTED tests

-- create tables with different lock modes
drop   table default1;
create table default1(c1 int);
drop   table row1;
create table row1(c1 int);
alter table row1 locksize row;
drop   table table1;
create table table1(c1 int);
alter table table1 locksize table;

set isolation = read uncommitted;
-- all selects should be row locked except for table1

select * from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all updates should be row locked except for table1
update default1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update default1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- all inserts should be row locked except for table1
insert into default1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into row1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into table1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all deletes should be row locked except for table1
delete from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from default1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();

-- alter table
-- first set to same value (stupid test)
alter table default1 locksize row;
alter table row1 locksize row;
alter table table1 locksize table;
select tablename, lockGranularity from sys.systables
where tablename in ('DEFAULT1', 'ROW1', 'TABLE1')
order by tablename;

-- set to opposite value 
alter table default1 locksize table;
alter table row1 locksize table;
alter table table1 locksize row;
select tablename, lockGranularity from sys.systables
where tablename in ('DEFAULT1', 'ROW1', 'TABLE1')
order by tablename;


-- SERIALIZABLE tests

-- create tables with different lock modes
drop   table default1;
create table default1(c1 int);
drop   table row1;
create table row1(c1 int);
alter table row1 locksize row;
drop   table table1;
create table table1(c1 int);
alter table table1 locksize table;

set isolation serializable;
-- all selects should be table locked since no where clause
select * from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all updates should be table locked
-- (No indexes, so will always do table scan)
update default1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update default1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- all inserts should be row locked except for table1
insert into default1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into row1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into table1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- scans for all deletes should be table locked
-- (No indexes, so will always do table scan)
delete from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from default1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();

-- alter table
-- first set to same value (stupid test)
alter table default1 locksize row;
alter table row1 locksize row;
alter table table1 locksize table;
select tablename, lockGranularity from sys.systables
where tablename in ('DEFAULT1', 'ROW1', 'TABLE1')
order by tablename;

-- set to opposite value 
alter table default1 locksize table;
alter table row1 locksize table;
alter table table1 locksize row;
select tablename, lockGranularity from sys.systables
where tablename in ('DEFAULT1', 'ROW1', 'TABLE1')
order by tablename;

set isolation read committed;
-- verify lock granularity changed for selects
select * from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify lock granularity changed for updates
update default1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update default1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update row1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
update table1 set c1 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify lock granularity changed for inserts
insert into default1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into row1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into table1 values 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify lock granularity changed for deletes
delete from default1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from default1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from row1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();
delete from table1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
CALL WAIT_FOR_POST_COMMIT();

-- bug 3819; delete from table would first
-- end up getting an IX lock on table then
-- an X lock on table; this can lead to
-- deadlocks with multiple threads doing
-- delete from table. fix is to choose 
-- row locking for deletes/updates in *all*
-- cases; this would result in an IX lock
-- on the table. means more locking but
-- increased concurrency.
insert into default1 values (1);
insert into default1 values (2);
select * from default1 order by c1;

set isolation to CURSOR STABILITY;

autocommit off;

delete from default1;

-- should see only one lock; earlier used to 
-- see 2, one IX and one for X.
select count(*)
 from new org.apache.derby.diag.LockTable() l 
 where tablename = 'DEFAULT1' and type = 'TABLE';

commit;

-- cleanup
drop procedure WAIT_FOR_POST_COMMIT;
drop table default1;
drop table row1;
drop table table1;
