--
-- General trigger test
--

create function printTriggerInfo() returns varchar(1) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.printTriggerInfo';
create function triggerFiresMin(s varchar(128)) returns varchar(1) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.triggerFiresMinimal';
create function triggerFires(s varchar(128)) returns varchar(1) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.triggerFires';
create function begInvRefToTECTest() returns varchar(1) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.beginInvalidRefToTECTest';
create procedure notifyDMLDone() PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.notifyDMLDone';

drop table x;
create table x (x int, y int, z int, constraint ck1 check (x > 0));
create view v as select * from x;

-- ok
create trigger t1 NO CASCADE before update of x,y on x for each row mode db2sql values 1;

-- trigger already exists
create trigger t1 NO CASCADE before update of x,y on x for each row mode db2sql values 1;
-- trigger already exists
create trigger app.t1 NO CASCADE before update of x,y on x for each row mode db2sql values 1;

-- make sure system tables look as we expect
select cast(triggername as char(10)), event, firingtime, type, state, referencedcolumns from sys.systriggers;

select cast(triggername as char(10)), text from sys.systriggers t, sys.sysstatements s 
		where s.stmtid = t.actionstmtid;

select cast(triggername as char(10)), tablename from sys.systriggers t, sys.systables tb
		where t.tableid = tb.tableid;

values SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', 'SYSTRIGGERS');
drop trigger t1;

-- not in sys schema
create trigger sys.tr NO CASCADE before insert on x for each row mode db2sql values 1;

-- not on table in sys schema
create trigger tr NO CASCADE before insert on sys.systables for each row mode db2sql values 1;

-- duplicate columns, not allowed
create trigger tr NO CASCADE before update of x, x on x for each row mode db2sql values 1;

-- no params in column list
create trigger tr NO CASCADE before update of x, ? on x for each row mode db2sql values 1;

-- invalid column
create trigger tr NO CASCADE before update of doesnotexist on x for each row mode db2sql values 1;

-- not on view
create trigger tr NO CASCADE before insert on v for each row mode db2sql values 1;

-- error to use table qualifier
create trigger tr NO CASCADE before update of x.x on x for each row mode db2sql values 1;

-- error to use schema.table qualifier
create trigger tr NO CASCADE before update of app.x.x on x for each row mode db2sql values 1;

-- no params in trigger action
-- bad
create trigger tr NO CASCADE before delete on x for each row mode db2sql select * from x where x = ?;

create trigger stmttrigger NO CASCADE before delete on x for each statement mode db2sql values 1;
select triggername, type from sys.systriggers where triggername = 'STMTTRIGGER';
drop trigger stmttrigger;

create trigger rowtrigger NO CASCADE before delete on x for each row mode db2sql values 1;
select triggername, type from sys.systriggers where triggername = 'ROWTRIGGER';
drop trigger rowtrigger;

-- fool around with depedencies

-- CREATE TRIGGER
create trigger t2 NO CASCADE before update of x,y on x for each row mode db2sql values 1;

-- CREATE CONSTRAINT
alter table x add constraint ck2 check(x > 0);

-- DROP VIEW
drop view v;

-- CREATE VIEW
create view v as select * from x;

-- CREATE INDEX
create index ix on x(x);

-- DROP TRIGGER: to the other types we have here
drop trigger t2;

-- DROP INDEX
drop index ix; 

-- DROP CONSTRAINT
alter table x drop constraint ck2;

-- MAKE SURE TRIGGER SPS IS RECOMPILED IF TABLE IS ALTERED.
create table y (x int, y int, z int);

create trigger tins after insert on x referencing new_table as newtab for each statement mode db2sql insert into y select x, y, z from newtab;

insert into x values (1, 1, 1);
alter table x add column w int default 100;
alter table x add constraint nonulls check (w is not null);
insert into x values (2, 2, 2, 2);
select * from y;
drop trigger tins;
drop table y;

-- prove that by dropping the underlying table, we have dropped the trigger
-- first, lets create a few other triggers
create trigger t2 NO CASCADE before update of x,y on x for each row mode db2sql values 1;
create trigger t3 after update of x,y on x for each statement mode db2sql values 1;
create trigger t4 after delete on x for each statement mode db2sql values 1;
select cast(triggername as char(10)), tablename from sys.systriggers t, sys.systables  tb
		where t.tableid = tb.tableid order by 1;
drop view v;
drop table x;
select cast(triggername as char(10)), tablename from sys.systriggers t, sys.systables  tb
		where t.tableid = tb.tableid order by 1;

--
-- schema testing
--
create table x (x int, y int, z int);
create schema test;

create trigger test.t1 NO CASCADE before delete on x for each row mode db2sql values 1;
set schema test;

create trigger t2 NO CASCADE before delete on app.x for each row mode db2sql values 1;

select schemaname, triggername from sys.systriggers t, sys.sysschemas s
	where s.schemaid = t.schemaid;

set schema app;
-- fails
drop schema test restrict;

drop trigger test.t2;

-- fails
drop schema test restrict;

set schema test;
drop trigger t1;
set schema app;

-- ok this time
drop schema test restrict;

--
-- Test the information in the trigger information context
--
create table t (x int, y int, c char(1));
create trigger t1 NO CASCADE before insert on t for each statement mode db2sql
	values app.printTriggerInfo();
insert into t values (1,1,'1');

delete from t;
drop trigger t1;
create trigger t1 after insert on t for each statement mode db2sql
	values app.printTriggerInfo();
insert into t values (1,1,'1');

drop trigger t1;
create trigger t1 NO CASCADE before update on t for each statement mode db2sql
	values app.printTriggerInfo();
update t set x = 2;
update t set y = 2, c = '2';

drop trigger t1;
create trigger t1 after update on t for each statement mode db2sql
	values app.printTriggerInfo();
update t set x = 3;
update t set y = 3, c = '3';

drop trigger t1;
create trigger t1 no cascade before delete on t for each statement mode db2sql
	values app.printTriggerInfo();
delete from t;
drop trigger t1;

insert into t values(3,3,'3');
create trigger t1 after delete on t for each statement mode db2sql
	values app.printTriggerInfo();
delete from t;
drop trigger t1;

--
-- Test trigger firing order
--
create trigger t1 after insert on t for each row mode db2sql
	values app.triggerFiresMin('3rd');
create trigger t2 after insert on t for each statement mode db2sql
	values app.triggerFiresMin('1st');
create trigger t3 no cascade before insert on t for each row mode db2sql
	values app.triggerFiresMin('4th');
create trigger t4 after insert on t for each row mode db2sql
	values app.triggerFiresMin('2nd');
create trigger t5 no cascade before insert on t for each statement mode db2sql
	values app.triggerFiresMin('5th');
insert into t values (1,1,'1');
drop trigger t1;
drop trigger t2;
drop trigger t3;
drop trigger t4;
drop trigger t5;

-- try multiple values, make sure result sets don't get screwed up
-- this time we'll print out result sets
create trigger t1 after insert on t for each row mode db2sql
	values app.triggerFires('3rd');
create trigger t2 no cascade before insert on t for each statement mode db2sql
	values app.triggerFires('1st');
create trigger t3 after insert on t for each row mode db2sql
	values app.triggerFires('4th');
create trigger t4 no cascade before insert on t for each row mode db2sql
	values app.triggerFires('2nd');
create trigger t5 after insert on t for each statement mode db2sql
	values app.triggerFires('5th');
insert into t values 
	(2,2,'2'),
	(3,3,'3'),
	(4,4,'4');

delete from t;
drop trigger t1;
drop trigger t2;
drop trigger t3;
drop trigger t4;
drop trigger t5;

--
-- Test firing on empty change sets, 
-- statement triggers fire, row triggers
-- do not.
--
create trigger t1 after insert on t for each row mode db2sql
	values app.triggerFires('ROW: empty insert, should NOT fire');
create trigger t2 after insert on t for each statement mode db2sql
	values app.triggerFires('STATEMENT: empty insert, ok');
insert into t select * from t;
drop trigger t1;
drop trigger t2;

create trigger t1 after update on t for each row mode db2sql
	values app.triggerFires('ROW: empty update, should NOT fire');
create trigger t2 after update on t for each statement mode db2sql
	values app.triggerFires('STATEMENT: empty update, ok');
update t set x = x;
drop trigger t1;
drop trigger t2;

create trigger t1 after delete on t for each row mode db2sql
	values app.triggerFires('ROW: empty delete, should NOT fire');
create trigger t2 after delete on t for each statement mode db2sql
	values app.triggerFires('STATEMENT: empty delete, ok');
delete from t;
drop trigger t1;
drop trigger t2;
drop table x;


--
-- After alter table, should pick up the new columns
--
create table talt(c1 int);
create trigger tins after insert on talt for each statement mode db2sql
	values app.printTriggerInfo();
create trigger tdel no cascade before delete on talt for each row mode db2sql
	values app.printTriggerInfo();
create trigger tupd after update on talt for each statement mode db2sql
	values app.printTriggerInfo();
insert into talt values (1);
alter table talt add column cnew int default null;
select * from talt;

insert into talt values (2,2);
delete from talt;
insert into talt values (3,3);
update talt set cnew = 666;
drop trigger tins;
drop trigger tdel;
drop trigger tupd;

-- make sure update w/ columns doesn't pick up new col
create trigger tupd after update of c1 on talt for each statement mode db2sql
	values app.printTriggerInfo();
alter table talt add column cnew2 int default null;
insert into talt values (1,1,1);
update talt set cnew2 = 666;

-- clean up
drop table talt;


--
-- Trigger ordering wrt constraints
--
create table p (x int not null, constraint pk primary key (x));
insert into p values 1,2,3;
create table f (x int, 
		constraint ck check (x > 0),
		constraint fk foreign key (x) references p);
create trigger t1 no cascade before insert on f for each row mode db2sql
	values app.triggerFiresMin('BEFORE constraints');
create trigger t2 after insert on f for each row mode db2sql
	values app.triggerFiresMin('AFTER constraints');

-- INSERT
-- fails, ck violated
insert into f values 0;

alter table f drop constraint ck;

-- fails, fk violated
insert into f values 0;

alter table f drop foreign key fk;

-- ok
insert into f values 0;

delete from f;
alter table f add constraint ck check (x > 0);
alter table f add constraint fk foreign key (x) references p;
drop trigger t1;
drop trigger t2;
insert into f values (1);


-- UPDATE
create trigger t1 no cascade before update on f for each row mode db2sql
	values app.triggerFiresMin('BEFORE constraints');
create trigger t2 after update on f for each row mode db2sql
	values app.triggerFiresMin('AFTER constraints');

-- fails, ck violated
update f set x = 0;

alter table f drop constraint ck;

-- fails, fk violated
update f set x = 0;

alter table f drop foreign key fk;

-- ok
update f set x = 0;

delete from f;
alter table f add constraint ck check (x > 0);
alter table f add constraint fk foreign key (x) references p;
drop trigger t1;
drop trigger t2;


-- DELETE
insert into f values 1;
create trigger t1 no cascade before delete on p for each row mode db2sql
	values app.triggerFiresMin('BEFORE constraints');
create trigger t2 after delete on p for each row mode db2sql
	values app.triggerFiresMin('AFTER constraints');

-- fails, fk violated
delete from p;

alter table f drop foreign key fk;

-- ok
delete from p;

drop table f;
drop table p;

--
-- Prove that we are firing the proper triggers based
-- on the columns we are changing;
--
drop table t;
create table t (c1 int, c2 int);
create trigger tins after insert on t for each row mode db2sql
	values app.triggerFiresMin('insert');
create trigger tdel after delete on t for each row mode db2sql
	values app.triggerFiresMin('delete');
create trigger tupc1 after update of c1 on t for each row mode db2sql
	values app.triggerFiresMin('update c1');
create trigger tupc2 after update of c2 on t for each row mode db2sql
	values app.triggerFiresMin('update c2');
create trigger tupc1c2 after update of c1,c2 on t for each row mode db2sql
	values app.triggerFiresMin('update c1,c2');
create trigger tupc2c1 after update of c2,c1 on t for each row mode db2sql
	values app.triggerFiresMin('update c2,c1');
insert into t values (1,1);
update t set c1 = 1;
update t set c2 = 1;
update t set c2 = 1, c1 = 1;
update t set c1 = 1, c2 = 1;
delete from t;

-- Make sure that triggers work with delimited identifiers
-- Make sure that text munging works correctly
create table trigtable("cOlUmN1" int, "cOlUmN2  " int, "cOlUmN3""""  " int);
create table trighistory("cOlUmN1" int, "cOlUmN2  " int, "cOlUmN3""""  " int);
insert into trigtable values (1, 2, 3);
create trigger "tt1" after insert on trigtable
referencing NEW as NEW for each row mode db2sql
insert into trighistory ("cOlUmN1", "cOlUmN2  ", "cOlUmN3""""  ") values
(new."cOlUmN1" + 5, "NEW"."cOlUmN2  " * new."cOlUmN3""""  ", 5);
maximumdisplaywidth 700;
select cast(triggername as char(10)), text from sys.systriggers t, sys.sysstatements s 
		where s.stmtid = t.actionstmtid and triggername = 'tt1';
insert into trigtable values (1, 2, 3);
select * from trighistory;
drop trigger "tt1";
create trigger "tt1" after insert on trigtable
referencing new as new for each row mode db2sql
insert into trighistory ("cOlUmN1", "cOlUmN2  ", "cOlUmN3""""  ") values
(new."cOlUmN1" + new."cOlUmN1", "NEW"."cOlUmN2  " * new."cOlUmN3""""  ", new."cOlUmN2  " * 3);
select cast(triggername as char(10)), text from sys.systriggers t, sys.sysstatements s 
		where s.stmtid = t.actionstmtid and triggername = 'tt1';
insert into trigtable values (1, 2, 3);
select * from trighistory;
drop table trigtable;
drop table trighistory;

-- trigger bug that got fixed mysteriously
-- between xena and buffy
create table trigtable1(c1 int, c2 int);
create table trighistory(trigtable char(30), c1 int, c2 int);
create trigger trigtable1 after update on trigtable1
referencing OLD as oldtable
for each row mode db2sql
insert into trighistory values ('trigtable1', oldtable.c1, oldtable.c2);
insert into trigtable1 values (1, 1);
update trigtable1 set c1 = 11, c2 = 11;
select * from trighistory;
drop table trigtable1;
drop table trighistory;

-- 
-- Lets make sure that the tec cannot be accessed once
-- the dml that caused it to be pushed is finished.
--
drop table t;
create table t (x int);
create trigger t no cascade before insert on t for each statement mode db2sql
	values app.begInvRefToTECTest();

-- causes the trigger to fire, which causes a thread
-- to be cranked up
insert into t values 1;

-- tell the background thread that dml is done,
-- it will now try to do some stuff with the stale
-- tec.  We MUST do this in a different thread lest
-- we block the background thread on connection 
-- synchronization
connect 'wombat' as conn2;
call app.notifyDMLDone();
disconnect;

set connection connection0;

-- Test for bug 3495 - triggers were causing deferred insert, which
-- caused the insert to use a TemporaryRowHolderImpl. This was not
-- being re-initialized properly when closed, and it was trying to
-- re-insert the row from the first insert.
autocommit off;
drop table t;
create table t (x int);
create trigger tr after insert on t for each statement mode db2sql values 1;
prepare ps as 'insert into t values (?)';
execute ps using 'values (1)';
execute ps using 'values (2)';
select * from t;

-- Test MODE DB2SQL not as reserved keyword. beetle 4546 
drop table db2sql;
drop table db2sql2;
create table db2sql  (db2sql int, mode int, yipng int);
create table db2sql2 (db2sql2 int);

-- Test MODE DB2SQL on trigger.  beetle 4546
drop trigger db2sqltr1;
create trigger db2sqltr1 after insert on db2sql 
for each row
MODE DB2SQL 
insert into db2sql2 values (1);

-- Test optimizer plan of trigger action. Beetle 4826
autocommit on;
drop table parent;

create table t1(a int not null primary key, b int);
create table parent (a int not null primary key, b int);

create trigger trig1 AFTER DELETE on t1
referencing OLD as OLD for each row mode db2sql
delete from parent where a = OLD.a;

insert into t1 values (0, 1);
insert into t1  values (1, 1);
insert into t1  values (2, 1);
insert into t1  values (3, 1);

insert into parent values (0, 1);
insert into parent values (1, 1);
insert into parent values (2, 1);
insert into parent values (3, 1);
insert into parent values (4, 1);

autocommit off ;
delete from t1 where a = 3;
select type, mode, tablename from new org.apache.derby.diag.LockTable() t order by tablename, type;
rollback;
autocommit on;
drop table t1;
drop table parent;

-- Test use of old AND new referencing names within the same trigger (beetle 5725).

create table x(x int);
insert into x values (2), (8), (78);
create table removed (x int);

-- statement trigger
create trigger t1 after update of x on x referencing
 old_table as old new_table as new for each statement mode db2sql insert into
 removed select * from old where x not in (select x from 
 new where x < 10);

select * from x;
select * from removed;
update x set x=18 where x=8;
select * from x;
select * from removed;

-- row trigger
create trigger t2 after update of x on x referencing
 old as oldrow new as newrow for each row mode db2sql insert into
 removed values (newrow.x + oldrow.x);

update x set x=28 where x=18;
select * from x;
select * from removed;

-- do an alter table, then make sure triggers recompile correctly.

alter table x add column y int;
update x set x=88 where x > 44;
select * from x;
select * from removed;

drop table x;
drop table removed;

create table x (x int, constraint ck check (x > 0));

-- after
create trigger tgood after insert on x for each statement mode db2sql insert into x values 666;
insert into x values 1;
select * from x;
drop trigger tgood;

create trigger tgood after insert on x for each statement mode db2sql delete from x;
insert into x values 1;
select * from x;
drop trigger tgood;

create trigger tgood after insert on x for each statement mode db2sql update x set x = x+100;
insert into x values 1;
select * from x;
drop trigger tgood;
delete from x;

drop table x;
