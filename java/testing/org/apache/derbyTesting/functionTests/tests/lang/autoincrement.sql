-- Adding new testcases for DB2 syntax "GENERATED ALWAYS AS IDENTITY"
-- We don't enhance "ALTER TABLE <T> MODIFY COLUMN" yet: DB2 uses "ALTER TABLE <T> ALTER COLUMN..."
-- try generated  values with all types.
-- Cloudscape specific syntax for the autoincrement clause can be found in store/bug3498.sql

create table ai_zero (i int, a_zero int generated always as identity);
create table ai_one (i int, a_one smallint generated always as identity);
create table ai_two (i int, a_two int generated always as identity);
create table ai_three (i int, a_three int generated always as identity);

select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME in ('A_ZERO', 'A_ONE', 'A_TWO', 'A_THREE');
drop table ai_zero;
drop table ai_one;
drop table ai_two;
drop table ai_three;

-- try a generated column spec with initial and start values.
create table ai (i  int, autoinc int generated always as identity (start with 100));

select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'AUTOINC';
drop table ai;

create table ai (i int, autoinc int generated always as identity (increment by 100));
select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'AUTOINC';

drop table ai;

create table ai (i int, 
				 autoinc int generated always as identity (start with 101, increment by 100));
select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'AUTOINC';
drop table ai;

-- try -ive numbers.
create table ai1 (i int, 
				  a1 int generated always as identity (start with  0, increment by -1));
create table ai2 (i int, 
				  a2 int generated always as identity (start with  +0, increment by -1));
create table ai3 (i int, 
				  a3 int generated always as identity (start with  -1, increment by -1));
create table ai4 (i int, 
				  a4 int generated always as identity (start with  -11, increment by +100));

select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'A1';
select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'A2';
select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'A3';
select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'A4';

drop table ai1;
drop table ai2;
drop table ai3;
drop table ai4;

-- **** simple increment tests.

create table ai_short (i int, 
				       ais smallint generated always as identity (start with 0, increment by 2));
insert into ai_short (i) values (0);
insert into ai_short (i) values (1);
insert into ai_short (i) values (2);
insert into ai_short (i) values (3);

select * from ai_short;
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC
from sys.syscolumns where COLUMNNAME = 'AIS';
drop table ai_short;

-- table with more than one generated column spec should fail
create table ai_multiple (i int, 
						  a0 int generated always as identity (start with  -1,
												    increment by -1),
						  a1 smallint generated always as identity,
						  a2 int generated always as identity (start with  0),
						  a3 bigint generated always as identity (start with  -100,
													increment by 10));

-- table with one generated column spec should succeed
create table ai_single1 (i int, a0 int generated always as identity 
									(start with  -1, increment by -1));
create table ai_single2 (i int, a1 smallint generated always as identity);
create table ai_single3 (i int, a2 int generated always as identity 
									(start with 0));
create table ai_single4 (i int, a3 bigint generated always as identity 
								        (start with  -100, increment by 10));

insert into ai_single1 (i) values (1);
insert into ai_single1 (i) values (2);
insert into ai_single1 (i) values (3);
insert into ai_single1 (i) values (4);
insert into ai_single1 (i) values (5);
insert into ai_single1 (i) values (6);
insert into ai_single1 (i) values (7);
insert into ai_single1 (i) values (8);
insert into ai_single1 (i) values (9);
insert into ai_single1 (i) values (10);
insert into ai_single2 (i) values (1);
insert into ai_single2 (i) values (2);
insert into ai_single2 (i) values (3);
insert into ai_single2 (i) values (4);
insert into ai_single2 (i) values (5);
insert into ai_single2 (i) values (6);
insert into ai_single2 (i) values (7);
insert into ai_single2 (i) values (8);
insert into ai_single2 (i) values (9);
insert into ai_single2 (i) values (10);
insert into ai_single3 (i) values (1);
insert into ai_single3 (i) values (2);
insert into ai_single3 (i) values (3);
insert into ai_single3 (i) values (4);
insert into ai_single3 (i) values (5);
insert into ai_single3 (i) values (6);
insert into ai_single3 (i) values (7);
insert into ai_single3 (i) values (8);
insert into ai_single3 (i) values (9);
insert into ai_single3 (i) values (10);
insert into ai_single4 (i) values (1);
insert into ai_single4 (i) values (2);
insert into ai_single4 (i) values (3);
insert into ai_single4 (i) values (4);
insert into ai_single4 (i) values (5);
insert into ai_single4 (i) values (6);
insert into ai_single4 (i) values (7);
insert into ai_single4 (i) values (8);
insert into ai_single4 (i) values (9);
insert into ai_single4 (i) values (10);
select a.i, a0, a1, a2, a3 from ai_single1 a 
       join ai_single2 b on a.i = b.i 
       join ai_single3 c on a.i = c.i 
       join ai_single4 d on a.i = d.i;

delete from ai_single1;
delete from ai_single2;
delete from ai_single3;
delete from ai_single4;
insert into ai_single1 (i) values (1);
insert into ai_single2 (i) values (1);
insert into ai_single3 (i) values (1);
insert into ai_single4 (i) values (1);
select a.i, a0, a1, a2, a3 from ai_single1 a 
       join ai_single2 b on a.i = b.i 
       join ai_single3 c on a.i = c.i 
       join ai_single4 d on a.i = d.i;

-- clean up
drop table ai_single1;
drop table ai_single2;
drop table ai_single3;
drop table ai_single4;

-- **** connection info tests {basic ones}

create table ai_test (x int generated always as identity (start with 2, increment by 2),
					  y int);
insert into ai_test (y) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);

-- should see 20.
values IDENTITY_VAL_LOCAL();

insert into ai_test (y) select y+10 from ai_test;
values IDENTITY_VAL_LOCAL();

values IDENTITY_VAL_LOCAL();

-- try some more connection info tests
create table ai_single1 (c char(100), a_odd int generated always as identity (start with 1, increment by 2));
create table ai_single2 (c char(100), a_even int generated always as identity (start with 0, increment by 2));
create table ai_single3 (c char(100), a_sum bigint generated always as identity (start with 1, increment by 2));

insert into ai_single1 (c) values ('a');
values IDENTITY_VAL_LOCAL();
insert into ai_single2 (c) values ('a');
values IDENTITY_VAL_LOCAL();
insert into ai_single3 (c) values ('a');
values IDENTITY_VAL_LOCAL();

insert into ai_single1 (c) values ('b');
values IDENTITY_VAL_LOCAL();
insert into ai_single2 (c) values ('b');
values IDENTITY_VAL_LOCAL();
insert into ai_single3 (c) values ('b');
values IDENTITY_VAL_LOCAL();

drop table ai_single1;
drop table ai_single2;
drop table ai_single3;
drop table ai_test;


-- nested, nested, nested stuff.
-- t1 --> trigger --> insert into t2 
-- insert row into t1.
-- I can get lastAutoincrementValue for t1 but not t2.
create table t1 (c1 int generated always as identity, name char(32));
create table t2 (c2 int generated always as identity, name char(32));

create trigger insert_trigger after insert on t1 for each row mode db2sql
	   insert into t2 (name) values ('Bob Finocchio');

insert into t1 (name) values ('Phil White');
select * from t1;
select * from t2;

values IDENTITY_VAL_LOCAL();

insert into t2 (name) values ('Jean-Yves Dexemier');
values IDENTITY_VAL_LOCAL();

-- insert into multiple tables in different schema names with same tablename,column names
-- make sure 
-- lastAutoincrementValue shouldn't get confused.....

drop table t1;
drop table t2;

-- APP.TAB1.A1 ==> -1,-2,-3
-- APP.TAB1.A2 ==> 1,2,3
-- APP.TAB2.A1 ==> 0,-2,-4
-- APP.TAB3.A2 ==> 0,2,4

create table tab1 (i int, a1 int generated always as identity (start with -1, increment by -1));
create table tab2 (i int, a2 smallint generated always as identity (start with 1, increment by +1));
create table tab3 (i int, a1 int generated always as identity (start with 0, increment by -2));
create table tab4 (i int, a2 bigint generated always as identity (start with 0, increment by 2));

create schema BPP;
set schema BPP;

-- BPP.TAB1.A1 ==> 100,101,102
-- BPP.TAB2.A2 ==> 100,99,98
-- BPP.TAB3.A1 ==> 100,102,104
-- BPP.TAB4.A2 ==> 100,98,96

create table tab1 (i int, a1 int generated always as identity (start with 100, increment by 1));
create table tab2 (i int, a2 bigint generated always as identity (start with 100, increment by -1));
create table tab3 (i int, a1 int generated always as identity (start with 100, increment by 2));
create table tab4 (i int, a2 smallint generated always as identity (start with 100, increment by -2));

insert into APP.tab1 (i) values (1);
insert into APP.tab2 (i) values (1);
insert into APP.tab3 (i) values (1);
insert into APP.tab4 (i) values (1);
insert into tab1 (i) values (1);
insert into tab1 (i) values (2);
insert into tab2 (i) values (1);
insert into tab2 (i) values (2);
insert into tab3 (i) values (1);
insert into tab3 (i) values (2);
insert into tab4 (i) values (1);
insert into tab4 (i) values (2);

select a.i, a1, a2 from app.tab1 a join app.tab2 b on a.i = b.i;
select a.i, a1, a2 from app.tab3 a join app.tab4 b on a.i = b.i;
select a.i, a1, a2 from tab1 a join tab2 b on a.i = b.i;
select a1, a2, a.i from tab3 a join tab4 b on a.i = b.i;

values IDENTITY_VAL_LOCAL();

set schema app;
drop table bpp.tab1;
drop table bpp.tab2;
drop table bpp.tab3;
drop table bpp.tab4;

drop schema bpp restrict;
drop table tab1;
drop table tab2;
drop table tab3;
drop table tab4;

-- trigger, 
-- insert into t2
--         ==> fires trigger which inserts into t1.
--                      
create table tab1 (s1 int generated always as identity, 
				   lvl int);
create table tab3 (c1 int);

create trigger tab1_after1 after insert on tab3 referencing new as newrow for each row mode db2sql insert into tab1 (lvl) values 1,2,3;

insert into tab3 values null;
select * from tab1;
select b.tablename, a.autoincrementvalue, a.autoincrementstart, a.autoincrementinc from sys.syscolumns a, sys.systables b where a.referenceid=b.tableid and a.columnname ='S1' and b.tablename = 'TAB1';

create table tab2 (lvl int, s1  bigint generated always as identity);

create trigger tab1_after2 after insert on tab3 referencing new as newrow for each row mode db2sql insert into tab2 (lvl) values 1,2,3;

insert into tab3 values null;
select * from tab2;
select b.tablename, a.autoincrementvalue, a.autoincrementstart, a.autoincrementinc from sys.syscolumns a, sys.systables b where a.referenceid=b.tableid and a.columnname ='S1' and b.tablename = 'TAB2';

-- clean up
drop trigger tab1_after1;
drop trigger tab1_after2;
drop table tab1;
drop table tab2;
drop table tab3;

-- some more variations of lastAutoincrementValue....
-- make sure we don't lose values from previous inserts.
create table t1 (x int, s1 int generated always as identity);
create table t2 (x smallint, s2 int generated always as identity (start with 0));

insert into t1 (x) values (1);

values IDENTITY_VAL_LOCAL();

insert into t1 (x) values (2);

values IDENTITY_VAL_LOCAL();

insert into t2 (x) values (1);

values IDENTITY_VAL_LOCAL();

-- alter table tests.
drop table t1;
drop table t2;

create table t1 (s1 int generated always as identity);
alter table t1 add column x int;
insert into t1 (x) values (1),(2),(3),(4),(5);
create table t2 (s2 int generated always as identity (start with 2));
alter table t2 add column x int;
insert into t2 (x) values (1),(2),(3),(4),(5);
create table t3 (s0 int generated always as identity (start with 0));
alter table t3 add column x int;
insert into t3 (x) values (1),(2),(3),(4),(5);

select a.x, s1, s2, s0 from t1 a join t2 b on a.x = b.x join t3 c on a.x = c.x;
values IDENTITY_VAL_LOCAL();

-- test some more generated column specs
create table trigtest (s1 smallint generated always as identity, lvl int);
insert into trigtest (lvl) values (0);
insert into trigtest (lvl) values (1),(2);
insert into trigtest (lvl) values (3),(4);
insert into trigtest (lvl) values (5),(6);
insert into trigtest (lvl) values (7),(8);
select * from trigtest;

drop table trigtest;

select count(*) from t1;
select a.x, s1, s2, s0 from t1 a join t2 b on a.x = b.x join t3 c on a.x = c.x;

delete from t1;
delete from t2;
delete from t3;
insert into t1 (x) values (1),(2),(3),(4),(5);
insert into t2 (x) values (1),(2),(3),(4),(5);
insert into t3 (x) values (1),(2),(3),(4),(5);

-- should have started from after the values in t1 due to alter.
select a.x, s1, s2, s0 from t1 a join t2 b on a.x = b.x join t3 c on a.x = c.x;

insert into t1 (x) values (6);
insert into t2 (x) values (6);
insert into t3 (x) values (6);
select a.x, s1, s2, s0 from t1 a join t2 b on a.x = b.x join t3 c on a.x = c.x;

values IDENTITY_VAL_LOCAL();

delete from t1;
delete from t2;
delete from t3;

insert into t1 (x) values (1),(2),(3),(4),(5);
insert into t2 (x) values (1),(2),(3),(4),(5);
insert into t3 (x) values (1),(2),(3),(4),(5);
select a.x, s1, s2, s0 from t1 a join t2 b on a.x = b.x join t3 c on a.x = c.x;

insert into t1 (x) values (6);
insert into t2 (x) values (6);
insert into t3 (x) values (6);
select a.x, s1, s2, s0 from t1 a join t2 b on a.x = b.x join t3 c on a.x = c.x;

values IDENTITY_VAL_LOCAL();

-- make sure we're doing nested xactions to update ai values.
drop table t1;
drop table t2;
drop table t3;

create table t1 (x int, yyy int generated always as identity (start with  0));

autocommit off;
insert into t1 (x) values (1);
insert into t1 (x) values (2);

select * from t1;
-- should see only locks on t1, no locks on system catalogs.
select  l.type, l.tablename, l.mode from new org.apache.derby.diag.LockTable() l order by tablename, type;

delete from t1;
commit;

-- locks should be gone now.
select  l.type, l.tablename, l.mode from new org.apache.derby.diag.LockTable() l order by tablename, type;
set isolation serializable;

-- this will get a share  lock on syscolumns
select columnname, autoincrementvalue
 from sys.syscolumns where columnname = 'YYY';

select  l.type, l.tablename, l.mode from new org.apache.derby.diag.LockTable() l order by tablename, type;

insert into t1 (x) values (3);

select  l.type, l.tablename, l.mode from new org.apache.derby.diag.LockTable() l order by tablename, type;
commit;

-- try using default keyword with ai.
drop table t1;

create table t1 (x char(2) default 'yy', y bigint generated always as identity);

insert into t1 (x, y) values ('aa', default);
insert into t1 values ('bb', default);
insert into t1 (x) values default;
insert into t1 (x) values null;
-- switch the order of the columns
insert into t1 (y, x) values (default, 'cc');
select * from t1;

-- bug 3450.
autocommit off;
create table testme (text varchar(10), autonum int generated always as identity);
commit;

prepare autoprepare as 'insert into testme (text) values ?';
execute autoprepare using 'values (''one'')';
execute autoprepare using 'values (''two'')';
execute autoprepare using 'values (''three'')';

select * from testme;

-- give exact query and make sure that the statment cache doesn't
-- mess up things.
insert into testme (text) values ('four');
insert into testme (text) values ('four');
select * from testme;
drop table testme;
commit;

-- go back to our commiting ways.
autocommit on;

-- negative tests from autoincrementNegative.sql
-- negative bind tests.
-- invalid types 
create table ni (x int, y char(1) generated always as identity);
create table ni (x int, y decimal(5,2) generated always as identity);
create table ni (x int, y float generated always as identity (start with 1, increment by 1));
create table ni (s int, y varchar(10) generated always as identity);

-- 0 increment 
-- pass in DB2 UDB
-- fail in DB2 CS
create table ni (x int, y int generated always as identity (increment by 0));
create table ni (x int, y int generated always as identity (start with 0, increment by 0));

create table ni (x int, y smallint generated always as identity (increment by 0));
create table ni (x int, y smallint generated always as identity (start with 0, increment by 0));

create table ni (x int, y int generated always as identity (increment by 0);
create table ni (x int, y int generated always as identity (start with 0, increment by 0));

create table ni (x int, y bigint generated always as identity (increment by 0));
create table ni (x int, y bigint generated always as identity (start with 0, increment by 0));

-- out of range start 
-- actually the first few are valid
create table ni (x int, y int generated always as identity (start with 127, increment by -1));
drop table ni;
create table ni (x int, y int generated always as identity (start with -128));
drop table ni;

--  now go through this exercise for all types!
create table ni (x int, y smallint generated always as identity (start with 32768));
create table ni (x int, y smallint generated always as identity (start with -32769));

create table ni (x int, y int generated always as identity (start with  2147483648));
create table ni (x int, y int generated always as identity (start with  -2147483649));

create table ni (x int, y bigint  
				 generated always as identity (start with  9223372036854775808));
create table ni (x int, y bigint 
				 generated always as identity (start with  -9223372036854775809));

-- attempt to update or insert into autoincrement columns.
create table ai (x smallint generated always as identity, y int);
insert into ai (y) values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
select * from ai;
delete from ai where y=8 OR y=4;
insert into ai (y) values (11),(13),(14),(15),(17),(18),(19);
select * from ai;

-- valid updates.
update ai set y=-y;
select * from ai order by x;
update ai set y=-y;
select * from ai order by x;

update ai set y=4 where y=3;
select * from ai order by x;
update ai set y=4 where x=3;
select * from ai order by x;

-- error, error!
update ai set x=4 where y=3;
insert into ai values (1,2);

-- overflow.
drop table ai;

create table ai (x int, y int generated always as identity (increment by 200000000));
insert into ai (x) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19);

-- should have been rolled back.
select * from ai;

-- but the entry in syscolumns has been updated! still can't do inserts.
insert into ai (x) values (1);

-- more overflow.
drop table ai;
create table ai (x int, y smallint generated always as identity (start with  -32760, increment by -1));
insert into ai (x) values (1),(2),(3),(4),(5),(6),(7),(8);
insert into ai (x) values (9),(10);
select * from ai;

-- try overflow with int and bigint.
drop table ai;
create table ai (x int, y int generated always as identity (start with  2147483646));
insert into ai (x) values (1);
insert into ai (x) values (2);
select * from ai;
insert into ai (x) select x from ai;

drop table ai;
-- for bigint we don't go to the end-- stop one value before....
create table ai (x int, y bigint generated always as identity 
				   (start with     9223372036854775805));

insert into ai (x) values (1),(2);
insert into ai (x) values (3);

select * from ai;

-- clean up
drop table ai;

--- alter table...
create table base (x int);
insert into base values (1),(2),(3),(4),(5),(6);
select * from base;
-- should fail because alter table add generated column is not supported
alter table base add column y smallint generated always as identity (start with  10);
alter table base add column y int generated always as identity (start with  10);
alter table base add column y bigint generated always as identity (start with  10);
-- make sure alter table failures above rolled themselves back 
select * from base;
drop table base;

-- testing non-reserved keywords: generated, start, always
-- should be successful
create table always (a int);
create table start (a int);
create table generated (a int);

drop table always;
drop table start;
drop table generated;

-- IDENTITY_VAL_LOCAL function, same as DB2, beetle 5354
drop table t1;

create table t1(c1 int generated always as identity, c2 int);

-- start
insert into t1(c2) values (8);
values IDENTITY_VAL_LOCAL();

select IDENTITY_VAL_LOCAL()+1, IDENTITY_VAL_LOCAL()-1 from t1;

insert into t1(c2) values (IDENTITY_VAL_LOCAL());
select * from t1;
values IDENTITY_VAL_LOCAL();
select IDENTITY_VAL_LOCAL()+1, IDENTITY_VAL_LOCAL()-1 from t1;

insert into t1(c2) values (8), (9);
-- multi-values insert, return value of the function should not change, same as DB2
values IDENTITY_VAL_LOCAL();
select * from t1;
insert into t1(c2) select c1 from t1;
-- insert with sub-select, return value should not change
values IDENTITY_VAL_LOCAL();
select * from t1;
delete from t1;
values IDENTITY_VAL_LOCAL();
insert into t1(c2) select c1 from t1;
values IDENTITY_VAL_LOCAL();

-- end of practice, back to start...
insert into t1(c2) values (8);
values IDENTITY_VAL_LOCAL();

drop table t1;

-- test cases for beetle 5404: inserting multiple rows of defaults into autoincrement column.
create table t1(c1 int generated always as identity);

-- this is okay
insert into t1 values (default);
select * from t1;

-- should fail
insert into t1 values (1), (1);
select * from t1;

-- this returns the right error
insert into t1 values (1), (default);
insert into t1 values (default), (1);
insert into t1 values (default), (default), (default), (2);
insert into t1 values (default), (default), (2);
insert into t1 values (default), (default), (2), (default);

-- this returns NPE
insert into t1 values (default), (default);
select * from t1;

insert into t1 values (default), (default), (default);
select * from t1;

insert into t1 values (default), (default), (default), (default);
select * from t1;

create table t2 (a int, b int generated always as identity);
insert into t2 values (1, default), (2, default);
select * from t2;

insert into t2 values (1, default), (2, 2);
insert into t2 values (1, default), (2, default), (2, 2);
insert into t2 values (1, 2), (2, default), (2, default);

create table t3(c1 int generated always as identity (increment by 3));

-- succeeded
insert into t3 values (default);

select * from t3;
insert into t3 values (default);
select * from t3;

-- should fail
insert into t3 values (1), (1);
select * from t3;

-- this returns the right error
insert into t3 values (1), (default);
insert into t3 values (default), (1);
insert into t3 values (default), (default), (default), (2);
insert into t3 values (default), (default), (2);
insert into t3 values (default), (default), (2), (default);
insert into t3 select * from t1;
insert into t3 select * from table (values (1)) as q(a);
insert into t3 select * from table (values (default)) as q(a);

-- this returns NPE
insert into t3 values (default), (default);
select * from t3;

insert into t3 values (default), (default), (default);
select * from t3;

insert into t3 values (default), (default), (default), (default);
select * from t3;

drop table t1;
drop table t2;
drop table t3;
