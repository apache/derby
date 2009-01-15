--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- Adding new testcases for DB2 syntax "GENERATED ALWAYS AS IDENTITY"
-- We don't enhance "ALTER TABLE <T> MODIFY COLUMN" yet: DB2 uses "ALTER TABLE <T> ALTER COLUMN..."
-- try generated  values with all types.

-- use query on lock table that only looks at locks held by user transactions,
-- to avoid picking up locks by background threads.
create view lock_table as
select
    cast(username as char(8)) as username,
    cast(t.type as char(8)) as trantype,
    cast(l.type as char(8)) as type,
    cast(lockcount as char(3)) as cnt,
    mode,
    cast(tablename as char(12)) as tabname,
    state,
    status
from
    syscs_diag.lock_table l right outer join syscs_diag.transaction_table t 
        on l.xid = t.xid 
where 
    t.type='UserTransaction' and l.lockcount is not null;

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

-- should return null as no single insert has been executed
values IDENTITY_VAL_LOCAL();


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

-- should see 0.
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

create trigger insert_trigger after insert on t1 for each row
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

create trigger tab1_after1 after insert on tab3 referencing new as newrow for each row insert into tab1 (lvl) values 1,2,3;

insert into tab3 values null;
select * from tab1;
select b.tablename, a.autoincrementvalue, a.autoincrementstart, a.autoincrementinc from sys.syscolumns a, sys.systables b where a.referenceid=b.tableid and a.columnname ='S1' and b.tablename = 'TAB1';

create table tab2 (lvl int, s1  bigint generated always as identity);

create trigger tab1_after2 after insert on tab3 referencing new as newrow for each row insert into tab2 (lvl) values 1,2,3;

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
select * from lock_table order by tabname, type desc, mode, cnt;

delete from t1;
commit;

-- locks should be gone now.
select * from lock_table order by tabname, type desc, mode, cnt;
set isolation serializable;

-- this will get a share  lock on syscolumns
select columnname, autoincrementvalue
 from sys.syscolumns where columnname = 'YYY';

select * from lock_table order by tabname, type desc, mode, cnt;

insert into t1 (x) values (3);

select * from lock_table order by tabname, type desc, mode, cnt;
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


-- Defaults/always
-- without increment option
create table t1(i int, t1_autogen int generated always as identity);
create table t2(i int, t2_autogen int generated by default as identity);

insert into t1(i) values(1);
insert into t1(i) values(1);
select * from t1;

insert into t2(i) values(1);
insert into t2(i) values(1);
select * from t2;

drop table t1;
drop table t2;

create table t1(i int, t1_autogen int generated always as identity);
create table t2(i int, t2_autogen int generated by default as identity);

insert into t1(i,t1_autogen) values(2,1);
insert into t1(i,t1_autogen) values(2,2);
insert into t1(i) values(2);
insert into t1(i) values(2);
select * from t1;

insert into t2(i,t2_autogen) values(2,1);
insert into t2(i,t2_autogen) values(2,2);
insert into t2(i) values(2);
insert into t2(i) values(2);
select * from t2;

drop table t1;
drop table t2;


--with increment by 

create table t1(i int, t1_autogen int generated always as identity(increment by 10));
create table t2(i int, t2_autogen int generated by default as identity(increment by 10));

insert into t1(i) values(1);
insert into t1(i) values(1);
select * from t1;

insert into t2(i) values(1);
insert into t2(i) values(1);
select * from t2;

drop table t1;
drop table t2;

create table t1(i int, t1_autogen int generated always as identity(increment by 10));
create table t2(i int, t2_autogen int generated by default as identity(increment by 10));

insert into t1(i,t1_autogen) values(2,1);
insert into t1(i,t1_autogen) values(2,2);
insert into t1(i) values(2);
insert into t1(i) values(2);
select * from t1;

insert into t2(i,t2_autogen) values(2,1);
insert into t2(i,t2_autogen) values(2,2);
insert into t2(i) values(2);
insert into t2(i) values(2);
select * from t2;

drop table t1;
drop table t2;


--with start with, increment by 

create table t1(i int, t1_autogen int generated always as identity(start with 100, increment by 20));
create table t2(i int, t2_autogen int generated by default as identity(start with 100, increment by 20));

insert into t1(i) values(1);
insert into t1(i) values(1);
select * from t1;

insert into t2(i) values(1);
insert into t2(i) values(1);
select * from t2;

drop table t1;
drop table t2;

create table t1(i int, t1_autogen int generated always as identity(start with 100, increment by 20));
create table t2(i int, t2_autogen int generated by default as identity(start with 100, increment by 20));

insert into t1(i,t1_autogen) values(2,1);
insert into t1(i,t1_autogen) values(2,2);
insert into t1(i) values(2);
insert into t1(i) values(2);
select * from t1;

insert into t2(i,t2_autogen) values(2,1);
insert into t2(i,t2_autogen) values(2,2);
insert into t2(i) values(2);
insert into t2(i) values(2);
select * from t2;

drop table t1;
drop table t2;


--with unique constraint

create table t3(i int,t3_autogen int generated by default as identity(start with 0, increment by 1) unique);

insert into t3(i,t3_autogen) values(1,0);
insert into t3(i,t3_autogen) values(2,1);

insert into t3(i) values(3);
insert into t3(i) values(4);
insert into t3(i) values(5);

select i,t3_autogen from t3;

drop table t3;

--with unique index

create table t4(i int,t4_autogen int generated by default as identity(start with 0, increment by 1));
create unique index idx_t4_autogen on t4(t4_autogen);

insert into t4(i,t4_autogen) values(1,0);
insert into t4(i,t4_autogen) values(2,1);

insert into t4(i) values(3);
insert into t4(i) values(4);
insert into t4(i) values(5);

select i,t4_autogen from t4;

drop index idx_t4_autogen;
drop table t4;

-- test IDENTITY_VAL_LOCAL function with 2 different connections
-- connection one
connect 'wombat' as conn1;
create table t1 (c11 int generated always as identity (start with 101, increment by 3), c12 int);
create table t2 (c21 int generated always as identity (start with 201, increment by 5), c22 int);
-- IDENTITY_VAL_LOCAL() will return NULL because no single row insert into table with identity column yet on this connection conn1
values IDENTITY_VAL_LOCAL();
commit;
-- connection two
connect 'wombat' as conn2;
-- IDENTITY_VAL_LOCAL() will return NULL because no single row insert into table with identity column yet on this connection conn2
values IDENTITY_VAL_LOCAL();
insert into t2 (c22) values (1);
-- IDENTITY_VAL_LOCAL() will return 201 because there was single row insert into table t2 with identity column on this connection conn2
values IDENTITY_VAL_LOCAL();
set connection conn1;
-- IDENTITY_VAL_LOCAL() will continue to return NULL because no single row insert into table with identity column yet on this connection conn1
values IDENTITY_VAL_LOCAL();
insert into t1 (c12) values (1);
-- IDENTITY_VAL_LOCAL() will return 101 because there was single row insert into table t1 with identity column on this connection conn1
values IDENTITY_VAL_LOCAL();
set connection conn2;
-- IDENTITY_VAL_LOCAL() on conn2 not impacted by single row insert into table with identity column on conn1
values IDENTITY_VAL_LOCAL();
-- notice that committing the transaction does not affect IDENTITY_VAL_LOCAL()
commit;
values IDENTITY_VAL_LOCAL();
-- notice that rolling the transaction does not affect IDENTITY_VAL_LOCAL()
values IDENTITY_VAL_LOCAL();
drop table t1;
drop table t2;

-- A table with identity column has an insert trigger which inserts into another table 
-- with identity column. IDENTITY_VAL_LOCAL will return the generated value for the 
-- statement table and not for the table that got modified by the trigger
create table t1 (c11 int generated always as identity (start with 101, increment by 3), c12 int);
create table t2 (c21 int generated always as identity (start with 201, increment by 5), c22 int);
create trigger t1tr1 after insert on t1 for each row insert into t2 (c22) values (1);
values IDENTITY_VAL_LOCAL();
insert into t1 (c12) values (1);
-- IDENTITY_VAL_LOCAL will return 101 which got generated for table t1. 
-- It will not return 201 which got generated for t2 as a result of the trigger fire.
values IDENTITY_VAL_LOCAL();
select * from t1;
select * from t2;
drop table t1;
drop table t2;

-- Test RESTART WITH syntax of ALTER TABLE for autoincrment columns
create table t1(c11 int generated by default as identity(start with 2, increment by 2), c12 int);
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';

insert into t1 values(2,2);
select * from t1;
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';

insert into t1(c12) values(9999);
select * from t1;
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';

-- try RESTART WITH on a non-autoincrement column. It should fail
alter table t1 alter column c12 RESTART WITH 2;
-- try RESTART WITH with a non-integer column
alter table t1 alter column c11 RESTART WITH 2.20;
alter table t1 alter column c11 RESTART WITH 2;
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';

autocommit off;
drop table t1;
create table t1(c11 int generated by default as identity (start with 1, increment by 1), c12 int);
--following puts locks on system table SYSCOLUMNS's row for t1.c11
--Later when a user tries to have the system generate a value for the
--t1.c11, system can't generate that value in a transaction of it's own
--and hence it reverts to the user transaction to generate the next value.
--This use of user transaction to generate a value can be problematic if
--user statement to generate the next value runs into statement rollback.
--This statement rollback will cause the next value generation to rollback
--too and system will not be able to consume the generated value. 
--In a case like this, user can use ALTER TABLE....RESTART WITH to change the
--start value of the autoincrement column as shown below.
create unique index t1i1 on t1(c11); 
insert into t1 values(1,1);
select * from t1;
-- you will notice that the next value for generated column is 1 at this point
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';
insert into t1(c12) values(3);
-- the insert above fails as expected because there is already a *1* in the table. 
--But the generated value doesn't get consumed and following select will still show 
--next value for generated column as 1. If autocommit was set to on, you would see
-- the next generated value at this point to be 2.
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';
select * from t1;
--the following insert will keep failing because it is going to use 1 as the generated
--value for c11 again and that will cause unique key violation
insert into t1(c12) values(3);
select * from t1;
--User can change the RESTART WITH for autoincrement column to say 2 at this point,
--and then the insert above will not fail
alter table t1 alter column c11 restart with 2;
select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC 
from sys.syscolumns where COLUMNNAME = 'C11';
insert into t1(c12) values(3);
select * from t1;

-- Since RESTART is not a reserved keyword, we should be able to create a table with name RESTART
create table restart (c11 int);
select * from restart;
create table newTable (restart int);
select * from newTable;
create table newTable2 (c11 int);
alter table newTable2 add column RESTART int;
select * from newTable2;

-- Verify that if we change the START WITH value for a GENERATED_BY_DEFAULT
-- column, the column is still GENERATED_BY_DEFAULT and its INCREMENT BY
-- value is preserved
CREATE TABLE DERBY_1495 (
  id INT GENERATED BY DEFAULT AS IDENTITY
		(START WITH 1, INCREMENT BY 1) NOT NULL
 ,col2 INT NOT NULL);

SELECT		col.columndefault, col.columndefaultid,
			col.autoincrementvalue, col.autoincrementstart,
			col.autoincrementinc
	FROM sys.syscolumns col
		INNER JOIN sys.systables tab ON col.referenceId = tab.tableid
	WHERE tab.tableName = 'DERBY_1495' AND ColumnName = 'ID';

-- Insert using an explicit value on the ID-field
INSERT INTO DERBY_1495(ID, COL2) VALUES(2, 2);

-- Reset the identity field
ALTER TABLE DERBY_1495 ALTER COLUMN id RESTART WITH 3;

SELECT		col.columndefault, col.columndefaultid,
			col.autoincrementvalue, col.autoincrementstart,
			col.autoincrementinc
	FROM sys.syscolumns col
		INNER JOIN sys.systables tab ON col.referenceId = tab.tableid
	WHERE tab.tableName = 'DERBY_1495' AND ColumnName = 'ID';

INSERT INTO DERBY_1495(ID, COL2) VALUES(4, 4);
INSERT INTO DERBY_1495(COL2) VALUES(4);

-- Similarly, verify that if we change the INCREMENT BY value for a
-- GENERATED_BY_DEFAULT column, the column remains GENERATED_BY_DEFAULT
-- and its START WITH value is preserved.
create table derby_1645 (
   TableId INTEGER GENERATED BY DEFAULT AS IDENTITY NOT NULL,
   StringValue VARCHAR(20) not null,
   constraint PK_derby_1645 primary key (TableId));

SELECT		col.columndefault, col.columndefaultid,
			col.autoincrementvalue, col.autoincrementstart,
			col.autoincrementinc
	FROM sys.syscolumns col
		INNER JOIN sys.systables tab ON col.referenceId = tab.tableid
	WHERE tab.tableName = 'DERBY_1645' AND ColumnName = 'TABLEID';

INSERT INTO derby_1645 (TableId, StringValue) VALUES (1, 'test1');
INSERT INTO derby_1645 (TableId, StringValue) VALUES (2, 'test2');
INSERT INTO derby_1645 (TableId, StringValue) VALUES (3, 'test3');

ALTER TABLE derby_1645 ALTER TableId SET INCREMENT BY 50;

SELECT		col.columndefault, col.columndefaultid,
			col.autoincrementvalue, col.autoincrementstart,
			col.autoincrementinc
	FROM sys.syscolumns col
		INNER JOIN sys.systables tab ON col.referenceId = tab.tableid
	WHERE tab.tableName = 'DERBY_1645' AND ColumnName = 'TABLEID';

INSERT INTO derby_1645 (StringValue) VALUES ('test53');
INSERT INTO derby_1645 (TableId, StringValue) VALUES (-999, 'test3');

-- Test cases related to DERBY-1644, which involve:
--  a) multi-row VALUES clauses
--  b) GENERATED BY DEFAULT autoincrement fields
--  c) insert statements which mention only a subset of the table's columns
-- First we have the actual case from the bug report. Then we have a number
-- of other similar cases, to try to cover the code area in question
create table D1644 (c1 int, c2 int generated by default as identity);
insert into D1644 (c2) values default, 10;

insert into D1644 (c2) values (11);
insert into D1644 (c2) values default;
insert into D1644 (c2) values (default);
insert into D1644 (c2) values 12, 13, 14;
insert into D1644 (c2) values 15, 16, default;
insert into D1644 values (17, 18);
insert into D1644 values (19, default);
insert into D1644 values (20, default), (21, 22), (23, 24), (25, default);
insert into D1644 (c2, c1) values (default, 26);
insert into D1644 (c2, c1) values (27, 28), (default, 29), (30, 31);
insert into D1644 (c2) values default, default, default, default;
insert into D1644 (c2, c1) values (default, 128),(default, 129),(default, 131);
select * from D1644;

create table D1644_A (c1 int, c2 int generated by default as identity, c3 int);
insert into D1644_A (c3, c1, c2) values (1, 2, default);
insert into D1644_A (c3, c1, c2) values (3,4,5), (6,7,default);
insert into D1644_A (c3, c2) values (8, default), (9, 10);
select * from D1644_A;
create table D1644_B (c1 int generated by default as identity);
insert into D1644_B (c1) values default, 10;
insert into D1644_B values default, 10;
select * from D1644_B;

-- Derby-2902: can't use LONG.MIN_VALUE as the start value for
-- an identity column. These tests verify that values less than MIN_VALUE
-- or greater than MAX_VALUE are rejected, but MIN_VALUE and MAX_VALUE
-- themeselves are accepted.
create table t2902_a (c1 bigint generated always as identity
(start with -9223372036854775807));
create table t2902_b (c1 bigint generated always as identity
(start with +9223372036854775807));
create table t2902_c (c1 bigint generated always as identity
(start with -9223372036854775808));
create table t2902_d (c1 bigint generated always as identity
(start with 9223372036854775808));
create table t2902_e (c1 bigint generated always as identity
(start with -9223372036854775809));
drop table t2902_a;
drop table t2902_b;
drop table t2902_c;

-- DERBY-4006: can't alter a column's default value to NULL. This problem
-- was a regression from the fixes for DERBY-1495 and DERBY-1645, and
-- involved an inability to distinguish between not specifying the DEFAULT
-- clause on the ALTER COLUMN statement at all, versus specify the clause
-- DEFAULT NULL
create table d4006 (x varchar(5) default 'abc');
insert into d4006 values default;
alter table d4006 alter column x with default null;
insert into d4006 values default;
alter table d4006 alter column x with default 'def';
insert into d4006 values default;
select * from d4006;
drop table d4006;
-- Note that if the column is GENERATED ALWAYS the default CAN be altered,
-- but this is probably incorrect. See DERBY-4011 for more discussion.
create table d4006_a (z int generated always as identity);
alter table d4006_a alter column z default 99; -- should fail DERBY-4011
alter table d4006_a alter column z default null; -- should fail DERBY-4011
drop table d4006_a;

