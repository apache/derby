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

--
-- this test is for basic update functionality
--



-- create the table
create table t1 (int_col int, smallint_col smallint, char_30_col char(30),
		 varchar_50_col varchar(50));
create table t2 (int_col int, smallint_col smallint, char_30_col char(30),
		 varchar_50_col varchar(50));

-- populate t1
insert into t1 values (1, 2, 'char_30_col', 'varchar_50_col');
insert into t1 values (null, null, null, null);
insert into t2 select * from t1;
select * from t1;

-- update with constants
update t1 set int_col = 3, smallint_col = 4, char_30_col = 'CHAR_30_COL',
	      varchar_50_col = 'VARCHAR_50_COL';
select * from t1;
update t1 set varchar_50_col = null, char_30_col = null, smallint_col = null,
	      int_col = null;
select * from t1;

update t1 set smallint_col = 6, int_col = 5, varchar_50_col = 'varchar_50_col',
	      char_30_col = 'char_30_col';
select * from t1;

-- update columns with column values
update t1 set smallint_col = int_col, int_col = smallint_col,
	      varchar_50_col = char_30_col, char_30_col = varchar_50_col;
select * from t1;
update t1 set int_col = int_col, smallint_col = smallint_col,
	      char_30_col = char_30_col, varchar_50_col = varchar_50_col;
select * from t1;

-- Negative test - column in SET clause twice
update t1 set int_col = 1, int_col = 2;

-- Negative test - non-existent column in SET clause
update t1 set notacolumn = int_col + 1;

-- target table in source - deferred update
--
-- first, populate table
delete from t1;
insert into t1 values (1, 1, 'one', 'one');
insert into t1 values (2, 2, 'two', 'two');
delete from t2;
insert into t2 select * from t1;

autocommit off;

select * from t1;
update t1 set int_col =
	(select t1.int_col
	 from t1, t2
	 where t1.int_col = t2.int_col and t1.int_col = 1);
select * from t1;
rollback;

update t1 set int_col =
	(select
		(select int_col
		 from t1
		 where int_col = 2)
	 from t2
	 where int_col = 1);
select * from t1;
rollback;

update t1 set int_col =
	(select 1
	 from t2
	 where int_col = 2
	 and 1 in
		(select int_col
		 from t1)
	);
select * from t1;
rollback;

update t1 set int_col =
	(select int_col
	 from
		(select int_col
		 from t1) a
	 where int_col = 2);
select * from t1;
rollback;

update t1 set int_col =
	(select int_col
	 from t2
	 where int_col = 37
	union
	 select int_col
	 from t1
	 where int_col = 2);
select * from t1;
rollback;

update t1 set int_col =
	(select int_col
	 from t2
	 where int_col = 37
	union
	 select int_col
	 from
		(select int_col
		 from t1
		 where int_col = 2) a
	);
select * from t1;
rollback;

-- single-row deferred update
update t1 set int_col =
	(select int_col
	 from t1
	 where int_col = 1)
where int_col = 2;
select * from t1;
rollback;

-- zero-row deferred update - degenerate case
update t1 set int_col =
	(select int_col
	 from t1
	 where int_col = 1)
where int_col = 37;
select * from t1;
rollback;

autocommit on;

-- drop the table
drop table t1;
drop table t2;


-- Show whether update is statement atomic or not
create table s (s smallint, i int);
insert into s values (1, 1);
insert into s values (1, 65337);
insert into s values (1, 1);
select * from s;
-- this should fail and no rows should change
update s set s=s+i;
-- this select should have the same results as the previous one.
select * from s;

-- Show that the table name can be used on the set column
update s set s.s=3;
-- and that it must match the target table
update s set t.s=4;
select * from s;

-- do some partial updates
create table t1 (c1 char(250), c2 varchar(100), c3 varchar(100));

insert into t1 values ('a', 'b', 'c');
insert into t1 values ('a', 'b', 'c');
insert into t1 values ('a', 'b', 'c');
insert into t1 values ('a', 'b', 'c');

update t1 set c1 = '1st';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c2 = '2nd';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c3 = '3rd';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c3 = '4th', c2 = '4th';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c1 = '5th', c3 = '5th';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c2 = 'expandingxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c3 = 'expandingxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c2 = 'shrink';
update t1 set c3 = 'shrink';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;
update t1 set c2 = 'expandingxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
			c3 = 'expandingxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1;

drop table t1;
create table t1 (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int);
insert into t1 values (1,2,3,4,5,6,7,8,9);
update t1 set c3 = 33, c5 = 55, c6 = 666, c8 = 88;
select * from t1;
update t1 set c9 = 99;
select * from t1;

drop table t1;

--
-- here we test extra state lying around in the
-- deleteResultSet on a prepared statement that
-- is executed multiple times.  if we don't
-- get a nasty error then we are ok
--
create table x (x int, y int);
create index ix on x(x);
create index iy on x(y);
insert into x values (1,1),(2,2),(3,3);
autocommit off;
prepare p as 'update x set x = x where x = ? and y = ?';
execute p using 'values (1,1)';
execute p using 'values (2,2)';
commit;

-- test extra state in update 
get cursor c1 as 'select * from x for update of x';
prepare p1 as 'update x set x = x where current of c1';
execute p1;
next c1;
execute p1;
next c1;
next c1;
execute p1;
close c1;
execute p1;

-- clean up
autocommit on;
drop table x;

-- bug 4318, possible deadlock if table first has IX, then X table lock; make
-- sure you don't have IX table lock and X table lock at the same time

create table tab1 (c1 int not null primary key, c2 int);
insert into tab1 values (1, 8);

autocommit off;

-- default read committed isolation level
update tab1 set c2 = c2 + 3 where c1 = 1;
select type, mode from syscs_diag.lock_table where CAST(tablename AS VARCHAR(128)) = 'TAB1' order by type;
rollback;

-- serializable isolation level
set current isolation to SERIALIZABLE;
update tab1 set c2 = c2 + 3 where c1 = 1;
select type, mode from syscs_diag.lock_table where CAST(tablename AS VARCHAR(128))  = 'TAB1' order by type;
rollback;

autocommit on;
drop table tab1;

--------------------------------------------
--
-- Test upgrade piece of the fix for bug171.
--
--------------------------------------------

create table bug171_employee( empl_id int, bonus int );
create table bug171_bonuses( empl_id int, bonus int );

insert into bug171_employee( empl_id, bonus ) values ( 1, 0 ), ( 2, 0 ), ( 3, 0 );
insert into bug171_bonuses( empl_id, bonus )
values
( 1, 100 ), ( 1, 100 ), ( 1, 100 ),
( 2, 200 ), ( 2, 200 ), ( 2, 200 ),
( 3, 300 ), ( 3, 300 ), ( 3, 300 );

select * from bug171_employee;
select * from bug171_bonuses;

--
-- The problem query. could not use correlation names in update.
--

update bug171_employee e
    set e.bonus =
    (
        select sum( b.bonus ) from bug171_bonuses b
        where b.empl_id = e.empl_id
    );
select * from bug171_employee;

-- positioned update with correlation names

autocommit off;
get cursor bug171_c1 as
'select * from bug171_employee where empl_id = 1 for update';

next bug171_c1;

update bug171_employee e
    set e.bonus =
    (
        select 2 * sum( b.bonus ) from bug171_bonuses b
        where b.empl_id = e.empl_id
    )
where current of bug171_c1;

close bug171_c1;
select * from bug171_employee;

autocommit on;

--
-- Cleanup
--

drop table bug171_employee;
drop table bug171_bonuses;

--
-- DERBY-1329: Correlated subquery in UPDATE ... SET ... WHERE CURRENT OF
--
CREATE TABLE BASICTABLE1(ID INTEGER, C3 CHAR(10));
CREATE TABLE BASICTABLE2(IID INTEGER, CC3 CHAR(10));
insert into BASICTABLE1 (C3, ID) values ('abc', 1);
insert into BASICTABLE2 (CC3, IID) values ('def', 1);

-- Check data.
select * from BASICTABLE1;
select * from BASICTABLE2;

autocommit off;
get cursor c1 as 'select c3, id from basictable1 for update';
next c1;

-- Before fix for DERBY-1329 the following statement would fail with
-- an ASSERT failure or an IndexOutOfBoundsException; after the fix
-- the statement should succeed and the update as well.
update BASICTABLE1 set C3 = (SELECT CC3 FROM BASICTABLE2
  WHERE BASICTABLE1.ID=BASICTABLE2.IID) where current of c1;

-- Check data; BASICTABLE1 should have been updated.
select * from BASICTABLE1;
select * from BASICTABLE2;

-- Cleanup.
rollback;
drop table BASICTABLE1;
drop table BASICTABLE2;

-- tests for DERBY-1043
CREATE TABLE DERBY10431 (ID SMALLINT GENERATED ALWAYS AS IDENTITY, A_COL VARCHAR(15) NOT NULL PRIMARY KEY);
CREATE TABLE DERBY10432 (TYPE VARCHAR(15) NOT NULL, A_COL VARCHAR(15) NOT NULL, AMOUNT SMALLINT NOT NULL DEFAULT 0);

INSERT INTO DERBY10431(A_COL) VALUES ('apples');
INSERT INTO DERBY10432 VALUES ('tree fruit','apples',1);

SELECT * FROM DERBY10431;
SELECT * FROM DERBY10432;

-- after fix for DERBY-1043 this update should cause an exception
UPDATE DERBY10432 SET DERBY10432.A_COL = DERBY10431.A_COL WHERE A_COL = 'apples';

DROP TABLE DERBY10431;
DROP TABLE DERBY10432;