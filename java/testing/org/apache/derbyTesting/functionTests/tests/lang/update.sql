
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
select type, mode from new org.apache.derby.diag.LockTable() as lockstable where tablename = 'TAB1' order by type;
rollback;

-- serializable isolation level
set current isolation to SERIALIZABLE;
update tab1 set c2 = c2 + 3 where c1 = 1;
select type, mode from new org.apache.derby.diag.LockTable() as lockstable where tablename = 'TAB1' order by type;
rollback;

autocommit on;
drop table tab1;
