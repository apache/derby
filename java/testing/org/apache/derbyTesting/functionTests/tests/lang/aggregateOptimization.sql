-- test various aggregate optimizations
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- create and populate tables
create table t1(c1 int, c2 char(200));
insert into t1 (c1) values 10, 9, 10, 9, 8, 7, 6, 1, 3;
update t1 set c2 = CHAR(c1);

-- distinct min -> min, distinct max -> max
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 7000;

select min(distinct c1), max(distinct(c1)) from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select min(distinct c1), max(distinct(c1)) from t1 group by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- min optimization
create index i1 on t1(c1);
-- min column is 1st column in index
select min(c1) from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
create index i2 on t1(c2, c1);
-- equality predicates on all key columns preceding min column 
select min(c1) from t1 where c2 = '10';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- equality predicates on all key columns preceding min column, 
-- not a unique index
select min(c2) from t1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
delete from t1;
drop index i1;
create unique index i1 on t1(c1);
insert into t1 values (1, '1'), (2, '2');
-- equality predicates on all key columns preceding min column, 
-- a unique index
select min(c2) from t1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- group by ordered on grouping columns
create table t2(c1 int, c2 int, c3 int, c4 int);
create index t2_i1 on t2(c1);
create index t2_i2 on t2(c1, c2);
-- empty table
select c1, sum(c2) from t2 group by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- 1 row table
insert into t2 values (1, 1, 1, 1);
select c1, sum(c2) from t2 group by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- multiple rows, 1 group
insert into t2 values (1, 2, 2, 2), (1, -1, -1, -1);
select c1, sum(c2) from t2 group by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- multiple rows, multiple groups
insert into t2 values (2, 3, 2, 2), (2, 3, -1, -1);
select c1, sum(c2) from t2 group by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- ordered, but in reverse order 
select c2, c1, sum(c3) from t2 group by c2, c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- clean up
drop table t1;
drop table t2;


--
-- max optimization: the optimization is to call the store
-- with a special request for the last row in an index.  so
-- we cannot deal with any predicates
--
set isolation read committed;
create table x (x int, y int);
create index ix on x(x);
create index ixy on x(x,y);
insert into x values (3,3),(7,7),(2,2),(666,6),(1,1);

select max(x) from x;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(x) from x;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- cannot use max opt
select max(x) from x;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(x) from x where x < 99;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(x) from x where x = 7;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(x) from x where y = 7;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(x) from x where y = 7;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(y) from x where y = 7;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select max(x) from x group by x;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- could do max optimization on this, but we don't 
-- really know much about qualifications
select max(x) from x where x > 99;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();


autocommit off;

prepare p as 'select max(x) from x';
execute p;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
insert into x values (99999,99999);
execute p;
rollback;
execute p;
delete from x;
execute p;
rollback;

-- since max uses some funky store interface, lets
-- check locking
connect 'wombat' as conn2;
set isolation to rr;

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
insert into x values (99999,null);

set connection connection0;
-- should deadlock
select max(x) from x;

set connection conn2;
commit;
insert into x values (99980,null);

set connection connection0;
-- ok - should not block on previous key (lock held by conn2 on 99980)
select max(x) from x;

set connection conn2;
delete from x where x = 99980;
delete from x where x = 99999;
commit;

set connection connection0;
-- ok
select max(x) from x;

set connection conn2;
insert into x values (-1,null);

set connection connection0;
-- does not deadlock in current implementation, as it handles cases where
-- the last row is deleted, but the maximum values is somewhere on the last
-- page.
select max(x) from x;

set connection conn2;
insert into x values (100000,null);
commit;

set connection connection0;
-- ok
select max(x) from x;

set connection connection0;
rollback;
disconnect;

set connection conn2;
rollback;
disconnect;


-- check case where all rows are deleted off the last page of index, store 
-- will fault over to doing full table scan, rather than max optimization.

connect 'wombat' as conn1;
set isolation to rr;

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
drop table x;
create table x (a bigint, b int);

-- insert enough rows so that there are multiple pages in the index.
insert into x values (1, 1);
insert into x (select a + 1,   b from x);
insert into x (select a + 2,   b from x);
insert into x (select a + 4,   b from x);
insert into x (select a + 8,   b from x);
insert into x (select a + 16,  b from x);
insert into x (select a + 32,  b from x);
insert into x (select a + 64,  b from x);
insert into x (select a + 128, b from x);
insert into x (select a + 256, b from x);
create index x_idx on x (a);
commit;

connect 'wombat' as conn2;
set isolation to rr;

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
commit;

set connection conn1;
-- get lock on first row of table
insert into x values (0, 0);

set connection conn2;
-- delete all the rows from the last page in the index, but don't commit or
-- else post commit will remove the page from the index.
delete from x where a > 4;

-- lock timeout in current implementation - to be fixed when row level locked
-- backward scan exists.
--
--      this one deadlocks because we have not done a complete implementation
--      of backward scan for max on btree.  If the last page in the table is
--      all deletes, then instead of doing a backward scan we fault over
--      to the un-optimized max code which does a forward scan from the 
--      beginnning of the table.
select max(a) from x;

-- cleanup
set connection conn1;
rollback;
disconnect;
set connection conn2;
drop table x;
commit;
-- test a table with null values to be sure we do the right thing on optimization
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
create table t1(a int, b int);
insert into t1 values (null, null);
insert into t1 values (10, 10), (9, 9), (10, 10), (9, 9), (8, 8), (7, 7), (6, 6), 
	(1,1), (3,3);
create index aindex on t1(a);
create index bindex on t1(b desc);
select min(a) from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- min of b should use max optimization whether b in nullable or not because NULLS are sorted high
select min(b) from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select max(a) from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select max(b) from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
create table t2 (a int not null, b int not null);
insert into t2 select a, b from t1 where a is not null and b is not null;
create index bindex2 on t2(b desc);
-- min of b should use max optimization since b is nullable or not because NULLS are sorted high
select min(b) from t2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
drop table t1;
drop table t2;
