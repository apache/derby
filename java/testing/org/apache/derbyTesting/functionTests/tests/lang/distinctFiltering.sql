-- test filtering of duplicates at language layer
-- for in-order distincts

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- create some tables
create table t1(c1 int, c2 char(50), c3 char(50));
create table t2(c1 int, c2 char(50), c3 char(50));
-- t1 gets non-unique indexes, t2 gets unique
create index t11 on t1(c1);
create index t12 on t1(c1, c2);
create index t13 on t1(c1, c3, c2);
create unique index t21 on t2(c1, c2);
create unique index t22 on t2(c1, c3);

-- populate 
insert into t1 values (1, '1', '1'), (1, '1', '1'),
	(1, '11', '11'), (1, '11', '11'), (2, '2', '2'),
	(2, '2', '3'), (2, '3', '2'), (3, '3', '3'),
	(null, null, null);
insert into t2 values (1, '1', '1'), (1, '2', '2'),
		      (2, '1', '1'), (2, '2', '2'),
		      (null, 'null', 'null');

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 7000;

-- first column of an index
select distinct c1 from t1 where 1=1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c1 from t1 where 1=1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- equality predicate on preceding key columns
select distinct c2 from t1 where c1 = 1 and c3 = '1';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- equality predicate on all key columns, non unique
select distinct c3 from t1 where c1 = 1 and c2 = '1';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- equality predicate on all key columns, non unique
select distinct c3 from t2 where c1 = 1 and c2 = '1';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- different orderings
select distinct c2, c1 from t1 where 1=1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c2 from t1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c2, c1 from t1 where c3 = '1';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c2 from t1 where c3 = '1' and c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ordered, but no where clause - uses distinct scan

-- the following approach is used because the ordering of the results from
-- the distinct is not guaranteed (it varies depending on the JVM hash 
-- implementation), but adding an order by to the query may
-- change how we execute the distinct and we want to test the code path without
-- the order by.  By adding the temp table, we can maintain a single master
-- file for all JVM's.

create table temp_result (result_column int);

insert into temp_result 
    (select distinct c1 from t1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select * from temp_result order by result_column;
drop table temp_result;

-- test distinct with an order by
select distinct c1 from t1 order by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- clean up
drop table t1;
drop table t2;
