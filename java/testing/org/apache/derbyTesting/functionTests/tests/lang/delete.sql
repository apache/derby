
--
-- this test is for basic delete functionality
--

-- create the table
create table t1 (c1 int);
create table t2 (c1 int);

-- negative tests

-- table name required for positioned delete and for searched delete
delete;

-- populate the table
insert into t1 values (1);
insert into t2 select * from t1;

-- delete all the rows (only 1)
select * from t1;
delete from t1;
select * from t1;

-- repopulate the table
insert into t1 values(2);
insert into t1 values(3);

-- delete all the rows (multiple rows)
select * from t1;
delete from t1;
select * from t1;

-- test atomicity of multi row deletes
create table atom_test (c1 smallint);
insert into atom_test values 1, 30000,0, 2;

-- overflow
delete from atom_test where c1 + c1 > 0;
select * from atom_test;

-- divide by 0
delete from atom_test where c1 / c1 = 1;
select * from atom_test;


-- target table in source, should be done in deferred mode

-- repopulate the tables
insert into t1 values(1);
insert into t1 values(2);
insert into t2 select * from t1;

autocommit off;

select * from t1;
delete from t1 where c1 <=
	(select t1.c1
	 from t1, t2
	 where t1.c1 = t2.c1
	 and t1.c1 = 2);
select * from t1;
rollback;

delete from t1 where c1 >=
	(select
		(select c1
		 from t1
		 where c1 = 1)
	 from t2
	 where c1 = 2);
select * from t1;
rollback;

delete from t1 where c1 >=
	(select
		(select c1
		 from t1 a
		 where c1 = 1)
	 from t2
	 where c1 = 2);
select * from t1;
rollback;

-- delete 0 rows - degenerate case for deferred delete
delete from t1 where c1 =
	(select 1
	 from t2
	 where 1 =
		(select c1
		 from t1
		 where c1 = 2)
	);
select * from t1;
rollback;

-- delete 1 row
delete from t1
where c1 =
	(select c1
	 from t1
	 where c1 = 2)
and c1 = 2;
select * from t1;
rollback;

delete from t1 where c1 <=
	(select c1
	 from
		(select c1
		 from t1) a
	 where c1 = 2);
select * from t1;
rollback;

delete from t1 where c1 <=
	(select c1
	 from t2
	 where c1 = 37
	union
	 select c1
	 from t1
	 where c1 = 2);
select * from t1;
rollback;

delete from t1 where c1 <=
	(select c1
	 from t2
	 where c1 = 37
	union
	 select c1
	 from
		(select c1
		from t1) a
	 where c1 = 2);
select * from t1;
rollback;

autocommit on;

-- drop the table
drop table t1;
drop table t2;
drop table atom_test;

--
-- here we test extra state lying around in the
-- deleteResultSet on a prepared statement that
-- is executed multiple times.  if we don't
-- get a nasty error then we are ok
--
create table x (x int, y int);
create index ix on x(x);
insert into x values (1,1),(2,2),(3,3);
autocommit off;
prepare p as 'delete from x where x = ? and y = ?';
execute p using 'values (1,1)';
execute p using 'values (2,2)';
commit;

-- clean up
autocommit on;
drop table x;
