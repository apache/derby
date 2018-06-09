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

--------------------------------------------
--
-- Test delete piece of the fix for bug171.
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
-- The problem query. could not use correlation names in delete.
--

delete from bug171_employee e
    where e.empl_id > 2 and e.bonus <
    (
        select sum( b.bonus ) from bug171_bonuses b
        where b.empl_id = e.empl_id
    );
select * from bug171_employee;

-- positioned delete with correlation names

autocommit off;

get cursor bug171_c2 as
'select * from bug171_employee where empl_id = 2 for update';
next bug171_c2;

delete from bug171_employee e where current of bug171_c2;

close bug171_c2;
select * from bug171_employee;

autocommit on;

--
-- Cleanup
--

drop table bug171_employee;
drop table bug171_bonuses;

--
-- Test case for DERBY-4585
--
create table d4585_t1 (id int primary key, a int);
create table d4585_t2 (id int primary key, b int,
                       constraint fk_t2 foreign key (b) references d4585_t1);
create table d4585_t3 (id int primary key, c int);
create table d4585_t4 (d int references d4585_t2);

insert into d4585_t1 values (16,51),(30,12),(39,24),(48,1),(53,46),(61,9);

insert into d4585_t2 values
    (2,16),(3,61),(4,16),(6,30),(7,16),(10,48),(13,30),(15,48),(17,61),
    (18,30),(21,48),(22,53),(23,61),(25,48),(26,30),(27,48),(29,16),(31,39),
    (33,30),(35,61),(37,30),(40,53),(42,53),(45,16),(49,30),(54,53),(57,53),
    (58,61),(60,30),(63,61),(64,30);

insert into d4585_t3 values
    (1,50),(5,50),(8,50),(9,50),(11,36),(12,50),(14,50),(19,50),(20,50),
    (24,36),(28,50),(32,50),(34,50),(38,50),(41,50),(43,50),(46,36),(47,36),
    (51,36),(52,50),(55,36),(56,44),(59,36),(62,36);

insert into d4585_t4 values (7), (33), (57);

-- The query below resulted in a NullPointerException if a certain query plan
-- was chosen. Use an optimizer override to force that plan.
delete from d4585_t4 where d in
  (select id from d4585_t2 --derby-properties constraint=fk_t2
    where b in (select t1.id
                       from d4585_t1 t1, d4585_t3 t3
                       where t1.a=t3.id and t3.c=36));

-- Verify that the correct rows were deleted.
select * from d4585_t4;

-- Clean up
drop table d4585_t4;
drop table d4585_t3;
drop table d4585_t2;
drop table d4585_t1;
