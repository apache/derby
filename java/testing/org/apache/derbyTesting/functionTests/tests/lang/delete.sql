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
