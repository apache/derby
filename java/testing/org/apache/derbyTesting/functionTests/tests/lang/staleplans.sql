-- This is the test for stale plan invalidation.  The system determines
-- at execution whether the tables used by a DML statement have grown or
-- shrunk significantly, and if so, causes the statement to be recompiled
-- at the next execution.
run resource 'createTestProcedures.subsql';

autocommit off;

-- Create and populate a table to be used for flushing the cache.
-- Flushing the cache causes all row count changes to be written,
-- which is necessary for the results of this test to be stable
-- (because otherwise the row count changes would be written
-- asynchronously)
create table flusher (c1 varchar(3000));
insert into flusher values (PADSTRING('a',3000));	-- 1 row
insert into flusher select c1 from flusher;		-- 2 rows
insert into flusher select c1 from flusher;		-- 4 rows
insert into flusher select c1 from flusher;		-- 8 rows
insert into flusher select c1 from flusher;		-- 16 rows
insert into flusher select c1 from flusher;		-- 32 rows
insert into flusher select c1 from flusher;		-- 64 rows
commit;

-- Negative test - try setting stalePlanCheckInterval to a value out of range
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.stalePlanCheckInterval', '2');

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 3500;

-- Make it check for stale plans every 10th execution.  The default is 100,
-- which would force the test to take a lot longer to run, due to more
-- executions.
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.stalePlanCheckInterval', '10');
commit;

create table t1 (c1 int, c2 int, c3 varchar(255));
commit;

create index t1x on t1(c1);
commit;

insert into t1 values (1, 100, PADSTRING('abc',255));
commit;

-- Make sure row count from insert is flushed out
select count(c1) from flusher;

prepare s1 as 'select count(c1 + c2) from t1 where c1 = 1';

execute s1;

-- Expect this to do a table scan
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Execute 11 more times, the plan should not change
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;

-- Expect this to do a table scan
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
commit;

-- Now increase the size of the table
insert into t1 values (2, 100, PADSTRING('abc',255));
insert into t1 values (3, 100, PADSTRING('abc',255));
insert into t1 values (4, 100, PADSTRING('abc',255));
insert into t1 values (5, 100, PADSTRING('abc',255));
insert into t1 values (6, 100, PADSTRING('abc',255));
insert into t1 values (7, 100, PADSTRING('abc',255));
insert into t1 values (8, 100, PADSTRING('abc',255));
insert into t1 values (9, 100, PADSTRING('abc',255));
insert into t1 values (10, 100, PADSTRING('abc',255));
commit;

-- Make sure row count from inserts is flushed out
select count(c1) from flusher;

-- Execute 11 times, the plan should change
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;

-- Expect this to use index
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
commit;

-- Now shrink the table back to its original size
delete from t1 where c1 >= 2;
commit;

-- Make sure row count from inserts is flushed out
select count(c1) from flusher;

-- Execute 11 times, the plan should change
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;
execute s1;

-- Expect this to do a table scan
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
commit;

-- Now do the same thing with a table that has many rows
create table t2 (c1 int, c2 int, c3 varchar(255));
commit;

create index t2x on t2(c1);
commit;

insert into t2 values (1, 1, PADSTRING('abc',255));
insert into t2 select c1 + 1, c2 + 1, c3 from t2;
insert into t2 select c1 + 2, c2 + 2, c3 from t2;
insert into t2 select c1 + 4, c2 + 4, c3 from t2;
insert into t2 select c1 + 8, c2 + 8, c3 from t2;
insert into t2 select c1 + 16, c2 + 16, c3 from t2;
insert into t2 select c1 + 32, c2 + 32, c3 from t2;
insert into t2 select c1 + 64, c2 + 64, c3 from t2;
insert into t2 select c1 + 128, c2 + 128, c3 from t2;
insert into t2 select c1 + 256, c2 + 256, c3 from t2;
insert into t2 select c1 + 512, c2 + 512, c3 from t2;
commit;

-- Make sure row count from inserts is flushed out
select count(c1) from flusher;

prepare s2 as 'select count(c1 + c2) from t2 where c1 = 1';

execute s2;

-- Expect this to use index
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
commit;

-- Change the row count a little bit
insert into t2 values (1025, 1025, PADSTRING('abc',255));
insert into t2 values (1026, 1026, PADSTRING('abc',255));
insert into t2 values (1027, 1027, PADSTRING('abc',255));
insert into t2 values (1028, 1028, PADSTRING('abc',255));
insert into t2 values (1029, 1029, PADSTRING('abc',255));
insert into t2 values (1030, 1030, PADSTRING('abc',255));
insert into t2 values (1031, 1031, PADSTRING('abc',255));
insert into t2 values (1032, 1032, PADSTRING('abc',255));
insert into t2 values (1033, 1033, PADSTRING('abc',255));
insert into t2 values (1034, 1034, PADSTRING('abc',255));
commit;


-- Change the data so a table scan would make more sense.
-- Use a qualifier to convince TableScanResultSet not to
-- update the row count in the store (which would make it
-- hard for this test to control when recompilation takes
-- place).
update t2 set c1 = 1 where c1 > 0;

-- Make sure row count from inserts is flushed out
select count(c1) from flusher;

-- Execute 11 more times, the plan should not change
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;

-- Expect this to use tables scan, as the above update has basically made
-- all the rows in the table be equal to "1", thus using the index does not
-- help if all the rows are going to qualify.
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Change the row count significantly
insert into t2 select c1, c2, c3 from t2 where c1 < 128;

-- Make sure row count from inserts is flushed out
select count(c1) from flusher;

-- Execute 11 times, the plan should change
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;

-- Expect this to do table scan
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();



-- Change the distribution back to where an index makes sense.

update t2 set c1 = c2;

-- Change the row count significantly
insert into t2 select c1, c2, c3 from t2;

-- Make sure row count from inserts is flushed out
select count(c1) from flusher;

-- Execute 11 times, the plan should change
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;
execute s2;

-- Expect this to do index to baserow.
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

rollback;

