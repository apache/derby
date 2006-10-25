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
------------------------------------------------------------------------------
-- TEST CASES SPECIFIC TO STORE IMPLEMENTATION OF HOLD CURSOR:
-- overview:
--    TEST  0: basic heap  scan tests (0 rows).
--    TEST  1: basic heap  scan tests (multiple rows)
--    TEST  2: basic btree scan tests (zero rows/update nonkey field)
--    TEST  3: basic btree scan tests (multiple rows/update nonkey field)
--    TEST  4: basic btree scan tests (zero rows/read only/no group fetch)
--    TEST  5: basic btree scan tests (multiple rows/read only/no group fetch)
--    TEST  6: basic tests for cursors with order by
--    TEST  7: test of hold cursor code in DistinctScalarAggregateResultSet.java
--    TEST  8: test of hold cursor code in GroupedAggregateResultSet.java
--    TEST  9: test scan positioned on a row which has been purged.
--    TEST 10: test scan positioned on a page which has been purged
--
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- TEST 0: basic heap scan tests (0 rows).
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

run resource 'createTestProcedures.subsql';
autocommit off;
create table foo (a int, data int);

-- the following for update cursors will all use group fetch = 1, thus each
-- next passes straight through to store.
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '1');
get with hold cursor test1 as 'select * from foo for update';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 1: basic heap scan tests (multiple rows)
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

insert into foo values (1, 10);
insert into foo values (1, 20);
insert into foo values (1, 30);
insert into foo values (1, 40);
insert into foo values (1, 50);

-- the following for update cursors will all use group fetch = 1, thus each
-- next passes straight through to store.
get with hold cursor test1 as 'select * from foo for update';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 'select * from foo for update';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 2: basic btree scan tests (zero rows/update nonkey field)
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

autocommit off;
drop table foo;
create table foo (a int, data int);
create index foox on foo (a);

-- the following for update cursors will all use group fetch = 1, thus each
-- next passes straight through to store.
get with hold cursor test1 as
    'select * from foo for update of data';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 3: basic btree scan tests (multiple rows/update nonkey field)
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

insert into foo values (1, 10);
insert into foo values (1, 20);
insert into foo values (1, 30);
insert into foo values (1, 40);
insert into foo values (1, 50);

-- the following for update of data cursors will all use group fetch = 1, thus each
-- next passes straight through to store.
get with hold cursor test1 as
    'select * from foo for update of data';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;

--  test negative case of trying non next operations after commit
get with hold cursor test1 as 
    'select * from foo for update of data';
next  test1;
commit;
delete from foo where current of test1;
next  test1;
commit;
update foo set data=-3000 where current of test1;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;

--  test positive case of trying delete/update after commit and next.
get with hold cursor test1 as 
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
delete from foo where current of test1;
commit;
next  test1;
next  test1;
update foo set data=-3000 where current of test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;

--  make sure above deletes/updates worked.
get with hold cursor test1 as 
    'select * from foo for update of data';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 4: basic btree scan tests (zero rows/read only/no group fetch)
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

-- basic tests for btree
autocommit off;
drop table foo;
create table foo (a int, data int);
create index foox on foo (a);

-- the following for read cursors will all use group fetch = 1, thus each
-- next passes straight through to store.  This select should only use the
-- index with no interaction with the base table.
get with hold cursor test1 as
    'select a from foo ';

close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 5: basic btree scan tests (multiple rows/read only/no group fetch)
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

insert into foo values (1, 10);
insert into foo values (1, 20);
insert into foo values (1, 30);
insert into foo values (1, 40);
insert into foo values (1, 50);

-- the following for read cursors will all use group fetch = 1, thus each
-- next passes straight through to store.  This select should only use the
-- index with no interaction with the base table.

get with hold cursor test1 as
    'select * from foo ';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 
    'select * from foo ';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 6: basic tests for cursors with order by
--     The following tests that no matter where commit comes in the state of
--     the scan that the scan will continue after the commit.  Tests various
--     states of scan like: before first next, after first next, before close,
--     after close.
------------------------------------------------------------------------------

-- basic tests for cursors which include an order by
autocommit off;
drop table foo;
create table foo (a int, data int);
create index foox on foo (a);

-- the following for update cursors will all use group fetch = 1, thus each
-- next passes straight through to store.  This select should only use the
-- index with no interaction with the base table.
get with hold cursor test1 as
    'select a,data from foo order by data desc';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

insert into foo values (1, 10);
insert into foo values (1, 20);
insert into foo values (1, 30);
insert into foo values (1, 40);
insert into foo values (1, 50);
-- insert into foo (select a + 5, data + 50 from foo);

-- the following for update of data cursors will all use group fetch = 1, thus each
-- next passes straight through to store.
get with hold cursor test1 as
    'select a,data from foo order by data desc';
close test1;
commit;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
commit;
close test1;
-- should fail
next test1;

get with hold cursor test1 as 
    'select a,data from foo order by data desc';
next  test1;
commit;
next  test1;
commit;
next  test1;
next  test1;
next  test1;
next  test1;
close test1;
commit;
-- should fail
next test1;


commit;

------------------------------------------------------------------------------
-- TEST 7: test of hold cursor code in DistinctScalarAggregateResultSet.java
--     Directed test of hold cursor as applies to sort scans opened by
--     DistinctScalarAggregateResultSet.java.
-----------------------------------------------------------------------------

drop table t1;
create table t1 (c1 int, c2 int);
insert into t1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10);
select * from t1;

select sum(distinct c1) from t1;

get with hold cursor test1 as 'select sum(distinct c1) from t1';
commit;
next test1;
close test1;

get with hold cursor test1 as 'select sum(distinct c1) from t1';
next test1;
commit;
next test1;
commit;
close test1;

commit;

------------------------------------------------------------------------------
-- TEST 8: test of hold cursor code in GroupedAggregateResultSet.java
--     Directed test of hold cursor as applies to sort scans opened by
--     GroupedAggregateResultSet.java.
-----------------------------------------------------------------------------

drop table t1;
create table t1 (c1 int, c2 int);
insert into t1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10);
select * from t1;

select sum(distinct c1) from t1 group by c2;
commit;

get with hold cursor test1 as 'select sum(distinct c1) from t1 group by c2';
commit;
next test1;
next test1;
commit;
next test1;
close test1;

get with hold cursor test1 as 'select sum(distinct c1) from t1 group by c2';
next test1;
commit;
next test1;
commit;
next test1;
close test1;


------------------------------------------------------------------------------
-- TEST 9: test scan positioned on a row which has been purged.
-----------------------------------------------------------------------------

drop table t1;
create table t1 (c1 int, c2 int);
create index tx on t1 (c1);
insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);

get with hold cursor test1 as 
    'select c1 from t1';
next test1;
commit;

-- at this point the btree scan is positioned by "key" on (1,1).  Make sure
-- deleting this key doesn't cause any problems.
delete from t1 where c1 = 1 or c1 = 2;

next test1;

-- at this point the btree scan is positioned on (3, 3), let's see what happens
-- if we delete (3,3) and look at current scan.

delete from t1 where c1 = 3;

-- position on (4,4)
next test1;

commit;

-- delete all the rows and hopefully get all rows to be purged by the time
-- the scan does the next.
delete from t1;

commit;

next test1;

close test1;

------------------------------------------------------------------------------
-- TEST 10: test scan positioned on a page which has been purged (should really
--          not be any different than a row being purged).
-----------------------------------------------------------------------------

drop table t1;
create table t1 (c1 varchar(1000), c2 int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create index tx on t1 (c1);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
insert into t1 values (PADSTRING('1',1000), 1), (PADSTRING('2',1000), 2), (PADSTRING('3',1000), 3), (PADSTRING('4',1000), 4), (PADSTRING('5',1000), 5), (PADSTRING('6',1000), 6), (PADSTRING('7',1000), 7);

get with hold cursor test1 as 
    'select c1 from t1';
next test1;
commit;

-- at this point the btree scan is positioned by "key" on (1,1).  Make sure
-- deleting this key doesn't cause any problems.
delete from t1 where c1 = PADSTRING('1',1000) or c1 = PADSTRING('2',1000);

next test1;

-- at this point the btree scan is positioned on (3, 3), let's see what happens
-- if we delete (3,3) and look at current scan.

delete from t1 where c1 = PADSTRING('3',1000);

-- position on (4,4)
next test1;

commit;

-- delete all the rows and hopefully get all rows to be purged by the time
-- the scan does the next.
delete from t1;

commit;


next test1;

close test1;

------------------------------------------------------------------------------
-- TEST 11: beetle 4902: test query plans which use reopenScan() on a btree to 
--          do the inner table processing of a join.  Prior to the fix a null
--          pointer exception would be thrown after the commit, as the code
--          did not handle keeping the resultset used for the inner table
--          open across commits in this case.
-----------------------------------------------------------------------------

drop table t1;
drop table t2;
create table t1 (i1 int, i2 int);
create table t2 (i1 int, i2 int);
create index t1_idx on t1 (i1);
create index t2_idx on t2 (i1);


insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
insert into t2 values (1, 10), (2, 20),          (4, 40), (5, 50);

commit;

-- force nestedLoop to make sure reopenScan() is used on inner table.
get with hold cursor test11 as
'select * from
    t1, t2
        where t1.i1 = t2.i1';

commit;
next test11;
commit;
next test11;
commit;
next test11;
next test11;
commit;
commit;
next test11;
commit;

close test11;

------------------------------------------------------------------------------
-- TEST 12: beetle 4902: test query plans which use reopenScan() on a base table
--          to do the inner table processing of a join.  Prior to the fix a null
--          pointer exception would be thrown after the commit, as the code
--          did not handle keeping the resultset used for the inner table
--          open across commits in this case.
-----------------------------------------------------------------------------

drop table t1;
drop table t2;
create table t1 (i1 int, i2 int);
create table t2 (i1 int, i2 int);


insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
insert into t2 values (1, 10), (2, 20),          (4, 40), (5, 50);

commit;

-- force nestedLoop to make sure reopenScan() is used on inner table.
get with hold cursor test12 as
'select * from
    t1, t2
        where t1.i1 = t2.i1';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');


commit;
next test12;
commit;
next test12;
commit;
next test12;
next test12;
commit;
commit;
next test12;
commit;

close test12;


drop table foo;
drop table t1;
drop table t2;
drop function padstring;
drop procedure wait_for_post_commit;
commit;

exit;
