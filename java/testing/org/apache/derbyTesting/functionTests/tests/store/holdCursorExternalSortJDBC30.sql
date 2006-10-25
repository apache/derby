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
-- TEST CASES SPECIFIC TO STORE IMPLEMENTATION OF HOLD CURSOR (external sort):
-- overview:
--    TEST  0: test hold cursor with external sort (order by).
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
--    TEST  0: test hold cursor with external sort (order by).
--     Cutover to external sort has been set to 4 rows by the test property 
--     file so with 10 rows we get a 1 level external sort.  This tests that
--     temp files will be held open across the commit if the cursor is held
--     open.
------------------------------------------------------------------------------

run resource 'createTestProcedures.subsql';
autocommit off;
create table foo (a int, data varchar(2000));
insert into foo values 
    (10,PADSTRING('10',2000)), (9,PADSTRING('9',2000)), (8,PADSTRING('8',2000)), (7,PADSTRING('7',2000)), (6,PADSTRING('6',2000)), (5,PADSTRING('5',2000)), (4,PADSTRING('4',2000)), (3,PADSTRING('3',2000)), (2,PADSTRING('2',2000)), (1,PADSTRING('1',2000));

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '1');
get with hold cursor test1 as 
    'select * from foo order by a';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next test1;
commit;

next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
commit;
close test1;

-- exercise the non-held cursor path also.

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '1');
get cursor test1 as 
    'select * from foo order by a';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next test1;
next test1;
next test1;
next test1;
next test1;
next test1;
next test1;
next test1;
next test1;
next test1;
next test1;
close test1;
commit;

------------------------------------------------------------------------------
--    TEST  1: test hold cursor with multi-level external sort (order by).
--     Cutover to external sort has been set to 4 rows by the test property 
--     file so with 10 rows we get a 1 level external sort.  This tests that
--     temp files will be held open across the commit if the cursor is held
--     open.
------------------------------------------------------------------------------

insert into foo select a + 100, data from foo;  
insert into foo select a + 10,  data from foo;  
insert into foo select a + 200, data from foo;  
insert into foo select a + 200, data from foo;  

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '1');
get with hold cursor test1 as 
    'select * from foo order by a';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next test1;
commit;

next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
next test1;
next test1;
commit;
next test1;
next test1;
commit;
close test1;

-- clean up
drop function PADSTRING;
drop procedure WAIT_FOR_POST_COMMIT;
drop table foo;
commit;
