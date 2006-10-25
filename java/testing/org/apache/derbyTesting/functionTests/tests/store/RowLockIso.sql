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
-- Very basic single user testing of row locking, verify that the right locks
-- are obtained for simple operations.  This test only looks at table and
-- row logical locks, it does not verify physical latches or lock ordering.
-- The basic methodology is:
--    start transaction
--    simple operation
--    print lock table which should match the master
--    end transation
-- 
run resource 'createTestProcedures.subsql';
run  resource 'LockTableQuery.subsql';
autocommit off;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create table a (a int, b int, c varchar(1900)) ;

commit;
set isolation read committed;
commit;

--------------------------------------------------------------------------------
-- Test select from empty heap table
--------------------------------------------------------------------------------
select a, b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test select from one row heap table
--------------------------------------------------------------------------------
insert into a values (1, -1, PADSTRING('one',1900));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a, b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test select from two row heap table - this will release one row lock as it
-- moves to the next one.
--------------------------------------------------------------------------------
insert into a values (2, -2, PADSTRING('two',1900));
commit;

select a,b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test select from three row heap table (multiple pages) - this will release 
-- one row lock as it moves to the next one.
--------------------------------------------------------------------------------
insert into a values (3, -3, PADSTRING('two',1900));
insert into a values (4, -4, PADSTRING('two',1900));
insert into a values (5, -5, PADSTRING('two',1900));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

delete from a where a.a = 3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a,b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test full read cursor scan over all the rows in the heap, no group fetch.
--------------------------------------------------------------------------------
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');

-- RESOLVE: missing row locks
-- WORKAROUND: creating an index and dropping it 
-- to force the query 'select a, b from a' to be recompiled
create index ix1 on a(a);
drop index ix1;
commit;

get cursor scan_cursor as
    'select a, b from a';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','16');
--------------------------------------------------------------------------------
-- Test full cursor scan over all the rows in the heap, with 2 row group fetch.
--------------------------------------------------------------------------------
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');

-- RESOLVE: missing row locks
-- WORKAROUND: creating an index and dropping it 
-- to force the query 'select a, b from a' to be recompiled
create index ix1 on a(a);
drop index ix1;
commit;

get cursor scan_cursor as
    'select a, b from a';
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','16');
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

--------------------------------------------------------------------------------
-- Test full cursor scan over all the rows in the heap, with default group fetch
--------------------------------------------------------------------------------
get cursor scan_cursor as
    'select a, b from a';

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

--------------------------------------------------------------------------------
-- Test full cursor for update scan over all the rows in the heap, 
-- with default group fetch.  Group fetch should be disabled.
--------------------------------------------------------------------------------
get cursor scan_cursor as
    'select a, b from a for update';

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

--------------------------------------------------------------------------------
-- Test full read cursor scan on a join over all the rows in the btree, 
-- 2 row group fetch.
--------------------------------------------------------------------------------

drop table a;
create table a (a int, b int, c varchar(1900), d int, e varchar(2000)) ;
create index a_idx on a (a, b, c) ;
commit;

create table b (a int, b int, c varchar(1900)) ;
insert into b values (1, -1, PADSTRING('one',1900));
insert into b values (2, -2, PADSTRING('two',1900));
insert into b values (3, -3, PADSTRING('three',1900));
insert into b values (4, -4, PADSTRING('four',1900));
insert into b values (5, -5, PADSTRING('five',1900));
commit;

--------------------------------------------------------------------------------
-- Test select from empty index
--------------------------------------------------------------------------------
select a, b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test select from one row index'd table
--------------------------------------------------------------------------------
insert into a values (5, -5, PADSTRING('five',1900), 5, PADSTRING('negative five',2000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a, b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test select from two row indexed heap table - this will release one row
-- lock as it moves to the next one.
--------------------------------------------------------------------------------
insert into a values (4, -4, PADSTRING('four',1900), 4, PADSTRING('negative four',2000));
commit;

select a,b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test select from three row indexed heap table (multiple pages) - this will
-- release one row lock as it moves to the next one.
--------------------------------------------------------------------------------
insert into a values (3, -3, PADSTRING('three',1900), 3, PADSTRING('negative three',2000));
insert into a values (2, -2, PADSTRING('two',1900),   2, PADSTRING('negative two',2000));
insert into a values (1, -1, PADSTRING('one',1900),   1, PADSTRING('negtive one',2000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

delete from a where a.a = 3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;



select a,b from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test full read cursor scan over all the rows in the indexed heap, 
-- no group fetch.  This should be a covered index scan (make sure rows come
-- back in order sorted by index).
--------------------------------------------------------------------------------
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');

-- RESOLVE: missing row locks
-- WORKAROUND: creating an index and dropping it 
-- to force the query 'select a, b from a' to be recompiled
create index ix1 on a(a);
drop index ix1;
commit;

get cursor scan_cursor as
    'select a, b from a';
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','16');

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--------------------------------------------------------------------------------
-- Test full cursor scan over all the rows in the index , 2 row group fetch.
--------------------------------------------------------------------------------
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');

-- RESOLVE: missing row locks
-- WORKAROUND: creating an index and dropping it 
-- to force the query 'select a, b from a' to be recompiled
create index ix1 on a(a);
drop index ix1;
commit;

get cursor scan_cursor as
    'select a, b from a';
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','16');

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

--------------------------------------------------------------------------------
-- Test full cursor scan over all the rows in the index, with default group
-- fetch
--------------------------------------------------------------------------------

-- RESOLVE: missing row locks
-- WORKAROUND: creating an index and dropping it 
-- to force the query 'select a, b from a' to be recompiled
create index ix1 on a(a);
drop index ix1;
commit;

get cursor scan_cursor as
    'select a, b from a';

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

close scan_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

--------------------------------------------------------------------------------
-- Test getting index lock on a drop index - track 1634
--------------------------------------------------------------------------------

drop table a;
commit;

create table a (a int);
create index a2 on a (a);
insert into a values (1);
commit;

drop index a2;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;
drop table a;
