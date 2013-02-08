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
--------------------------------------------------------------------------------
-- Test multi user lock interaction under isolation level 2.  default isolation
-- level has been set as a property to serializable.
--------------------------------------------------------------------------------
run resource '/org/apache/derbyTesting/functionTests/tests/store/createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');

autocommit off;

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

--------------------------------------------------------------------------------
-- Test 0: verify isolation level by seeing if a read lock is released or not.
--------------------------------------------------------------------------------
connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as writer;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- set up
set connection scanner;
autocommit off;
create table test_0 (a int);
insert into test_0 values (1);
commit;

set connection writer;
autocommit off;

-- isolation 2 scanner should release read lock on table after statement.
set connection scanner;
select * from test_0;

-- writer should be able to insert into table - scanner released read lock.
set connection writer;
insert into test_0 values (2);

-- scanner will now block on uncommitted insert, and get lock timeout
set connection scanner;
select * from test_0;
commit;

-- commit writer - releasing all locks.
set connection writer;
commit;

-- scanner will now see 2 rows
set connection scanner;
select * from test_0;
commit;

-- cleanup
set connection scanner;
drop table test_0;
commit;
disconnect;
set connection writer;
disconnect;


--------------------------------------------------------------------------------
-- Test 1: make sure a leaf root growing get's the right lock.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Test setup - create a 1 page btree, with the page ready to split.
--------------------------------------------------------------------------------
connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as rootgrower;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

set connection scanner;
autocommit off;
create table a (a varchar(1200), b varchar(1000)) ;
insert into a values (PADSTRING('a',1200), PADSTRING('a',1000));
insert into a values (PADSTRING('b',1200), PADSTRING('b',1000));
insert into a values (PADSTRING('c',1200), PADSTRING('c',1000));
create index a_idx on a (a) ;
commit;

set connection rootgrower;
autocommit off;
commit;


--------------------------------------------------------------------------------
-- Set up scanner to be doing a row locked covered scan on the index.
--------------------------------------------------------------------------------
set connection scanner;
autocommit off;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');
get cursor scan_cursor as 
    'select a from a where a >= PADSTRING(''a'',1200) and a < PADSTRING(''c'',1200) ';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');
next scan_cursor;

--------------------------------------------------------------------------------
-- Before DERBY-2991 the attempt to split the root would time out because the
-- scan had locked the first page.
--------------------------------------------------------------------------------
set connection rootgrower;
autocommit off;
insert into a values (PADSTRING('d',1200), PADSTRING('d',1000));
rollback;

--------------------------------------------------------------------------------
-- The scan should continue unaffected.
--------------------------------------------------------------------------------
set connection scanner;
next scan_cursor;
next scan_cursor;

--------------------------------------------------------------------------------
-- This insert will block on the previous key lock of the scanner.
--------------------------------------------------------------------------------
set connection rootgrower;
insert into a values (PADSTRING('ab',1200), PADSTRING('ab',1000));

--------------------------------------------------------------------------------
-- Now the grow root should be allowed (note that cursor scan has locks
-- on the leaf page being grown - just not the scan lock).
-- (Scan locks are no longer used after DERBY-2991.)
--------------------------------------------------------------------------------
set connection rootgrower;
insert into a values (PADSTRING('d',1200), PADSTRING('d',1000));

select a from a;


--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection rootgrower;
commit;
disconnect;
set connection scanner;
commit;
drop table a;
commit;
disconnect;


--------------------------------------------------------------------------------
-- Test 2: make sure previous key locks are gotten correctly.
--------------------------------------------------------------------------------
connect 'wombat' as client_1;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as client_2;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

set connection client_1;
autocommit off;
create table a (a varchar(1000), b varchar(1000)) ;
create unique index a_idx on a (a) ;
insert into a values (PADSTRING('b',1000), PADSTRING('b',1000));
insert into a values (PADSTRING('c',1000), PADSTRING('c',1000));
insert into a values (PADSTRING('e',1000), PADSTRING('e',1000));
insert into a values (PADSTRING('f',1000), PADSTRING('f',1000));
insert into a values (PADSTRING('g',1000), PADSTRING('g',1000));
commit;

set connection client_2;
autocommit off;

--------------------------------------------------------------------------------
-- client 1 will get exclusive locks on 'c'.
--------------------------------------------------------------------------------
set connection client_1;
update a set b = 'new value' where a > 'b' and a <= 'd';

-- run resource '/org/apache/derbyTesting/functionTests/tests/store/LockTableQuery.subsql';

set connection client_2;

--------------------------------------------------------------------------------

-- the following will not time out, the insert
-- will get a previous key insert lock which will not conflict with the
-- non-insert read-committed exclusive lock on 'c'.

--------------------------------------------------------------------------------
insert into a values (PADSTRING('d',1000), PADSTRING('d',1000));

--------------------------------------------------------------------------------
-- the following should NOT cause a time out
--------------------------------------------------------------------------------
insert into a values (PADSTRING('a',1000), PADSTRING('a',1000));

--------------------------------------------------------------------------------
-- the following will block because it is a unique index, and the insert is of
-- the same row being locked by client_1
--------------------------------------------------------------------------------
insert into a values (PADSTRING('c',1000), PADSTRING('c',1000));

-- run resource '/org/apache/derbyTesting/functionTests/tests/store/LockTableQuery.subsql';

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection client_1;
select * from a;
commit;
set connection client_2;
commit;
select * from a;
drop table a;
commit;

--------------------------------------------------------------------------------
-- Test 3: make sure an exact key insert into unique key index blocks.
--------------------------------------------------------------------------------
set connection client_1;
autocommit off;
create table a (a varchar(1000), b varchar(1000)) ;
create unique index a_idx on a (a) ;
commit;
insert into a values (PADSTRING('b',1000), PADSTRING('b',1000));

set connection client_2;
autocommit off;
--------------------------------------------------------------------------------
-- the following should cause a time out, as the previous
-- key lock will conflict with client_1's lock on 'b'
--------------------------------------------------------------------------------
insert into a values (PADSTRING('b',1000), PADSTRING('b',1000));

--------------------------------------------------------------------------------
-- Test 4: make sure that row lock wait in a heap scan works
--------------------------------------------------------------------------------
set connection client_1;
autocommit off;
create table test_4 (a int, b varchar(1000), c varchar(1000)) ;
commit;
set connection client_2;
autocommit off;
commit;

-- client_1 will get a single row lock in the heap.
set connection client_1;
insert into test_4 values (1, PADSTRING('a',1000), PADSTRING('b',1000)); 

-- client_2 scans table, blocking on a row lock on the client_1 insert row, 
-- will get timeout message.
set connection client_2;
select * from test_4;

-- release the insert lock.
set connection client_1;
commit;

-- reader should be able to see row now.
set connection client_2;
select * from test_4;
commit;

-- cleanup
set connection client_1;
drop table test_4;
commit;

--------------------------------------------------------------------------------
-- Test 5: make sure a that a group fetch through a secondary index correctly
--         handles a row that is deleted after it has read a row from the index
--         but before it has read the row from the base table.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Test setup - create a 1 page btre, with the page ready to split.
--------------------------------------------------------------------------------
connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as deleter;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;


set connection scanner;
autocommit off;
create table test_5 (a int, a2 int, b varchar(1000), c varchar(1000)) ;
insert into test_5 values (1, 10, PADSTRING('a',1000), PADSTRING('a',1000));
insert into test_5 values (2, 20, PADSTRING('b',1000), PADSTRING('b',1000));
insert into test_5 values (3, 30, PADSTRING('c',1000), PADSTRING('c',1000));
insert into test_5 values (4, 40, PADSTRING('d',1000), PADSTRING('d',1000));
insert into test_5 values (5, 50, PADSTRING('e',1000), PADSTRING('e',1000));
insert into test_5 values (6, 60, PADSTRING('f',1000), PADSTRING('f',1000));
create index test_5_idx on test_5 (a);
commit;

set connection deleter;
autocommit off;
commit;


--------------------------------------------------------------------------------
-- Set up scanner to be doing a row locked index to base row scan on the index.
-- By using group fetch it will read and release locks on multiple rows from
-- the index and save away row pointers from the index.
--------------------------------------------------------------------------------
set connection scanner;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','10');
get cursor scan_cursor as 
    'select a, a2 from test_5 where a > 1 ';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

-- because of group locking will get locks on 1, 2, 3, 4, and 5 and then will
-- release the locks on 1, 2, 3, and 4.  The last one is released on close or
-- on next call emptying the cursor.
next scan_cursor;

--------------------------------------------------------------------------------
-- Delete a row that the scanner has looked at but not reported back to the
-- caller.
--------------------------------------------------------------------------------
set connection deleter;
delete from test_5 where a = 4;

--------------------------------------------------------------------------------
-- The scan will requalify rows when it goes to the base table, thus it will
-- see 3, but block when it gets to the key of deleted row (4).
--------------------------------------------------------------------------------
set connection scanner;
next scan_cursor;
next scan_cursor;

-- commit the delete
set connection deleter;
commit;

-- scanner should see 1,2,3,4,6
set connection scanner;
close scan_cursor;

select a,b from test_5;

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection deleter;
commit;
disconnect;
set connection scanner;
commit;
drop table test_5;
commit;
disconnect;


--------------------------------------------------------------------------------
-- Test 6: make sure a that heap scans which cross page boundaries release
--         locks correctly.
--------------------------------------------------------------------------------

-- Test setup - create a heap with one row per page.

connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as deleter;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;


set connection scanner;
autocommit off;
create table test_6 (a int, a2 int, b varchar(2000), c varchar(2000)) ;
insert into test_6 values (1, 10, PADSTRING('a',2000), PADSTRING('a',2000));
insert into test_6 values (2, 20, PADSTRING('b',2000), PADSTRING('b',2000));
insert into test_6 values (3, 30, PADSTRING('c',2000), PADSTRING('c',2000));
insert into test_6 values (4, 40, PADSTRING('d',2000), PADSTRING('d',2000));
insert into test_6 values (5, 50, PADSTRING('e',2000), PADSTRING('e',2000));
create index test_6_idx on test_6 (a);
commit;

set connection deleter;
autocommit off;
commit;


--------------------------------------------------------------------------------
-- Set up scanner to be doing a row locked index to base row scan on the index.
-- By using group fetch it will read and release locks on multiple rows from
-- the index and save away row pointers from the index.
--------------------------------------------------------------------------------
set connection scanner;
get cursor scan_cursor as 
    'select a, a2 from test_6';

next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;

--------------------------------------------------------------------------------
-- Delete all rows that the scanner has looked at, and should have released the
-- lock on.
--------------------------------------------------------------------------------
set connection deleter;
delete from test_6 where a = 1;
delete from test_6 where a = 2;
delete from test_6 where a = 3;
delete from test_6 where a = 4;

--------------------------------------------------------------------------------
-- The scan should either block on the delete or continue and not return the
-- the deleted row.
--------------------------------------------------------------------------------
set connection scanner;
next scan_cursor;
close scan_cursor;

-- commit the delete
set connection deleter;
delete from test_6 where a = 5;
commit;

-- scanner should see no rows.
set connection scanner;
select a,b from test_6;

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection deleter;
commit;
disconnect;
set connection scanner;
commit;
drop table test_6;
commit;
disconnect;


--------------------------------------------------------------------------------
-- Test 7: make sure that 2 heap cursor scans in same transaction work (at one
--         point there was a problem where releasing locks in one of the cursors
--         released locks in the other cursor).
--------------------------------------------------------------------------------

-- Test setup - create a heap with one row per page.

connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as deleter;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;


--------------------------------------------------------------------------------
-- HEAP SCAN
--------------------------------------------------------------------------------
set connection scanner;
autocommit off;
create table test_7 (a int, a2 int, b varchar(2000), c varchar(2000)) ;
insert into test_7 values (1, 10, PADSTRING('a',2000), PADSTRING('a',2000));
insert into test_7 values (2, 20, PADSTRING('b',2000), PADSTRING('b',2000));
insert into test_7 values (3, 30, PADSTRING('c',2000), PADSTRING('c',2000));
insert into test_7 values (4, 40, PADSTRING('d',2000), PADSTRING('d',2000));
insert into test_7 values (5, 50, PADSTRING('e',2000), PADSTRING('e',2000));
commit;

set connection deleter;
autocommit off;
commit;


-- Set up scanner to be doing a row locked heap scan, going one row at a time. 
set connection scanner;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');
get cursor scan_cursor_1 as 
    'select a, a2 from test_7';
get cursor scan_cursor_2 as 
    'select a, a2 from test_7';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next scan_cursor_1;
next scan_cursor_1;
next scan_cursor_1;
next scan_cursor_1;
next scan_cursor_1;

next scan_cursor_2;

close scan_cursor_2;

-- Get exclusive table lock on test_7.  Should fail with table cannot be locked.
set connection deleter;
lock table test_7 in exclusive mode;

-- release all read locks, by moving the cursor past all the rows.
set connection scanner;
next scan_cursor_1;
close scan_cursor_1;

-- Get exclusive table lock on test_7.  Now that both scan closed this should
-- work.
set connection deleter;
delete from test_7;
commit;

-- scanner should see no rows.
set connection scanner;
select a,b from test_7;
commit;

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection deleter;
commit;
disconnect;
set connection scanner;
commit;
drop table test_7;
commit;
disconnect;

--------------------------------------------------------------------------------
-- Test 8: Exercise post commit cases, force the code through the path, no easy
--         way to make sure the post commit work is actually doing something.
--         All these case were run with lock debugging by hand to make sure the
--         right thing was happening:
--         
--         8.1 - heap post commit successfully gets table X lock and cleans up.
--         8.2 - heap post commit can't get table X lock, so gives up and let's
--               client continue on with work.
--         8.3 - btree post commit successfully gets table X lock and cleans up.
--         8.4 - btree post commit can't get table X lock, so gives up and let's
--               client continue on with work.
--               client continue on with work.
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 8.1 setup 
--------------------------------------------------------------------------------
set connection client_1;
create table test_8 (a int, a2 int, b varchar(2000), c char(10)) 
    ;
create index test_8_idx on test_8 (a);
insert into test_8 values (1, 10, PADSTRING('a',2000), 'test 8.1');
insert into test_8 values (2, 20, PADSTRING('b',2000), 'test 8.1');
insert into test_8 values (3, 30, PADSTRING('c',2000), 'test 8.1');
insert into test_8 values (4, 40, PADSTRING('d',2000), 'test 8.1');
insert into test_8 values (5, 50, PADSTRING('e',2000), 'test 8.1');
commit;

--------------------------------------------------------------------------------
-- 8.1 test - simply delete rows from table, heap post commit will run and 
--            reclaim all pages.
--------------------------------------------------------------------------------

set connection client_1;
delete from test_8;
commit;
select a from test_8;
commit;

--------------------------------------------------------------------------------
-- 8.2 setup 
--------------------------------------------------------------------------------
drop table test_8;
create table test_8 (a int, a2 int, b varchar(1000), c char(10))
    ;
create index test_8_idx on test_8 (a);
insert into test_8 values (1, 10, PADSTRING('a',1000), 'test 8.2');
insert into test_8 values (2, 20, PADSTRING('b',1000), 'test 8.2');
insert into test_8 values (3, 30, PADSTRING('c',1000), 'test 8.2');
insert into test_8 values (4, 40, PADSTRING('d',1000), 'test 8.2');
insert into test_8 values (5, 50, PADSTRING('e',1000), 'test 8.2');
commit;

--------------------------------------------------------------------------------
-- 8.2 test - client 1 holds row lock which will prevent client 2 post commit.
--------------------------------------------------------------------------------

set connection client_1;
insert into test_8 values (6, 60, PADSTRING('a',1000), 'test 8.2');

set connection client_2;
delete from test_8 where a < 5;
commit;

set connection client_1;
select a from test_8;
commit;

set connection client_2;
select a from test_8;
commit;

--------------------------------------------------------------------------------
-- 8.3 setup 
--------------------------------------------------------------------------------
drop table test_8;
create table test_8 (a int, a2 int, b varchar(1500), c char(10));
create index test_8_idx on test_8 (a, b)
    ;
insert into test_8 values (1, 10, PADSTRING('a',1500), 'test 8.3');
insert into test_8 values (2, 20, PADSTRING('b',1500), 'test 8.3');
insert into test_8 values (3, 30, PADSTRING('c',1500), 'test 8.3');
insert into test_8 values (4, 40, PADSTRING('d',1500), 'test 8.3');
insert into test_8 values (5, 50, PADSTRING('e',1500), 'test 8.3');
commit;

--------------------------------------------------------------------------------
-- 8.3 test - simply delete rows from index table, btree post commit will run
--            and reclaim all pages.
--------------------------------------------------------------------------------

set connection client_1;
delete from test_8;
commit;
select a from test_8;
commit;

--------------------------------------------------------------------------------
-- 8.4 setup 
--------------------------------------------------------------------------------
drop table test_8;
create table test_8 (a int, a2 int, b varchar(1500), c char(10)) ;
create index test_8_idx1 on test_8 (a);
create index test_8_idx2 on test_8 (a, b)
    ;
insert into test_8 values (1, 10, PADSTRING('a',1500), 'test 8.4');
insert into test_8 values (2, 20, PADSTRING('b',1500), 'test 8.4');
insert into test_8 values (3, 30, PADSTRING('c',1500), 'test 8.4');
insert into test_8 values (4, 40, PADSTRING('d',1500), 'test 8.4');
insert into test_8 values (5, 50, PADSTRING('e',1500), 'test 8.4');
commit;

--------------------------------------------------------------------------------
-- 8.4 test - client 1 holds row lock which will prevent client 2 post commit.
--------------------------------------------------------------------------------

set connection client_1;
insert into test_8 values (6, 60, PADSTRING('a',1500), 'test 8.4');

set connection client_2;
delete from test_8 where a < 5;
commit;

set connection client_1;
select a from test_8;
commit;

set connection client_2;
select a from test_8;
commit;

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------

set connection client_1;
drop table test_8;
commit;
disconnect;

set connection client_2;
commit;
disconnect;

--------------------------------------------------------------------------------
-- Test 9: Make sure scan positioning in the beginning of a unique scan
--         properly gets the scan lock to block with splits.
--         (Scan locks are no longer used after DERBY-2991.)
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 9.1 setup 
--------------------------------------------------------------------------------
connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
commit;

connect 'wombat' as splitter;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
commit;

set connection scanner;
create table test_9 (a int, a2 int, b varchar(1000), c char(10)) 
    ;
insert into test_9 values (5, 50, PADSTRING('e',1000), 'test 9.1');
insert into test_9 values (4, 40, PADSTRING('d',1000), 'test 9.1');
insert into test_9 values (3, 30, PADSTRING('c',1000), 'test 9.1');
insert into test_9 values (2, 20, PADSTRING('b',1000), 'test 9.1');
insert into test_9 values (1, 10, PADSTRING('a',1000), 'test 9.1');
create unique index test_9_idx on test_9 (b) ;
commit;

--------------------------------------------------------------------------------
-- 9.1 test - open a cursor for update on table, and make sure splitter waits
--            on the scan position.
--------------------------------------------------------------------------------

set connection scanner;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');
get cursor scan_cursor as
    'select b from test_9 where b >= ''a'' ';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');
next scan_cursor;
next scan_cursor;

-- the following will get a couple of rows and then block on the split.
set connection splitter;
insert into test_9 values (0, 10, PADSTRING('aa',1000), 'test 9.1');
commit;
insert into test_9 values (0, 10, PADSTRING('ab',1000), 'test 9.1');
commit;

-- insert ahead in the cursor to make sure we pick it up later.
-- This would time out before DERBY-2991.
insert into test_9 values (0, 10, PADSTRING('dd',1000), 'test 9.1');
rollback;

set connection scanner;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
commit;


--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------

set connection scanner;
drop table test_9;
commit;
disconnect;

set connection splitter;
commit;
disconnect;

--------------------------------------------------------------------------------
-- Test 10: Make sure a ddl does not block the lock table vti.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 10 setup 
--------------------------------------------------------------------------------
connect 'wombat' as ddl;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
commit;

connect 'wombat' as locktable;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
autocommit off;
commit;

set connection ddl;
run resource '/org/apache/derbyTesting/functionTests/tests/store/LockTableQuery.subsql';
commit;



--------------------------------------------------------------------------------
-- 10 test - do ddl in one connection and look at lock table in another 
--           connection.
--------------------------------------------------------------------------------

set connection ddl;

create table test_10 (a int, a2 int, b varchar(1000), c char(10)) 
    ;
insert into test_10 values (4, 40, PADSTRING('d',1000), 'test 9.1');
insert into test_10 values (3, 30, PADSTRING('c',1000), 'test 9.1');
insert into test_10 values (2, 20, PADSTRING('b',1000), 'test 9.1');
insert into test_10 values (1, 10, PADSTRING('a',1000), 'test 9.1');

set connection locktable;
-- this should not block on the other thread.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------

set connection ddl;
drop table test_10;
commit;
disconnect;

set connection locktable;
commit;
disconnect;



--------------------------------------------------------------------------------
-- Test 11: test update locks
--------------------------------------------------------------------------------

connect 'wombat' as t11scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as t11updater;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as t11writer;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- set up
set connection t11updater;
autocommit off;
create table test_11 (a int, b int);
insert into test_11 values (1,1);
insert into test_11 values (2,2);
insert into test_11 values (8,8);
create index test11_idx on test_11 (a);
commit;

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--==================================================
-- t11updater gets an update lock on row where a=1
--==================================================

get cursor update_cursor as
    'select b from test_11 where a=1 for update of b';

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next update_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

set connection t11scanner;
autocommit off;

--
--------------------------------------------------
-- try to scan the table, readers are compatible with update lock.
--------------------------------------------------
select * from test_11;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--------------------------------------------------
-- try to update the table, should timeout
--------------------------------------------------
update test_11 set b=99 where a = 1;

--
--------------------------------------------------
-- try to update the table, should go through
--------------------------------------------------
update test_11 set b=99 where a = 8;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--------------------------------------------------
-- try to get an update lock
--------------------------------------------------
get cursor update_cursor2 as
    'select b from test_11 where a=1 for update of b';
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--------------------------------------------------
-- should timeout (other transaction has a shared lock on this row)
--------------------------------------------------
next update_cursor2;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--------------------------------------------------
-- should succeed (no other transaction has a shared lock on this row)
--------------------------------------------------
get cursor update_cursor3 as
    'select b from test_11 where a=8 for update of b';
select type, cnt, mode, tabname, lockname, state from lock_table2 order by tabname, type desc, mode, cnt, lockname;

next update_cursor3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
close update_cursor2;
close update_cursor3;

set connection t11updater;
commit;
close update_cursor;

set connection t11scanner;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--==================================================
-- t11scanner gets a read lock
--==================================================

select b from test_11 where a=1;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;


--
--------------------------------------------------
-- should succeed (can get an update lock if there is already a shared lock)
--------------------------------------------------
set connection t11updater;
get cursor update_cursor as
    'select b from test_11 where a=1 for update of b';
next update_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
close update_cursor;

set connection t11scanner;
commit;

--
--==================================================
-- t11writer gets a write lock
--==================================================

set connection t11writer;
autocommit off;
update test_11 set b=77 where a=2;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

set connection t11updater;
get cursor update_cursor as
    'select b from test_11 where a=2 for update of b';
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
--
----------------------------------------------------
-- should timeout
----------------------------------------------------
next update_cursor;


--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection t11updater;
close update_cursor;
drop table test_11;
commit;
disconnect;
set connection t11scanner;
disconnect;
set connection t11writer;
disconnect;


exit;
