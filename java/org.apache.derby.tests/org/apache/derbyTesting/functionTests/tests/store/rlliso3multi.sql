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
-- Test multi user lock interaction under isolation level 3.  default isolation
-- level has been set as a property to serializable.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Test 0: verify isolation level by seeing if a read lock is released or not.
--------------------------------------------------------------------------------
connect 'wombat' as scanner;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as writer;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

run resource '/org/apache/derbyTesting/functionTests/tests/store/createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');

-- set up
set connection scanner;
autocommit off;
create table test_0 (a int);
insert into test_0 values (1);
commit;

set connection writer;
autocommit off;

-- scanner should hold read lock on table until end of transaction.
set connection scanner;
select * from test_0;

-- writer should get a lock timeout.
set connection writer;
insert into test_0 values (2);

-- scanner should only see the original row.
set connection scanner;
select * from test_0;
commit;
select * from test_0;

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
-- Test setup - create a 1 page btre, with the page ready to split.
--------------------------------------------------------------------------------
connect 'wombat' as scanner;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as rootgrower;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

set connection scanner;
autocommit off;
create table a (a varchar(1000), b varchar(1000)) ;
insert into a values (PADSTRING('a',1000), PADSTRING('a',1000));
insert into a values (PADSTRING('b',1000), PADSTRING('b',1000));
insert into a values (PADSTRING('c',1000), PADSTRING('c',1000));
create index a_idx on a (a) ;;
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
    'select a from a where a >= PADSTRING(''a'',1000) and a < PADSTRING(''c'',1000) ';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');
next scan_cursor;

--------------------------------------------------------------------------------
-- Before DERBY-2991 the attempt to split the root would time out because the
-- scan had locked the first page.
--------------------------------------------------------------------------------
set connection rootgrower;
autocommit off;
insert into a values (PADSTRING('d',1000), PADSTRING('d',1000));

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
insert into a values (PADSTRING('ab',1000), PADSTRING('ab',1000));

--------------------------------------------------------------------------------
-- Now the grow root should be allowed (note that cursor scan has locks
-- on the leaf page being grown - just not the scan lock).
-- (Scan locks are no longer used after DERBY-2991.)
--------------------------------------------------------------------------------
set connection rootgrower;
insert into a values (PADSTRING('d',1000), PADSTRING('d',1000));

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
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as client_2;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

set connection client_1;
autocommit off;
create table a (a varchar(1000), b varchar(1000)) ;
create index a_idx on a (a) ;
commit;
insert into a values (PADSTRING('b',1000), PADSTRING('b',1000));

set connection client_2;
autocommit off;
--------------------------------------------------------------------------------

-- the following will not cause a time out, as the previous
-- key insert lock will not conflict with other insert locks, only other
-- select locks, or non insert update locks.

--------------------------------------------------------------------------------
insert into a values (PADSTRING('c',1000), PADSTRING('c',1000));

--------------------------------------------------------------------------------
-- the following should NOT cause a time out
--------------------------------------------------------------------------------
insert into a values (PADSTRING('a',1000), PADSTRING('a',1000));

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection client_1;
commit;
set connection client_2;
commit;
drop table a;
commit;

--------------------------------------------------------------------------------
-- Test 3: make sure an exact key insert into unique key index blocks.
--------------------------------------------------------------------------------
set connection client_1;
autocommit off;
create table a (a varchar(1000), b varchar(1000));
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
-- will get timout message.
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
--         handles the previous to first key locking.  In serializable the
--         scanner should hold the previous to first key lock until end of 
--         transaction, thus blocking the attempted insert to the range.
--------------------------------------------------------------------------------

connect 'wombat' as scanner;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as inserter;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;


set connection scanner;
autocommit off;
create table test_5 (a int, b varchar(1000), c varchar(1000));
insert into test_5 values (1, PADSTRING('a',1000), PADSTRING('a',1000));
insert into test_5 values (2, PADSTRING('b',1000), PADSTRING('b',1000));
create index test_5_idx on test_5 (a);
commit;

set connection inserter;
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
    'select a, b from test_5 where a <= 2 ';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

-- because of group locking will get locks on 1, 2, 3, 4, and 5 and then will
-- release the locks on 1, 2, 3, and 4.  The last one is released on close or
-- on next call emptying the cursor.
next scan_cursor;

--------------------------------------------------------------------------------
-- Insert a row previous to all other rows, this should block and back out.
--------------------------------------------------------------------------------
set connection inserter;
insert into test_5 values (0, PADSTRING('b',1000), PADSTRING('b',1000));

--------------------------------------------------------------------------------
-- The scan should finish fine without blocking.
--------------------------------------------------------------------------------
set connection scanner;
next scan_cursor;
next scan_cursor;

-- commit the insert
set connection inserter;
commit;

-- scanner should now see 1 and 2
set connection scanner;
close scan_cursor;

select a from test_5;

--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection inserter;
commit;
disconnect;
set connection scanner;
commit;
drop table test_5;
commit;
disconnect;


--------------------------------------------------------------------------------
-- Test 6: test update locks
--------------------------------------------------------------------------------

connect 'wombat' as t6scanner;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as t6updater;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as t6writer;
set isolation to rr;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

run resource '/org/apache/derbyTesting/functionTests/tests/store/LockTableQuery.subsql';

-- set up
set connection t6updater;
autocommit off;
create table test_6 (a int, b int);
insert into test_6 values (1,1);
insert into test_6 values (2,2);
insert into test_6 values (8,8);
create index test6_idx on test_6 (a);
commit;

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--==================================================
-- t6updater gets an update lock on row where a=1
--==================================================

get cursor update_cursor as
    'select b from test_6 where a=1 for update of b';

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

next update_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

set connection t6scanner;
autocommit off;

--
--------------------------------------------------
-- try to scan the table, should timeout
--------------------------------------------------
select * from test_6;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--------------------------------------------------
-- try to update the table, should timeout
--------------------------------------------------
update test_6 set b=99 where a = 1;

--
--------------------------------------------------
-- try to update the table, should timeout (previous key is locked)
--------------------------------------------------
update test_6 set b=99 where a = 2;

--
--------------------------------------------------
-- try to update the table, should go through
--------------------------------------------------
update test_6 set b=99 where a = 8;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--------------------------------------------------
-- try to get an update lock
--------------------------------------------------
get cursor update_cursor2 as
    'select b from test_6 where a=1 for update of b';
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
    'select b from test_6 where a=8 for update of b';
select type, cnt, mode, tabname, lockname, state from lock_table2 order by tabname, type desc, mode, cnt, lockname;

next update_cursor3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
close update_cursor2;
close update_cursor3;

set connection t6updater;
commit;
close update_cursor;

set connection t6scanner;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

--
--==================================================
-- t6scanner gets a read lock
--==================================================

select b from test_6 where a=1;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;


--
--------------------------------------------------
-- should succeed (can get an update lock if there is already a shared lock)
--------------------------------------------------
set connection t6updater;
get cursor update_cursor as
    'select b from test_6 where a=1 for update of b';
next update_cursor;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
close update_cursor;

set connection t6scanner;
commit;

--
--==================================================
-- t6writer gets a write lock
--==================================================

set connection t6writer;
autocommit off;
update test_6 set b=77 where a=2;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

set connection t6updater;
get cursor update_cursor as
    'select b from test_6 where a=2 for update of b';
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
--
----------------------------------------------------
-- should timeout
----------------------------------------------------
next update_cursor;


--------------------------------------------------------------------------------
-- cleanup.
--------------------------------------------------------------------------------
set connection t6updater;
close update_cursor;
drop table test_6;
commit;
disconnect;
set connection t6scanner;
disconnect;
set connection t6writer;
disconnect;

exit;
