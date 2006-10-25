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
-- Test multi user lock interaction of ddl. 
--------------------------------------------------------------------------------
run resource 'createTestProcedures.subsql';
autocommit off;

connect 'wombat' as deleter;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

connect 'wombat' as scanner;
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- set up
set connection scanner;
set isolation CS;
run resource 'LockTableQuery.subsql';
autocommit off;
drop table data;
-- create a table with 2 rows per page.
create table data (keycol int, data varchar(2000)) ;
insert into data values (0, PADSTRING('0',2000));
insert into data values (10, PADSTRING('100',2000));
insert into data values (20, PADSTRING('200',2000));
insert into data values (30, PADSTRING('300',2000));
insert into data values (40, PADSTRING('400',2000));
insert into data values (50, PADSTRING('100',2000));
insert into data values (60, PADSTRING('200',2000));
insert into data values (70, PADSTRING('300',2000));
insert into data values (80, PADSTRING('400',2000));
commit;

set connection deleter;
set current isolation = cursor stability;
autocommit off;
commit;

--------------------------------------------------------------------------------
-- Test 0: position scanner in the middle of the dataset using group commit
--         in a read commited scan which uses zero duration locks, then have
--         deleter remove all the rows in the table except for the last one, 
--         and wait long enough for the post commit job to reclaim the page 
--         that the scanner is positioned on.  Then do a next on the scanner
--         and verify the scanner goes to the last page.
--------------------------------------------------------------------------------

set connection scanner;

create table just_to_block_on (a int);
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');
next scan_cursor;

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- now delete all rows but the last one, space should be reclaimed before
-- the scanner gets a chance to run.
set connection deleter;

select 
    conglomeratename, isindex, 
    numallocatedpages, numfreepages, pagesize, estimspacesaving
from 
    new org.apache.derby.diag.SpaceTable('DATA') t
        order by conglomeratename; 
commit;

delete from data where keycol < 80;

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

-- give post commit a chance to run, by hanging on a lock.
drop table just_to_block_on;

commit;

select 
    conglomeratename, isindex, 
    numallocatedpages, numfreepages, pagesize, estimspacesaving
from 
    new org.apache.derby.diag.SpaceTable('DATA') t
        order by conglomeratename; 
commit;

set connection scanner;

-- this will return 10, from the group buffer (this looks wierd as 10 is 
-- deleted at this point - but that is what you get with read committed).
next scan_cursor;

-- this will now go through the code which handles jumping over deleted pages.
next scan_cursor;

commit;

--------------------------------------------------------------------------------
-- cleanup
--------------------------------------------------------------------------------
set connection deleter;
commit;
disconnect;
set connection scanner;
drop table data;
drop table just_to_block_on;
commit;
disconnect;

--------------------------------------------------------------------------------
-- Test 1: position scanner in the middle of the dataset using group commit
--         in a read commited scan which uses zero duration locks.  Now arrange
--         for the row the scan is positioned on to be purged by post commit,
--         but leave a row on the page for scan to reposition to.
--------------------------------------------------------------------------------

---------------
-- setup
---------------
autocommit off;

connect 'wombat' as deleter1;

connect 'wombat' as deleter2;

connect 'wombat' as scanner;

connect 'wombat' as lockholder;

-- set up
set connection scanner;
set isolation to read committed;
autocommit off;
drop table data;
-- create a table with 4 rows per page.
create table data (keycol int, data varchar(900));
insert into data values (0, PADSTRING('0',900));
insert into data values (10, PADSTRING('100',900));
insert into data values (20, PADSTRING('200',900));
insert into data values (30, PADSTRING('300',900));
insert into data values (40, PADSTRING('400',900));
insert into data values (50, PADSTRING('100',900));
insert into data values (60, PADSTRING('200',900));
insert into data values (70, PADSTRING('300',900));
insert into data values (80, PADSTRING('400',900));
create unique index idx on data (keycol);
commit;

set connection deleter1;
set isolation read committed;
autocommit off;
commit;
set connection deleter2;
set isolation READ COMMITTED;
autocommit off;
commit;
set connection lockholder;
set CURRENT isolation TO CS;
autocommit off;
commit;

--------------
-- run the test
--------------

set connection lockholder;
create table just_to_block_on (a int);

set connection scanner;

CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next scan_cursor;
next scan_cursor;

-- scan is now positioned on row (10, 100), as it group fetched 2 rows.

-- in deleter1 thread delete the last row on the page, but don't commit.
-- in the other deleter thread delete the rest of the rows on the page and
-- commit it, which will result in a post commit to try and reclaim all the
-- rows on the page, but it won't be able to reclaim the one that has not
-- been committed by deleter1.

-- delete in this transaction keycol (30, 300).
set connection deleter1;
delete from data where keycol = 30;

-- delete in this transaction the rest of rows on the page.
set connection deleter2;
delete from data where keycol = 0;
delete from data where keycol = 10;
delete from data where keycol = 20;
commit;

-- block deleter threads on a lock to give post commit a chance to run.
set connection deleter2;
select * from just_to_block_on;
set connection deleter1;
select * from just_to_block_on;

-- now assume post commit has run, roll back deleter1 so that one non-deleted
-- row remains on the page.
set connection deleter1;
rollback;

-- the scanner gets a chance to run.
set connection scanner;

-- now at this point the scanner will resume and find the row it is positioned
-- on has been purged, and it will reposition automatically to (30, 300) on
-- the same page.
next scan_cursor;
next scan_cursor;
next scan_cursor;
commit;

select * from data;

commit;

--------------------------------------------------------------------------------
-- cleanup
--------------------------------------------------------------------------------
set connection scanner;
disconnect;
set connection deleter1;
disconnect;
set connection deleter2;
disconnect;
set connection lockholder;
disconnect;

--------------------------------------------------------------------------------
-- Test 2: position scanner in the middle of the dataset using group commit
--         in a read commited scan which uses zero duration locks.  Now arrange
--         for the row the scan is positioned on to be purged by post commit,
--         but leave a row on the page for scan to reposition to, as did Test 1.
--         This time make the row left on the page be deleted, so when the
--         scan repositions, it should jump over the deleted row.
--------------------------------------------------------------------------------

---------------
-- setup
---------------
connect 'wombat' as deleter1;

connect 'wombat' as deleter2;

connect 'wombat' as scanner;

connect 'wombat' as lockholder;

-- set up
set connection scanner;
set isolation read committed;
autocommit off;
drop table data;
-- create a table with 4 rows per page.
create table data (keycol int, data varchar(900)) ;
insert into data values (0, PADSTRING('0',900));
insert into data values (10, PADSTRING('100',900));
insert into data values (20, PADSTRING('200',900));
insert into data values (30, PADSTRING('300',900));
insert into data values (40, PADSTRING('400',900));
insert into data values (50, PADSTRING('100',900));
insert into data values (60, PADSTRING('200',900));
insert into data values (70, PADSTRING('300',900));
insert into data values (80, PADSTRING('400',900));
create unique index idx on data (keycol);
commit;

set connection deleter1;
set isolation read committed;
autocommit off;
commit;
set connection deleter2;
set isolation read committed;
autocommit off;
commit;
set connection lockholder;
set isolation read committed;
autocommit off;
commit;

--------------
-- run the test
--------------

set connection lockholder;
create table just_to_block_on (a int);

set connection scanner;

CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next scan_cursor;
next scan_cursor;

-- scan is now positioned on row (10, 100), as it group fetched 2 rows.

-- In the deleter1 thread delete the last row on the page, but don't commit.
-- in the other deleter thread delete the rest of the rows on the page and
-- commit it, which will result in a post commit to try and reclaim all the
-- rows on the page, but it won't be able to reclaim the one that has not
-- been committed by deleter1.

-- delete in this transaction keycol (30, 300).
set connection deleter1;
delete from data where keycol = 30;

-- delete in this transaction the rest of rows on the page.
set connection deleter2;
delete from data where keycol = 0;
delete from data where keycol = 10;
delete from data where keycol = 20;
commit;

-- block deleter threads on a lock to give post commit a chance to run.
set connection deleter2;
select * from just_to_block_on;

-- now assume post commit has run, commit deleter1 so that one deleted
-- row remains on the page after the positioned row.
set connection deleter1;
commit;

-- the scanner gets a chance to run.
set connection scanner;

-- now at this point the scanner will resume and find the row it is positioned
-- on has been purged, the only rows following it to be deleted and it will 
-- reposition automatically to (40, 400) on the next page.
next scan_cursor;
next scan_cursor;
next scan_cursor;
commit;

select * from data;

commit;

--------------------------------------------------------------------------------
-- cleanup
--------------------------------------------------------------------------------
set connection scanner;
disconnect;
set connection deleter1;
disconnect;
set connection deleter2;
disconnect;
set connection deleter2;
disconnect;
set connection lockholder;
disconnect;

--------------------------------------------------------------------------------
-- Test 3: position scanner in the middle of the dataset using group commit
--         in a read commited scan which uses zero duration locks.  Now arrange
--         for the row the scan is positioned on, and all rows following it on
--         the page to be purged by post commit, but leave at least one row on
--         the page so that the page is not removed.  The reposition code will
--         position on the page, find the row has disappeared, ask for the
--         "next" row on the page, find that no such row exists on the page,
--         and finally move to the next page.
--------------------------------------------------------------------------------

---------------
-- setup
---------------
connect 'wombat' as deleter1;

connect 'wombat' as deleter2;

connect 'wombat' as scanner;

connect 'wombat' as lockholder;

-- set up
set connection scanner;
set isolation read committed;
autocommit off;
drop table data;
-- create a table with 4 rows per page.
create table data (keycol int, data varchar(900)) ;
insert into data values (0, PADSTRING('0',900));
insert into data values (10, PADSTRING('100',900));
insert into data values (20, PADSTRING('200',900));
insert into data values (30, PADSTRING('300',900));
insert into data values (40, PADSTRING('400',900));
insert into data values (50, PADSTRING('100',900));
insert into data values (60, PADSTRING('200',900));
insert into data values (70, PADSTRING('300',900));
insert into data values (80, PADSTRING('400',900));
create unique index idx on data (keycol);
commit;

set connection deleter1;
set isolation read committed;
autocommit off;
commit;
set connection deleter2;
set isolation read committed;
autocommit off;
commit;
set connection lockholder;
set isolation read committed;
autocommit off;
commit;

--------------
-- run the test
--------------

set connection lockholder;
create table just_to_block_on (a int);

set connection scanner;

CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;

-- scan is now positioned on row (50, 500), as it group fetched in 2 row chunks.

-- In the deleter1 thread delete the 1st row on the page, but don't commit:
-- (40, 400).
-- In the deleter2 thread delete the current row and the rows following on the
-- page, and commit: (50, 500), (60, 600), (70, 700).  This will result in
-- the code seeing a page with all rows deleted and then queue a post commit on
-- the page which will purge 50, 60, and 70, but it won't be able to reclaim 
-- the one that has not been committed by deleter1.

-- delete in this transaction keycol (30, 300).
set connection deleter1;
delete from data where keycol = 40;

-- delete in this transaction the rest of rows on the page.
set connection deleter2;
delete from data where keycol = 50;
delete from data where keycol = 60;
delete from data where keycol = 70;
commit;

-- block deleter threads on a lock to give post commit a chance to run.
set connection deleter2;
select * from just_to_block_on;

-- now assume post commit has run, commit deleter1 so that one deleted
-- row remains on the page after the positioned row.
set connection deleter1;
commit;

-- the scanner gets a chance to run.
set connection scanner;

-- now at this point the scanner will resume and find the row it is positioned
-- on has been purged, the only rows following it to be deleted and it will 
-- reposition automatically to (80, 800) on the next page.
next scan_cursor;
next scan_cursor;
commit;

select * from data;

commit;

--------------------------------------------------------------------------------
-- cleanup
--------------------------------------------------------------------------------
set connection scanner;
disconnect;
set connection deleter1;
disconnect;
set connection deleter2;
disconnect;
set connection deleter2;
disconnect;
set connection lockholder;
disconnect;

--------------------------------------------------------------------------------
-- Test 4: position scanner in the middle of the dataset using group commit
--         in a read commited scan which uses zero duration locks.  Now arrange
--         for all rows in the table to be purged.  The reposition code will
--         attempt to position on the "next" page, and find no more pages.
--------------------------------------------------------------------------------

---------------
-- setup
---------------
connect 'wombat' as deleter1;

connect 'wombat' as deleter2;

connect 'wombat' as scanner;

connect 'wombat' as lockholder;

-- set up
set connection scanner;
set isolation read committed;
autocommit off;
drop table data;
-- create a table with 4 rows per page.
create table data (keycol int, data varchar(900)) ;
insert into data values (0, PADSTRING('0',900));
insert into data values (10, PADSTRING('100',900));
insert into data values (20, PADSTRING('200',900));
insert into data values (30, PADSTRING('300',900));
insert into data values (40, PADSTRING('400',900));
insert into data values (50, PADSTRING('100',900));
insert into data values (60, PADSTRING('200',900));
insert into data values (70, PADSTRING('300',900));
insert into data values (80, PADSTRING('400',900));
create unique index idx on data (keycol);
commit;

set connection deleter1;
set isolation read committed;
autocommit off;
commit;
set connection deleter2;
set isolation read committed;
autocommit off;
commit;
set connection lockholder;
set isolation read committed;
autocommit off;
commit;

--------------
-- run the test
--------------

set connection lockholder;
create table just_to_block_on (a int);

set connection scanner;

CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','2');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');

next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;

-- scan is now positioned on row (50, 500), as it group fetched in 2 row chunks.

-- In the deleter1 thread delete all the rows, allowing all rows/pages to be
-- reclaimed.

-- delete in this transaction all rows.
set connection deleter1;
delete from data where keycol >= 0 ;
commit;

-- block deleter threads on a lock to give post commit a chance to run.
set connection deleter2;
select * from just_to_block_on;

-- now assume post commit has run, commit deleter1 so that one deleted
-- row remains on the page after the positioned row.
commit;

-- the scanner gets a chance to run.
set connection scanner;

-- now at this point the scanner will resume and find the row it is positioned
-- on has been purged, and no rows or pages remaining in the table.
next scan_cursor;
next scan_cursor;
commit;

select * from data;

commit;

--------------------------------------------------------------------------------
-- cleanup
--------------------------------------------------------------------------------
set connection scanner;
disconnect;
set connection deleter1;
disconnect;
set connection deleter2;
disconnect;
set connection deleter2;
disconnect;
set connection lockholder;
disconnect;

exit;
