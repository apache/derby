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
-- TEST CASES SPECIFIC TO STORE IMPLEMENTATION OF READ UNCOMMITTED:
-- overview:
--    TEST 0: Test a scan positioned on a row which is deleted from it.
--    TEST 1: Test a scan positioned on a row which is purged from it.
------------------------------------------------------------------------------
--
------------------------------------------------------------------------------
run resource 'createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
connect 'wombat' as scanner;

autocommit off;

connect 'wombat' as deleter;
autocommit off;

------------------------------------------------------------------------------
-- TEST 0: Test a scan positioned on a row which is deleted from it.
------------------------------------------------------------------------------

set connection scanner;
set current isolation to UR;
drop table data;

-- create a table with 2 rows per index page.
create table data (keycol int, data varchar(1600)) ;
insert into data values (0, PADSTRING('0',1600));
insert into data values (10, PADSTRING('100',1600));
insert into data values (20, PADSTRING('200',1600));
insert into data values (30, PADSTRING('300',1600));
insert into data values (40, PADSTRING('400',1600));
insert into data values (50, PADSTRING('100',1600));
insert into data values (60, PADSTRING('200',1600));
insert into data values (70, PADSTRING('300',1600));
insert into data values (80, PADSTRING('400',1600));
create index idx on data (keycol, data) ;
commit;

set connection deleter;
SET ISOLATION READ COMMITTED;
commit;

-- position scanner with no bulk fetch on 40,400

set connection scanner;

CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;

-- now delete all the rows except for 70 and 80
set connection deleter;
delete from data where keycol < 70;

-- scanner should automatically jump to 70
set connection scanner;
next scan_cursor;

-- cleanup
close scan_cursor;
commit;

set connection deleter;
commit;

------------------------------------------------------------------------------
-- TEST 1: Test a scan positioned on a row which is purged.
------------------------------------------------------------------------------

set connection scanner;
set isolation read uncommitted;
drop table data;

-- create a table with 3 rows per index page.
create table data (keycol int, data varchar(1200));
insert into data values (0, PADSTRING('0',1200));
insert into data values (10, PADSTRING('100',1200));
insert into data values (20, PADSTRING('200',1200));
insert into data values (30, PADSTRING('300',1200));
insert into data values (40, PADSTRING('400',1200));
insert into data values (50, PADSTRING('100',1200));
insert into data values (60, PADSTRING('200',1200));
insert into data values (70, PADSTRING('300',1200));
insert into data values (80, PADSTRING('400',1200));
create index idx on data (keycol, data) ;
commit;

-- position scanner with no bulk fetch on 0,0 (first row in btree)

set connection scanner;
CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault','1');
get cursor scan_cursor as
    'select keycol from data';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.language.bulkFetchDefault', '16');
next scan_cursor;


-- delete all the rows in the table except for the last few pages, and the 1st;
set connection deleter;
delete from data where keycol < 50 and keycol > 0;

-- insert enough rows after the first to force a split by the scanner on the 1st
-- page, it will now be positioned by key on the (0, 0) key.   Then delete the
-- rows that we just inserted.
set connection scanner;
insert into data values (9, '3'), (9, '2'), (9, '1');
delete from data where keycol = 9;

-- delete the key that the scan is positioned on.  
set connection deleter;
delete from data where keycol = 0;
commit;

set connection scanner;
-- this should now cause another split on the first page in the btree, this
-- time it should reclaim row 0.
insert into data values (8, '3'), (8, '2'), (8, '1');

-- scanner should automatically jump to 8, handling the fact that row (0,0)
-- no longer exists in the table.
set connection scanner;
next scan_cursor;
next scan_cursor;
next scan_cursor;
next scan_cursor;

-- delete all the rows that remain except the last;
set connection deleter;
delete from data where keycol > 10 and keycol < 80;
commit;

-- position scan on last row of scan.
set connection scanner;
next scan_cursor;

-- now repeat process from above to make the current scan position disappear to
-- test code path executed when closing a scan where the last scan position has
-- disappeared.
set connection scanner;
insert into data values (82, '3'), (82, '2'), (82, '1');
delete from data where keycol = 81;
set connection deleter;
delete from data where keycol = 80;
commit;
set connection scanner;
-- this statement will purge (80, 800) from the table.
insert into data values (81, '3'), (81, '2'), (81, '1');
delete from data where keycol = 81;

-- this statement will execute code which will look for last key positioned on
-- while closing the statement.
close scan_cursor;

-- cleanup
set connection scanner;
commit;

set connection deleter;
commit;
