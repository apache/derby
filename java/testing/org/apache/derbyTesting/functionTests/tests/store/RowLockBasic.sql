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

set isolation to RR;

run resource 'createTestProcedures.subsql';
run resource 'LockTableQuery.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
autocommit off;

create table a (a int);

commit;

--------------------------------------------------------------------------------
-- Test insert into empty heap, should just get row lock on row being inserted
--------------------------------------------------------------------------------
insert into a values (1);
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test insert into heap with one row, just get row lock on row being inserted
--------------------------------------------------------------------------------
insert into a values (2);
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

drop table a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

create table a (a int, b varchar(1000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

create index a_idx on a (a, b) ;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test insert into empty btree, should just get row lock on row being 
-- inserted and an instant duration lock on "first key in table" row (id 3).
--------------------------------------------------------------------------------
insert into a values (1, PADSTRING('a',1000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Test insert into non-empty btree, should get row lock on row being 
-- inserted and an instant duration lock on the one before it.
--------------------------------------------------------------------------------
insert into a values (2, PADSTRING('b',1000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Cause split and check locks that are obtained as part of inserting after
-- the split.  This causes the code to get a previous lock on a previous page.
-- 
-- RESOLVE (mikem) - the row lock on (1,9) is a result of raw store getting
-- a lock while it tries to fit the row on the original page record id, but
-- there is not enough room, so it eventually allocates a new page/row and 
-- locks that one - but the old lock is left around.
--
-- btree just before commit:
-- leftmost leaf: (1,6), (1,7)
-- next leaf:     (1,8), (2,6)
--------------------------------------------------------------------------------
insert into a values (3, PADSTRING('c',1000));
commit;
insert into a values (4, PADSTRING('d',1000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Cause an insert on a new page that inserts into the 1st slot on the btree
-- page.
--
-- btree just before commit:
-- leftmost leaf: (1,6), (1,7)
-- next leaf:     (2,7), (2,6)
--------------------------------------------------------------------------------
drop table a;
create table a (a int, b varchar(1000));
create unique index a_idx on a (a, b) ;
insert into a values (1, PADSTRING('a',1000));
insert into a values (2, PADSTRING('b',1000));
insert into a values (3, PADSTRING('c',1000));
insert into a values (4, PADSTRING('d',1000));
select a from a;
delete from a where a = 3;
select a from a;
commit;
insert into a values (3, PADSTRING('c',1000));
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

drop table a;
create table a (a int, b char(200));
create unique index a_idx on a (a);
insert into a values (1, 'a');
insert into a values (2, 'b');
insert into a values (3, 'c');
insert into a values (4, 'd');
commit;

--------------------------------------------------------------------------------
-- Do full covered index scan.
--------------------------------------------------------------------------------
select a from a;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Do single probe into covered index (first key in table).
--------------------------------------------------------------------------------
select a from a where a = 1;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Do single probe into covered index (last key in table).
--------------------------------------------------------------------------------
select a from a where a = 4;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Do set of range scans that all return 1 row from covered index.
--------------------------------------------------------------------------------
select a from a where a <= 1;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a from a where a >= 2 and a < 3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a from a where a > 3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

--------------------------------------------------------------------------------
-- Do range scans that all return 0 row from covered index.
--------------------------------------------------------------------------------

select a from a where a < 1;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a from a where a > 4;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;

select a from a where a > 2 and a < 3;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;


--------------------------------------------------------------------------------
-- Verify that create index does table level locking
--------------------------------------------------------------------------------
drop table a;
create table a (a int, b char(200));
create table b (a int, b char(200));
insert into a values (1, 'a');
insert into a values (2, 'b');
insert into a values (3, 'c');
insert into a values (4, 'd');
commit;
create unique index a_idx on a (a);
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
commit;
select a from a;
select a from b;
commit;

-- clean up
autocommit on;
drop index a_idx;
drop table a;
drop table b;
