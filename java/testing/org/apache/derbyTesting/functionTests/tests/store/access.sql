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
set isolation to RR;
run resource 'createTestProcedures.subsql';

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

autocommit off;

--------------------------------------------------------------------------
-- test qualifier skip code on fields with length having the 8th bit set in low
-- order length byte.
--------------------------------------------------------------------------

drop table a;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '32768');
create table a
    (
        i1      int,
        col00   varchar(384),
        col01   varchar(390),
        i2      int
    );
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

insert into a values (1, PADSTRING('10',384), PADSTRING('100',390), 1000);
insert into a values (2, PADSTRING('20',384), PADSTRING('200',390), 2000);
insert into a values (3, PADSTRING('30',384), PADSTRING('300',390), 3000);

select i1, i2 from a where i2 = 3000;

drop table a;

commit;

-- test case for track 2590
--    The problem was that the btree split would self deadlock while trying
--    to reclaim rows during the split.  Fixed by just giving up if btree 
--    can't get the locks during the reclaim try.


create table foo (a int, b varchar(900), c int);
insert into foo values (1, PADSTRING('1',900), 1); 
insert into foo values (2, PADSTRING('2',900), 1); 
insert into foo values (3, PADSTRING('3',900), 1); 
insert into foo values (4, PADSTRING('4',900), 1); 
insert into foo values (5, PADSTRING('5',900), 1); 
insert into foo values (6, PADSTRING('6',900), 1); 
insert into foo values (7, PADSTRING('7',900), 1); 
insert into foo values (8, PADSTRING('8',900), 1); 
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create index foox on foo (a, b);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

commit;

delete from foo where foo.a <> 2;

-- Test full cursor for update scan over all the rows in the heap, 
-- with default group fetch.  Group fetch should be disabled.
--------------------------------------------------------------------------------
-- force index until optimizer fixes problem where it does not pick index.
get cursor scan_cursor as
    'select a, b, c from foo for update of c';

next scan_cursor;

-- these inserts would cause a lock wait timeout before the bug fix.
insert into foo values (1, PADSTRING('11',900), 1);
insert into foo values (1, PADSTRING('12',900), 1);
insert into foo values (1, PADSTRING('13',900), 1);
insert into foo values (1, PADSTRING('14',900), 1);
insert into foo values (1, PADSTRING('15',900), 1);

commit;

drop table foo;
commit;



-- test case for track 735
--    The problem was that when the level of btree grew, raw store would
--    incorrectly report that there was not enough space to move all the
--    rows from the root page to a newly allocated leaf page, so the create
--    index operation would fail with a message saying that a row was too
--    big.

-- create and load a table with values from 1024 down to 1, the reverse order
-- is important to reproduce the bug.
create table foo (a int);
insert into foo values (1024);
insert into foo (select foo.a - 1   from foo); 
insert into foo (select foo.a - 2   from foo); 
insert into foo (select foo.a - 4   from foo); 
insert into foo (select foo.a - 8   from foo); 
insert into foo (select foo.a - 16  from foo); 
insert into foo (select foo.a - 32  from foo); 
insert into foo (select foo.a - 64  from foo); 
insert into foo (select foo.a - 128 from foo); 
insert into foo (select foo.a - 256 from foo); 
insert into foo (select foo.a - 512 from foo); 

-- this create index use to fail.
create index a on foo (a);

-- Check the consistency of the indexes
VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'FOO');

-- a delete of the whole table also exercises the index well.
delete from foo;

drop table foo;

-- ----------------------------------------------------------------------------
-- stress the conglomerate directory.  abort of an alter table will clear
-- the cache.
-- ----------------------------------------------------------------------------
autocommit off;
create table a (a int);
commit;
alter table a add column c1 int;
rollback;
select * from a;
drop table a;
commit;

-- ----------------------------------------------------------------------------
-- test case for partial row runtime statistics.
-- ----------------------------------------------------------------------------

create table foo (a int, b int, c int, d int, e int);
insert into foo values (1, 2, 3, 4, 5);
insert into foo values (10, 20, 30, 40, 50);


call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2500;

-- all the columns
select * from foo;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- just last column - should be 5 and 50
select e from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 5,3,1 and 50,30,10
select e, c, a from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier in list - should be 5,3,1 and 50,30,10
select e, c, a from foo where foo.e = 5;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier not in list 
--   - should be 5,3,1 and 50,30,10
select e, c, a from foo where foo.b = 20;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 1,2 and 10,20
select a, b from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- now check index scans - force the index just to 
-- make sure it does an index scan.
create index foo_cover on foo (e, d, c, b, a);

-- all the columns
select * from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- just last column - should be 5 and 50
select e from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 5,3,1 and 50,30,10
select e, c, a from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier in list - should be 5,3,1 and 50, 30, 10
select e, c, a from foo where foo.e = 5;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier not in list - should be 5,3,1 
-- and 50, 30, 10
select e, c, a from foo where foo.b = 20;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 1,2 and 10, 20
select a, b from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- check deleted row feature
insert into foo values (100, 2, 3, 4, 5);
insert into foo values (1000, 2, 3, 4, 5);
delete from foo where foo.a = 100;
delete from foo where foo.a = 1000;

-- now check with deleted rows involved.

-- all the columns
select * from foo;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- just last column - should be 5 and 50
select e from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 5,3,1 and 50,30,10
select e, c, a from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier in list - should be 5,3,1 and 50,30,10
select e, c, a from foo where foo.e = 5;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier not in list 
--   - should be 5,3,1 and 50,30,10
select e, c, a from foo where foo.b = 20;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 1,2 and 10,20
select a, b from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- now check index scans - force the index just to 
-- make sure it does an index scan.
create index foo_cover on foo (e, d, c, b, a);

-- all the columns
select * from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- just last column - should be 5 and 50
select e from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 5,3,1 and 50,30,10
select e, c, a from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier in list - should be 5,3,1 and 50, 30, 10
select e, c, a from foo where foo.e = 5;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns, with qualifier not in list - should be 5,3,1 
-- and 50, 30, 10
select e, c, a from foo where foo.b = 20;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- as subset of columns - should be 1,2 and 10, 20
select a, b from foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ----------------------------------------------------------------------------
-- test case for costing - make sure optimizer picks obvious covered query.
-- ----------------------------------------------------------------------------
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

create table base_table (a int, b varchar(1000));

insert into base_table values (1,  PADSTRING('1',1000));
insert into base_table values (2,  PADSTRING('2',1000));
insert into base_table values (3,  PADSTRING('3',1000));
insert into base_table values (4,  PADSTRING('4',1000));
insert into base_table values (5,  PADSTRING('5',1000));
insert into base_table values (6,  PADSTRING('6',1000));
insert into base_table values (7,  PADSTRING('7',1000));
insert into base_table values (8,  PADSTRING('8',1000));
insert into base_table values (9,  PADSTRING('9',1000));
insert into base_table values (10, PADSTRING('10',1000));

create index cover_idx on base_table(a);

-- make sure covered index is chosen
select a from base_table;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ----------------------------------------------------------------------------
-- test for key too big error message.
-- ----------------------------------------------------------------------------
create table d (id int not null, t_bigvarchar varchar(400), unique (id));
create index t_bigvarchar_ind on d ( t_bigvarchar);
alter table d alter t_bigvarchar set data type varchar(4096);
insert into d (id, t_bigvarchar) values (1, 

'1111111123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

89012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012

34567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

890123456789012345678901234567890123456789012345678901234567890123456');


-- ----------------------------------------------------------------------------
-- test space for update
-- ----------------------------------------------------------------------------

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', '1');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', '0');
create table testing (a varchar(100));
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', NULL);

insert into testing values ('a');
insert into testing (select testing.a from testing); 
insert into testing (select testing.a from testing); 
insert into testing (select testing.a from testing); 
insert into testing (select testing.a from testing); 
insert into testing (select testing.a from testing); 
insert into testing (select testing.a from testing); 
insert into testing (select testing.a from testing); 
update testing set a = 'abcd' where a = 'a';
create index zz on testing (a);

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', '1');
create table t1 (a varchar(100));
insert into t1 values ('a');
insert into t1 (select t1.a from t1); 
insert into t1 (select t1.a from t1); 
insert into t1 (select t1.a from t1); 
insert into t1 (select t1.a from t1); 
insert into t1 (select t1.a from t1); 
insert into t1 (select t1.a from t1); 
insert into t1 (select t1.a from t1); 
update t1 set a = 'abcd' where a = 'a';
create index zz1 on t1 (a);

-- ----------------------------------------------------------------------------
-- test load with long columns with index creation
-- ----------------------------------------------------------------------------
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create table long1 (a varchar(32000), b int, c int);
insert into long1 values ('this is a long row which will get even longer and longer to force a stream', 1, 2);
insert into long1 values ('this is another long row which will get even longer and longer to force a stream', 2, 3);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

update long1 set a = a||a||a||a||a||a;
update long1 set a = a||a||a||a||a||a;
update long1 set a = a||a;

select LENGTH(a) from long1;

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create table long2 (a varchar(16384), b int, c int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '16384');
create index long2i1 on long2 (a);
create index long2i2 on long2 (a,b);
create index long2i3 on long2 (a,b,c);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

insert into long2 select * from long1;
select LENGTH(a) from long2;

-- DefectId 1346 
insert into long2 select * from long1;
select LENGTH(a) from long2;

delete from long2;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create index long2small on long2 (a, c);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

-- this small index should cause the insert to fail
insert into long2 select * from long1;

-- DefectId 1346 
-- the small index should cause this insert to also fail
insert into long2 select * from long1;

select LENGTH(a) from long2;
--

-- test case for track 1346
drop table long1;
drop table long2;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create table long1 (a varchar(32000), b int, c int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
insert into long1 values
	('this is a long row which will get even longer', 1, 2);
insert into long1 values
	('a second row that will also grow very long', 2, 3);
update long1 set a = a||a||a||a||a||a;
update long1 set a = a||a||a||a||a||a;
update long1 set a = a||a;
select LENGTH(a) from long1;

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create table long2 (a varchar(30000), b int, c int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '16384');
create index long2i1 on long2 (a);
create index long2i2 on long2 (b, a);
create index long2i3 on long2 (b, a, c);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
insert into long2 select * from long1;
insert into long2 select * from long1;
select LENGTH(a) from long2;

drop table long1;
drop table long2;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create table long1 (a varchar(32000), b int, c int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
insert into long1 values
	('this is a long row which will get even longer', 1, 2);
insert into long1 values
	('a second row that will also grow very long', 2, 3);
update long1 set a = a||a||a||a||a||a;
update long1 set a = a||a||a||a||a||a;
update long1 set a = a||a;
select LENGTH(a) from long1;

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024');
create table long2 (a varchar(32000), b int, c int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '16384');
create index long2i1 on long2 (a);
create index long2i2 on long2 (b, a);
create index long2i3 on long2 (b, a, c);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
-- insert into the second table multiple times
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
insert into long2 select * from long1;
select LENGTH(a) from long2;
select count(*) from long2;

-- test case for track 1552
--    Make sure that a full scan which needs columns not in index does not
--    use the index.  Before the fix, access costing would make the optimizer
--    pick the index because it incorrectly costed rows spanning pages.
drop table a;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096');
create table a (a int, b varchar(4000), c varchar(4000), d varchar(4000)); 
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
create index a_idx on a (a);
insert into a values (5, PADSTRING('a',4000), PADSTRING('a',4000), PADSTRING('a',4000));
insert into a values (4, PADSTRING('a',4000), PADSTRING('a',4000), PADSTRING('a',4000));
insert into a values (3, PADSTRING('a',4000), PADSTRING('a',4000), PADSTRING('a',4000));
insert into a values (2, PADSTRING('a',4000), PADSTRING('a',4000), PADSTRING('a',4000));
insert into a values (1, PADSTRING('a',4000), PADSTRING('a',4000), PADSTRING('a',4000));

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

select a, d from a;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

autocommit on;
-- test case for track 2241
--    The problem was that when the level of btree grew, sometimes a long
--    row would be chosen as the branch delimiter, and the branch code did
--    not throw the correct error noSpaceForKey error.

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', NULL);
create table b2241 (a int, b varchar(32000));
insert into b2241 values (1024, '01234567890123456789012345678901234567890123456789');


insert into b2241 (select b2241.a + 1  , b from b2241); 
insert into b2241 (select b2241.a + 2  , b from b2241); 
insert into b2241 (select b2241.a + 4  , b from b2241); 
insert into b2241 (select b2241.a + 8  , b from b2241); 
insert into b2241 (select b2241.a + 16  , b from b2241); 
insert into b2241 (select b2241.a + 32  , b from b2241); 
insert into b2241 (select b2241.a + 64  , b from b2241); 

update b2241 set b = b||b;
update b2241 set b = b||b;
update b2241 set b = b||b;
update b2241 set b = b||b;
update b2241 set b = b||b;

select LENGTH(b) from b2241 where a = 1025;

insert into b2241 (select 1, b||b||b||b||b||b||b||b from b2241 where a = 1024); 
insert into b2241 (select 8000, b||b||b||b||b||b||b||b from b2241 where a = 1024); 

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
-- this create index use to fail with an assert - 
-- should fail with key too big error.
create index a on b2241 (b, a);
-- make sure table still accessable
create index a on b2241 (b, a);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

-- delete 2 big records and then index should work.
delete from b2241 where a = 1;
delete from b2241 where a = 8000;
create index a on b2241 (b, a);

-- Check the consistency of the indexes
VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'FOO');

drop table b2241;

-- test case for reclaiming deleted rows during split.
--    o insert bunch of rows with sequential keys.
--    o create non-unique index.
--    o delete every other one - this will make normat post commit not fire.
--    o commit
--    o now reinsert rows into the "holes" which before the fix would cause
--      splits, but now will force reclaim space and reuse existing space in
--      btree.
autocommit off;

-- set page size back to default.
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', '1');

commit;
drop table foo;
drop table foo2;

-- create and load a table with values from 1024 down to 1, 
create table foo (a int, b char(200), c int);
insert into foo values (1024,             'even', 0);
insert into foo        (select foo.a - 1, 'odd' , 1 from foo);

insert into foo (select foo.a - 2,   foo.b, foo.c from foo);
insert into foo (select foo.a - 4,   foo.b, foo.c from foo);
insert into foo (select foo.a - 8,   foo.b, foo.c from foo);
insert into foo (select foo.a - 16,  foo.b, foo.c from foo);
insert into foo (select foo.a - 32,  foo.b, foo.c from foo);
insert into foo (select foo.a - 64,  foo.b, foo.c from foo);
insert into foo (select foo.a - 128, foo.b, foo.c from foo);
insert into foo (select foo.a - 256, foo.b, foo.c from foo);
insert into foo (select foo.a - 512, foo.b, foo.c from foo);

-- insert into the "holes", but different keys (even2 instead of even)
create table foo2 (a int, b char(200), c int);
insert into foo2 (select * from foo);
delete from foo2 where foo2.c = 1;


-- create "packed" index.
create index a on foo (a, b);

-- delete ever other row
delete from foo where foo.c = 0;

-- turn all the deletes into "committed deletes"
commit;


insert into foo (select foo2.a, 'even2', foo2.c from foo2);
commit;

-- insert dups 
insert into foo (select foo2.a, 'even2', foo2.c from foo2);
commit;

-- a delete of the whole table also exercises the btree well.
delete from foo;

drop table foo;
drop table foo2;

-- try same test with unique index.

-- create and load a table with values from 1024 down to 1, 
create table foo (a int, b char(200), c int);
insert into foo values (1024,             'even', 0);
insert into foo        (select foo.a - 1, 'odd' , 1 from foo);

insert into foo (select foo.a - 2,   foo.b, foo.c from foo);
insert into foo (select foo.a - 4,   foo.b, foo.c from foo);
insert into foo (select foo.a - 8,   foo.b, foo.c from foo);
insert into foo (select foo.a - 16,  foo.b, foo.c from foo);
insert into foo (select foo.a - 32,  foo.b, foo.c from foo);
insert into foo (select foo.a - 64,  foo.b, foo.c from foo);
insert into foo (select foo.a - 128, foo.b, foo.c from foo);
insert into foo (select foo.a - 256, foo.b, foo.c from foo);
insert into foo (select foo.a - 512, foo.b, foo.c from foo);

-- insert into the "holes", but different keys (even2 instead of even)
create table foo2 (a int, b char(200), c int);
insert into foo2 (select * from foo);
delete from foo2 where foo2.c = 1;


-- create "packed" unique index.
create unique index a on foo (a, b);

-- delete ever other row
delete from foo where foo.c = 0;

-- turn all the deletes into "committed deletes"
commit;


insert into foo (select foo2.a, 'even2', foo2.c from foo2);
commit;

-- insert dups will cause error
insert into foo (select foo2.a, 'even2', foo2.c from foo2);
commit;

-- a delete of the whole table also exercises the btree well.
delete from foo;

drop table foo;
drop table foo2;
commit;

-- another simple test of reclaim deleted row code paths.
-- this test should not reclaim rows as deletes are not committed.


create table foo (a int, b varchar(1100), c int);
create index a on foo (a, b);
insert into foo values (1, PADSTRING('a',1100), 1);
insert into foo values (2, PADSTRING('a',1100), 1);
insert into foo values (3, PADSTRING('a',1100), 1);
commit;
delete from foo where foo.a = 1;
delete from foo where foo.a = 2;
insert into foo values (-1, PADSTRING('ab',1100), 1);
insert into foo values (-2, PADSTRING('ab',1100), 1);
rollback;

drop table foo;

-- another simple test of reclaim deleted row code paths.
-- this test should reclaim rows as deletes are not committed.

create table foo (a int, b varchar(1100), c int);
create index a on foo (a, b);
insert into foo values (1, PADSTRING('a',1100), 1);
insert into foo values (2, PADSTRING('a',1100), 1);
insert into foo values (3, PADSTRING('a',1100), 1);
commit;
delete from foo where foo.a = 1;
delete from foo where foo.a = 2;
commit;
insert into foo values (-1, PADSTRING('ab',1100), 1);
insert into foo values (-2, PADSTRING('ab',1100), 1);
rollback;

drop table foo;

-- this test will not reclaim rows because the parent xact has table level lock.
create table foo (a int, b varchar(1100), c int);
create index a on foo (a, b);
insert into foo values (1, PADSTRING('a',1100), 1);
insert into foo values (2, PADSTRING('a',1100), 1);
insert into foo values (3, PADSTRING('a',1100), 1);
delete from foo where foo.a = 1;
insert into foo values (0, PADSTRING('a',1100), 1);
insert into foo values (1, PADSTRING('a',1100), 1);
rollback;
drop table foo;

-- test case for track 2778
--    Make sure that an update which causes a row to go from a non long row
--    to a long row can be aborted correctly.  Prior to this fix the columns
--    moving off the page would be corrupted.

-- create a base table that contains 2 rows, 19 columns, that leaves just
-- 1 byte free on the page.  freeSpace: 1, spareSpace: 10, PageSize: 2048

drop table t2778;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '2048');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', '10');
create table t2778 
    (
        col00 char(2),
        col01 char(1),
        col02 char(99),
        col03 char(11),
        col04 char(7),
        col05 char(11),
        col06 char(6),
        col07 char(6),
        col08 char(2),
        col09 char(6),
        col10 varchar(1000),
        col11 char(2),
        col12 char(1),
        col13 char(7),
        col14 char(24),
        col15 char(1),
        col16 char(166),
        col17 char(207),
        col18 char(2)
    );
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', NULL);

create unique index a_idx on t2778 (col00);

commit;

insert into t2778 values (
        '0_',
        '0',
        '0_col02',
        '0_col03',
        '0_col04',
        '0_col05',
        '0_06',
        '0_07',
        '0_',
        '0_09',
        '0_col10llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll012340_col10lllllllllll',
        '0_',
        '0',
        '0_col13',
        '0_col14',
        '0',
        '0_col16',
        '0_col17',
        '0_'
        );

insert into t2778 values (
        '1_',
        '1',
        '1_col02',
        '1_col03',
        '1_col04',
        '1_col05',
        '1_06',
        '1_07',
        '1_',
        '1_09',
        '1_col10llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll012340_col10lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll012340_col10lllllllllllxxxxxxxxxxxxxxxxxxx',
        '1_',
        '1',
        '1_col13',
        '1_col14',
        '1',
        '1_col16',
        '1_col17',
        '1_'
        );

commit;

select col16, col17, col18 from t2778; 

commit;

update t2778 --derby-properties index = a_idx 
    set col10 = 
        '0_col10llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll012340_col10lllllllllllxxxxxx'
    where col00 = '0_';

rollback;

-- prior to the fix col17 and col18 would come back null.
select col01, col02, col03, col04, col05,  col06, col07, col08, col09, col10, col11, col12, col13, col14, col15, col16, col17, col18 from t2778; 

commit;



-- test case for track 3149, improving max on btree optimization
autocommit off;

create table foo (a int, b varchar(500), c int);
insert into foo values (1, PADSTRING('1',500), 1); 
insert into foo values (11, PADSTRING('11',500), 1); 
insert into foo values (12, PADSTRING('12',500), 1); 
insert into foo values (13, PADSTRING('13',500), 1); 
insert into foo values (14, PADSTRING('14',500), 1); 
insert into foo values (15, PADSTRING('15',500), 1); 
insert into foo values (16, PADSTRING('16',500), 1); 
insert into foo values (17, PADSTRING('17',500), 1); 
insert into foo values (18, PADSTRING('18',500), 1); 
insert into foo values (2, PADSTRING('2',500), 1); 
insert into foo values (3, PADSTRING('3',500), 1); 
insert into foo values (4, PADSTRING('4',500), 1); 
insert into foo values (5, PADSTRING('5',500), 1); 
insert into foo values (6, PADSTRING('6',500), 1); 
insert into foo values (7, PADSTRING('7',500), 1); 
insert into foo values (8, PADSTRING('8',500), 1); 
insert into foo values (9, PADSTRING('9',500), 1); 
create index foox on foo (b);

commit;

-- normal max optimization, last row in index is not deleted.
select max(b) from foo;

-- new max optimization, last row in index is deleted but others on page aren't.
delete from foo where a = 9;
select max(b) from foo;

-- new max optimization, last row in index is deleted but others on page aren't.
delete from foo where a = 8;
select max(b) from foo;

-- max optimization does not work - fail over to scan, all rows on last page are
-- deleted.
delete from foo where a > 2;
select max(b) from foo;
commit;

drop table foo;

------------------------------------------------------------------------
-- regression test for bugs 3368, 3370
-- the bugs arose for the edge case where pageReservedSpace = 100
-- before bug 3368 was fixed, a short row insert caused 2 pages to be
-- allocated per short row insert.

drop table a;

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', '100');
create table a (a int);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', NULL);

insert into a values (1);
select numallocatedpages from new org.apache.derby.diag.SpaceTable('A') as a;

insert into a values (2);
select numallocatedpages from new org.apache.derby.diag.SpaceTable('A') as a;

insert into a values (1);
select numallocatedpages from new org.apache.derby.diag.SpaceTable('A') as a;

insert into a values (2);
select numallocatedpages from new org.apache.derby.diag.SpaceTable('A') as a;

------------------------------------------------------------------------
-- regression test for bug 4595, make sure index used in unique key update
-- even if table has zero rows.
--

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 9000;

-- try delete/update statement compiled against table with 0 rows
drop table foo;
create table foo (a int, b int);
create unique index foox on foo (a);

-- delete against table with 0 rows.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- update against table with 0 rows.
update foo set b = 1 where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select * from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- now insert one row and make sure still same plan.  Previous to 4595 
-- 0 row plan was a table scan and it would not change when 1 row was inserted.
insert into foo values (1, 1);


-- update against table with 1 row.
update foo set b = 2 where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- delete against table with 1 row.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- try delete/update statement compiled against table with 1 row.
drop table foo;
create table foo (a int, b int);
insert into foo values (1, 1);
create unique index foox on foo (a);

-- update against table with 1 row.
update foo set b = 2 where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- delete against table with 1 row.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- repeat set of 4595 tests against table with primary key, vs. unique index - 
-- there should be no difference in plan shape.

-- try delete/update statement compiled against table with 0 rows
drop table foo;
create table foo (a int not null primary key, b int);

-- delete against table with 0 rows.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- update against table with 0 rows.
update foo set b = 1 where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select * from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- now insert one row and make sure still same plan.  Previous to 4595 
-- 0 row plan was a table scan and it would not change when 1 row was inserted.
insert into foo values (1, 1);


-- update against table with 1 row.
update foo set b = 2 where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- delete against table with 1 row.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- try delete/update statement compiled against table with 1 row.
drop table foo;
create table foo (a int not null primary key, b int);
insert into foo values (1, 1);

-- update against table with 1 row.
update foo set b = 2 where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- delete against table with 1 row.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 1 row.
select * from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 1 row.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 1 row.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- repeat set of 4595 tests against table with non-unique index with no
-- statistics.

-- there should be no difference in plan shape.

-- try delete/update statement compiled against table with 0 rows
drop table foo;
create table foo (a int, b int);
create index foox on foo (a);

-- delete against table with 0 rows.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- update against table with 0 rows.
update foo set b = 1 where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select * from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 0 rows.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- now insert one row and make sure still same plan.  Previous to 4595 
-- 0 row plan was a table scan and it would not change when 1 row was inserted.
insert into foo values (1, 1);

-- update against table with 1 row.
update foo set b = 2 where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- delete against table with 1 row.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- try delete/update statement compiled against table with 1 row.
drop table foo;
create table foo (a int, b int);
create index foox on foo (a);
insert into foo values (1, 1);

-- update against table with 1 row.
update foo set b = 2 where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- delete against table with 1 row.
delete from foo where a = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 1 row.
select * from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 1 row.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- select against table with 1 row.
select a from foo where a = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

------------------------------------------------------------------------
-- simple regression test for qualifier work.
------------------------------------------------------------------------
drop table foo;
commit;

create table foo (a int, b int, c int);
insert into foo values (1, 10, 100);
insert into foo values (2, 20, 200);
insert into foo values (3, 30, 300);

-- should return no rows
select a, b, c from foo where a = 1 and b = 20;

-- should return one row
select a, b, c from foo where a = 3 and b = 30;
select a, b, c from foo where a = 3 or c = 40;

-- should return 2 rows
select a, b, c from foo where a = 1 or b = 20;
select a, b, c from foo where a = 1 or a = 3;

DROP FUNCTION PADSTRING;
DROP PROCEDURE WAIT_FOR_POST_COMMIT;
exit;
