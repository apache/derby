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
-- test lock escalation.  derby.locks.escalationThreshold=100 property 
-- has been set to force lock escalation
-- to occur at the minimum level of 100  locks.

run resource 'createTestProcedures.subsql';
run resource 'LockTableQuery.subsql';


autocommit off;

-- TEST 1 - make sure IX row locks are escalated to a persistent X table lock.

create table foo (a int);

commit;

-- first insert 90 rows
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

-- check to make sure we have IX table and X row locks.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- now insert 10 more rows, pushing the lock over the escalation limit.
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

-- check to make sure we now just have a X table lock.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- insert 10 more rows to make sure we don't get rows locks from now on.
insert into foo values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

-- check to make sure we now just have a X table lock.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;


-- TEST 2 - make sure IS row locks are escalated to a persistent X table lock.

create index foox on foo (a);

commit;

set isolation serializable;

-- get IS row locks on just under 100 of the rows;
select a from foo where a < 5;

-- check to make sure we have IS table and S row locks.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- now get enough IS row locks to push over the lock escalation limit
select a from foo where a >= 5;

-- check to make sure we now just have a S table lock.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- make sure subsequent IS locks are recognized as covered by the S table lock.
select a from foo where a = 8;

-- check to make sure we now just have a S table lock.
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

-- TEST 3 - reproduce abort failure similar to bug 4328

create table aborttest (keycol int, data varchar(1000));

-- first insert 110 rows
insert into aborttest values (0, PADSTRING('0',1000)), (0, PADSTRING('1',1000)), (0, PADSTRING('2',1000));
insert into aborttest values (0, PADSTRING('3',1000)), (0, PADSTRING('4',1000)), (0, PADSTRING('5',1000));
insert into aborttest values (0, PADSTRING('6',1000)), (0, PADSTRING('7',1000)), (0, PADSTRING('8',1000)), (0, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values  (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));
insert into aborttest values (0, PADSTRING('0',1000)), (1, PADSTRING('1',1000)), (2, PADSTRING('2',1000));
insert into aborttest values (3, PADSTRING('3',1000)), (4, PADSTRING('4',1000)), (5, PADSTRING('5',1000));
insert into aborttest values  (6, PADSTRING('6',1000)), (7, PADSTRING('7',1000)), (8, PADSTRING('8',1000)), (9, PADSTRING('9',1000));

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create index idx on aborttest (keycol, data);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
commit;

delete from aborttest where keycol < 3;

-- check to make sure we have a X row locks and IX table lock;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

delete from aborttest where keycol >= 3 ;

-- check to make sure we escalated;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- now cause space reclamation on the leftmost leaf
insert into aborttest values (-1, '-1'), (-1, '-1');

-- check to make sure we escalated;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

-- Before fix to bug 4328/4330 the following rollback would cause a recovery
-- error which would shut down the server, and cause recovery to always fail.
-- The problem was that the lock escalation bug would remove locks on 
-- uncommitted deleted rows, and then the above insert would try and succeed
-- at purging rows that it should not have been able.  When undo comes along to
-- undo the delete it can't find the row because it has been purged by a
-- committed nested internal transaction.
rollback;

select count(*) from aborttest;
select keycol from aborttest;

commit;


-- TEST 4 - (beetle 4764) make sure no lock timeout if escalate is blocked by 
-- another user.
-- 
connect 'wombat' as block_escalate_connection;
set connection block_escalate_connection;
autocommit off;
drop table foo;
create table foo (a int, data char(10));
commit;
insert into foo values (1, 'blocker');

connect 'wombat' as escalate_connection;
set connection escalate_connection;
autocommit off;
commit;

-- insert 100 rows which should try to escalate the lock but then fail, because
-- it is blocked by the block_escalate_connection
insert into foo values (0, '0'), (0, '1'), (0, '2'), (0, '3'), (0, '4'), (0, '5'), (0, '6'), (0, '7'), (0, '8'), (0, '9');
insert into foo values (10, '0'), (11, '1'), (12, '2'), (13, '3'), (14, '4'), (15, '5'), (16, '6'), (17, '7'), (18, '8'), (19, '9');
insert into foo values (20, '0'), (21, '1'), (22, '2'), (23, '3'), (24, '4'), (25, '5'), (26, '6'), (27, '7'), (28, '8'), (29, '9');
insert into foo values (30, '0'), (31, '1'), (32, '2'), (33, '3'), (34, '4'), (35, '5'), (36, '6'), (37, '7'), (38, '8'), (39, '9');
insert into foo values (40, '0'), (41, '1'), (42, '2'), (43, '3'), (44, '4'), (45, '5'), (46, '6'), (47, '7'), (48, '8'), (49, '9');
insert into foo values (50, '0'), (51, '1'), (52, '2'), (53, '3'), (54, '4'), (55, '5'), (56, '6'), (57, '7'), (58, '8'), (59, '9');
insert into foo values (60, '0'), (61, '1'), (62, '2'), (63, '3'), (64, '4'), (65, '5'), (66, '6'), (67, '7'), (68, '8'), (69, '9');
insert into foo values (70, '0'), (71, '1'), (72, '2'), (73, '3'), (74, '4'), (75, '5'), (76, '6'), (77, '7'), (78, '8'), (79, '9');
insert into foo values (80, '0'), (81, '1'), (82, '2'), (83, '3'), (84, '4'), (85, '5'), (86, '6'), (87, '7'), (88, '8'), (89, '9');
insert into foo values (90, '0'), (91, '1'), (92, '2'), (93, '3'), (94, '4'), (95, '5'), (96, '6'), (97, '7'), (98, '8'), (99, '9');
insert into foo values (100, '0'), (101, '1'), (102, '2'), (103, '3'), (104, '4'), (105, '5'), (106, '6'), (107, '7'), (108, '8'), (109, '9');

-- check to make sure we have not escalated;
select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;

set connection block_escalate_connection;
commit;

-- see if all the data made it.
select a, data from foo;
