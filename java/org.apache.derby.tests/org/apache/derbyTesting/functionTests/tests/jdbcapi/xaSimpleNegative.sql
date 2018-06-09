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
-- 
-- some negative test for error checking
--
xa_datasource 'wombat';
xa_connect user 'negativeTest' password 'xxx';

-- start new transaction
xa_start xa_noflags 0;

-- ERROR: cannot start without end
xa_start xa_noflags 1;

xa_getconnection;

-- ERROR: cannot get connection again
xa_getconnection;

-- ERROR: connot commit/rollback an xa connection
commit;

-- ERROR: connot commit/rollback an xa connection
rollback;

drop table APP.negative;
create table APP.negative (a char(10), b int);
create unique index negativei on APP.negative(b);
run resource '/org/apache/derbyTesting/functionTests/tests/store/global_xactTable.view';
insert into APP.negative values ('xyz', 1);
select * from APP.negative;

-- ERROR: cannot commit/prepare/rollback without end
xa_commit xa_1phase 0;
-- ERROR: cannot commit/prepare/rollback without end
xa_rollback 0;
-- ERROR: cannot commit/prepare/rollback without end
xa_prepare 0;

-- OK suspend it
xa_end xa_suspend 0;

-- ERROR: duplicate xid
xa_start xa_noflags 0;

-- ERROR: cannot commit/prepare/rollback with suspended
xa_commit xa_1phase 0;
-- ERROR: cannot commit/prepare/rollback with suspended
xa_rollback 0;
-- ERROR: cannot commit/prepare/rollback with suspended
xa_prepare 0;
-- ERROR: cannot commit/prepare/rollback with suspended
xa_commit xa_2phase 0;

xa_end xa_success 0;
xa_prepare 0;
xa_commit xa_2phase 0;

-- should be able to use this xid again
xa_start xa_noflags 0;

-- ERROR: cannot start without end
xa_start xa_noflags 0;

-- ERROR: duplicate key exception, statement level rollback
insert into APP.negative values ('rollback', 1);

select * from APP.negative;
insert into APP.negative values ('ok', 2);

select * from global_xactTable order by gxid, status, username, type;

disconnect;
xa_end xa_fail 0;

xa_start xa_noflags 2;
xa_getconnection;

insert into APP.negative values ('ok', 3);

-- ERROR: cannot suspend some other xid
xa_end xa_suspend 3;

-- ERROR: cannot end some other xid while I am still attached
xa_end xa_success 0;

xa_end xa_suspend 2;

-- ERROR: cannot join an xid I just suspended have to resume
xa_start xa_join 2;

xa_start xa_resume 2;
xa_end xa_suspend 2;

xa_rollback 0;
-- ERROR: should not find this xid any more
xa_prepare 0;

select * from global_xactTable order by gxid, status, username, type;

xa_end xa_success 2;
disconnect;

-- ERROR: can only join a successful branch, not resume 
xa_start xa_resume 2;

-- this is OK
xa_start xa_join 2;
xa_getconnection;

-- ERROR: another dup 
insert into APP.negative values ('rollback', 3);

xa_end xa_suspend 2;
xa_end xa_success 2;

-- ERROR: cannot call fail now
xa_end xa_fail 2;

-- rollback is OK
xa_rollback 2;

-- ERROR: cannot join something that is not there
xa_start xa_join 2;
-- ERROR: cannot join something that is not there
xa_start xa_resume 2;

-- start one
xa_start xa_noflags 1;

-- ERROR: can only forget heuristically completed transaction
xa_forget 1;

delete from APP.negative;

xa_end xa_success 1;

-- ERROR: now try some bad flag 
xa_start xa_suspend 1;
-- ERROR: now try some bad flag 
xa_start xa_fail 1;

xa_prepare 1;

-- can only forget heuristically completed transaction
xa_forget 1;

xa_start xa_noflags 2;

-- ERROR: deadlock, transaction trashed
select * from APP.negative;

-- ERROR: should have no connection underneath
select * from APP.negative;

-- ERROR: should have no connection underneath and xid 2 is gone
xa_end xa_suspend 2;
-- ERROR: should have no connection underneath and xid 2 is gone
xa_end xa_fail 2;
xa_rollback 2;
disconnect;

xa_start xa_noflags 3;
xa_getconnection;
select * from global_xactTable order by gxid, status, username, type;
drop table foo;
create table foo (a int);
xa_end xa_suspend 3;

-- ERROR: cannot join a prepared transaction
xa_start xa_join 1;
-- ERROR: cannot resume a prepared transaction
xa_start xa_resume 1;
-- ERROR: bad flag
xa_start xa_fail 1;
-- ERROR: bad flag
xa_start xa_noflags 1;

-- rollback prepared transaction is OK
xa_rollback 1;

-- ERROR: dup id
xa_start xa_noflags 3;
xa_start xa_resume 3;

-- now that 1 is rolled back, this should succeed
select * from APP.negative;
select * from global_xactTable order by gxid, status, username, type;

-- ERROR: bad flag
xa_end xa_noflags 3;
xa_end xa_fail 3;
xa_rollback 3;

-- ensure switching back and forward does not commit
-- the xact due to the commit in setAutoCommit();
AUTOCOMMIT ON;

create table t44g(a int);
insert into t44g values 1,2;

select * from t44g where a > 4000;
create table t44(i int);

xa_start xa_noflags 44;
insert into t44g values(4400);
insert into t44g values(4401);

xa_end xa_suspend 44;

values (1,2,3);

commit;

AUTOCOMMIT OFF;

insert into t44 values(1);
insert into t44 values(2);

commit;

insert into t44 values(3);
insert into t44 values(4);

rollback;

AUTOCOMMIT ON;

-- fail with lock issues
select * from t44g;

xa_start xa_resume 44;
insert into t44g values(4500);
insert into t44g values(4501);

xa_end xa_success 44;

insert into t44 values(5);
insert into t44 values(6);
commit;

AUTOCOMMIT OFF;
insert into t44 values(7);
insert into t44 values(8);
commit;

AUTOCOMMIT ON;

xa_start xa_join 44;
select * from t44g where a > 4000;
xa_end xa_success 44;

-- fail with lock issues
select * from t44g;

xa_rollback 44;

-- should be empty if no commit occurred in the middle;
select * from t44g where a > 4000;

select * from t44;
