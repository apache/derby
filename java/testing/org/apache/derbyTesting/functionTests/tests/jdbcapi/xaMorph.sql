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
disconnect;
--
-- testing using an xaConnection alternately produce local and global
-- transaction 
--
--
xa_datasource 'wombat' ;
xa_connect user 'sku' password 'testmorph';

-- get a local connection thru the XAConnection
xa_getconnection;
run resource '/org/apache/derbyTesting/functionTests/tests/store/global_xactTable.view';
select * from global_xactTable where gxid is not null order by gxid,username;

drop table foo;
create table foo (a int);
commit;

autocommit off;
insert into foo values (1);
select * from global_xactTable where gxid is not null order by gxid,username;
commit;

autocommit on;
insert into foo values (2);
select * from global_xactTable where gxid is not null order by gxid,username;

-- morph the connection to a global transaction
xa_start xa_noflags 1;
select * from global_xactTable where gxid is not null order by gxid,username;

insert into foo values (3);
-- disallowed
commit;

-- disallowed
rollback;

-- disallowed
autocommit on;

-- OK
autocommit off;
select * from foo;

xa_end xa_success 1;
xa_prepare 1;

-- dup id
xa_start xa_noflags 1;

xa_start xa_noflags 2;

-- still should disallow autommit;
autocommit on;

-- still should disallow commit and rollback 
commit;
rollback;

select * from global_xactTable where gxid is not null order by gxid,username;

xa_end xa_suspend 2;

-- get local connection again
xa_getconnection;

insert into foo values (5);

-- autocommit should be on by default;
commit;

autocommit off;
insert into foo values (6);

-- commit and rollback is allowed on local connection
rollback;

insert into foo values (6);
commit;

select * from global_xactTable where gxid is not null order by gxid,username;

-- I am still able to commit other global transactions while I am attached to a
-- local transaction.
xa_commit xa_2phase 1;
xa_end xa_success 2;
xa_rollback 2;

-- still connected locally
select * from global_xactTable where gxid is not null order by gxid,username;

disconnect;

xa_getconnection;
select * from global_xactTable where gxid is not null order by gxid,username;
select * from foo;
autocommit off;
delete from foo;

-- yanking a local connection away should rollback the changes
-- this really depends on if the connection pool manage is doing the job, roll
-- it back by hand here since we don't have a way to call local pooled
-- connection close method.
rollback;

-- yank it
xa_getconnection;

-- getting a new connection handle will revert it to the default autocommit on
-- commit should fail
commit;

autocommit off;
select * from global_xactTable where gxid is not null order by gxid,username;
select * from foo;

-- cannot morph it if the local transaction is not idle
xa_start xa_noflags 3;

commit;

-- now morph it to a global transaction
xa_start xa_noflags 3;

-- now I shouldn't be able to yank it
xa_getconnection;

-- the following does not use the view, or the method alias, so that
-- the act of executing this vti does not change the state of the transaction.
-- Using the view would sometimes changes the results of the query depending
-- on the order of the rows in the vti.
select 
    cast(global_xid as char(2)) as gxid,
    status, 
    username, 
    type 
from syscs_diag.transaction_table order by gxid, status, username, type;

select * from foo;
delete from foo;

xa_end xa_fail 3;
xa_rollback 3;

-- local connection again
xa_getconnection;
select * from global_xactTable where gxid is not null order by gxid,username;
select * from foo;
