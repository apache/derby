xa_datasource 'wombat';

---------------------------------------------
-- a single connection and 1 phase commit
---------------------------------------------
xa_connect ;
xa_start xa_noflags 0;
xa_getconnection;
drop table foo;
create table foo (a int);
insert into foo values (0);
select * from foo;
run resource '/org/apache/derbyTesting/functionTests/tests/store/global_xactTable.view';
select * from global_xactTable where gxid is not null order by gxid;
xa_end xa_success 0;
xa_commit xa_1phase 0;

xa_datasource 'wombat' shutdown;

---------------------------------------------
-- two interleaving connections and prepare/commit prepare/rollback
---------------------------------------------
xa_datasource 'wombat';
xa_connect user 'sku' password 'testxa' ;

xa_start xa_noflags 1;
xa_getconnection;
insert into APP.foo values (1);
xa_end xa_suspend 1;

xa_start xa_noflags 2;
insert into APP.foo values (2);
xa_end xa_suspend 2;

xa_start xa_resume 1;
insert into APP.foo values (3);
xa_end xa_suspend 1;

xa_start xa_resume 2;
insert into APP.foo values (4);
select * from APP.global_xactTable where gxid is not null order by gxid;
-- this prepare won't work since transaction 1 has been suspended - XA_PROTO
xa_prepare 1;

select * from APP.global_xactTable where gxid is not null order by gxid;
xa_end xa_success 2;

-- this assumes a resume
xa_end xa_success 1;
xa_prepare 1;
xa_prepare 2;

-- both transactions should be prepared
select * from APP.global_xactTable where gxid is not null order by gxid;

-- NOTE: The following call to "xa_recover xa_startrscan" is apt to
-- return the result set rows in reverse order when changes to
-- the Derby engine affect the number of transactions that it takes
-- to create a database.  The transactions are stored in a hash table
-- based on a global and local id, and when the number of transactions
-- changes, the (internal) local id can change, which may lead to a
-- change in the result set order.  This order is determined by the
-- JVM's hashing algorithm. Examples of changes to the engine that
-- can affect this include ones that cause more commits or that
-- change the amount of data being stored, such as changes to the
-- metadata statements (which is what prompted this explanation in
-- the first place).  Ultimately, the problem is that there is no
-- way to order the return values from "xa_recover" since it is an
-- ij internal statement, not SQL...
xa_recover xa_startrscan;
xa_recover xa_noflags;

xa_commit xa_2Phase 1;
xa_rollback 2;

-- check results
xa_start xa_noflags 3;
select * from APP.global_xactTable where gxid is not null order by gxid;
select * from APP.foo;
xa_end xa_success 3;

xa_prepare 3;

-- should fail with XA_NOTA because we prepared a read only transaction 
xa_commit xa_1Phase 3;
disconnect;

---------------------------------------------
-- 3 interleaving xa connections and a local connection
---------------------------------------------
xa_start xa_noflags 4;
xa_end xa_suspend 4;
xa_start xa_noflags 5;
xa_end xa_suspend 5;
xa_start xa_noflags 6;
xa_end xa_suspend 6;
connect 'wombat' as local;
select * from foo;

xa_start xa_resume 4;
xa_getconnection;

insert into APP.foo values (4);
disconnect;

set connection local;
insert into foo values (77);

xa_end xa_suspend 4;
xa_end xa_success 4;

-- this getconnection should get a local connection
-- this has problems
--xa_getconnection;
--insert into APP.foo values (88);
--commit;
--disconnect;

xa_start xa_resume 5;
xa_getconnection;
insert into APP.foo values (5);
xa_end xa_success 5;

xa_start xa_resume 6;
insert into APP.foo values (6);
select * from APP.global_xactTable where gxid is not null order by gxid;

xa_commit xa_1Phase 4;

insert into APP.foo values (6);
select * from APP.global_xactTable where gxid is not null order by gxid;
xa_end xa_fail 6;
xa_rollback 6;
xa_start xa_join 5;

select * from APP.global_xactTable where gxid is not null order by gxid;
select * from APP.foo;
xa_end xa_success 5;
xa_prepare 5;
xa_commit xa_2Phase 5;
