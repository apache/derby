xa_datasource 'wombat';

---------------------------------------------
-- a single connection and 1 phase commit
---------------------------------------------
xa_connect ;
xa_start xa_noflags 0;
xa_getconnection;
-- Global transactions can not have hold cursor over commit. And hence we need to make sure the holdability is false for all jdks
-- In jdk13 and lower, this Brokered Connection has its holdability false over commit so we are fine. 
-- In jdk14 and higher, this Brokered Connection has its holdability true over commit. In order to set it to false, we have NoHoldForConnection 
-- NoHoldForConnection uses setHoldability api on Connection to set the holdability to false. But this api exists only for jdk14 and higher
-- And that is why, in jkd13 master, we see an exception nosuchmethod 
NoHoldForConnection;
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
-- Global transactions can not have hold cursor over commit. And hence we need to make sure the holdability is false for all jdks
-- In jdk13 and lower, this Brokered Connection has its holdability false over commit so we are fine. 
-- In jdk14 and higher, this Brokered Connection has its holdability true over commit. In order to set it to false, we have NoHoldForConnection 
-- NoHoldForConnection uses setHoldability api on Connection to set the holdability to false. But this api exists only for jdk14 and higher
-- And that is why, in jkd13 master, we see an exception nosuchmethod 
NoHoldForConnection;
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
-- Global transactions can not have hold cursor over commit. And hence we need to make sure the holdability is false for all jdks
-- In jdk13 and lower, this Brokered Connection has its holdability false over commit so we are fine. 
-- In jdk14 and higher, this Brokered Connection has its holdability true over commit. In order to set it to false, we have NoHoldForConnection 
-- NoHoldForConnection uses setHoldability api on Connection to set the holdability to false. But this api exists only for jdk14 and higher
-- And that is why, in jkd13 master, we see an exception nosuchmethod 
NoHoldForConnection;

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
-- Global transactions can not have hold cursor over commit. And hence we need to make sure the holdability is false for all jdks
-- In jdk13 and lower, this Brokered Connection has its holdability false over commit so we are fine. 
-- In jdk14 and higher, this Brokered Connection has its holdability true over commit. In order to set it to false, we have NoHoldForConnection 
-- NoHoldForConnection uses setHoldability api on Connection to set the holdability to false. But this api exists only for jdk14 and higher
-- And that is why, in jkd13 master, we see an exception nosuchmethod 
NoHoldForConnection;
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
