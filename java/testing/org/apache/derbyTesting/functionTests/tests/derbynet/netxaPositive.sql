
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
xa_end xa_success 2;


xa_start xa_resume 1;
insert into APP.foo values(5);
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


-- DERBY-246 xa_end after connection close should be ok.  
-- Also to reuse xaconnection in the same global transaction
xa_getconnection;
xa_start xa_noflags 4;
create table APP.derby246  (i int);
insert into APP.derby246 values(1);
disconnect;
xa_getconnection;
insert into APP.derby246 values(2);
disconnect;
xa_end xa_success 4;
xa_prepare 4;
xa_commit xa_2phase 4;

-- now connect with a local connection to make sure locks are released 
-- and our values are there. Should see two rows
connect 'wombat';
select * from APP.derby246;

disconnect;



