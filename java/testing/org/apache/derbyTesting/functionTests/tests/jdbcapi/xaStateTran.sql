disconnect;

-- This test tests state transitons with XA

xa_datasource 'wombat';
xa_connect; 
xa_getconnection;
-- set up some stuff for the test
xa_start xa_noflags 0;
run resource '/org/apache/derbyTesting/functionTests/tests/store/global_xactTable.view';
create table xastate(a int);
xa_end xa_success 0;
xa_commit xa_1phase 0;

---------------------------------------------
-- INIT STATE (Transaction not started)
---------------------------------------------
-- the following should work
xa_start xa_noflags 10;
select * from global_xactTable where gxid is not null order by gxid;
xa_end xa_success 10;
xa_rollback 10;
-- the following should error XAER_NOTA
xa_start xa_join 11;
-- the following should error XAER_NOTA
xa_start xa_resume 11;
-- the following should error XAER_NOTA
xa_end xa_success 11;
-- the following should error XAER_NOTA
xa_end xa_fail 11;
-- the following should error XAER_NOTA
xa_end xa_suspend 11;
-- the following should error XAER_NOTA
xa_prepare 11;
-- the following should error XAER_NOTA
xa_commit xa_1phase 11;
-- the following should error XAER_NOTA
xa_commit xa_2phase 11;
-- the following should error XAER_NOTA
xa_rollback 11;
-- the following should error XAER_NOTA
xa_forget 11;
---------------------------------------------
-- NOTASSOCIATED (Transaction started but not associated with a resource)
---------------------------------------------
xa_start xa_noflags 20;
xa_end xa_success 20;
select * from global_xactTable where gxid is not null order by gxid;
-- the following should error XAER_DUPID
xa_start xa_noflags 20;
-- the following should work
xa_start xa_join 20;
xa_end xa_success 20;
select * from global_xactTable where gxid is not null order by gxid;
-- the following should error (transaction wasn't suspended) XAER_PROTO
xa_start xa_resume 20;
-- the following should work
xa_start xa_join 20;
xa_end xa_suspend 20;
xa_start xa_resume 20;
xa_end xa_success 20;
select * from global_xactTable where gxid is not null order by gxid;
-- the following should work (xa_success after xa_suspend assume xa_start xa_resume)
xa_start xa_join 20;
xa_end xa_suspend 20;
xa_end xa_success 20;
select * from global_xactTable where gxid is not null order by gxid;
-- the following should error XAER_PROTO
xa_end xa_success 20;
-- the following should error XAER_PROTO
xa_end xa_fail 20;
-- the following should error XAER_PROTO
xa_end xa_suspend 20;
-- the following should work 
xa_prepare 20;
select * from global_xactTable where gxid is not null order by gxid;
-- the following should error (since xact was readonly we have already forgotten
--	about the transaction) XAER_NOTA
--
xa_commit xa_1phase 20; 
-- the following should work
xa_start xa_noflags 21;
insert into xastate values(1);
xa_end xa_success 21;
xa_commit xa_1phase 21;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should error(since xact has been committed) XAER_NOTA
xa_commit xa_2phase 21;

select * from global_xactTable where gxid is not null order by gxid;

xa_start xa_noflags 22;
insert into xastate values(2);
xa_end xa_success 22;

select * from global_xactTable where gxid is not null order by gxid;

-- in not associated state, this should fail since we haven't done prepare XAER_PROTO
xa_commit xa_2phase 22;
-- the following should work
xa_rollback 22;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should error XAER_PROTO (transaction wasn't prepared)
xa_start xa_noflags 23;
insert into xastate values(1);
xa_end xa_success 23;

select * from global_xactTable where gxid is not null order by gxid;

xa_forget 23;
--
-- clean up transaction
xa_rollback 23;
---------------------------------------------
-- ASSOCIATED (Transaction started and associated with this resource)
---------------------------------------------
xa_start xa_noflags 40;
select * from global_xactTable where gxid is not null order by gxid;

-- the following should error XAER_PROTO
xa_start xa_noflags 40;
-- the following should error XAER_PROTO
xa_start xa_join 40;
-- the following should error (transaction wasn't suspended) XAER_PROTO
xa_start xa_resume 40;

select * from global_xactTable where gxid is not null order by gxid;
-- the following should work
xa_end xa_success 40;
select * from global_xactTable where gxid is not null order by gxid;

xa_rollback 40;

-- get back in associated state
xa_start xa_noflags 40;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should work
xa_end xa_fail 40;

select * from global_xactTable where gxid is not null order by gxid;
xa_rollback 40;

-- get back in associated state
xa_start xa_noflags 40;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should work
xa_end xa_suspend 40;

select * from global_xactTable where gxid is not null order by gxid;

xa_end xa_success 40;
xa_rollback 40;

-- get back in associated state
xa_start xa_noflags 40;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should error XAER_PROTO
xa_prepare 40;
-- the following should error XAER_PROTO
xa_commit xa_1phase 40;
-- the following should error XAER_PROTO
xa_commit xa_2phase 40;
-- the following should error XAER_PROTO
xa_rollback 40;
-- the following should error XAER_PROTO
xa_forget 40;
--
-- clean up transaction
xa_end xa_success 40;
xa_rollback 40;
---------------------------------------------
-- DEAD STATE (Transaction started and ended with a fail (rollback only))
---------------------------------------------
-- set up dead state
xa_start xa_noflags 40;
select * from global_xactTable where gxid is not null order by gxid;
xa_end xa_fail 40;

select * from global_xactTable where gxid is not null order by gxid;
xa_rollback 40;

-- the following should work <transaction is rolled back and forgotten)
xa_start xa_noflags 40;

select * from global_xactTable  where gxid is not null order by gxid;

-- set up dead state
xa_end xa_fail 40;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should error XA_RBROLLBACK
xa_start xa_join 40;
-- the following should error XA_RBROLLBACK
xa_start xa_resume 40;
-- the following should error XA_RBROLLBACK
xa_end xa_success 40;
-- the following should error XA_RBROLLBACK
xa_end xa_fail 40;
-- the following should error XA_RBROLLBACK
xa_end xa_suspend 40;
-- the following should error XA_RBROLLBACK
xa_prepare 40;
-- the following should error XA_RBROLLBACK
xa_commit xa_1phase 40;
-- the following should error XA_RBROLLBACK
xa_commit xa_2phase 40;
-- the following should error XAER_PROTO
xa_forget 40;			
xa_rollback 40;
--
---------------------------------------------
-- PREPARE STATE (Transaction started and prepared)
---------------------------------------------
-- set up prepare state
xa_start xa_noflags 50;
insert into xastate values(2);
xa_end xa_success 50;
xa_prepare 50;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should error XAER_DUPID
xa_start xa_noflags 50;
-- the following should error XAER_PROTO
xa_start xa_join 50;
-- the following should error XAER_PROTO
xa_start xa_resume 50;
-- the following should error XAER_PROTO
xa_end xa_success 50;
-- the following should error XAER_PROTO
xa_end xa_fail 50;
-- the following should error XAER_PROTO
xa_end xa_suspend 50;
-- the following should error XAER_PROTO
xa_prepare 50;
-- the following should error XAER_PROTO
xa_commit xa_1phase 50; 
-- the following should work 
xa_commit xa_2phase 50;

-- get back into prepared state
xa_start xa_noflags 50;
insert into xastate values(2);
xa_end xa_success 50;
xa_prepare 50;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should work
xa_rollback 50;

select * from global_xactTable where gxid is not null order by gxid;

-- get back into prepared state
xa_start xa_noflags 50;
insert into xastate values(2);
xa_end xa_success 50;
xa_prepare 50;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should error XAER_NOTA 
xa_forget 50;
--
-- clean up transaction
xa_rollback 50;
---------------------------------------------
--  COMMIT STATE (Transaction started and commited)
---------------------------------------------
-- set up commit state
xa_start xa_noflags 60;
insert into xastate values(3);
xa_end xa_success 60;
xa_commit xa_1phase 60;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should work starting a new transaction
xa_start xa_noflags 60;

select * from global_xactTable where gxid is not null order by gxid;
-- get back to commit state
insert into xastate values(4);
xa_end xa_success 60;
xa_commit xa_1phase 60;

-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_start xa_join 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_start xa_resume 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_end xa_success 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_end xa_fail 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_end xa_suspend 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_prepare 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_commit xa_1phase 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_commit xa_2phase 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_rollback 60;
-- the following should error XAER_NOTA (transaction committed and forgotten)
xa_forget 60;
--
---------------------------------------------
--  ABORT STATE (Transaction started and rolledback)
---------------------------------------------
-- set up rollback state
xa_start xa_noflags 70;
insert into xastate values(5);
xa_end xa_success 70;
xa_rollback 70;

select * from global_xactTable where gxid is not null order by gxid;

-- the following should work - start a new transaction
xa_start xa_noflags 70;

-- get back to rollback state
insert into xastate values(4);
xa_end xa_success 70;
xa_rollback 70;

-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_start xa_join 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_start xa_resume 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_end xa_success 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_end xa_fail 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_end xa_suspend 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_prepare 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_commit xa_1phase 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_commit xa_2phase 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_rollback 70;
-- the following should error XAER_NOTA (transaction rolled back and forgotten)
xa_forget 70;

--
-- cleanup
-- 
drop table xastate;
drop view global_xactTable;
