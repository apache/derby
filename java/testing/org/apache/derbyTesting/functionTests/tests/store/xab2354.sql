xa_datasource 'wombat' create;
xa_connect;
xa_start xa_noflags 1;
xa_getconnection;
-- Global transactions can not have hold cursor over commit. And hence we need to make sure the holdability is false for all jdks
-- In jdk13 and lower, this Brokered Connection has its holdability false over commit so we are fine. 
-- In jdk14 and higher, this Brokered Connection has its holdability true over commit. In order to set it to false, we have NoHoldForConnection 
-- NoHoldForConnection uses setHoldability api on Connection to set the holdability to false. But this api exists only for jdk14 and higher
-- And that is why, in jkd13 master, we see an exception nosuchmethod 
NoHoldForConnection;
create table foo (a int);
insert into foo values (1);
xa_end xa_success 1;
xa_commit xa_1phase 1;

xa_start xa_noflags 1;
insert into foo values (2);
select cast(global_xid as char(2)) as gxid, status from new org.apache.derby.diag.TransactionTable() t where gxid is not null order by gxid, status;
xa_end xa_success 1;
xa_prepare 1;

xa_getconnection ;
select cast(global_xid as char(2)) as gxid, status from new org.apache.derby.diag.TransactionTable() t where gxid is not null order by gxid, status;
xa_datasource 'wombat' shutdown;

xa_datasource 'wombat';
xa_connect;

-- this works correctly
xa_start xa_noflags 1;

xa_getconnection;

-- this was the bug, this statement should also get DUPID error
xa_start xa_noflags 1;

-- should see two transactions, one global transaction and one local
select cast(global_xid as char(2)) as gxid, status from new org.apache.derby.diag.TransactionTable() t where gxid is not null order by gxid, status;
