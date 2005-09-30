xa_datasource 'wombat' create;
xa_connect;
xa_start xa_noflags 1;
xa_getconnection;
create table foo (a int);
insert into foo values (1);
xa_end xa_success 1;
xa_commit xa_1phase 1;

xa_start xa_noflags 1;
insert into foo values (2);
select cast(global_xid as char(2)) as gxid, status from syscs_diag.transaction_table where gxid is not null order by gxid, status;
xa_end xa_success 1;
xa_prepare 1;

xa_getconnection ;
select cast(global_xid as char(2)) as gxid, status from syscs_diag.transaction_table where gxid is not null order by gxid, status;
xa_datasource 'wombat' shutdown;

xa_datasource 'wombat';
xa_connect;

-- this works correctly
xa_start xa_noflags 1;

xa_getconnection;

-- this was the bug, this statement should also get DUPID error
xa_start xa_noflags 1;

-- should see two transactions, one global transaction and one local
select cast(global_xid as char(2)) as gxid, status from syscs_diag.transaction_table where gxid is not null order by gxid, status;
