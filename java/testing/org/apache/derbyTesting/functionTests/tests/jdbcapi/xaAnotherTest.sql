disconnect;

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
drop table APP.foo;
create table APP.foo (a int);
insert into APP.foo values (0);
select * from APP.foo;
run resource '/org/apache/derbyTesting/functionTests/tests/store/global_xactTable.view';
select * from global_xactTable where gxid is not null order by gxid, username, status;
xa_end xa_success 0;
xa_commit xa_1phase 0;

xa_datasource 'wombat' shutdown;

---------------------------------------------
-- 5 interleaving xa connections
---------------------------------------------
xa_datasource 'wombat';
xa_connect user 'mamta' password 'mamta' ;

-- global connection 1
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

-- global connection 2
xa_start xa_noflags 2;
insert into APP.foo values (2);
xa_end xa_suspend 2;

-- global connection 3
xa_start xa_noflags 3;
insert into APP.foo values (3);
xa_end xa_suspend 3;

-- global connection 4
xa_start xa_noflags 4;
insert into APP.foo values (4);
xa_end xa_suspend 4;

-- global connection 5
xa_start xa_noflags 5;
insert into APP.foo values (5);
xa_end xa_suspend 5;

xa_start xa_resume 1;
insert into APP.foo values (11);
xa_end xa_suspend 1;

xa_start xa_resume 5;
insert into APP.foo values (55);
xa_end xa_suspend 5;

xa_start xa_resume 2;
insert into APP.foo values (22);
xa_end xa_suspend 2;

xa_start xa_resume 4;
insert into APP.foo values (44);
xa_end xa_suspend 4;

xa_start xa_resume 3;
insert into APP.foo values (33);
xa_end xa_suspend 3;

-- prepare all the global connections except the first one. This way, we will see all
-- the global transactions prepared so far after the database shutdown and restart.
xa_end xa_success 2;
xa_prepare 2;
xa_end xa_success 3;
xa_prepare 3;
xa_end xa_success 4;
xa_prepare 4;
xa_end xa_success 5;
xa_prepare 5;

-- local connection 1
connect 'wombat' as local1;
autocommit off;

-- at this point, should see 4 global connections in the prepared mode and one global
-- connection in active mode and one local connection.
select * from global_xactTable where gxid is not null order by gxid, username, status;
select count(*) from new org.apache.derby.diag.LockTable() t where mode = 'X' or mode = 'IX';

xa_datasource 'wombat' shutdown;

-- after shutdown and restart, should see only 4 prepared global connection from
-- earlier boot of the database. The local connections made during that time and
-- unprepared global connection will all rollback at the startup time and hence
-- we won't see them
xa_datasource 'wombat';
xa_connect user 'mamta1' password 'mamta1';

connect 'wombat' as local2;
autocommit off;
-- this will time out because there are locks on the table APP.foo from the global
-- transactions
select * from APP.foo;
-- should see 4 global transactions and a local connection
select * from global_xactTable where gxid is not null order by gxid, username, status;

-- rollback first global transactions 2 and 3 and commit the 3rd one.
xa_rollback 2;
xa_rollback 3;
xa_commit xa_2Phase 4;

-- add couple more global transactions
xa_start xa_noflags 6;
xa_getconnection;
-- Global transactions can not have hold cursor over commit. And hence we need to make sure the holdability is false for all jdks
-- In jdk13 and lower, this Brokered Connection has its holdability false over commit so we are fine. 
-- In jdk14 and higher, this Brokered Connection has its holdability true over commit. In order to set it to false, we have NoHoldForConnection 
-- NoHoldForConnection uses setHoldability api on Connection to set the holdability to false. But this api exists only for jdk14 and higher
-- And that is why, in jkd13 master, we see an exception nosuchmethod 
NoHoldForConnection;
insert into APP.foo values (6);
xa_end xa_suspend 6;

xa_start xa_noflags 7;
insert into APP.foo values (7);
xa_end xa_suspend 7;

xa_start xa_noflags 8;
insert into APP.foo values (8);
xa_end xa_suspend 8;

-- once a transaction is in prepare mode, can't resume it. Can only commit/rollback
-- so the following will give an error
xa_start xa_resume 5;

xa_start xa_resume 6;
insert into APP.foo values (66);
xa_end xa_suspend 6;

xa_start xa_resume 8;
insert into APP.foo values (88);
xa_end xa_suspend 8;

xa_start xa_resume 7;
insert into APP.foo values (77);
xa_end xa_suspend 7;

-- prepare the global transactions added after the database boot
xa_end xa_success 6;
xa_prepare 6;
xa_end xa_success 7;
xa_prepare 7;
xa_end xa_success 8;
xa_prepare 8;

-- make a local connection and at this point, should see 4 global transactions
-- and 2 local connections
connect 'wombat' as local3;
autocommit off;
select * from global_xactTable where gxid is not null order by gxid, username, status;

xa_datasource 'wombat' shutdown;

-- shutdown the datbase, restart and check the transactions in the transaction table.
xa_datasource 'wombat';
xa_connect user 'mamta2' password 'mamta2';

connect 'wombat' as local4;
autocommit off;
-- this will time out as expected
select * from APP.foo;
-- will see 4 global transactions and 1 local transaction
select * from global_xactTable where gxid is not null order by gxid, username, status;

xa_datasource 'wombat' shutdown;

-- shutdown and restart and check the transaction table
xa_datasource 'wombat';
xa_connect user 'mamta3' password 'mamta3';

connect 'wombat' as local5;
autocommit off;
insert into APP.foo values(90);

connect 'wombat' as local6;
autocommit off;
insert into APP.foo values(101);

-- 4 global transactions and 2 local transactions
select * from global_xactTable where gxid is not null order by gxid, username, status;

-- rollback few global transactions and commit few others
xa_rollback 5;
xa_rollback 6;
xa_commit xa_2Phase 7;
xa_rollback 8;

-- at this point, still time out because there are 2 local transactions
-- holding locks on table APP.foo
select * from APP.foo;
select * from global_xactTable where gxid is not null order by gxid, username, status;

xa_datasource 'wombat' shutdown;

-- shutdown and restart. There should be no global transactions at this point.
xa_datasource 'wombat';
xa_connect user 'mamta4' password 'mamta4';

connect 'wombat' as local7;
autocommit off;
-- no more locks on table APP.foo and hence select won't time out.
select * from APP.foo;
-- no more global transactions, just one local transaction
select * from global_xactTable where gxid is not null order by gxid, username, status;

