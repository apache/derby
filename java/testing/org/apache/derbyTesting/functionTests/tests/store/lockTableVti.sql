--test to make sure WAIT state is displayed when lock table is printed
connect 'jdbc:derby:wombat;user=c1' AS C1;
create procedure c1.sleep(t INTEGER) dynamic result sets 0  language java external name 'java.lang.Thread.sleep' parameter style java;
create table c1.account (a int primary key not null, b int);
autocommit off;
insert into c1.account values (0, 1);
insert into c1.account values (1, 1);
insert into c1.account values (2, 1);
--setting to -1 (wait for ever to handle timing issues in the test)
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.locks.waitTimeout', '-1');
commit ;
-- call sleep once now  we don't have a timing problem later
call c1.sleep(200);

update c1.account set b = b + 11;
connect 'jdbc:derby:wombat;user=c2' AS C2;
autocommit off;
async C2S1 'update c1.account set b = b + 11';
set connection C1;
call c1.sleep(200);
select state from new org.apache.derby.diag.LockTable() t order by state;
commit;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.locks.waitTimeout', '180');
commit;
set connection c2 ;
wait for C2S1;
select state from new org.apache.derby.diag.LockTable() t order by state;
commit;


