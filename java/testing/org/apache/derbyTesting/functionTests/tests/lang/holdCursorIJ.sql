-- create a table
create table t1(c11 int, c12 int);

-- insert data into tables
insert into t1 values(1,1);
insert into t1 values(2,2);

-- set autocommit off
autocommit off;

-- first test - make sure that only cursors created with holdability true
-- have open resultsets after commit

-- declare 3 different kind of cursors one for each jdbc release so far
get with nohold cursor jdk1 as 'SELECT * FROM t1';
get scroll insensitive with nohold cursor jdk2 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';

-- do fetches from these cursors
next jdk1;
next jdk2;
next jdk4;

--commit
commit;

-- now try the fetch on cursors again after commit
-- cursors jdk1 and jdk2 will give errors
next jdk1;
next jdk2;
next jdk4;
-- end of resultset for jdk4, but try next again
next jdk4;
close jdk4;
next jdk4;

-- second test - make sure that all the cursors (including holdability true)
-- have their resultsets closed after rollback.

-- declare the cursors again, this time, try with rollback
get with nohold cursor jdk1 as 'SELECT * FROM t1';
get scroll insensitive with nohold cursor jdk2 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';

-- do fetches from these cursors
next jdk1;
next jdk2;
next jdk4;

--rollback
rollback;

-- now try the fetch on cursors again after rollback
-- all the cursors will give errors
next jdk1;
next jdk2;
next jdk4;

-- third test - Define a hold cursor on a table. Shouldn't be able to drop that
-- table before & after commit. Have to close the cursor before table can be dropped.

get with nohold cursor jdk1 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';
next jdk1;
next jdk4;
-- wont' be able to drop table because of cursors jdk1 and jdk4
drop table t1;
commit;

-- drop table still won't work because jdk4 is still open after commit
drop table t1;

-- close cursor jdk4 and try then deleting the table
close jdk4;
drop table t1;

-- recreate and populate the table for next test
create table t1(c11 int, c12 int);
insert into t1 values(1,1);
insert into t1 values(2,2);

-- fourth test - try to change the isolation level while there are
-- held cursors
get with nohold cursor jdk1 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';
next jdk1;
next jdk4;

-- try to change the isolation level. will give error because of jdk1 and jdk4
set current isolation RR;

commit;

-- attempt to change isolation level should give error because of jdk4 hold cursor
set isolation = REPEATABLE READ;

-- close jdk4 and then should be able to change isolation
close jdk4;
set isolation to serializable;

-- fifth test - try isolation level change alongwith changing the isolation
-- level of just one statement
get with hold cursor jdk4 as 'SELECT * FROM t1';
get with nohold cursor jdk1 as 'SELECT * FROM t1 WITH CS';
next jdk4;
next jdk1;
-- following should fail because of cursor jdk4
set isolation RS;
-- following should fail because of cursor jdk4
set isolation UR;
close jdk4;
-- should be able to change the isolation now
set isolation READ UNCOMMITTED;
set isolation RS;

-- sixth test - try positioned update with hold cursor
get with hold cursor jdk4 as 'SELECT * FROM t1 FOR UPDATE';
-- following should give error because cursor is not positioned on any row
update t1 set c12=12 where current of jdk4;
select * from t1;
next jdk4;
update t1 set c12=12 where current of jdk4;
select * from t1;
commit;
-- after commit, the next transaction should do a fetch again before doing
-- any positioned update
update t1 set c12=123 where current of jdk4;
select * from t1;
next jdk4;
update t1 set c12=23 where current of jdk4;
select * from t1;
close jdk4;
update t1 set c12=234 where current of jdk4;
select * from t1;

-- seventh test - try positioned delete with hold cursor
get with hold cursor jdk4 as 'SELECT * FROM t1 FOR UPDATE';
-- following should give error because cursor is not positioned on any row
delete from t1 where current of jdk4;
select * from t1;
next jdk4;
delete from  t1 where current of jdk4;
select * from t1;
commit;
-- after commit, the next transaction should do a fetch again before doing
-- any positioned delete
delete from t1 where current of jdk4;
select * from t1;
next jdk4;
delete from t1 where current of jdk4;
select * from t1;
close jdk4;
delete from t1 where current of jdk4;
select * from t1;

-- populate the table for next test
insert into t1 values(1,1);
insert into t1 values(2,2);

-- eighth test - scrollable cursors
get scroll insensitive with hold cursor jdk4 as 'SELECT * FROM t1';
commit;
previous jdk4;
after last jdk4;
before first jdk4;
first jdk4;
last jdk4;
next jdk4;
previous jdk4;
next jdk4;
close jdk4;
first jdk4;

-- ninth test - close the updateable holdable cursor after commit
-- we get npe
get with hold cursor jdk4 as 'SELECT * FROM T1 FOR UPDATE';
next jdk4;
commit;
close jdk4;

-- tenth test - bug 4515 - have a more useful message
-- update where current of fails in autocommit=true, held open cursor

autocommit on;
get with hold cursor scrollCursor as 'select * from t1 for update of c12';
next scrollCursor;
update t1 set c12=c12+1 where current of scrollCursor;



