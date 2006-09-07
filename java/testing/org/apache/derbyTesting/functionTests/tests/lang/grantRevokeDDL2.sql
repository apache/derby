connect 'grantRevokeDDL2;create=true' user 'user1' as user1;
connect 'grantRevokeDDL2;create=true' user 'user2' as user2;
connect 'grantRevokeDDL2;create=true' user 'user3' as user3;

-- DERBY-1729
-- test grant and revoke in Java stored procedure with triggers.
-- Java stored procedure that contains grant or revoke statement 
-- requires MODIFIES SQL DATA to execute.
-- Since only 2 of the 8 Java stored procedures(which contains
-- grant or revoke statement) are declared with MODIFIES SQL DATA, 
-- the rest are expected to fail in this test.
 
-- setup the environment
set connection user1;

-- table used in the procedures
drop table t1;
create table t1 (i int primary key, b char(15));
insert into t1 values (1, 'XYZ');
insert into t1 values (2, 'XYZ');
insert into t1 values (3, 'XYZ');
insert into t1 values (4, 'XYZ');
insert into t1 values (5, 'XYZ');
insert into t1 values (6, 'XYZ');
insert into t1 values (7, 'XYZ');
insert into t1 values (8, 'XYZ');

-- table used in this test
drop table t2;
create table t2 (x integer, y integer);

create procedure grant_select_proc1() 
       parameter style java
       dynamic result sets 0 language java 
       NO SQL
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure grant_select_proc2() 
       parameter style java
       dynamic result sets 0 language java 
       CONTAINS SQL
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure grant_select_proc3() 
       parameter style java
       dynamic result sets 0 language java 
       READS SQL DATA 
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure grant_select_proc4() 
       parameter style java
       dynamic result sets 0 language java 
       MODIFIES SQL DATA  
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'; 

create procedure revoke_select_proc1() 
       parameter style java
       dynamic result sets 0 language java 
       NO SQL  
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

create procedure revoke_select_proc2() 
       parameter style java
       dynamic result sets 0 language java 
       CONTAINS SQL  
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

create procedure revoke_select_proc3() 
       parameter style java
       dynamic result sets 0 language java 
       READS SQL DATA 
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

create procedure revoke_select_proc4() 
       parameter style java
       dynamic result sets 0 language java 
       MODIFIES SQL DATA 
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'; 

-- tests

create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc1();
-- should fail
delete from t1 where i = 1;
-- check delete failed
select * from t1 where i = 1;
drop trigger grant_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc2();
-- should fail
delete from t1 where i = 2;
-- check delete failed
select * from t1 where i = 2; 
drop trigger grant_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc3();
-- should fail
delete from t1 where i = 3;
-- check delete failed
select * from t1 where i = 3; 
drop trigger grant_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
create trigger grant_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call grant_select_proc4();
-- ok
delete from t1 where i = 4;
-- check delete
select * from t1 where i = 4;
drop trigger grant_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc1();
-- should fail
delete from t1 where i = 5;
-- check delete failed
select * from t1 where i = 5;
drop trigger revoke_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc2();
-- should fail
delete from t1 where i = 6;
-- check delete failed
select * from t1 where i = 6;
drop trigger revoke_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc3();
-- should fail
delete from t1 where i = 7;
-- check delete failed
select * from t1 where i = 7; 
drop trigger revoke_select_trig;
set connection user2;
-- should be successful
select * from user1.t1 where i = 1;

set connection user1;
create trigger revoke_select_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call revoke_select_proc4();
-- ok
delete from t1 where i = 8;
-- check delete 
select * from t1 where i = 8; 
drop trigger revoke_select_trig;
set connection user2;
-- should fail
select * from user1.t1 where i = 1;

set connection user1;
