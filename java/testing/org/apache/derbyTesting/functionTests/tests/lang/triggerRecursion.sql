--
-- Trigger recursion test
--

-- test the maximum recursion level to be less than 16
create table t1 (x int);
create table t2 (x int);
create table t3 (x int);
create table t4 (x int);
create table t5 (x int);
create table t6 (x int);
create table t7 (x int);
create table t8 (x int);
create table t9 (x int);
create table t10 (x int);
create table t11 (x int);
create table t12 (x int);
create table t13 (x int);
create table t14 (x int);
create table t15 (x int);
create table t16 (x int);
create table t17 (x int);
create trigger tr1 after insert on t1 for each row MODE DB2SQL insert into t2 values 666;
create trigger tr2 after insert on t2 for each row MODE DB2SQL insert into t3 values 666;
create trigger tr3 after insert on t3 for each row MODE DB2SQL insert into t4 values 666;
create trigger tr4 after insert on t4 for each row MODE DB2SQL insert into t5 values 666;
create trigger tr5 after insert on t5 for each row MODE DB2SQL insert into t6 values 666;
create trigger tr6 after insert on t6 for each row MODE DB2SQL insert into t7 values 666;
create trigger tr7 after insert on t7 for each row MODE DB2SQL insert into t8 values 666;
create trigger tr8 after insert on t8 for each row MODE DB2SQL insert into t9 values 666;
create trigger tr9 after insert on t9 for each row MODE DB2SQL insert into t10 values 666;
create trigger tr10 after insert on t10 for each row MODE DB2SQL insert into t11 values 666;
create trigger tr11 after insert on t11 for each row MODE DB2SQL insert into t12 values 666;
create trigger tr12 after insert on t12 for each row MODE DB2SQL insert into t13 values 666;
create trigger tr13 after insert on t13 for each row MODE DB2SQL insert into t14 values 666;
create trigger tr14 after insert on t14 for each row MODE DB2SQL insert into t15 values 666;
create trigger tr15 after insert on t15 for each row MODE DB2SQL insert into t16 values 666;
create trigger tr16 after insert on t16 for each row MODE DB2SQL insert into t17 values 666;
create trigger tr17 after insert on t17 for each row MODE DB2SQL values 1;

-- here we go
;
insert into t1 values 1;

-- prove it
select * from t1;

-- The following should work, but because of defect 5602, it raises NullPointerException.
-- After the fix for 5602, we could enable the following part of the test.
-- Reduce the recursion level to 16. It should pass now.
-- drop trigger tr17;

-- insert  into t1 values 2;

-- prove it
-- select * from t1;

