
autocommit off;

create table tab1 (c1 int, c2 varchar(20000));
create table tab2 (c1 int, c2 varchar(20000));
create table tab3 (c1 int, c2 varchar(2000));
create table tab4 (c1 int, c2 varchar(2000));
create procedure INSERTDATA() language java parameter style java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.maxMemPerTabTest';
call INSERTDATA();

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2500;

-- should use nested loop join due to maxMemoryPerTable property setting
select * from tab1, tab2 where tab1.c2 = tab2.c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use hash join, maxMemoryPerTable property value is big enough
select * from tab3, tab4 where tab3.c2 = tab4.c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

rollback;
