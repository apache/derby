-- single user test for the various isolation levels

prepare getIsolation as 'values current isolation';

autocommit off;
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

-- create a table
create table t1(c1 int not null constraint asdf primary key);
commit;

-- insert a row
insert into t1 values 1;
-- verify table scan gets row lock at read committed
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- verify matching index scan gets row lock at read committed
select * from t1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify WITH clause works
select * from t1 with rr;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify SET ISOLATION commits and changes isolation level
set isolation RR;
execute getIsolation;

-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

set isolation reset;
execute getIsolation;
-- verify matching index scan gets row lock at read committed
select * from t1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify SET ISOLATION commits and changes isolation level
set isolation read committed;
execute getIsolation;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

set current isolation = reset;
execute getIsolation;

-- verify SET ISOLATION commits and changes isolation level
set current isolation = RS;
execute getIsolation;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

set isolation to reset;
execute getIsolation;

-- verify SET ISOLATION commits and changes isolation level
set isolation = dirty read;
execute getIsolation;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- test WITH clause
set isolation serializable;
execute getIsolation;
select * from t1 with cs;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

set isolation cursor stability;
execute getIsolation;
select * from t1 with RR;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

set isolation serializable;
execute getIsolation;
select * from t1 with RS;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

set current isolation to read committed;
execute getIsolation;
select * from t1 with ur;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- unknown isolation level
select * from t1 with rw;

-- check the db2 isolation levels can be used as identifiers
create table db2iso(cs int, rr int, ur int, rs int);
select cs, rr, ur, rs from db2iso;
-- cleanup
drop table t1;
