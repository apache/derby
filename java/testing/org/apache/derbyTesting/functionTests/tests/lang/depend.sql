
--
-- this test shows the dependency system in action;
--

autocommit off;

create table t(i int);
create table s(i int);

prepare ins as 'insert into t (i) values (1956)';
prepare ins_s as 'insert into s (i) values (1956)';
prepare sel as 'select i from t';
prepare sel2 as 'select i from (select i from t) a';
prepare sel_s as 'select i from s where i = (select i from t)';
prepare upd as 'update t set i = 666 where i = 1956';
prepare del as 'delete from t where i = 666';
prepare ins_sel as 'insert into t select * from s';

execute ins;
execute ins_s;
execute sel;
execute sel2;
execute sel_s;
execute upd;
execute sel;
execute del;
execute sel;
execute ins_sel;
execute sel;

drop table t;
-- these should fail, can't find table
execute ins;
execute sel;
execute sel2;
execute upd;
execute del;
execute sel_s;
execute ins_sel;

create table t(i int);
-- these should recompile and work, table now found
execute ins;
-- expect one row only
execute sel; 
execute sel2;
execute sel_s;
execute upd;
-- test update
execute sel;
execute del;
-- test delete
execute sel;
execute ins_sel;
execute sel;

rollback;

-- these should fail, the table will disappear at the rollback
execute ins;
execute sel;
execute sel2;
execute sel_s;
execute upd;
execute del;

-- recreate t again
create table t(i int);
-- these should recompile and work, table now found
execute ins;

-- open a cursor on t
get cursor c1 as 'select * from t';

-- dropping t should fail, due to open cursor
drop table t;

-- insert should still succeed, since table not dropped
execute ins;

-- close cursor
close c1;

-- drop table should succeed
drop table t;

-- verify that invalidate worked this time
execute ins;
execute sel;
execute sel2;
execute upd;
execute del;
execute ins_sel;

-- cleanup, roll everything back to the beginning
rollback;

-- verify that cascading invalidations work
create table t1(c1 int);
insert into t1 values 1, 2;
get cursor c1 as 'select c1 from t1 for update of c1';
-- positioned update dependent on cursor c1
prepare u1 as 'update t1 set c1 = c1 + 1 where current of c1';
next c1;
close c1;
execute u1;

-- cleanup, roll everything back to the beginning
rollback;

-- verify that create index invalidates based on table and
-- drop index invalidates based on the index

create table t1(c1 int, c2 int);
insert into t1 values (1,1), (2, 1), (3,3);
create index i1 on t1(c1);
get cursor c1 as 'select c1 from t1 where c2 = 1 for update of c1';
next c1;
prepare u1 as 'update  t1 set c1 = c1 + 1 ';
prepare i1 as 'insert into t1 values (4, 4)';
prepare d1 as 'delete from t1 where c2 = 3';
drop index i1;

-- u1 should be recompiled succesfully
execute u1;
select * from t1;

-- recreate index i1, this time on c2
create index i1 on t1(c2);
next c1;
close c1;

-- i1 and d1 should have been invalidated and recompiled
execute i1;
-- check the state of the index
select * from t1 where c2 > 0;

execute d1;
-- check the state of the index
select * from t1 where c2 > 0;

-- cleanup, roll everything back to the beginning
rollback;
