-- tests for constant expression evaluation
autocommit off;

-- create and populate a table
create table t1(c1 int);
insert into t1 values 1, 2, 3;

-- false constant expressions
select * from t1 where 1 <> 1;
select * from t1 where 1 = 1 and 1 = 0;
select * from t1 where 1 = (2 + 3 - 2);
select * from t1 where (case when 1 = 1 then 0 else 1 end) = 1;
select * from t1 where 1 in (2, 3, 4);
select * from t1 where 1 between 2 and 3;

prepare p1 as 'select * from t1 where ? = 1';
prepare p2 as 'select * from t1 where cast(? as int) = 1';
execute p1 using 'values 0';
execute p2 using 'values 0';

-- true constant expressions
select * from t1 where 1 = 1;
select * from t1 where 1 = 0 or 1 = 1;
select * from t1 where 1 + 2 = (2 + 3 - 2);
select * from t1 where (case when 1 = 1 then 1 else 0 end) = 1;
select * from t1 where 1 in (2, 3, 4, 4, 3, 2, 1);
select * from t1 where 1 + 1 between 0 and 3;

execute p1 using 'values 1';
execute p2 using 'values 1';

-- clean up
drop table t1;
