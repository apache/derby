-- negative tests for ungrouped aggregates

-- create a table
create table t1 (c1 int);
create table t2 (c1 int);
insert into t2 values 1,2,3;

-- mix aggregate and non-aggregate expressions in the select list
select c1, max(c1) from t1;
select c1 * max(c1) from t1;

-- aggregate in where clause
select c1 from t1 where max(c1) = 1;

-- aggregate in ON clause of inner join
select * from t1 join t1 as t2 on avg(t2.c1) > 10;

-- correlated subquery in select list
select max(c1), (select t2.c1 from t2 where t1.c1 = t2.c1) from t1;

-- noncorrelated subquery that returns more than 1 row
select max(c1), (select t2.c1 from t2) from t1;

-- drop the table
drop table t1
