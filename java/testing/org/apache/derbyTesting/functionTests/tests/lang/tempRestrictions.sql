-- Tests for temporary restrictions in the language

-- create a table
create table t1(c1 int);

-- No aggregates in the where clause
-- (Someday will will allow correlated aggregates ...)
select c1 from t1 where max(c1) = 1;

-- drop the table
drop table t1;
