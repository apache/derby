
-- this tests precedence of operators.


-- test precedence of operators other than and, or, and not
-- that return boolean.

-- expect 'true' row:
create table t1(c11 int);
insert into t1 values(1);

select c11 from t1 where 1 in (1,2,3) = (1=1);

--
select c11 from t1 where 'acme widgets' like 'acme%' in (1=1);

select c11 from t1 where 1 between -100 and 100 is not null;

--
select c11 from t1 where exists(select * from (values 1) as t) not in (1=2);
