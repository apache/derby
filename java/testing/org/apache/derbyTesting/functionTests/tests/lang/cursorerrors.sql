-- What happens when language exceptions are thrown on a next?
-- NOTE: this test is dependent on no optimization, i.e., always getting
-- the same access methods (table scans and join order matching the from list)

-- create the tables
create table t1 (c1 int);
create table t2 (c1 int);

-- populate the tables
insert into t1 values 1, 0, 2;
insert into t2 values 1, 0, 2;

autocommit off;

-- What happens on a fetch after a divide by 0 error?

-- error in select list
-- single table query
get cursor c1 as 'select c1, c1/c1 from t1';
next c1;
-- divide by 0
next c1;
-- Verify that cursor closed on error
next c1;
close c1;

-- join #1
get cursor c2 as 
	'select a.c1, b.c1, a.c1/a.c1 from t1 a, t1 b where a.c1 = b.c1';
next c2;
-- divide by 0
next c2;
-- Verify that cursor closed on error
next c2;
close c2;

-- join #2
get cursor c3 as 
	'select a.c1, b.c1, b.c1/a.c1 from t1 a, t1 b';
next c3;
next c3;
next c3;
-- divide by 0
next c3;
-- Verify that cursor closed on error
next c3;
close c3;

-- union all
get cursor c4 as
	'select c1, c1/c1 from t1 union all select c1, c1/c1 from t1';
next c4;
-- divide by 0 on left side
next c4;
-- Verify that cursor closed on error
next c4;
close c4;

-- error in where clause

-- single table
get cursor c10 as 'select * from t1 where c1/c1 = 1';
-- (1)
next c10;
-- divide by 0
next c10;
-- Verify that cursor closed on error
next c10;
close c10;

-- join #1, error on open (1st row in left)
-- (cursor will not exist after error on open)
get cursor c12 as 'select * from t1 a, t1 b where a.c1 <> 1 and a.c1/a.c1 = 1';
-- next should fail, since no cursor
next c12;

-- join #2, error on 2nd row on left
get cursor c13 as 'select * from t1 a, t1 b where b.c1 = 1 and a.c1/a.c1 = 1';
-- (1, 1)
next c13;
-- divide by 0 from left
next c13;
-- Verify that cursor closed on error
next c13;
close c13;

-- join #3, error on 1st row in right
get cursor c14 as 'select * from t1 a, t1 b where b.c1 <> 1 and b.c1/b.c1 = 1';
-- divide by 0 from right
next c14;
-- Verify that cursor closed on error
next c14;
close c14;

-- join #4, error on 2nd row in right
get cursor c15 as 'select * from t1 a, t1 b where b.c1 <> 2 and b.c1/b.c1 = 1';
-- (1, 1)
next c15;
-- divide by 0 from right
next c15;
-- Verify that cursor closed on error
next c15;
close c15;

-- union all
get cursor c11 as 'select * from t1 where c1/c1 = 1 union all
				   select * from t1 where c1/c1 = 1';
-- (1) from left
next c11;
-- divide by 0 from left
next c11;
-- Verify that cursor closed on error
next c11;
close c11;

-- error in join clause
get cursor c5 as 'select * from t1, t2 where t1.c1/t2.c1 = 1';
-- (1, 1)
next c5;
-- (1, 0) -> divide by 0
next c5;
-- Verify that cursor closed on error
next c5;
close c5;

-- error in subquery

-- subquery in select list

-- single table query
get cursor c8 as 'select c1, (select c1/c1 from t2 where t1.c1 = c1) from t1';
-- (1, 1)
next c8;
-- divide by 0
next c8;
-- Verify that cursor closed on error
next c8;
close c8;

-- join
get cursor c9 as 'select a.c1, (select c1/c1 from t2 where c1 = a.c1) from t1 a, t1 b
				  where a.c1 = b.c1';
-- (1, 1)
next c9;
-- divide by 0
next c9;
-- Verify that cursor closed on error
next c9;
close c9;

-- subquery in where clause

-- single table query
get cursor c6 as 'select * from t1 
				  where c1 = (select c1/c1 from t2 where t1.c1 = c1) or c1 = 2';
-- (1)
next c6;
-- divide by 0
next c6;
-- Verify that cursor closed on error
next c6;
close c6;

-- join
get cursor c7 as 'select * from t1 a, t1 b
				  where a.c1 = b.c1 and 
						(a.c1 = (select c1/c1 from t2 where a.c1 = c1) or a.c1 = 2)';
-- (1, 1)
next c7;
-- divide by 0
next c7;
-- Verify that cursor closed on error
next c7;
close c7;

-- drop the tables
drop table t1;
drop table t2;
