
-- negative tests for group by and having clauses

create table t1 (a int, b int, c int);
create table t2 (a int, b int, c int);
insert into t2 values (1,1,1), (2,2,2);

-- group by position
select * from t1 group by 1;

-- column in group by list not in from list
select a as d from t1 group by d;

-- column in group by list not in select list
select a as b from t1 group by b;
select a from t1 group by b;
select a, char(b) from t1 group by a;

-- columns in group by list must be unique
select a, b from t1 group by a, a;
select a, b from t1 group by a, t1.a;

-- cursor with group by is not updatable
get cursor c1 as 'select a from t1 group by a for update';

-- noncorrelated subquery that returns too many rows
select a, (select a from t2) from t1 group by a;

-- correlation on outer table
select t2.a, (select b from t1 where t1.b = t2.b) from t1 t2 group by t2.a;

-- having clause

-- cannot contain column references which are not grouping columns
select a from t1 group by a having c = 1;
select a from t1 o group by a having a = (select a from t1 where b = b.o);

-- ?s in group by
select a from t1 group by ?;

-- group by on long varchar type
create table unmapped(c1 long varchar);
select c1, max(1) from unmapped group by c1;

-- clean up
drop table t1;
drop table t2;
drop table unmapped;
-- Test group by and having clauses with no aggregates

-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(2) for bit data, lbv long varchar for bit data);
create table tab1 (
				i integer, 
				s smallint, 
				l bigint,
				c char(30),
				v varchar(30),
				lvc long varchar,
				d double precision,
				r real,
				dt date, 
				t time, 
				ts timestamp);

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (1, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 200, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 2000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'goodbye', 'everyone is here', 'adios, muchachos',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'noone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0f0f', X'ABCD');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0f0f', X'1234');
insert into t values (0, 100, 1000000,
					  'hello', 'everyone is here', 'what the heck do we care?',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'ffff', X'ABCD');
-- bit maps to Byte[], so can't test for now
insert into tab1
select i, s, l, c, v, lvc, d, r, dt, t, ts from t;

-- simple grouping
select i from t group by i order by i;
select s from t group by s order by s;
select l from t group by l order by l;
select c from t group by c order by c;
select v from t group by v order by v;
select d from t group by d order by d;
select r from t group by r order by r;
select dt from t group by dt order by dt;
select t from t group by t order by t;
select ts from t group by ts order by ts;
select b from t group by b order by b;
select bv from t group by bv order by bv;
-- grouping by long varchar [for bit data] cols should fail in db2 mode
select lbv from t group by lbv order by lbv;

-- multicolumn grouping
select i, dt, b from t where 1=1 group by i, dt, b order by i,dt,b;
select i, dt, b from t group by i, dt, b order by i,dt,b;
select i, dt, b from t group by b, i, dt order by i,dt,b;
select i, dt, b from t group by dt, i, b order by i,dt,b;

-- group by expression
select expr1, expr2
from (select i * s, c || v from t) t (expr1, expr2) group by expr2, expr1 order by expr2,expr1;

-- group by correlated subquery
select i, expr1
from (select i, (select distinct i from t m where m.i = t.i) from t) t (i, expr1)
 group by i, expr1 order by i,expr1;

-- distinct and group by
select distinct i, dt, b from t group by i, dt, b order by i,dt,b;

-- order by and group by
-- same order
select i, dt, b from t group by i, dt, b order by i, dt, b;
-- subset in same order
select i, dt, b from t group by i, dt, b order by i, dt;
-- different order
select i, dt, b from t group by i, dt, b order by b, dt, i;
-- subset in different order
select i, dt, b from t group by i, dt, b order by b, dt;

-- group by without having in from subquery
select * from
(select i, dt from t group by i, dt) t (t_i, t_dt),
(select i, dt from t group by i, dt) m (m_i, m_dt)
where t_i = m_i and t_dt = m_dt order by t_i,t_dt,m_i,m_dt;

select * from
(select i, dt from t group by i, dt) t (t_i, t_dt),
(select i, dt from t group by i, dt) m (m_i, m_dt)
group by t_i, t_dt, m_i, m_dt order by t_i,t_dt,m_i,m_dt;

select * from
(select i, dt from t group by i, dt) t (t_i, t_dt),
(select i, dt from t group by i, dt) m (m_i, m_dt)
where t_i = m_i and t_dt = m_dt
group by t_i, t_dt, m_i, m_dt order by t_i,t_dt,m_i,m_dt;

select t.*, m.* from
(select i, dt from t group by i, dt) t (t_i, t_dt),
(select i, dt from t group by i, dt) m (t_i, t_dt)
where t.t_i = m.t_i and t.t_dt = m.t_dt
group by t.t_i, t.t_dt, m.t_i, m.t_dt order by t.t_i,t.t_dt,m.t_i,m.t_dt;

select t.t_i, t.t_dt, m.* from
(select i, dt from t group by i, dt) t (t_i, t_dt),
(select i, dt from t group by i, dt) m (t_i, t_dt)
where t.t_i = m.t_i and t.t_dt = m.t_dt
group by t.t_i, t.t_dt, m.t_i, m.t_dt order by t.t_i,t.t_dt,m.t_i,m.t_dt;


-- additional columns in group by list not in select list
select i, dt, b from t group by i, dt, b order by i,dt,b;
select t.i from t group by i, dt, b order by i;
select t.dt from t group by i, dt, b order by dt;
select t.b from t group by i, dt, b order by b;

select t.t_i, m.t_i from
(select i, dt from t group by i, dt) t (t_i, t_dt),
(select i, dt from t group by i, dt) m (t_i, t_dt)
where t.t_i = m.t_i and t.t_dt = m.t_dt
group by t.t_i, t.t_dt, m.t_i, m.t_dt order by t.t_i,m.t_i;

-- having

-- parameters in having clause
prepare p1 as 'select i, dt, b from t group by i, dt, b having i = ? order by i,dt,b';
execute p1 using 'values 0';
remove p1;

-- group by with having in from subquery
select * from
(select i, dt from t group by i, dt having 1=1) t (t_i, t_dt),
(select i, dt from t group by i, dt having i = 0) m (m_i, m_dt)
where t_i = m_i and t_dt = m_dt order by t_i,t_dt,m_i,m_dt;

select * from
(select i, dt from t group by i, dt having 1=1) t (t_i, t_dt),
(select i, dt from t group by i, dt having i = 0) m (m_i, m_dt)
group by t_i, t_dt, m_i, m_dt order by t_i,t_dt,m_i,m_dt;

select * from
(select i, dt from t group by i, dt having 1=1) t (t_i, t_dt),
(select i, dt from t group by i, dt having i = 0) m (m_i, m_dt)
where t_i = m_i and t_dt = m_dt
group by t_i, t_dt, m_i, m_dt
having t_i * m_i = m_i * t_i order by t_i,t_dt,m_i,m_dt;

-- correlated subquery in having clause
select i, dt from t
group by i, dt
having i = (select distinct i from tab1 where t.i = tab1.i) order by i,dt;
select i, dt from t
group by i, dt
having i = (select i from t m group by i having t.i = m.i) order by i,dt;
-- column references in having clause match columns in group by list
select i as outer_i, dt from t
group by i, dt
having i = (select i from t m group by i having t.i = m.i) order by outer_i,dt;

-- additional columns in group by list not in select list
select i, dt from t group by i, dt order by i,dt;
select t.dt from t group by i, dt having i = 0 order by t.dt;
select t.dt from t group by i, dt having i <> 0 order by t.dt;
select t.dt from t group by i, dt having i != 0 order by t.dt;

-- drop tables
drop table t;
drop table tab1;
-- negative tests for selects with a having clause without a group by

-- create a table
create table t1(c1 int, c2 int);

-- binding of having clause
select 1 from t1 having 1;

-- column references in having clause not allowed if no group by
select * from t1 having c1 = 1;
select 1 from t1 having c1 = 1;

-- correlated subquery in having clause
select * from t1 t1_outer 
having 1 = (select 1 from t1 where c1 = t1_outer.c1);

-- drop the table
drop table t1;


-- bug 5653
-- test (almost useful) restrictions on a having clause without a group by clause

-- create the table
create table t1 (c1 float);

-- populate the table
insert into t1 values 0.0, 90.0;

-- this is the only query that should not fail
-- filter out all rows
select 1 from t1 having 1=0;

-- all 6 queries below should fail after bug 5653 is fixed
-- select * 
select * from t1 having 1=1;

-- select column
select c1 from t1 having 1=1;

-- select with a built-in function sqrt
select sqrt(c1) from t1 having 1=1;

-- non-correlated subquery in having clause
select * from t1 having 1 = (select 1 from t1 where c1 = 0.0);

-- expression in select list
select (c1 * c1) / c1 from t1 where c1 <> 0 having 1=1;

-- between
select * from t1 having 1 between 1 and 2;

-- drop the table
drop table t1;

-- bug 5920
-- test that HAVING without GROUPBY makes one group
create table t(c int, d int);
insert into t(c,d) values (1,10),(2,20),(2,20),(3,30),(3,30),(3,30);
select avg(c) from t having 1 < 2;
-- used to give several rows, now gives only one
select 10 from t having 1 < 2;
-- ok, gives one row
select 10,avg(c) from t having 1 < 2;
drop table t;
