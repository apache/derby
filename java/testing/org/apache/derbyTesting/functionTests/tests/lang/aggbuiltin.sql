-- Note that bug 5704 occurs throughout this test
-- Decimal results may be outside the range of valid types in Cloudscape

-- ** insert avg.sql
-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, lbv long varchar for bit data,
				dc decimal(5,2));

-- empty table
create table empty (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, lbv long varchar for bit data,
				dc decimal(5,2));

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', x'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (1, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 200, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 222.22);
insert into t values (0, 100, 2000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 222.22);
insert into t values (0, 100, 1000000,
					  'goodbye', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'noone is here', 'jimmie noone was here',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'1111111111111111', X'1234', 111.11);

--------------------------------------
-- NEGATIVE TESTS
--------------------------------------

-- cannot aggregate datatypes that don't support NumberDataValue
select avg(c) from t;
select avg(v) from t;
select avg(lvc) from t;
select avg(dt) from t;
select avg(t) from t;
select avg(ts) from t;
select avg(b) from t;
select avg(bv) from t;
select avg(lbv) from t;

select avg(c) from t group by c;
select avg(v) from t group by c;
select avg(lvc) from t group by c;
select avg(dt) from t group by c;
select avg(t) from t group by c;
select avg(ts) from t group by c;
select avg(b) from t group by c;
select avg(bv) from t group by c;
select avg(lbv) from t group by c;

-- long varchar datatypes too
create table t1 (c1 long varchar);
select avg(c1) from t1;
drop table t1;

-- constants
select avg('hello') from t;
select avg(X'11') from t;
select avg(date('1999-06-06')) from t;
select avg(time('12:30:30')) from t;
select avg(timestamp('1999-06-06 12:30:30')) from t;

---------------------------
-- NULL AGGREGATION
---------------------------

-- scalar
select avg(i) from empty;
select avg(s) from empty;
select avg(d) from empty;
select avg(l) from empty;
select avg(r) from empty;
select avg(dc) from empty;

-- variations
select avg(i), avg(s), avg(r), avg(l) from empty;
select avg(i+1) from empty;

-- vector
select avg(i) from empty group by i;
select avg(s) from empty group by s;
select avg(d) from empty group by d;
select avg(l) from empty group by l;
select avg(r) from empty group by r;
select avg(dc) from empty group by dc;


--------------------------------
-- BASIC ACCEPTANCE TESTS
--------------------------------
select avg(i) from t;
select avg(s) from t;
select avg(d) from t;
select avg(l) from t;
select avg(r) from t;
select avg(dc) from t;

select avg(i) from t group by i;
select avg(s) from t group by s;
select avg(d) from t group by d;
select avg(l) from t group by l;
select avg(r) from t group by r;
select avg(dc), sum(dc), count(dc) from t group by dc;


-- constants
select avg(1) from t;
select avg(1.1) from t;
select avg(1e1) from t;

select avg(1) from t group by i;
select avg(1.1) from t group by r;
select avg(1e1) from t group by r;

-- multicolumn grouping
select avg(i), avg(l), avg(r) from t group by i, dt, b;
select i, dt, avg(i), avg(r), avg(l), l from t group by i, dt, b, l; 

-- group by expression
select avg(expr1), avg(expr2)
from (select i * s, r * 2 from t) t (expr1, expr2) group by expr2, expr1;

-- distinct and group by
select distinct avg(i) from t group by i, dt;

-- insert select
create table tmp (x int, y smallint);
insert into tmp (x, y) select avg(i), avg(s) from t;
select * from tmp;
insert into tmp (x, y) select avg(i), avg(s) from t group by b;
select * from tmp;
drop table tmp;

-- some accuracy tests
create table tmp (x int);
insert into tmp values (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647);

values(2147483647);
select avg(x) from tmp;
select avg(-(x - 1)) from tmp;
select avg(x) from tmp group by x;
select avg(-(x - 1)) from tmp group by x;
drop table tmp;

-- now lets try some simple averages to see what
-- type of accuracy we get
create table tmp(x double precision, y int);
prepare scalar as 'select avg(x) from tmp';
prepare vector as 'select avg(x) from tmp group by y';
insert into tmp values (1,1);
execute scalar;
execute vector;
insert into tmp values (2,1);
execute scalar;
execute vector;
insert into tmp values (3,1);
execute scalar;
execute vector;
insert into tmp values (4,1);
execute scalar;
execute vector;
insert into tmp values (5,1);
execute scalar;
execute vector;
insert into tmp values (6,1);
execute scalar;
execute vector;
insert into tmp values (7,1);
execute scalar;
execute vector;
insert into tmp values (10000,1);
execute scalar;
execute vector;
remove vector;
remove scalar;
drop table tmp;

-- drop tables
drop table t;
drop table empty;
-- ** insert count.sql
-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, lbv long varchar for bit data,
				dc decimal(5,2));

-- empty table
create table empty (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, lbv long varchar for bit data,
				dc decimal(5,2));
-- bit maps to Byte[], so can't test for now

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (1, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 200, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 222.22);
insert into t values (0, 100, 2000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 222.22);
insert into t values (0, 100, 1000000,
					  'goodbye', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'noone is here', 'jimmie noone was here',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0000111100001111', X'1234', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'1111111111111111', X'1234', 111.11);

---------------------------
-- NULL AGGREGATION
---------------------------

-- scalar
select count(i) from empty;
select count(s) from empty;
select count(l) from empty;
select count(c) from empty;
select count(v) from empty;
select count(lvc) from empty;
select count(d) from empty;
select count(r) from empty;
select count(dt) from empty;
select count(t) from empty;
select count(ts) from empty;
select count(b) from empty;
select count(bv) from empty;
-- bug: should fail in db2 mode
-- after for bit data is completely implemented
select count(lbv) from empty;
select count(dc) from empty;

-- variations
select count(i), count(b), count(i), count(s) from empty;
select count(i+1) from empty;

-- vector
select count(i) from empty group by i;
select count(s) from empty group by s;
select count(l) from empty group by l;
select count(c) from empty group by c;
select count(v) from empty group by v;
select count(d) from empty group by d;
select count(r) from empty group by r;
select count(dt) from empty group by dt;
select count(t) from empty group by t;
select count(ts) from empty group by ts;
select count(b) from empty group by b;
select count(bv) from empty group by bv;
select count(lbv) from empty group by lbv;
select count(dc) from empty group by dc;


--------------------------------
-- BASIC ACCEPTANCE TESTS
--------------------------------
select count(i) from t;
select count(s) from t;
select count(l) from t;
select count(c) from t;
select count(v) from t;
select count(lvc) from t;
select count(d) from t;
select count(r) from t;
select count(dt) from t;
select count(t) from t;
select count(ts) from t;
select count(b) from t;
select count(bv) from t;
select count(lbv) from t;
select count(dc) from t;

select count(i) from t group by i;
select count(s) from t group by s;
select count(l) from t group by l;
select count(c) from t group by c;
select count(v) from t group by v;
select count(d) from t group by d;
select count(r) from t group by r;
select count(dt) from t group by dt;
select count(t) from t group by t;
select count(ts) from t group by ts;
select count(b) from t group by b;
select count(bv) from t group by bv;
select count(lbv) from t group by lbv;
select count(dc) from t group by dc;


-- constants
select count(1) from t;
select count('hello') from t;
select count(1.1) from t;
select count(1e1) from t;
select count(X'11') from t;
select count(date('1999-06-06')) from t;
select count(time('12:30:30')) from t;
select count(timestamp('1999-06-06 12:30:30')) from t;

select count(1) from t group by i;
select count('hello') from t group by c;
select count(1.1) from t group by dc;
select count(1e1) from t group by r;
select count(X'11') from t group by b;
select count(date('1999-06-06')) from t group by dt;
select count(time('12:30:30')) from t group by t;
select count(timestamp('1999-06-06 12:30:30')) from t group by ts;


-- multicolumn grouping
select count(i), count(dt), count(b) from t group by i, dt, b;
select l, dt, count(i), count(dt), count(b), i from t group by i, dt, b, l; 

-- group by expression
select count(expr1), count(expr2)
from (select i * s, c || v from t) t (expr1, expr2) group by expr2, expr1;


-- distinct and group by
select distinct count(i) from t group by i, dt;


-- insert select
create table tmp (x int, y smallint);
insert into tmp (x, y) select count(i), count(c) from t;
select * from tmp;
insert into tmp (x, y) select count(i), count(c) from t group by b;
select * from tmp;
drop table tmp;

-- drop tables
drop table t;
drop table empty;
-- ** insert countStar.sql
-- Test the COUNT() aggregate

-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, lbv long varchar for bit data);

-- empty table
create table empty (i int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, lbv long varchar for bit data);

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (1, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 200, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 2000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'goodbye', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'noone is here', 'jimmie noone was here',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0000111100001111', X'ABCD');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0000111100001111', X'1234');
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'1111111111111111', X'ABCD');

---------------------------
-- NULL AGGREGATION
---------------------------

-- scalar
select count(*) from empty;

-- variations
select count(*), count(*) from empty;

-- vector
select count(*) from empty group by i;

--------------------------------
-- BASIC ACCEPTANCE TESTS
--------------------------------
select count(*) from t;

select count(*) from t group by i;


-- multicolumn grouping
select count(*), count(*), count(*) from t group by i, dt, b;

-- group by expression
select count(*), count(*)
from (select i * s, c || v from t) t (expr1, expr2) group by expr2, expr1;

-- distinct and group by
select distinct count(*) from t group by i, dt;

-- view
create view v1 as select * from t;
select count(*) from v1;
select count(*)+count(*) from v1;
drop view v1;

-- insert select 
create table tmp (x int, y smallint);
insert into tmp (x, y) select count(*), count(*) from t;
select * from tmp;
insert into tmp (x, y) select count(*), count(*) from t group by b;
select * from tmp;
drop table tmp;

-- drop tables
drop table t;
drop table empty;
-- ** insert sum.sql
--BUGS: sum() on decimal may overflow the decimal,
--w/o the type system knowing.  so, given dec(1,0),
--result might be dec(2,0), but return length passed
--to connectivity is 1 which is wrong.  if we allow
--the decimal to grow beyond the preset type, we need
--to all the type system to get it.  alternatively, 
--need to cast/normalize/setWidth() the result to ensure 
--right type.

-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), 
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, dc decimal(5,2));

-- empty table
create table empty (i int, s smallint, l bigint,
				c char(10), v varchar(50), 
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, dc decimal(5,2));
-- bit maps to Byte[], so can't test for now

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (1, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 200, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 222.22);
insert into t values (0, 100, 2000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 222.22);
insert into t values (0, 100, 1000000,
					  'goodbye', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'noone is here', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'1111111111111111', 111.11);
-- bit maps to Byte[], so can't test for now

--------------------------------------
-- NEGATIVE TESTS
--------------------------------------

-- cannot aggregate datatypes that don't support NumberDataValue
select sum(c) from t;
select sum(v) from t;
select sum(dt) from t;
select sum(t) from t;
select sum(ts) from t;
select sum(b) from t;
select sum(bv) from t;

select sum(c) from t group by c;
select sum(v) from t group by c;
select sum(dt) from t group by c;
select sum(t) from t group by c;
select sum(ts) from t group by c;
select sum(b) from t group by c;
select sum(bv) from t group by c;

-- long varchar datatypes too
create table t1 (c1 long varchar);
select sum(c1) from t1;
drop table t1;

-- constants
select sum('hello') from t;
select sum(X'11') from t;
select sum(date('1999-06-06')) from t;
select sum(time('12:30:30')) from t;
select sum(timestamp('1999-06-06 12:30:30')) from t;

---------------------------
-- NULL AGGREGATION
---------------------------

-- scalar
select sum(i) from empty;
select sum(s) from empty;
select sum(d) from empty;
select sum(l) from empty;
select sum(r) from empty;
select sum(dc) from empty;

-- variations
select sum(i), sum(s), sum(r), sum(l) from empty;
select sum(i+1) from empty;

-- vector
select sum(i) from empty group by i;
select sum(s) from empty group by s;
select sum(d) from empty group by d;
select sum(l) from empty group by l;
select sum(r) from empty group by r;
select sum(dc) from empty group by dc;


--------------------------------
-- BASIC ACCEPTANCE TESTS
--------------------------------
select sum(i) from t;
select sum(s) from t;
select sum(d) from t;
select sum(l) from t;
select sum(r) from t;
select sum(dc) from t;

select sum(i) from t group by i;
select sum(s) from t group by s;
select sum(d) from t group by d;
select sum(l) from t group by l;
select sum(r) from t group by r;
select sum(dc) from t group by dc;

-- constants
select sum(1) from t;
select sum(1.1) from t;
select sum(1e1) from t;

select sum(1) from t group by i;
select sum(1.1) from t group by r;
select sum(1e1) from t group by r;

-- multicolumn grouping
select sum(i), sum(l), sum(r) from t group by i, dt, b;
select i, dt, sum(i), sum(r), sum(l), l from t group by i, dt, b, l; 

-- group by expression
select sum(expr1), sum(expr2)
from (select i * s, r * 2 from t) t (expr1, expr2) group by expr2, expr1;

-- distinct and group by
select distinct sum(i) from t group by i, dt;

-- insert select
create table tmp (x int, y smallint);
insert into tmp (x, y) select sum(i), sum(s) from t;
select * from tmp;
insert into tmp (x, y) select sum(i), sum(s) from t group by b;
select * from tmp;
drop table tmp;

-- overflow
create table tmp (x int);
insert into tmp values (2147483647),
                     (2147483647);
select sum(x) from tmp;
drop table tmp;

create table tmp (x double precision);
insert into tmp values (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647),
                     (2147483647);
select sum(x) from tmp;
drop table tmp;

-- drop tables
drop table t;
drop table empty;
-- ** insert max.sql
-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), 
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, dc decimal(5,2));

-- empty table
create table empty (i int, s smallint, l bigint,
				c char(10), v varchar(50), 
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, dc decimal(5,2));
-- bit maps to Byte[], so can't test for now

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (1, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 200, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 222.22);
insert into t values (0, 100, 2000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 222.22);
insert into t values (0, 100, 1000000,
					  'goodbye', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'noone is here', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'1111111111111111', 111.11);

--------------------------------------
-- NEGATIVE TESTS
--------------------------------------

-- long varchar datatypes too
create table t1 (c1 long varchar);
select max(c1) from t1;
drop table t1;

---------------------------
-- NULL AGGREGATION
---------------------------

-- scalar
select max(i) from empty;
select max(s) from empty;
select max(l) from empty;
select max(c) from empty;
select max(v) from empty;
select max(d) from empty;
select max(r) from empty;
select max(dt) from empty;
select max(t) from empty;
select max(ts) from empty;
select max(b) from empty;
select max(bv) from empty;
select max(dc) from empty;

-- variations
select max(i), max(b), max(i), max(s) from empty;
select max(i+1) from empty;

-- vector
select max(i) from empty group by i;
select max(s) from empty group by s;
select max(l) from empty group by l;
select max(c) from empty group by c;
select max(v) from empty group by v;
select max(d) from empty group by d;
select max(r) from empty group by r;
select max(dt) from empty group by dt;
select max(t) from empty group by t;
select max(ts) from empty group by ts;
select max(b) from empty group by b;
select max(bv) from empty group by bv;
select max(dc) from empty group by dc;


--------------------------------
-- BASIC ACCEPTANCE TESTS
--------------------------------
select max(i) from t;
select max(s) from t;
select max(l) from t;
select max(c) from t;
select max(v) from t;
select max(d) from t;
select max(r) from t;
select max(dt) from t;
select max(t) from t;
select max(ts) from t;
select max(b) from t;
select max(bv) from t;
select max(dc) from t;

select max(i) from t group by i;
select max(s) from t group by s;
select max(l) from t group by l;
select max(c) from t group by c;
select max(v) from t group by v;
select max(d) from t group by d;
select max(r) from t group by r;
select max(dt) from t group by dt;
select max(t) from t group by t;
select max(ts) from t group by ts;
select max(b) from t group by b;
select max(bv) from t group by bv;
select max(dc) from t group by dc;

-- constants
select max(1) from t;
select max('hello') from t;
select max(1.1) from t;
select max(1e1) from t;
select max(X'11') from t;
select max(date('1999-06-06')) from t;
select max(time('12:30:30')) from t;
select max(timestamp('1999-06-06 12:30:30')) from t;

select max(1) from t group by i;
select max('hello') from t group by c;
select max(1.1) from t group by dc;
select max(1e1) from t group by d;
select max(X'11') from t group by b;
select max(date('1999-06-06')) from t group by dt;
select max(time('12:30:30')) from t group by t;
select max(timestamp('1999-06-06 12:30:30')) from t group by ts;

-- multicolumn grouping
select max(i), max(dt), max(b) from t group by i, dt, b;
select l, dt, max(i), max(dt), max(b), i from t group by i, dt, b, l; 

-- group by expression
select max(expr1), max(expr2)
from (select i * s, c || v from t) t (expr1, expr2) group by expr2, expr1;

-- distinct and group by
select distinct max(i) from t group by i, dt;

-- insert select
create table tmp (x int, y char(20));
insert into tmp (x, y) select max(i), max(c) from t;
select * from tmp;
insert into tmp (x, y) select max(i), max(c) from t group by b;
select * from tmp;
drop table tmp;

-- drop tables
drop table t;
drop table empty;
-- ** insert min.sql
-- create an all types tables
create table t (i int, s smallint, l bigint,
				c char(10), v varchar(50), 
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, dc decimal(5,2));

-- empty table
create table empty (i int, s smallint, l bigint,
				c char(10), v varchar(50), 
				d double precision, r real, 
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(8) for bit data, dc decimal(5,2));

-- populate tables
insert into t (i) values (null);
insert into t (i) values (null);

insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (1, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 200, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 222.22);
insert into t values (0, 100, 2000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 222.22);
insert into t values (0, 100, 1000000,
					  'goodbye', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'noone is here', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  100.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 100.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-09-09'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:55:55'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:55:55'),
					  X'12af', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'ffff', X'0000111100001111', 111.11);
insert into t values (0, 100, 1000000,
					  'duplicate', 'this is duplicated', 
					  200.0e0, 200.0e0, 
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'1111111111111111', 111.11);

--------------------------------------
-- NEGATIVE TESTS
--------------------------------------

-- long varchar datatypes too
create table t1 (c1 long varchar);
select min(c1) from t1;
drop table t1;

---------------------------
-- NULL AGGREGATION
---------------------------

-- scalar
select min(i) from empty;
select min(s) from empty;
select min(l) from empty;
select min(c) from empty;
select min(v) from empty;
select min(d) from empty;
select min(r) from empty;
select min(dt) from empty;
select min(t) from empty;
select min(ts) from empty;
select min(b) from empty;
select min(bv) from empty;
select min(dc) from empty;

-- variations
select min(i), min(b), min(i), min(s) from empty;
select min(i+1) from empty;

-- vector
select min(i) from empty group by i;
select min(s) from empty group by s;
select min(l) from empty group by l;
select min(c) from empty group by c;
select min(v) from empty group by v;
select min(d) from empty group by d;
select min(r) from empty group by r;
select min(dt) from empty group by dt;
select min(t) from empty group by t;
select min(ts) from empty group by ts;
select min(b) from empty group by b;
select min(bv) from empty group by bv;
select min(dc) from empty group by dc;


--------------------------------
-- BASIC ACCEPTANCE TESTS
--------------------------------
select min(i) from t;
select min(s) from t;
select min(l) from t;
select min(c) from t;
select min(v) from t;
select min(d) from t;
select min(r) from t;
select min(dt) from t;
select min(t) from t;
select min(ts) from t;
select min(b) from t;
select min(bv) from t;
select min(dc) from t;

select min(i) from t group by i;
select min(s) from t group by s;
select min(l) from t group by l;
select min(c) from t group by c;
select min(v) from t group by v;
select min(d) from t group by d;
select min(r) from t group by r;
select min(dt) from t group by dt;
select min(t) from t group by t;
select min(ts) from t group by ts;
select min(b) from t group by b;
select min(bv) from t group by bv;
select min(dc) from t group by dc;

-- constants
select min(1) from t;
select min('hello') from t;
select min(1.1) from t;
select min(1e1) from t;
select min(X'11') from t;
select min(date('1999-06-06')) from t;
select min(time('12:30:30')) from t;
select min(timestamp('1999-06-06 12:30:30')) from t;

select min(1) from t group by i;
select min('hello') from t group by c;
select min(1.1) from t group by dc;
select min(1e1) from t group by d;
select min(X'11') from t group by b;
select min(date('1999-06-06')) from t group by dt;
select min(time('12:30:30')) from t group by t;
select min(timestamp('1999-06-06 12:30:30')) from t group by ts;

-- multicolumn grouping
select min(i), min(dt), min(b) from t group by i, dt, b;
select l, dt, min(i), min(dt), min(b), i from t group by i, dt, b, l; 

-- group by expression
select min(expr1), min(expr2)
from (select i * s, c || v from t) t (expr1, expr2) group by expr2, expr1;

-- distinct and group by
select distinct min(i) from t group by i, dt;

-- insert select
create table tmp (x int, y char(20));
insert into tmp (x, y) select min(i), min(c) from t;
select * from tmp;
insert into tmp (x, y) select min(i), min(c) from t group by b;
select * from tmp;
drop table tmp;

-- drop tables
drop table t;
drop table empty;
