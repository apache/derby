-- ** insert decimal.sql
--
-- Test DECIMAL and NUMERIC.  Note that we
-- know that DECIMAL and NUMERIC are pretty much the
-- same thing, so we don't do much testing with
-- the two types other than to make sure the 
-- syntax is the same.


-- test some of the meta data
drop table tmp;
create table tmp (tmpcoldecimal dec(8,4), tmpcolnumeric numeric);
select columndatatype 
	from sys.syscolumns 
	where columnname like 'TMPCOL%';
drop table tmp;

-- Negative tests, bad precision/scale
create table bad (d decimal(11,12));
create table bad (d decimal(0,0));
create table bad (d decimal(0));
create table bade(d decimal(32));
create table bade(d decimal(31,32));
create table bade(d decimal(32,32));

-- Simple acceptance test
values cast (1 as dec);
values cast (1 as decimal);
values cast (1 as decimal(5));
values cast (1 as dec(5));
values cast (1.1 as dec(5,3));
values cast (1.1 as numeric(5,3));

-- cast to all valid types
values cast (1.1 as int);
values cast (1.1 as bigint);
values cast (1.1 as smallint);
values cast (1.1 as real);
values cast (1.1 as float);
values cast (1.1 as char(10));

-- cast all valid types to dec
values cast ((cast (1 as int)) as dec);
values cast ((cast (1 as bigint)) as dec);
values cast ((cast (1 as smallint)) as dec);
values cast ((cast (1 as real)) as dec);
values cast ((cast (1 as float)) as dec);
values cast ((cast (1 as char(10))) as dec);

-- cast overflow,
-- make a number bigger than everything but 
-- decimal, and then try to cast it
drop table tmp;
create table tmp(d decimal(31 ,0));
insert into tmp values (cast (
'100000000000000000000000000000' as dec(31,0)));
update tmp set d = d * d;
select cast(d as int) from tmp;
select cast(d as smallint) from tmp;
select cast(d as bigint) from tmp;
select cast(d as float) from tmp;
select cast(d as real) from tmp;
select cast(d as double precision) from tmp;
-- test alternative syntax
select cast(d as double) from tmp;
insert into tmp values (+1.79769E+308);
select * from tmp;
drop table tmp;

-- try inserting various types into decimal.
-- we expect silent truncation of the fraction
drop table tmp;
create table tmp (d decimal(5,2));
insert into tmp values (100);
insert into tmp values (cast (100 as smallint));
insert into tmp values (cast (100 as bigint));
insert into tmp values (cast (100 as real));
insert into tmp values (cast (100 as double precision));
insert into tmp values (cast (100.999 as real));
insert into tmp values (100.999e0);
insert into tmp values (100.999);
--too big
insert into tmp values (1000);
insert into tmp values (cast (1000 as smallint));
insert into tmp values (cast (1000 as bigint));
insert into tmp values (cast (1000 as real));
insert into tmp values (cast (1000 as double precision));
insert into tmp values (cast (1000.999 as real));
insert into tmp values (1000.999e0);
insert into tmp values (1000.999);

--try a few values that hit borders in how java.lang.Double work
--(this is really tied to some details in the internals of
-- SQLDecimal)
insert into tmp values (1000);
insert into tmp values (10000);
insert into tmp values (100000);
insert into tmp values (1000000);
insert into tmp values (10000000); 
insert into tmp values (100000000);
insert into tmp values (1000000000);
insert into tmp values (10000000000);
insert into tmp values (100000000000);
insert into tmp values (1000000000000);
insert into tmp values (10000000000000);
insert into tmp values (100000000000000);
insert into tmp values (-1000);
insert into tmp values (-10000);
insert into tmp values (-100000);
insert into tmp values (-1000000);
insert into tmp values (-10000000); 
insert into tmp values (-100000000);
insert into tmp values (-1000000000);
insert into tmp values (-10000000000);
insert into tmp values (-100000000000);
insert into tmp values (-1000000000000);
insert into tmp values (-10000000000000);
insert into tmp values (-100000000000000);
drop table tmp;

create table tmp(d dec(1,1));
insert into tmp values (0.0);
insert into tmp values (-0.0);
insert into tmp values (0.1);
insert into tmp values (-0.1);
insert into tmp values (0.1e0);
insert into tmp values (-0.1e0);
select * from tmp;
delete from tmp;

insert into tmp values (0);
insert into tmp values (0.0e0);
insert into tmp values (0.0e10);
insert into tmp values (-0);
insert into tmp values (-0.0e0);
insert into tmp values (-0.0e10);
insert into tmp values (cast (0 as smallint));
insert into tmp values (cast (0 as bigint));
insert into tmp values (cast (0 as real));
select * from tmp;
drop table tmp;

create table tmp(d dec(1,0));
insert into tmp values (1.0);
insert into tmp values (1);
insert into tmp values (1.0e0);
insert into tmp values (-1.0);
insert into tmp values (-1);
insert into tmp values (-1.0e0);
insert into tmp values (cast (1 as smallint));
insert into tmp values (cast (1 as bigint));
insert into tmp values (cast (1 as real));
select * from tmp;
drop table tmp;

-- Using the DOUBLE built-in function
-- test that double maps to the double data type
-- all of the following should work if DOUBLE appears in the COLUMNDATATYPE column
create table tmp (x double);
insert into tmp values (1);
select columnname, columndatatype 
       from sys.syscolumns c, sys.systables t 
       where c.referenceid = t .tableid and t.tablename='TMP';
drop table tmp;

-- cast dec as as a numeric type in a select list
create table tmp (d decimal);
insert into tmp values (1.1);
--should all pass
insert into tmp values (1);
select cast(d as int) from tmp;
select cast(d as smallint) from tmp;
select cast(d as bigint) from tmp;
select cast(d as float) from tmp;
select cast(d as real) from tmp;
select cast(d as double precision) from tmp;
select cast(d as dec(10,2)) from tmp;
select cast(d as dec(10,8)) from tmp;
drop table tmp;


drop table t;
create table t (i int, 
				l bigint,
				s smallint, 
				d double precision,
				r real,
				dc decimal(10,2));

insert into t values (null, null, null, null, null, null);

insert into t values (10,		-- int
						10,		-- bigint
						10,		-- smallint	
						10,		-- double
						10,		-- real
						10		-- decimal(10,2)		
						 );
insert into t values (-10,		-- int
						-10,	-- bigint
						-10,	-- smallint	
						-10,	-- double
						-10,	-- real
						-10		-- decimal(10,2)		
						 );


insert into t values (0,		-- int
						0,		-- bigint
						0,		-- smallint	
						0,		-- double
						0,		-- real
						0		-- decimal(10,2)		
						 );

select dc from t;

select dc + i, dc + s, dc + r, dc + dc from t;
select dc - i, dc - s, dc - r, dc - dc from t;
select dc * i, dc * s, dc * r, dc * dc from t;
select dc / i, dc / s, dc / r, dc / dc from t;

-- try unary minus, plus
select -(dc * 100 / 100e0 ), +(dc * 100e0 / 100 ) from t;

-- test null/null, constant/null, null/constant
select dc, i / dc, 10 / dc, dc / 10e0 from t;

-- test for divide by 0
select dc / i from t;

select 20e0 / 5e0 / 4e0, 20e0 / 4e0 / 5 from t;

-- test positive/negative, negative/positive and negative/negative
select dc, dc / -dc, (-dc) / dc, (-dc) / -dc from t;

-- test some "more complex" expressions
select dc, dc + 10e0, dc - (10 - 20e0), dc - 10, dc - (20 - 10) from t;

-- make sure we get the right scale/precision during arithmetic
values (9.0 + 9.0);
values (9.9 + 9.9);

values (-9.0 - 9.0);
values (-9.9 - 9.9);

values (9.0 * 9.0);
values (9.9 * 9.9);
values (0.9 * 0.9);
values (0.9999 * 0.9);
values (0.9 * 0.9999);
values (0.9999 * 0.9999);

values (1.0 / 3.0);
values (1.0 / 0.3);
values (1.0 / 0.03);
values (1.0 / 0.000003);
values (10000.0 / 0.000003);
values (0.0001 / 0.0003);
values (0.1 / 3.0);

-- huge number
values (
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)) *
		cast(1.7e3 as dec(31)));

values cast(1.7e30 as dec(31));

--try a tiny number 
-- the following seems to be asking a bit
-- too much of poor old biginteger, so try
-- something smaller
--values (cast(1.7e-307 as dec(2147483647,2147483640)) /
--		(cast(1.7e308 as dec(2147483647)) *
--		 cast(1.7e308 as dec(2147483647)) *
--		 cast(1.7e308 as dec(2147483647)) *
--		 cast(1.7e308 as dec(2147483647)) *
--		 cast(1.7e308 as dec(2147483647)) *
--		 cast(1.7e308 as dec(2147483647)) *
--		 cast(1.7e308 as dec(2147483647))));
--

values cast(1 as dec(31, 20));
-- test the arithmetic operators on a type we know they don't work on
create table w (x dec, y long varchar);
select x + y from w;
select x - y from w;
select x * y from w;
select x / y from w;

-- clean up after ourselves
drop table w;

--
-- comparisons
--

insert into t values (123,			-- int
						123,		-- bigint
						123,		-- smallint	
						1234.56,	-- double
						1234.56,	-- real
						1234.56		-- decimal(10,2)		
						 );

-- test =
select dc from t where dc is null;
select dc from t where dc = 10;
select dc from t where dc = -10;
select dc from t where dc = 0;
select dc from t where dc = 1234.45;

select dc from t where dc = i;
select dc from t where dc = l;
select dc from t where dc = s;
select dc from t where dc = r;
select dc from t where dc = d;
select dc from t where dc = dc;


-- test >
select dc from t where dc > 10;
select dc from t where dc > -10;
select dc from t where dc > 0;
select dc from t where dc > 1234.45;

select dc from t where dc > i;
select dc from t where dc > l;
select dc from t where dc > s;
select dc from t where dc > r;
select dc from t where dc > d;
select dc from t where dc > dc;


-- test >=
select dc from t where dc >= 10;
select dc from t where dc >= -10;
select dc from t where dc >= 0;
select dc from t where dc >= 1234.45;

select dc from t where dc >= i;
select dc from t where dc >= l;
select dc from t where dc >= s;
select dc from t where dc >= r;
select dc from t where dc >= d;
select dc from t where dc >= dc;



-- test <
select dc from t where dc < 10;
select dc from t where dc < -10;
select dc from t where dc < 0;
select dc from t where dc < 1234.45;

select dc from t where dc < i;
select dc from t where dc < l;
select dc from t where dc < s;
select dc from t where dc < r;
select dc from t where dc < d;
select dc from t where dc < dc;


-- test <=
select dc from t where dc <= 10;
select dc from t where dc <= -10;
select dc from t where dc <= 0;
select dc from t where dc <= 1234.45;

select dc from t where dc <= i;
select dc from t where dc <= l;
select dc from t where dc <= s;
select dc from t where dc <= r;
select dc from t where dc <= d;
select dc from t where dc <= dc;



-- test <>
select dc from t where dc <> 10;
select dc from t where dc <> -10;
select dc from t where dc <> 0;
select dc from t where dc <> 1234.45;

select dc from t where dc <> i;
select dc from t where dc <> l;
select dc from t where dc <> s;
select dc from t where dc <> r;
select dc from t where dc <> d;
select dc from t where dc <> dc;


--
-- test a variety of inserts and updates
--

drop table t2;
create table t2 (i int, 
				l bigint,
				s smallint, 
				d double precision,
				r real,
				dc decimal(10,2));

insert into t2 select * from t;

-- add a few indexes
create index dcindex on t2(dc);
create unique index dcuniqueindex on t2(dc);

-- now do updates and confirm they are ok
update t2 set dc = dc + 1.1;
select dc from t2;
update t2 set dc = dc - 1.1;
select dc from t2;
update t2 set dc = dc / 1.1;
select dc from t2;
update t2 set dc = dc * 1.1;
select dc from t2;

-- try some deletes
delete from t2 where dc > 0;
select dc from t2;
delete from t2 where dc = 0;
select dc from t2;
delete from t2 where dc < 0;
select dc from t2;

drop table t2;
drop table t;

-- test that we recycle values correctly
-- when reading from a decimal table with
-- variable length byte arrays stored
-- via write external
create table t (c1 char(1), d dec(20,4), c2 char(1));
create unique index tu on t(d);
insert into t values ('a', 1.123, 'Z');
insert into t values ('a', 11111.123, 'Z');
insert into t values ('a', 11111111.123, 'Z');
insert into t values ('a', 6.123, 'Z');
insert into t values ('a', 666.123, 'Z');
insert into t values ('a', .6, 'Z');
insert into t values ('a', 0, 'Z');
insert into t values ('a', 666666.123, 'Z');
insert into t values ('a', 99999999999999.123, 'Z');
insert into t values ('a', 9.123, 'Z');

select * from t;

update t set d = d + .0007;
select * from t;
drop table tmp;
drop table bad;
drop table t;

-- ** insert double.sql

--
-- Test the builtin type 'double precision'
-- assumes these builtin types exist:
--	int, smallint, char, varchar
--
-- other things we might test:
-- show how doubles lose precision on computations

--
-- Test the arithmetic operators
--

create table t (i int, s smallint, c char(10), v varchar(50),
	d double precision);

insert into t values (null, null, null, null, null);
insert into t values (0, 100, 'hello', 'everyone is here', 200.0e0);
insert into t values (-1, -100, 'goodbye', 'everyone is there', -200.0e0);

select d + d, i + d, s + d from t;

select d + d + d, d + 100 + 432e0 from t;

select d - i, i - d, d - s, s - d from t;

select d - d - d, d - 100 - 432e0 from t;

select i, d, i * d, d * i, d * d, d * 2, d * 2.0e0 from t;

-- try unary minus, plus
select -(d * 100 / 100e0 ), +(d * 100e0 / 100 ) from t;

-- test null/null, constant/null, null/constant
select i, d, i / d, 10 / d, d / 10e0 from t;

-- test for divide by 0
select d / i from t;

select 20e0 / 5e0 / 4e0, 20e0 / 4e0 / 5 from t;

-- test positive/negative, negative/positive and negative/negative
select d, d / -d, (-d) / d, (-d) / -d from t;

-- test some "more complex" expressions
select d, d + 10e0, d - (10 - 20e0), d - 10, d - (20 - 10) from t;

-- show that decimals will go into doubles:
select d+1.1 from t;
insert into t (d) values(1.1);
select d from t where d=1.1;

drop table t;

-- test overflow

create table s (d double precision, p double);

insert into s values (null, null);
insert into s values (0, 100);
insert into s values (1, 101);

select d + 1.7e+308 from s;

-- these are close enough to the infinities to overflow
-- the null row will still get returned
select 1.798e+308, - 1.798e+308, 'This query should not work' from s;
select 1.8e+1000, - 1.8e+1000, 'This query should not work' from s;

-- these are far enough from the infinities to work
select 1.797e+308, - 1.797e+308, 'This query should work' from s;
select 1.6e+308, - 1.6e+308, 'This query should work' from s;

-- the null row will still get returned
select d - 1.6e+308 - 0, 'This query should work' from s;
select d - 1.6e+308 - 1.6e+308, 'This query should fail' from s;

-- these should fail
select p * 1.6e+308 from s;
select p * -1.6e+308 from s;

-- these work
insert into s values (-1.6e+308, 0);
insert into s values (-1.797e+308, 0);
-- these don't work
insert into s values (-1.798e+308, 0);
insert into s values (-1.8e+308, 0);

-- see two more rows
select -d from s;

drop table s;

-- test the arithmetic operators on a type we know they don't work on
create table w (x double precision, y long varchar);

select x + y from w;

select x - y from w;

select x * y from w;

select x / y from w;

-- clean up after ourselves
drop table w;

--
-- comparisons
--
create table c (i int, s smallint, d double precision, p double precision);

-- insert some values
insert into c values (0, 0, 0e0, 0e0);
insert into c values (null, null, 5e0, null);
insert into c values (1, 1, 1e0, 2e0);
insert into c values (1956475, 1956, 1956475e0, 1956475e0);


-- select each one in turn
select d from c where d = 0e0;
select d from c where d = 1e0;
select d from c where d = 1956475e0;

-- now look for a value that isn't in the table
select d from c where p = 2e0;

-- now test null = null semantics
select d from c where d = d;

-- now test <>, <, >
select d from c where d <> 0e0;
select d from c where d <> 1e0;
select d from c where d < 1956475e0;
select d from c where d < 2e0;
select d from c where d > d;
select d from c where d > p;

-- now test <=, >=
select d from c where d <= 0e0;
select d from c where d <= 1e0;
select d from c where d <= 2e0;
select d from c where d >= 1956475e0;
select d from c where d >= d;
select d from c where d >= p;

-- test comparisons with int and smallint
select d from c where d <= i;
select d from c where d < s;
select d from c where d > i;
select d from c where d >= s;
select d from c where d <> i;
select d from c where d = s;

-- test that the smallint gets promoted to double, and not vice versa.  65537
-- when converted to short becomes 1
select d from c where s = 65537e0;

-- test =SQ
-- this gets cardinality error
select d from c where d = (select d from c);
-- this works
select d from c where d = (select d from c where d=5);

-- show that double is comparable to real

create table o (c char(10), v varchar(30), dc decimal);

select d from c,o where d <> dc;

-- clean up
drop table c;
drop table o;


--
-- test alternate syntax: just double will work for DB2 compatibility
--
create table db2version (d double);
drop table db2version;
--
-- test a variety of inserts and updates
--
create table source (i int, s smallint, c char(10), v varchar(50),
	d double precision);
create table target (p double precision not null);

-- we have already tested inserting integer and double literals.

insert into source values (1, 2, '3', '4', 5);

-- these will all work:
insert into target select i from source;
insert into target select s from source;
insert into target select d from source;

-- these will all fail:

delete from source;
insert into source values (null, null, null, null, null);
-- these fail because the target won't take a null -- of any type
insert into target values(null);
insert into target select i from source;
insert into target select s from source;
insert into target select d from source;

-- expect 4 rows in target: 1, 2, 5, and 1:
select * from target;

update target set p = p + 1;
select * from target;

update target set p = p - 1;
select * from target;

update target set p = p / 10;
select * from target;

update target set p = p * 10;
select * from target;

-- these should work
update source set i = 1.4e8;
update source set s = 1.4e4;

select i, s from source where i=1.4e8 or s=1.4e4;

-- these should get overflow
update source set i = 1.4e12;
update source set s = 1.4e12;

drop table source;
drop table target;

create table abcfloat (numtest float(20));
insert into abcfloat values (1.23456789);
insert into abcfloat values (.123456789);
insert into abcfloat values (-.123456789);
insert into abcfloat values (0.223456789);
insert into abcfloat values (-0.223456789);
insert into abcfloat values (12345678.9);
select * from abcfloat;
drop table abcfloat;


-- ** insert float.sql
--
-- Test the builtin type 'float'
-- Float is a synonym for double or real, depending on
-- the precision specified; so all we need to do is
-- show the mapping here; the double and real tests
-- show how well those types behave.
--

-- this shows several working versions of float, the default
-- and all of the boundary values:

create table t (d double precision, r real, f float, f1 float(1),
	f23 float(23), f24 float(24), f53 float(52));

select columnname, columndatatype 
from sys.syscolumns c, sys.systables t
where c.referenceid = t.tableid and t.tablename='T';

-- invalid float values
insert into t(r) values 'NaN';
insert into t(r) values +3.4021E+38;
insert into t(r) values -3.4021E+38;
create table tt(c char(254));
insert into tt values -3.402E+38;
insert into t(r) select * from tt;
insert into t(r) values '1.0';
update t set r = NaN;
update t set r = +3.4021E+38;
update t set r = -3.4021E+38;

drop table t;
drop table tt;

-- these get errors for invalid precision values:
create table t1 (d double precision, r real, f float(-10));
--
create table t2 (d double precision, r real, f float(-1));
create table t3 (d double precision, r real, f float(0));
create table t4 (d double precision, r real, f float(100));
create table t5 (d double precision, r real, f float(53));
create table t6 (d double precision, r real, f float(12.3));

-- ** insert real.sql
--
-- Test the builtin type 'real'
-- assumes these builtin types exist:
--	int, smallint, char, varchar, double precision
--
-- other things we might test:
-- show how reals lose precision on computations

--
-- Test the arithmetic operators
--

create table t (i int, s smallint, c char(10), v varchar(50),
	d double precision, r real);

insert into t values (null, null, null, null, null, null);
insert into t values (0, 100, 'hello', 'everyone is here', 200.0e0, 200.0e0);
insert into t values (-1, -100, 'goodbye', 'everyone is there', -200.0e0, -200.0e0);

select r + r, d + r, i + r, s + r, r + i from t;

select r + r + r, r + 100 + 432e0 from t;

select r - r, r - d, d - r, r - i, i - r, r - s, s - r from t;

select r - r - r, r - 100 - 432e0 from t;

select i, d, s, r, i * r, r * i, s * r, d * r, r * r, r * 2, r * 2.0e0 from t;

-- try unary minus, plus
select -(r * 100 / 100e0 ), +(r * 100e0 / 100 ) from t;

-- test null/null, constant/null, null/constant
select i, d, r, d / r, i / r, 10 / r, r / d, r / 10e0 from t;

-- test for divide by 0
select r / i from t;

-- test positive/negative, negative/positive and negative/negative
select r, r / -r, (-r) / r, (-r) / -r from t;

-- test some "more complex" expressions
select r, r + 10e0, r - (10 - 20e0), r - 10, r - (20 - 10) from t;

drop table t;

-- test overflow

create table s (d real, p real);

insert into s values (null, null);
insert into s values (0, 100);
insert into s values (1, 101);

select d + 3.4e+38 from s;

-- these are close enough to the infinities to overflow
-- Can't test simple select of literal because literals are doubles
insert into s values(3.403e+38, 3.403e+38);
insert into s values(- 3.403e+38, - 3.403e+38);
insert into s values(1.8e+100, 1.8e+100);
insert into s values(- 1.8e+100, - 1.8e+100);
select * from s;

-- these are far enough from the infinities to work
insert into s values(3.402e+38, - 3.402e+38);
insert into s values(3.3e+38, - 3.3e+38);

-- these show that math is promoted to double because of the double
-- literals. If it was real math, it would fail
select d - 3.3e+38 - 3.3e+38, p * 3.3e+38, p * -3.3e+38 from s;

-- see two more rows
select -d from s;

-- to do the math as reals, we have to keep it in the columns
delete from s;
insert into s values (1,3.3e+38);
-- these will fail, because the math is done as reals
select d - p - p from s;
select p * p from s;
select p * -p from s;

delete from s;

-- select values between 0 and 1
insert into s values (.111, 1e-1);
insert into s values (0.222, 0.222);
select * from s;
delete from s;

insert into s values (10, 1e-10);

-- underflow calculation doesn't round off, gives error.
update s set d=d*1.4e-55, p=p*1.4e-45;
select d, p from s;

update s set d=d + 1.4e-46;
select d from s;

drop table s;

-- test the arithmetic operators on a type we know they don't work on
create table w (x real, y char);

select x + y from w;

select x - y from w;

select x * y from w;

select x / y from w;

-- clean up after ourselves
drop table w;

--
-- comparisons
--

create table c (i int, s smallint, d double precision, r real, l real);

-- insert some values
insert into c values (0, 0, 0e0, 0e0, 0e0);
insert into c values (null, null, 5e0, null, null);
insert into c values (1, 1, 1e0, 2e0, 3e0);
insert into c values (1956475, 1956, 1956475e0, 1956475e0, 1956475e0);


-- select each one in turn
select r from c where r = 0e0;
select r from c where r = 1e0;
select r from c where r = 1956475e0;

-- now look for a value that isn't in the table
select r from c where l = 2e0;

-- now test null = null semantics
select r from c where r = r;

-- now test <>, <, >, <=, >=
select r from c where r <> 0e0;
select r from c where r <> 1e0;
select r from c where r < 1956475e0;
select r from c where r < 2e0;
select r from c where r > d;
select r from c where r <= l;
select r from c where r >= r;

-- test comparisons with int and smallint and double
select r from c where r <= i;
select r from c where r < s;
select r from c where r > i;
select r from c where r >= s;
select r from c where r <> i;
select r from c where r = s;
select r from c where r = d;
select r from c where r >= d;

-- show that real is comparable to decimal

create table o (c char(10), v varchar(30), dc decimal);

select r from c,o where r <> dc;

-- clean up
drop table c;
drop table o;


--
-- test a variety of inserts and updates
--
create table source (i int, s smallint, c char(10), v varchar(50),
	d double precision, r real);
create table target (t real not null);

-- we have already tested inserting integer and double literals.

insert into source values (1, 2, '3', '4', 5, 6);

-- these will all work:
insert into target select i from source;
insert into target select s from source;
insert into target select d from source;
insert into target select r from source;

delete from source;
insert into source values (null, null, null, null, null, null);
insert into source values (1, 2, '3', '4', 5, 6);
-- these fail because the target won't take a null -- of any type
insert into target values(null);
insert into target select i from source;
insert into target select s from source;
insert into target select d from source;
insert into target select r from source;

-- expect 5 rows in target: 1, 2, 5, 6, and 1:
select * from target;

update target set t = t + 1;
select * from target;

update target set t = t - 1;
select * from target;

update target set t = t / 10;
select * from target;

update target set t = t * 10;
select * from target;

-- these should work
update source set r = 1.4e4;
update source set i = r, s=r, d=r;

select i, s, d from source where i=1.4e4 or s=1.4e4 or d=1.4e4;

-- just curious, do columns see the before or after values, and
-- does it matter if they are before or after the changed value?
update source set i = r, r = 0, s = r;
select i, r, s from source where r = 0;

-- these should get overflow
update source set r = 1.4e12;
update source set i = r;
update source set s = r;

drop table source;
drop table target;




-- ============================================================
--          TESTS FOR DB2 FLOAT/DOUBLEs LIMITS
-- ============================================================
create table fake(r real);

-- ============================================================
-- different errmsg for DB2: "value of of range", CS: "NumberFormatException"
values 5e-325;
values 5e-324;

-- --- TEST SPECIAL VALUES

-- DB2 (should succed)
insert into fake values( -3.402E+38 );
insert into fake values( +3.402E+38 ); 
insert into fake values -1;

insert into fake values( -1.175E-37 ); 
insert into fake values( +1.175E-37 );
insert into fake values -2;

-- CS (should fail)
insert into fake values( -3.4028235E38 );
insert into fake values( +3.4028235E38 );
insert into fake values -3;

insert into fake values( -1.4E-45 );
insert into fake values( +1.4E-45 );
insert into fake values -4;

-- ============================================================
-- variants of ZERO
insert into fake values (+0);
insert into fake values (+0.0);
insert into fake values (+0.0E-37);
insert into fake values (+0.0E-38);
insert into fake values (+0.0E-500);
values (+0.0E-500);
values (+1.0E-300);

-- approx ZERO (java rounds to zero, but not DB2)
insert into fake values (+1.0E-300);
insert into fake values (+1.0E-900);
insert into fake values (cast(+1.0E-900 as real));
values (cast(+1.0E-300 as real));
values (+1.0E-900);
values (cast(+1.0E-900 as real));
insert into fake values -11;

-- ============================================================

-- DB2 MAX_VALUES (first succeed, second fail)
insert into fake values( -3.4019E+38 );
insert into fake values( -3.4021E+38 );
insert into fake values -21;

insert into fake values( +3.4019E+38 ); 
insert into fake values( +3.4021E+38 ); 
insert into fake values -22;

-- DB2 MIN_VALUES (first fail, second succeed)
insert into fake values( -1.1749E-37 ); 
insert into fake values( -1.1751E-37 ); 
insert into fake values -23;

insert into fake values( +1.1749E-37 );
insert into fake values( +1.1751E-37 );
insert into fake values -24;

-- CS (fail)
insert into fake values( -3.4028234E38 );
insert into fake values( -3.40282349E38 );
insert into fake values( -3.40282351E38 );
insert into fake values( -3.4028236E38 );
insert into fake values -25;

insert into fake values( +3.4028234E38 );
insert into fake values( +3.40282349E38 );
insert into fake values( +3.40282351E38 );
insert into fake values( +3.4028236E38 );
insert into fake values -26;

insert into fake values( -1.39E-45 );
insert into fake values( -1.399E-45 );
insert into fake values( -1.401E-45 );
insert into fake values( -1.41E-45 );
insert into fake values -27;

insert into fake values( +1.39E-45 );
insert into fake values( +1.399E-45 );
insert into fake values( +1.401E-45 );
insert into fake values( +1.41E-45 );
insert into fake values -28;

-- checkpoint
select * from fake;

drop table fake;
create table fake(r real);

-- ============================================================
-- ---underflow aritmetic
-- underflow to small real but / makes double!=0, so we catch

-- ok
values cast(5e-37/1e0 as real);
-- fail
values cast(5e-37/1e1 as real);
values cast(5e-37/1e300 as real);
values cast(5e-37 as real)/cast(1e10 as real);

-- ok
insert into fake values 5e-37/1e0;
-- fail
insert into fake values 5e-37/1e1;
insert into fake values 5e-37/1e300;
insert into fake values cast(5e-37 as real)/cast(1e10 as real);

drop table fake;

-- makes double to small, so java double rounds to 0. need to catch (fail)
values 5e-37 / 1e300;
values cast(5e-37 / 1e300 as real);

-- ok, zero result (succeed)
values cast(cast(0.0e0 as real) - cast(0.0e0 as real) as real);
values cast(cast(1.0e-30 as real) - cast(1.0e-30 as real) as real);

-- java (and CS previously) rounded result to zero, but now gives errors like DB2 (fail)
values cast(cast(5e-37 as real) - cast(4e-37 as real) as real);
values cast(5e-37 - 4e-37 as real);
values cast(5e-37 - 4.99e-37 as real);

values cast(5e-308 - 4e-308 as real);

values cast(5e-37 + -4e-37 as real);
values cast(5e-324 - 4e-324 as real);

values cast(5e-37 * 4e-37 as real);
values cast(cast(5e-37 as real) * cast(4e-37 as real) as real);

-- double trouble, underflow detection (fail)
values cast(5e-300 * 4e-300 as real);

-- underflow aritmetic DOUBLE (fail)
values -3e-305/1e100;
values -3e-305/1e100; 

-- negative zeros not allowed (succeed)
values 0.0e5/-1;

-- 30 characters limit to be enforced ) (first fail, second ok)
values 01234567890123456789012345678e1;
values 0123456789012345678901234567e1;

-- ============================================================

--- Marks tests

-- Examples in Cloudscape 5.2:
-- these 2 insert statements should raise error msgs in compat mode because 
-- the values are between the -mpv and +mpv (fail)
create table t1 (c1 real);
insert into t1 values -1.40129846432481700e-46;
insert into t1 values +1.40129846432481700e-46;
select * from t1;

-- these 2 insert statements should raise an error msg in compat mode
-- because the values are greater db2's limits (fail)
insert into t1 values 3.40282346638528860e+38;
insert into t1 values -3.40282346638528860e+38;
select * from t1;

drop table t1;

-- Examples in DB2 UDB for LUW 8.1.4:
-- these 2 insert statements raise ERROR 22003 because
-- the values are between the -mpv and +mpv (fail)
create table t1 (c1 real);
insert into t1 values -1.40129846432481700e-46;
insert into t1 values +1.40129846432481700e-46;
select * from t1;

-- these 2 insert statements raise ERROR 22003 because
-- the values are greater db2's limits (fail)
insert into t1 values 3.40282346638528860e+38;
insert into t1 values -3.40282346638528860e+38;
select * from t1;

drop table t1;

-- ============================================================

-- bug 5704 - make sure we catch the overflow correctly for multiplication operator
values cast(1e30 as decimal(31))*cast(1e30 as decimal(31));
values cast('1e30' as decimal(31))*cast('1e30' as decimal(31));


create table tiger(d decimal(12,11));

insert into tiger values (1.234);
insert into tiger values (0.1234);
insert into tiger values (0.01234);
insert into tiger values (0.001234);
insert into tiger values (0.001234);
insert into tiger values (0.0001234);
insert into tiger values (0.00001234);
insert into tiger values (0.000001234);
insert into tiger values (0.0000001234);
insert into tiger values (0.00000001234);
insert into tiger values (0.00000001234);

select d from tiger order by 1;



