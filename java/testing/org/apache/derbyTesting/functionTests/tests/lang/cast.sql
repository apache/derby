-- tests for cast expressions
-- refer to casting.java for a complete analysis on casting

--==================================
--
-- simple test cases
--
--==================================

-- shrink/grow bit and char
-- no exceptions should be raised.
-- once we have warnings we'll expect
-- a warning when shrinking non space/zeros

-- shrink
values (cast ('hell' as char(2)));
values (cast ('hell' as varchar(2)));
-- shrink, whitespace only
values (cast ('he  ' as char(2)));
-- expand, check lengths
values (cast ('hell' as char(20))); 
values (cast ('hell' as varchar(20))); 
values length(cast ('hell' as char(20)));
values length(cast ('hell' as varchar(20)));

----------------
--char->bit data
----------------
-- shrink
values (cast (X'1111' as char(1) for bit data));
-- shrink, zero only
values (cast (X'1100' as char(1) for bit data));
-- expand
values (cast (X'1111' as char(2) for bit data));
-- w/o format
-- DB2 UDB PASS
-- DB2 CS FAIL
values (cast ('1234' as char(2) for bit data));

-- extra tests for shrinking parts of bits
values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);

values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);
values cast (X'00111111' as char(1) for bit data);

values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);
values cast (X'00111111' as char(1) for bit data);
values cast (X'00011111' as char(1) for bit data);

values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);
values cast (X'00111111' as char(1) for bit data);
values cast (X'00011111' as char(1) for bit data);
values cast (X'00001111' as char(1) for bit data);

values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);
values cast (X'00111111' as char(1) for bit data);
values cast (X'00011111' as char(1) for bit data);
values cast (X'00001111' as char(1) for bit data);
values cast (X'00000111' as char(1) for bit data);

values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);
values cast (X'00111111' as char(1) for bit data);
values cast (X'00011111' as char(1) for bit data);
values cast (X'00001111' as char(1) for bit data);
values cast (X'00000111' as char(1) for bit data);
values cast (X'00000011' as char(1) for bit data);

values cast (X'11111111' as char(1) for bit data);
values cast (X'01111111' as char(1) for bit data);
values cast (X'00111111' as char(1) for bit data);
values cast (X'00011111' as char(1) for bit data);
values cast (X'00001111' as char(1) for bit data);
values cast (X'00000111' as char(1) for bit data);
values cast (X'00000011' as char(1) for bit data);
values cast (X'00000001' as char(1) for bit data);
values cast (X'0011111111111111' as char(1) for bit data);
values cast (X'1111111100111111' as char(2) for bit data);

---------
--numbers
---------
values (cast (1.1 as int));
values (cast (1.1 as smallint));
values (cast (1.1 as bigint));
values (cast (1.1 as double precision));
values (cast (1.1 as numeric(2,1)));
values (cast (1.1 as decimal(2,1)));
values (cast (1.1 as numeric(2,0)));
values (cast (1.1 as decimal(2,0)));
values (cast (1.1 as float));
values (cast (1.1 as real));

values (cast (1.9 as int));
values (cast (1.9 as smallint));
values (cast (1.9 as bigint));
values (cast (1.9 as double precision));
values (cast (1.9 as numeric(2,1)));
values (cast (1.9 as decimal(2,1)));
values (cast (1.9 as numeric(2,0)));
values (cast (1.9 as decimal(2,0)));
values (cast (1.9 as float));
values (cast (1.9 as real));

-- bug 4352,4358 loss of precision on casts
--   9223372036854775807 is Long::MAX_VALUE
values (
  9223372036854775807,
  cast (9223372036854775807 as DECIMAL(24,1)),
  cast (
  cast (9223372036854775807 as DECIMAL(24,1)) as BIGINT)
  );

values (
  cast ('9223372036854775807' as DECIMAL(24,1)),
  cast (cast ('9223372036854775807' as DECIMAL(24,1)) as BIGINT)
  );
values (
  cast ('9223372036854775806' as DECIMAL(24,1)),
  cast (cast ('9223372036854775806' as DECIMAL(24,1)) as BIGINT)
  );
-- only this should fail
values (
  cast ('9223372036854775808' as DECIMAL(24,1)),
  cast (cast ('9223372036854775808' as DECIMAL(24,1)) as BIGINT)
  );
values (
  cast ('9223372036854775807.9' as DECIMAL(24,1)),
  cast (cast ('9223372036854775807.9' as DECIMAL(24,1)) as BIGINT)
  );


--   -9223372036854775808 is Long::MIN_VALUE

values (
  cast ('-9223372036854775808' as DECIMAL(24,1)),
  cast (cast ('-9223372036854775808' as DECIMAL(24,1)) as BIGINT)
  );
values (
  cast ('-9223372036854775807' as DECIMAL(24,1)),
  cast (cast ('-9223372036854775807' as DECIMAL(24,1)) as BIGINT)
  );
-- only this should fail
values (
  cast ('-9223372036854775809' as DECIMAL(24,1)),
  cast (cast ('-9223372036854775809' as DECIMAL(24,1)) as BIGINT)
  );
values (
  cast ('-9223372036854775808.9' as DECIMAL(24,1)),
  cast (cast ('-9223372036854775808.9' as DECIMAL(24,1)) as BIGINT)
  );

values (
  cast ('32767' as DECIMAL(24,1)),
  cast (cast ('32767' as DECIMAL(24,1)) as SMALLINT)
  );
values (
  cast ('32766' as DECIMAL(24,1)),
  cast (cast ('32766' as DECIMAL(24,1)) as SMALLINT)
  );
values (
  cast ('32768' as DECIMAL(24,1)),
  cast (cast ('32768' as DECIMAL(24,1)) as SMALLINT)
  );
-- only this should fail
values (
  cast ('32767.9' as DECIMAL(24,1)),
  cast (cast ('32767.9' as DECIMAL(24,1)) as SMALLINT)
  );

values (
  cast ('-32768' as DECIMAL(24,1)),
  cast (cast ('-32768' as DECIMAL(24,1)) as SMALLINT)
  );
values (
  cast ('-32767' as DECIMAL(24,1)),
  cast (cast ('-32767' as DECIMAL(24,1)) as SMALLINT)
  );
-- only this should fail
values (
  cast ('-32769' as DECIMAL(24,1)),
  cast (cast ('-32769' as DECIMAL(24,1)) as SMALLINT)
  );
values (
  cast ('-32768.9' as DECIMAL(24,1)),
  cast (cast ('-32768.9' as DECIMAL(24,1)) as SMALLINT)
  );

values (
  cast ('2147483647' as DECIMAL(24,1)),
  cast (cast ('2147483647' as DECIMAL(24,1)) as INTEGER)
  );
values (
  cast ('2147483646' as DECIMAL(24,1)),
  cast (cast ('2147483646' as DECIMAL(24,1)) as INTEGER)
  );
-- only this should fail
values (
  cast ('2147483648' as DECIMAL(24,1)),
  cast (cast ('2147483648' as DECIMAL(24,1)) as INTEGER)
  );
values (
  cast ('2147483647.9' as DECIMAL(24,1)),
  cast (cast ('2147483647.9' as DECIMAL(24,1)) as INTEGER)
  );

values (
  cast ('-2147483647' as DECIMAL(24,1)),
  cast (cast ('-2147483647' as DECIMAL(24,1)) as INTEGER)
  );
values (
  cast ('-2147483646' as DECIMAL(24,1)),
  cast (cast ('-2147483646' as DECIMAL(24,1)) as INTEGER)
  );
-- only this should fail
values (
  cast ('-2147483649' as DECIMAL(24,1)),
  cast (cast ('-2147483649' as DECIMAL(24,1)) as INTEGER)
  );
values (
  cast ('-2147483648.9' as DECIMAL(24,1)),
  cast (cast ('-2147483648.9' as DECIMAL(24,1)) as INTEGER)
  );
--numbers to char
values (cast (1.1 as char(10)));
values (cast (1.1 as varchar(10)));
values (cast (1e1 as varchar(10)));
values (cast (1e1 as char(10)));
values (cast (1 as char(10))); 
values (cast (1 as varchar(10))); 
values (cast (1e200 as char(10)));
values (cast (1e200 as varchar(10)));
values (cast (1 as long varchar));
values (cast (1.1 as long varchar));
values (cast (1e1 as long varchar));

--char to numbers
values (cast ('123' as smallint));
values (cast ('123' as int));
values (cast ('123' as bigint));
values (cast ('123' as double precision));
values (cast ('123' as float));
values (cast ('123' as real));
values (cast ('123' as numeric(3,0)));
values (cast ('123' as decimal(3,0)));

-- char (with decimal) to numbers  (truncates where needed Track #3756)
-- bug 5568
values (cast ('123.45' as smallint));
values (cast ('123.45' as int));
values (cast ('123.45' as bigint));
values (cast ('123.45' as double precision));
values (cast ('123.45' as float));
values (cast ('123.45' as real));
values (cast ('123.45' as numeric(5,1)));
values (cast ('123.45' as decimal(5,1)));

values (cast ('123.99' as smallint));
values (cast ('123.99' as int));
values (cast ('123.99' as bigint));
values (cast ('123.99' as double precision));
values (cast ('123.99' as float));
values (cast ('123.99' as real));
values (cast ('123.99' as numeric(5,1)));
values (cast ('123.99' as decimal(5,1)));


--bad
values (cast (1 as char(2) for bit data));
values (cast (1 as date)); 
values (cast (1 as time)); 
values (cast (1 as timestamp)); 

-------------------
--char -> date/time
-------------------
values (cast ('TIME''11:11:11''' as time));
values (cast ('11:11:11' as time));

values (cast ('DATE''1999-09-09''' as date));
values (cast ('1999-09-09' as date));

values (cast ('TIMESTAMP''1999-09-09 11:11:11''' as timestamp));
values (cast ('1999-09-09 11:11:11' as timestamp));

------------------
--date/time ->other
------------------
values (cast (TIME('11:11:11') as char(20)));
values (cast (DATE('1999-09-09') as char(20)));
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as char(40)));
values (cast (TIME('11:11:11') as varchar(20)));
values (cast (DATE('1999-09-09') as varchar(20)));
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as varchar(40)));
values (cast (TIME('11:11:11') as long varchar));
values (cast (DATE('1999-09-09') as long varchar));
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as long varchar));

-- truncation errors
values (cast (TIME('11:11:11') as char(2)));
values (cast (DATE('1999-09-09') as char(2)));
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as char(2)));

-- to date/time
values (cast (TIME('11:11:11') as time));
values (cast (TIME('11:11:11') as date));

-- this piece of convoluted logic is to ensure that we
-- get the current date for a conversion of time to timestamp
values cast (cast (TIME('11:11:11') as timestamp) as char(50)).substring(0, 10).equals(cast (current_date as char(10)));
-- now make sure we got the time right
values cast (cast (TIME('11:11:11') as timestamp) as char(30)).substring(11,21);


values (cast (DATE('1999-09-09') as date));
values (cast (DATE('1999-09-09') as time));
values (cast (DATE('1999-09-09') as timestamp));

values (cast (TIMESTAMP('1999-09-09 11:11:11' )as date));
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as time));
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as timestamp));

--bad
values (cast (TIMESTAMP('1999-09-09 11:11:11' )as int));
values (cast (DATE('1999-09-09') as int));
values (cast (TIME('11:11:11') as int));

values (cast (TIMESTAMP('1999-09-09 11:11:11' )as smallint));
values (cast (DATE('1999-09-09') as smallint));
values (cast (TIME('11:11:11') as smallint));

values (cast (TIMESTAMP('1999-09-09 11:11:11' )as bigint));
values (cast (DATE('1999-09-09') as bigint));
values (cast (TIME('11:11:11') as bigint));

values (cast (TIMESTAMP('1999-09-09 11:11:11' )as numeric));
values (cast (DATE('1999-09-09') as numeric));
values (cast (TIME('11:11:11') as numeric));

values (cast (TIMESTAMP('1999-09-09 11:11:11' )as decimal));
values (cast (DATE('1999-09-09') as decimal));
values (cast (TIME('11:11:11') as decimal));

values (cast (TIMESTAMP('1999-09-09 11:11:11' ) as char(13) for bit data));
values (cast (DATE('1999-09-09') as char(13) for bit data));
values (cast (TIME('11:11:11') as char(13) for bit data));

------------
--bit ->char
------------
values (cast (X'00680065006c006c006f' as char(10)));
--small bit
values (cast (X'11' as char(10)));
values (cast (X'11' as varchar(10)));
values (cast (X'11' as long varchar));
--values (cast (X'00' as char(10)));

--odd length won't work anymore
values (cast (X'123' as char(20)));

--truncate, (should be warning in future)
values (cast ('1234' as char(1) for bit data));
--truncate, ok
values (cast ('1200' as char(1) for bit data));

------------------------------------------------
-- Casting
-----------------------------------------------
create table tab1 (
				i integer, 
				s integer, 
				b integer, 
				l bigint,
				c char(10),
				v varchar(10),
				d double precision,
				r real,
				dt date,
				t time,
				ts timestamp,
				dc decimal);
insert into tab1 values(1, 
				cast(1 as smallint), 
				cast(1 as int), 
				cast(1 as bigint), 
				'char', 
				'varchar', 
				cast(1.1 as double precision), 
				cast(1.1 as real), 
				DATE('1990-10-10'),
				TIME('11:11:11'), 
				TIMESTAMP('1990-11-11 11:11:11'),
				1.1);

insert into tab1 values (null,
				null,
				null,	
				null,	
				null,	
				null,	
				null,	
				null,	
				null,	
				null,	
				null,	
				null);

-- tab1 type -> its tab1 type
select cast(i as integer) from tab1;
select cast(s as smallint) from tab1;
select cast(l as bigint) from tab1; 
select cast(c as char(10)) from tab1; 
select cast(v as char varying(10)) from tab1;
select cast(d as double precision) from tab1;
select cast(r as float) from tab1;
select cast(dt as date) from tab1;
select cast(t as time) from tab1;
select cast(ts as timestamp) from tab1;
select cast(dc as dec) from tab1;

-- try a few others where we try all conversions
select cast(i as integer) from tab1;
select cast(i as smallint) from tab1;
select cast(i as bigint) from tab1; 
select cast(i as char(10)) from tab1; 
select cast(i as char varying(10)) from tab1;
select cast(i as double precision) from tab1;
select cast(i as float) from tab1;
select cast(i as date) from tab1;
select cast(i as time) from tab1;
select cast(i as timestamp) from tab1;
select cast(i as dec) from tab1;

-- try a few others
select cast(c as integer) from tab1;
select cast(c as smallint) from tab1;
select cast(c as bigint) from tab1; 
select cast(c as char(10)) from tab1; 
select cast(c as char varying(10)) from tab1;
select cast(c as double precision) from tab1;
select cast(c as float) from tab1;
select cast(c as date) from tab1;
select cast(c as time) from tab1;
select cast(c as timestamp) from tab1;
select cast(c as dec) from tab1;

select cast(t as integer) from tab1;
select cast(t as smallint) from tab1;
select cast(t as bigint) from tab1; 
select cast(t as char(10)) from tab1; 
select cast(t as char varying(10)) from tab1;
select cast(t as double precision) from tab1;
select cast(t as float) from tab1;
select cast(t as date) from tab1;
select cast(t as time) from tab1;
select cast(t as timestamp) from tab1;
select cast(t as dec) from tab1;

drop table tab1;

---------------------------------------------------------------
-- Other Tests
---------------------------------------------------------------
autocommit off;

-- create tables
create table t1 (bt char(1) for bit data, btv varchar(1) for bit data,
				 c char(30), d double precision, i int, r real, 
				 s smallint, dc decimal(18), num numeric(18),
				 dt date, t time, ts timestamp, v varchar(30), 
				 lvc long varchar);

create table strings(c30 char(30));

-- we need a 1 row table with date/time columns because of problems
-- with single quotes in using 'values DATE('')'
create table temporal_values (dt date, t time, ts timestamp);
insert into temporal_values values(DATE('9876-5-4'), TIME('1:02:34'),
								   TIMESTAMP('9876-5-4 1:02:34'));

-- negative
-- pass wrong type for parameter
prepare a1 as 'values cast(? as smallint)';
execute a1 using 'values 1';

-- uninitialized parameter
values cast(? as int);

-- positive

-- test casting null to all builtin types
insert into t1 (bt) values cast(null as char(1) for bit data);
insert into t1 (btv) values cast(null as varchar(1) for bit data);
insert into t1 (c) values cast(null as char(30));
insert into t1 (d) values cast(null as double precision);
insert into t1 (i) values cast(null as int);
insert into t1 (r) values cast(null as real);
insert into t1 (s) values cast(null as smallint);
insert into t1 (dc) values cast(null as decimal);
insert into t1 (num) values cast(null as numeric);
insert into t1 (dt) values cast(null as date);
insert into t1 (t) values cast(null as time);
insert into t1 (ts) values cast(null as timestamp);
insert into t1 (v) values cast(null as varchar(30));
insert into t1 (lvc) values cast(null as long varchar);

-- expect 10 rows of nulls
select * from t1;

-- make sure casting works correctly on nulls
select cast (bt as char(1) for bit data) from t1;
select cast (btv as varchar(1) for bit data) from t1;
select cast (c as char(30)) from t1;
select cast (d as double precision) from t1;
select cast (r as real) from t1;
select cast (s as smallint) from t1;
select cast (num as numeric) from t1;
select cast (dc as decimal) from t1;
select cast (dt as date) from t1;
select cast (t as time) from t1;
select cast (ts as timestamp) from t1;
select cast (v as varchar(30)) from t1;
select cast (lvc as long varchar) from t1;

-- clean up t1
delete from t1;

-- test casting ? to all builtin types
prepare q1 as 'insert into t1 (bt) values cast(? as char(1) for bit data)';
prepare q2 as 'insert into t1 (btv) values cast(? as varchar(1) for bit data)';
prepare q4 as 'insert into t1 (c) values cast(? as char(30))';
prepare q5 as 'insert into t1 (d) values cast(? as double precision)';
prepare q6 as 'insert into t1 (i) values cast(? as int)';
prepare q7 as 'insert into t1 (r) values cast(? as real)';
prepare q8 as 'insert into t1 (s) values cast(? as smallint)';
prepare q10 as 'insert into t1 (num) values cast(? as numeric(18))';
prepare q11 as 'insert into t1 (dc) values cast(? as decimal(18))';
prepare q12 as 'insert into t1 (dt) values cast(? as date)';
prepare q13 as 'insert into t1 (t) values cast(? as time)';
prepare q14 as 'insert into t1 (ts) values cast(? as timestamp)';
prepare q15 as 'insert into t1 (v) values cast(? as varchar(30))';
prepare q16 as 'insert into t1 (lvc) values cast(? as long varchar)';

execute q1 using 'values X''aa''';
execute q2 using 'values X''aa''';
execute q4 using 'values char(123456)';
execute q5 using 'values 123456.78e0';
execute q6 using 'values 4321';
-- bug 5421 - support db2 udb compatible built-in functions
execute q7 using 'values REAL(4321.01234)';
execute q8 using 'values SMALLINT(12321)';
execute q10 using 'values 123456.78';
execute q11 using 'values 123456.78';
execute q12 using 'select dt from temporal_values';
execute q13 using 'select t from temporal_values';
execute q14 using 'select ts from temporal_values';
execute q15 using 'values char(654321)';
execute q16 using 'values char(987654)';

select * from t1;

-- clean up t1
delete from t1;

-- more ? tests
-- Truncation exception expected in non-parameter cases
-- RESOLVE, no truncation expected in parameter cases
-- where parameter value is not a string.  This is
-- currently an "extension".
create table x(c1 char(1));
prepare param1 as 'insert into x values cast(? as char(1))';
insert into x values cast('12' as char(1));
execute param1 using 'values ''34''';
select * from x;
delete from x;
insert into x values cast(12 as char(1));
execute param1 using 'values 34';
select * from x;
delete from x;
insert into x values cast(time('12:12:12') as char(1));
execute param1 using 'values time(''21:12:12'')';
select * from x;
delete from x;
drop table x;

-- method resolution tests

-- clean up the prepared statements
remove a1;
remove q1;
remove q2;
remove q4;
remove q5;
remove q6;
remove q7;
remove q8;
remove q10;
remove q11;
remove q12;
remove q13;
remove q14;
remove q15;

-- reset autocomiit
commit;
autocommit on;

-- bind time casting tests

-- negative
values cast('asdf' as smallint);
values cast('asdf' as int);
values cast('asdf' as bigint);
values cast('asdf' as real);
values cast('asdf' as double precision);
values cast('asdf' as decimal(5,4));
values cast('asdf' as date);
values cast('asdf' as time);
values cast('asdf' as timestamp);

values cast('2999999999' as int);
values cast(2999999999 as int);
values cast('99999' as smallint);
values cast(99999 as smallint);

values cast(cast(99 as int) as char);
values cast(cast(-9 as int) as char);
values cast(cast(99 as smallint) as char);
values cast(cast(99 as bigint) as char);
values cast(cast(9.9 as real) as char);
values cast(cast(9.9 as double precision) as char);

-- positive
values cast(1 as int);
values cast(1 as smallint);
values cast(1 as bigint);
values cast(1 as char);
values cast('true' as char(4));


-- drop the tables
drop table t1;
drop table temporal_values;
drop table strings;

-- ISO time/timestamp formats
values (cast ('08.08.08' as TIME));
values (cast ('2001-01-01-08.08.08.123456' as TIMESTAMP));

-- char, varchar
values (char('abcde', 5));
values (char('abcde', 6));
values (char('abcde', 4));
values (varchar('', 20));
create table t1 (c5 date, c6 time, c7 timestamp, c8 char(5), c9 varchar(5));
insert into t1 values ('2003-09-10', '16:44:02', '2003-09-08 12:20:30.123456', 'abc', 'abcde');
insert into t1 values ('2005-09-10', '18.44.02', '2004-09-08-12.20.30.123456', 'cba', 'c');
select char(c5), char(c6), char(c7), char(c8), char(c9) from t1;
select varchar(c5), varchar(c6), varchar(c7), varchar(c8), varchar(c9) from t1;
select char(c8, 10), varchar(c9, 9) from t1;
select { fn concat(c8, char(c8)) } from t1;
select { fn concat(c8, varchar(c9)) } from t1;
select { fn concat(varchar(c9, 20), char(c8, 8)) } from t1;
select { fn concat(char(c9, 20), varchar(c8, 8)) } from t1;

-- clean up
drop table t1;

-- bug 5421 - support db2 udb compatible built-in functions
values CHAR(INT(67890));
values CHAR(INTEGER(12345));
values CHAR(DEC(67.21,4,2));
values CHAR(DECIMAL(67.10,4,2));
values CHAR(DOUBLE(5.55));
values CHAR(DOUBLE_PRECISION(5.555));
values CHAR(BIGINT(1));
values CHAR(BIGINT(-1));

values LENGTH(CAST('hello' AS CHAR(25)));
values LENGTH(CAST('hello' AS VARCHAR(25)));
values LENGTH(CAST('hello' AS LONG VARCHAR));

values CAST (X'03' as CHAR(5) for bit data);
values CAST (X'04' as VARCHAR(5) for bit data);
values CAST (X'05' as LONG VARCHAR for bit data);

-- clean up
drop table t1;
