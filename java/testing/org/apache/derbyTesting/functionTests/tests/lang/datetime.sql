--
-- Test the builtin date/time types
-- assumes these builtin types exist:
--	int, smallint, char, varchar, real
--
-- other things we might test:
-- interaction with UUID and other user defined types
-- compatibility with dynamic parameters and JDBC getDate etc. methods

--
-- Test the arithmetic operators
--


create table t (i int, s smallint, c char(10), v varchar(50), 
	d double precision, r real, e date, t time, p timestamp);

insert into t values (null, null, null, null, null, null, null, null, null);
insert into t values (0, 100, 'hello', 'everyone is here', 200.0e0,
	300.0e0, date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'));
insert into t values (-1, -100, 'goodbye', 'everyone is there', -200.0e0,
	-300.0e0, date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:45'));

-- date/times don't support math, show each combination
select e + e from t;
select i + e from t;
select p / p from t;
select p * s from t;
select t - t from t;
select -t from t;
select +e from t;

--
-- comparisons
--

-- select each one in turn
-- each pair gets the same result
select e from t where e = date('1992-01-01');
select e from t where date('1992-01-01') = e;
select t from t where t > time('09:30:15');
select t from t where time('09:30:15') < t;
select p from t where p < timestamp('1997-06-30 01:01:01');
select p from t where timestamp('1997-06-30 01:01:01' )> p;

-- now look for a value that isn't in the table
select e from t where e <> date('1992-01-01');
select e from t where date('1992-01-01') <> e;

-- now test null = null semantics
select e, t, p from t where e = e or t = t or p = p;

-- now test <=, >=, <>
-- each pair gets the same result
select e from t where e >= date('1990-01-01');
select e from t where date('1990-01-01')<= e;
select t from t where t <= time('09:30:15');
select t from t where time('09:30:15') >= t;
select p from t where p <> timestamp('1997-06-30 01:01:01');
select p from t where timestamp('1997-06-30 01:01:01' )<> p;

-- show comparisons with mixed types don't work
select e from t where e <= i;
select e from t where t < s;
select e from t where p > d;
select e from t where e >= t;
select e from t where t <> p;
select e from t where p = e;

-- check limit values
values( date('0001-1-1'), date('9999-12-31'), date('2/29/2000'), date('29.2.2004'));
values( time('00:00:00'), time('23:59:59'));
values( time('00 AM'), time( '12:59 AM'), time('1 PM'), time('12:59 PM'));
values( time('00.00.00'), time('23.59.59'), time('24.00.00'));
values( timestamp('0001-1-1 00:00:00'), timestamp('9999-12-31 23:59:59.999999'));

-- show that overflow and underflow are not allowed
-- (SQL92 would have these report errors)
values( date('0000-01-01'));
values( date('2000-00-01'));
values( date('2000-01-00'));
values( date('10000-01-01'));
values( date('2000-13-01'));
values( date('2000-01-32'));
values( date('1900-02-29'));
values( date('2001-02-29'));

values( time('25.00.00'));
values( time('24.00.01'));
values( time('0:60:00'));
values( time('00:00:60'));

-- show garbage in == errors out
select date( 'xxxx') from t where p is null;
select time( '') from t where p is null;
select timestamp( 'is there anything here?' )from t where p is null;
select timestamp( '1992-01- there anything here?' )from t where p is null;
select timestamp( '--::' )from t where p is null;
select time('::::') from t where p is null;

-- show is not null at work
select * from t 
where e is not null
and t is not null
and p is not null;

-- test =SQ
-- this gets cardinality error
select 'fail' from t where e = (select e from t);
-- this works
select 'pass' from t where e = (select e from t where d=200);
-- this gets cardinality error
select 'fail' from t where t = (select t from t);
-- this works
select 'pass' from t where t = (select t from t where d=200);
-- this gets cardinality error
select 'fail' from t where p = (select p from t);
-- this works
select 'pass' from t where p = (select p from t where d=200);

drop table t;

--
-- test syntax: precision cannot be specified
--
create table wrong (t time(-100));
create table wrong (t time(0));
create table wrong (t time(23));
create table wrong (t timestamp(-100));
create table wrong (t timestamp(0));
create table wrong (t timestamp(6));
create table wrong (t timestamp(9));
create table wrong (t timestamp(23));

--
-- test a variety of inserts and updates
--
create table source (i int, s smallint, c char(10), v varchar(50), 
	d double precision, r real, e date, t time, p timestamp);
create table target (e date not null, t time not null, p timestamp not null);

-- we have already tested inserting literals.

insert into source values (1, 2, '3', '4', 5, 6, date('1997-07-07'),
	time('08:08:08'),timestamp('1999-09-09 09:09:09'));

-- these work:
insert into target select e,t,p from source;

-- these will all fail:
insert into target select p,e,t from source;
insert into target select i,s,d from source;
insert into target (t,p) select c,r from source;

delete from source;
insert into source values (null, null, null, null, null, null, null, null, null);
-- these fail because the target won't take a null -- of any type
insert into target values(null, null, null);
insert into target select e,t,p from source;
-- these still fail with type errors:
insert into target select p,e,t from source;
insert into target select i,s,d from source;
insert into target (t,p)select c,r from source;

-- expect 1 row in target:
select * from target;

-- unchanged:
update target set e = e, t = t, p = p;
select * from target;

-- alters the row:
update target set e = date('1990-01-01');
select * from target;

-- not settable to null
update target set e = null;
select * from target;

-- nullable col can be set to null:
update source set e = date('1492-10-01');
select e from source;
update source set e = null;
select e from source;

-- these should get type errors
update target set e = 1;
update source set p = 1.4e10;
update source set i = date('1001-01-01');


-- tests with current functions:
delete from source;
delete from target;
insert into source values (1, 2, '3', '4', 5, 6, date('1997-06-07'),
	time('08:08:08'),timestamp('9999-09-09 09:09:09'));

-- these tests are 'funny' so that the masters won't show a diff
-- every time.
select 'pass' from source 
where current_date = current_date
and current_time = current_time
and current_timestamp = current_timestamp;

select 'pass' from source 
where current_date > date('1996-12-31')
and current_time <= time(	'23:59:59') -- may oopsie on leap second days
and current_timestamp <> timestamp( -- this comment is just more whitespace
    '1996-12-31 00:00:00');

-- test with DB2 compatible syntax
select 'pass' from source 
where current date = current date
and current time = current time
and current timestamp = current timestamp;

select 'pass' from source 
where current date > date('1996-12-31')
and current time <= time(	'23:59:59') -- may oopsie on leap second days
and current timestamp <> timestamp( -- this comment is just more whitespace
    '1996-12-31 00:00:00');

-- test escaped functions
-- CURRENT_DATE escaped function not supported in DB2 UDB
-- CURRENT_TIME escaped function not supported in DB2 UDB
select 'pass' from source 
where current_date = {fn current_date()}
and current_time = {fn current_time()}
and current_timestamp = current_timestamp;

select 'pass' from source 
where current_date = {fn curdate()}
and current_time = {fn curtime()}
and current_timestamp = current_timestamp;

-- current_date() and current_time() not valid in DB2. curdate() and curtime()
-- are as escaped functions only.
values curdate();
values curtime();
values current_date();
values current_time();

values {fn current_date()};
values {fn current_time()};

-- DB2 UDB compatible test for escaped functions
select 'pass' from source 
where hour(current_time) = {fn hour(current_time)}
and minute(current_time) = {fn minute(current_time)}
and second(current_time) = {fn second(current_time)}
and year(current_date)   = {fn year(current_date)};

-- valid jdbc date and time escaped functions
values {fn hour('23:38:10')};
values {fn minute('23:38:10')};
values {fn second('23:38:10')};
values {fn year('2004-03-22')};

-- currents do have types, these inserts fail:
insert into source values (0, 0, '0', '0', 0, 0,
	current_time, current_time, current_timestamp);
insert into source values (0, 0, '0', '0', 0, 0,
	current_date, current_timestamp, current_timestamp);
insert into source values (0, 0, '0', '0', 0, 0,
	current_date, current_time, current_date);

-- this insert works
insert into source values (0, 0, '0', '0', 0, 0,
	current_date, current_time, current_timestamp);

-- test with DB2 syntax
-- this insert works
insert into source values (0, 0, '0', '0', 0, 0,
	current date, current time, current timestamp);


-- this test will diff if the select is run just after midnight,
-- and the insert above was run just before midnight...
select * from source where e <> current_date and p <> current_timestamp;
-- test with DB2 syntax
select * from source where e <> current date and p <> current timestamp;

select 'pass' from source 
where e <= current_date and p <= current_timestamp;

-- reduce it back to one row
delete from source where i=0;

-- tests with extract:
select year( e),
	month( e),
	day( date( '1997-01-15')),
	hour( t),
	minute( t),
	second( time( '01:01:42')),
	year( p),
	month( p),
	day( p),
	hour( timestamp( '1992-01-01 14:11:23')),
	minute( p),
	second( p)
from source;

-- extract won't work on other types
select month( i) from source;
select hour( d) from source;

-- extract won't work on certain field/type combos
select month( t) from source;
select day( t) from source;
select year( t) from source;
select hour( e) from source;
select minute( e) from source;
select second( e) from source;

update source set i=month( e), s=minute( t),
	d=second( p);

-- should be true and atomics should match field named as label in date/times
select i,e as "month",s,t as "minute",d,p as "second" 
	from source 
	where       (i = month(e))
		and (s = minute(t))
		and (d = second(p));

-- fields should match the fields in the date (in order)
select p, year( p) as "year",
	month( p) as "month",
	day( p) as "day",
	hour( p) as "hour",
	minute( p) as "minute",
	second( p) as "second"
from source;

-- jdbc escape sequences
values ({d '1999-01-12'}, {t '11:26:35'}, {ts '1999-01-12 11:26:51'});
values year( {d '1999-01-12'});
values hour( {t '11:28:10'});
values day( {ts '1999-01-12 11:28:23'});

drop table source;
drop table target;

-- random tests for date
create table sertest(d date, s Date,
	o Date);
insert into sertest values (date('1992-01-03'), null, null);
select * from sertest;
update sertest set s=d;
update sertest set o=d;
insert into sertest values (date( '3245-09-09'), date( '1001-06-07'),
	date( '1999-01-05'));
select * from sertest;
select * from sertest where d > s;
update sertest set d=s;

-- should get type errors:
insert into sertest values (date('3245-09-09'), time('09:30:25'), null);
insert into sertest values (null, null, time('09:30:25'));
insert into sertest values (null, null, timestamp('1745-01-01 09:30:25'));

-- should work...
update sertest set d=o;

select * from sertest where s is null and o is not null;

-- should work
select month(s) from sertest where s is not null;
select day(o) from sertest;

drop table sertest;

-- conversion tests
drop table convtest;
create table convtest(d date, t time, ts timestamp);
insert into convtest values(date('1932-03-21'),  time('23:49:52'), timestamp('1832-09-24 10:11:43.32'));
insert into convtest values(date('0001-03-21'),  time('5:22:59'), timestamp('9999-12-31 23:59:59.999999'));
insert into convtest values(null, null, null);
-- these should fail
select CAST (d AS time) from convtest;
select CAST (t AS date) from convtest;
-- these should work
select CAST (t AS time) from convtest;
select CAST (d AS date) from convtest;
select CAST (ts AS time) from convtest;	
select CAST (ts AS date) from convtest;	
-- show time and date separately as timestamp will be filtered out
select CAST(CAST (ts AS timestamp) AS date),
	CAST(CAST (ts AS timestamp) AS time) 
from convtest;	
-- casting from a time to a timestamp sets the date to current date
select 'pass', CAST (CAST(t AS timestamp) AS time) from convtest
where CAST(CAST(t AS timestamp) AS date)=current_date;
-- time should be 0
select CAST (CAST (d AS timestamp) AS date),
	CAST(CAST(d AS timestamp) AS time) from convtest;	
-- convert from strings
create table convstrtest(d varchar(30), t char(30), ts long varchar);
insert into convstrtest values('1932-03-21',  '23:49:52', '1832-09-24 10:11:43.32');
insert into convstrtest values(null, null, null);
-- these should fail - note when casting from character string the format has to
-- be correct
select CAST (d AS time) from convstrtest;
select CAST (t AS date) from convstrtest;
select CAST (ts AS time) from convstrtest;	
select CAST (ts AS date) from convstrtest;	
-- these should work
select CAST (t AS time) from convstrtest;
select CAST (d AS date) from convstrtest;
-- show time and date separately as timestamp will be filtered out
select CAST(CAST (ts AS timestamp) AS date),
	CAST(CAST (ts AS timestamp) AS time) 
from convstrtest;	
-- test aggregates
-- sum should fail
select sum(d) from convtest;
select sum(t) from convtest;
select sum(ts) from convtest;
-- these should work
select count(d) from convtest;
select count(t) from convtest;
select count(ts) from convtest;
insert into convtest values(date('0001-03-21'),  time('5:22:59'), timestamp('9999-12-31 23:59:59.999999'));
-- distinct count should be 2 not 3
select count(distinct d) from convtest;
select count(distinct t) from convtest;
select count(distinct ts) from convtest;
-- min should not be null!!!!!!!!
select min(d) from convtest;
select min(t) from convtest;
-- show time and date separately as timestamp will be filtered out
select CAST(CAST (min(ts) AS timestamp) AS date),
	CAST(CAST (min(ts) AS timestamp) AS time) 
from convtest;	
select max(d) from convtest;
select max(t) from convtest;
-- show time and date separately as timestamp will be filtered out
select CAST(CAST (max(ts) AS timestamp) AS date),
	CAST(CAST (max(ts) AS timestamp) AS time)
from convtest;
drop table convtest;
drop table convstrtest;

create table ts (ts1 timestamp, ts2 timestamp);

-- ISO format
-- leading zeros may be omited from the month, day and part of the timestamp
insert into ts values ('2003-03-05-17.05.43.111111', '2003-03-05 17:05:43.111111');
insert into ts values ('2003-3-03-17.05.43.111111', '2003-3-03 17:05:43.111111');
insert into ts values ('2003-3-2-17.05.43.111111', '2003-3-2 17:05:43.111111');
insert into ts values ('2003-03-2-17.05.43.111111', '2003-03-2 17:05:43.111111');
insert into ts values ('2003-3-1-17.05.43.1', '2003-3-1 17:05:43.1');
insert into ts values ('2003-3-1-17.05.43.12', '2003-3-1 17:05:43.12');
insert into ts values ('2003-3-1-17.05.43.123', '2003-3-1 17:05:43.123');
insert into ts values ('2003-3-1-17.05.43.1234', '2003-3-1 17:05:43.1234');
insert into ts values ('2003-3-1-17.05.43.12345', '2003-3-1 17:05:43.12345');
insert into ts values ('2003-3-1-17.05.43.123456', '2003-3-1 17:05:43.123456');
insert into ts values ('2003-3-1-17.05.43', '2003-3-1 17:05:43');

-- trailing blanks are allowed
insert into ts values ('2002-03-05-17.05.43.111111   ', '2002-03-05 17:05:43.111111   ');
insert into ts values ('2002-03-05-17.05.43.1   ', '2002-03-05 17:05:43.1   ');
insert into ts values ('2002-03-05-17.05.43    ', '2002-03-05 17:05:43    ');

-- UDB allows this by "appending a zero"; so, cloudscape follows
insert into ts values ('2003-3-1-17.05.43.', '2003-3-1 17:05:43'); 

insert into ts values ('2003-3-1-17.05.43.0', '2003-3-1 17:05:43.0'); 

insert into ts values ('0003-03-05-17.05.43.111111', '0003-03-05 17:05:43.111111');

select * from ts;

select * from ts where ts1=ts2;

delete from ts;

-- should be rejected because leading zero in year is missing
insert into ts (ts1) values ('03-03-05-17.05.43.111111');
insert into ts (ts1) values ('103-03-05-17.05.43.111111');
insert into ts (ts1) values ('3-03-05-17.05.43.111111');

-- not valid Time format in the timestamp strings: cloudscape rejects
insert into ts (ts1) values ('2003-3-24-13.1.02.566999');
insert into ts (ts1) values ('2003-3-24-13.1.1.569');
insert into ts (ts1) values ('2003-3-24-1.1.1.56');
insert into ts (ts1) values ('2003-3-24-1.1.1');
insert into ts (ts1) values ('2003-3-1-17.05.4.'); 
insert into ts (ts1) values ('2003-03-05-7.05.43.111111');

-- invalid ISO format: cloudscape rejects
insert into ts (ts1) values ('2003-3-1 17.05.43.123456'); 

-- Don't allow more than microseconds in ISO format: cloudscape rejects
insert into ts (ts1) values ('2003-03-05-17.05.43.999999999'); 
insert into ts (ts1) values ('2003-03-05-17.05.43.999999000'); 

select * from ts;

drop table ts;

-- Test the timestamp( d, t) function
create table t (datecol date, dateStr varchar(16), timecol time, timeStr varchar(16), expected timestamp);
insert into t( dateStr, timeStr) values( '2004-03-04', '12:01:02');
insert into t( dateStr, timeStr) values( null, '12:01:03');
insert into t( dateStr, timeStr) values( '2004-03-05', null);
update t set datecol = date( dateStr), timecol = time( timeStr);
update t set expected = timestamp( dateStr || ' ' || timeStr) where dateStr is not null and timeStr is not null;
select dateStr, timeStr from t
  where (expected is not null and (expected <> timestamp( dateCol, timeCol) or timestamp( dateCol, timeCol) is null))
    or (expected is null and timestamp( dateCol, timeCol) is not null);
select dateStr, timeStr from t
  where (expected is not null and (expected <> timestamp( dateStr, timeStr) or timestamp( dateStr, timeStr) is null))
    or (expected is null and timestamp( dateStr, timeStr) is not null);
select dateStr, timeStr from t
  where (expected is not null and timestamp( dateStr, timeStr) <> timestamp( dateCol, timeCol))
    or (expected is null and timestamp( dateStr, timeStr) is not null);
select dateStr, timeStr from t
  where expected is not null and date( timestamp( dateCol, timeCol)) <> dateCol;
select dateStr, timeStr from t
  where expected is not null and time( timestamp( dateCol, timeCol)) <> timeCol;
-- Error cases
select timestamp( dateCol, dateCol) from t where dateCol is not null;
select timestamp( timeCol, timeCol) from t where timeCol is not null;
values timestamp( 'xyz', '12:01:02');
values timestamp( '2004-03-04', 'xyz');
drop table t;

create table t (t time);

-- ISO format: UDB is okay.
insert into t values ('17.05.44');
insert into t values ('17.05.00');
insert into t values ('00.05.43');
insert into t values ('00.00.00');
-- DB2 keeps '24:00:00' but Cloudcape returns '00:00:00'
insert into t values ('24.00.00');

-- trailing blanks are allowed
insert into t values ('17.05.11  ');
insert into t values ('17:05:11  ');

-- 7 rows
select * from t;

delete from t;

-- end value tests...
insert into t values ('24.60.60');
insert into t values ('04.00.60');
insert into t values ('03.60.00');

-- not valid Time string ISO format: HH.MM.SS
insert into t values ('07.5.44');
insert into t values ('07.05.4');
insert into t values ('7.5.44');
insert into t values ('7.5.4');
insert into t values ('7.5.0');
insert into t values ('-4.00.00');

insert into t values ('A4.00.00');
insert into t values ('7.5.999');
insert into t values ('07.05.111');
insert into t values ('111.05.11');
insert into t values ('11.115.00');

-- no row
select * from t;

drop table t;

values time('2004-04-15 16:15:32.387');
values time('2004-04-15-16.15.32.387');
values time('2004-04-15-16.15.32.387 zz');
values time('x-04-15-16.15.32.387');

values date('2004-04-15 16:15:32.387');
values date('2004-04-15-16.15.32.387');
values date('2004-04-15-16.15.32.387 zz');
values date('2004-04-15-16.15.32.y');
