-- order by tests
-- in V52, we allow "select a from t order by b" where the ORDERBY column doesn't necessarily appear in the SELECT list.

autocommit off;

-- . order by on values
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 1,2,3;
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 1,3;
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 2,1;
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 2;
-- . order by on position < 1, > range (error)
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 0;
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 4;
-- . order by doesn't see generated names
values (1,0,1),(1,0,0),(0,0,1),(0,1,0);
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by "SQLCol1";
values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by "SQLCol2";

values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 1,1,2,3;

-- rollback should release the prepared statements
rollback;

-- . order by on select
-- . order by with duplicate rows in source
set schema app;
create table obt (i int, v varchar(40));
insert into obt (i) values (null);
insert into obt values (1, 'hello');
insert into obt values (2, 'planet');
insert into obt values (1, 'world');
insert into obt values (3, 'hello');

-- save the data we've created
commit;

select * from obt order by i;
select * from obt order by v;
-- . order by all select columns
select * from obt order by i,v;
select * from obt order by v,i;
-- . order by asc/desc mix
select * from obt order by v desc, i asc;
-- reverse prior order
select * from obt order by i asc, v desc;
-- . order by with duplicates but different asc/desc attributes (ok)
select * from obt order by i asc, i desc;

select * from obt order by i, v, i;
select v from obt order by i, v, i;
select v from obt order by i desc, v, i;

-- . order by on position < 1, > range (error)
select * from obt order by 1, 0;
select * from obt order by 1,2,3,4,5,6,7,8,9;
select * from obt order by 32767;

-- rollback should release the prepared statements
rollback ;

-- . order by on union all
create table obt2 (i2 int, v varchar(40));
insert into obt2 values (3, 'hello'), (4, 'planet'), (1, 'shoe'), (3, 'planet');

-- save the data we've created
commit ;

select * from obt union all select * from obt2 order by v;

select * from obt union all select * from obt order by i;

select * from obt union all select * from obt order by i, i;

-- . order by on union with differing column names on sources. Error
select * from obt union all select * from obt2 order by i;

select * from obt union all values (1,'hello') order by i;

values (1,'hello') union all select * from obt order by i;

-- . order by can not see generated names, though OK by position
values (1,'hello') union all select * from obt; 

values (1,'hello') union all select * from obt order by "SQLCol1"; 

values (1,'hello') union all select * from obt order by 1;

values (1,'hello') union all select * from obt order by 1, 1;

-- rollback should release the prepared statements
rollback ;

select i from obt union all values (1) order by 1;

-- sees noname on both sides although second side is named
values (1) union all select i from obt order by i;

-- rollback should release the prepared statements
rollback ;

-- i2's name is hidden by obt, fails
select * from obt union all select * from obt2 order by i2;

-- . order by position/name mix
select * from obt order by 1,i;
select * from obt order by 1,v;

-- . order by with duplicate positions
select * from obt order by 1,2,1;

-- . order by with duplicate names
select * from obt order by v,i,v;

-- . order by name gets select name, not underlying name
select i as i2, v from obt order by i2;
-- error, i is not seen by order by
select i as i2, v from obt order by i;

-- rollback should release the prepared statements
rollback ;

-- . order without by (error)
select i, v from obt order i;
select i, v from obt by i;

-- . show order, by are reserved keywords
select order from obt;
select by from obt;

-- . order by on column not in query (error)
select i from obt order by c;

-- . order by on column not in select, in table (error)
select i from obt order by v;

-- . order by on expression (not allowed)
select i from obt order by i+1;

-- . order by on qualified column name, incorrect correlation name (not allowed)
select i from obt t order by obt.i;

-- . order by on qualified column name, incorrect column name (not allowed)
select i from obt t order by obt.notexists;

-- . order by on qualified column name
create table t1(c1 int);
create table t2(c1 int);
create table t3(c3 int);
insert into t1 values 2, 1;
insert into t2 values 4, 3;
insert into t3 values 6, 5;
select t1.c1, t2.c1 from t1, t2 order by t1.c1;
select t1.c1, t2.c1 from t1, t2 order by t2.c1;
select t1.c1, t2.c1 from t1, t1 t2 order by t2.c1;
select t1.c1, t2.c1 from t1, t1 t2 order by t1.c1;
-- bug 5716 - qualified column name not allowed in order by when union/union all is used - following 4 test cases for that
select c1 from t1 union select c3 as c1 from t3 order by t1.c1;
select * from obt union all select * from obt2 order by obt.v;
select * from obt union all select * from obt2 order by obt2.v;
select * from obt union all select * from obt2 order by abc.v;
select * from t1 inner join t2 on 1=1 order by t1.c1;
select * from t1 inner join t2 on 1=1 order by t2.c1;
select c1 from t1 order by app.t1.c1;
select c1 from app.t1 order by app.t1.c1;
select c1 from app.t1 order by t1.c1;
select c1 from app.t1 order by c1;
select c1 from app.t1 c order by c1;
select c1 from app.t1 c order by c.c1;
select c1 from t1 order by c1;

-- negative
-- shouldn't find exposed name
select c1 from t1 union select c3 from t3 order by t3.c3;
select c1 from t1 union select c3 from t3 order by asdf.c3;
select c1 from t1 order by sys.t1.c1;
select c1 from app.t1 order by sys.t1.c1;
select c1 from t1 c order by app.c.c1;
select c1 from app.t1 c order by app.t1.c1;

-- a is not a column in t1
select 1 as a from t1 order by t1.a;

-- t3.c1 does not exist
select * from t1, t3 order by t3.c1;

-- rollback should release the prepared statements
rollback ;

-- . order by on join
select obt.i, obt2.i2+1, obt2.v from obt, obt2 order by 2, 3;

select obt.i, obt2.i2+1, obt2.v from obt2, obt where obt.i=obt2.i2 order by 2, 3;

-- . order by with spaces at end of values
values 'hello ', 'hello    ', 'hello  ', 'hello' order by 1;

-- . order by on select items that are expressions
select i+1, v, {fn length(v)} from obt order by 2, 1 desc, 3;

-- rollback should release the prepared statements
rollback ;

-- . redundant order by on distinct, ?non-redundant (different ordering)
select distinct i from obt order by i;
select distinct i,v from obt order by v;
select distinct i,v from obt order by v desc, i desc, v desc;

-- . redundant order by on distinct, redundant (subset/prefix)
select distinct i,v from obt order by i;

-- . redundant order by on index scan (later)

-- rollback should release the prepared statements
rollback ;

-- . order by with empty source, nulls in source, etc.
delete from obt;
select * from obt order by 1;
select * from obt order by v;
rollback ;

-- . order by with close values (doubles)
create table d (d double precision);
insert into d values 1e-300,2e-300;
select d,d/1e5 as dd from d order by dd,d;
rollback ;

-- . order by with long values (varchars)
create table v (v varchar(1200));
insert into v values 'itsastart';
insert into v values 'hereandt';
update v set v = v || v || v;
update v set v = v || v || v;
update v set v = v || v;
update v set v = v || v;
update v set v = v || v;
update v set v = v || v;
update v set v = v || v;
select v from v order by v desc;
rollback ;
drop table v;

-- . order by on all data types
create table missed (s smallint, r real, d date, t time, ts timestamp, c char(10), l bigint);
insert into missed values (1,1.2e4, '1992-01-01','23:01:01', '1993-02-04 12:02:00.001', 'theend', 2222222222222);
insert into missed values (1,1.2e4, '1992-01-01', '23:01:01', '1993-02-04 12:02:00.001', 'theend', 3333333333333);
insert into missed values (2,1.0e4, '1992-01-01', '20:01:01', '1997-02-04 12:02:00.001', 'theend', 4444444444444);
insert into missed values (2,1.0e4, '1992-01-01', '20:01:01', '1997-02-04 12:02:00.001', null,     2222222222222);
select s from missed order by s;
select r from missed order by r;
select d,c from missed order by c,d;
select ts,t from missed order by ts desc, t;
select l from missed order by l;
select l from missed order by l desc;
rollback ;

-- . order by on char column
create table ut (u char(10));
insert into ut values (null);
insert into ut values (cast ('hello' as char(10)));
insert into ut values ('world');
insert into ut values ('hello');
insert into ut values ('world  ');
-- rollback should release the prepared statements
rollback ;

-- . order by and explicit for update (no, some cols)
get cursor c1 as 'select i from obt order by i for update of v';

-- . order by and explicit read only (ok)
get cursor c1 as 'select i from obt order by i for read only';
next c1;
close c1;

-- . order by is implicitly read only
get cursor c1 as 'select i from obt order by i';
next c1;
-- error
update obt set v='newval' where current of c1;
close c1;
-- no rows
select v from obt where v='newval';

-- rollback should release the prepared statements
rollback ;

-- . order by only allowed on cursor spec, not subquerys (error) 
select v from obt where i in (select i from obt2 order by i);

select v from obt where i = (select i from obt2 order by i);

select v from (select i,v from obt2 order by i);

-- rollback should release the prepared statements
rollback ;

-- order by allowed on datatypes, 
-- but not non-mapped user types
-- bit maps to Byte[], so can't test for now
create table tab1 (
				i integer, 
				tn integer, 
				s integer, 
				l integer,
				c char(10), 
				v char(10),
				lvc char(10),
				d double precision,
				r real,
				dt date,
				t time,
				ts timestamp,
				dc decimal(2,1));
insert into tab1 values (1, cast(1 as int), cast(1 as smallint), cast(1 as bigint), '1', '1', '1', cast(1.1 as double precision), cast(1.1 as real), '1996-01-01', '11:11:11','1996-01-01 11:10:10.1', cast(1.1 as decimal(2,1)));
insert into tab1 values (2, cast(2 as int), cast(2 as smallint), cast(2 as bigint), '2', '2', '2', cast(2.2 as double precision), cast(2.2 as real), '1995-02-02', '12:12:12', '1996-02-02 12:10:10.1', cast(2.2 as decimal(2,1)));
select * from tab1 order by 1;
rollback;

-- bug 2769 (correlation columns, group by and order by)
create table bug2769(c1 int, c2 int);
insert into bug2769 values (1, 1), (1, 2), (3, 2), (3, 3);
select a.c1, sum(a.c1) from bug2769 a group by a.c1 order by a.c1;
rollback;

-- reset autocommit
autocommit on;

-- cleanup
drop table obt;
drop table obt2;

create table t (a int, b int, c int);
insert into t values (1, 2, null), (2, 3, null), (3, 0, null), (1, 3, null);

select * from t order by a;
select * from t order by a, a;
select * from t order by a, a, a;
select * from t order by a, b;

select a, b, c from t order by a, a;
select a, b, c from t order by a, b;

select a, c from t order by b; 
select a, c from t order by b, b; 
select a, b, c from t order by b; 

select a from t order by b, c;
select a, c from t order by b, c;
select a, c from t order by b, c, b, c;
select a, b, c from t order by b, c;
select b, c from t order by app.t.a;

-- error case
select * from t order by d;
select t.* from t order by d;
select t.* from t order by t.d;
select s.* from t s order by s.d;

select *, d from t order by d;
select t.*, d from t order by d;
select t.*, d from t order by t.d;
select t.*, d from t order by app.t.d;
select s.*, d from t s order by s.d;
select t.*, t.d from t order by t.d;
select s.*, s.d from t s order by s.d;


select a, b, c from t order by d;
select a from t order by d;
select t.a from t order by t.d;
select s.a from t s order by s.d;

drop table t;
