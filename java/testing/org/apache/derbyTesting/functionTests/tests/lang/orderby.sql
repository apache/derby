--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
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

-- . order by on expression (allowed)
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
select bug2769.c1 as x, sum(bug2769.c1) as y from bug2769 group by bug2769.c1 order by bug2769.c1;
select bug2769.c1 as x, sum(bug2769.c1) as y from bug2769 group by bug2769.c1 order by x;
select c1 as x, c2 as y from bug2769 group by bug2769.c1, bug2769.c2 order by c1 + c2;
select c1 as x, c2 as y from bug2769 group by bug2769.c1, bug2769.c2 order by -(c1 + c2);
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


--Test addtive expression in order clause

create table test_word(value varchar(32));
insert into test_word(value) values('anaconda');
insert into test_word(value) values('America');
insert into test_word(value) values('camel');
insert into test_word(value) values('Canada');

select * from test_word order by value;
select * from test_word order by upper(value);

drop table test_word;

create table test_number(value integer);
insert into test_number(value) values(-1);
insert into test_number(value) values(0);
insert into test_number(value) values(1);
insert into test_number(value) values(2);
insert into test_number(value) values(3);
insert into test_number(value) values(100);
insert into test_number(value) values(1000);
select * from test_number order by value;
select * from test_number order by value + 1;
select * from test_number order by value - 1;
select * from test_number order by value * 1;
select * from test_number order by value / 1;
select * from test_number order by 1 + value;
select * from test_number order by 1 - value;
select * from test_number order by 1 * value;
select * from test_number where value <> 0 order by 6000 / value;
select * from test_number order by -1 + value;
select * from test_number order by -1 - value;
select * from test_number order by - 1 * value;
select * from test_number where value <> 0 order by - 6000 / value;
select * from test_number order by abs(value);
select * from test_number order by value desc;
select * from test_number order by value + 1 desc;
select * from test_number order by value - 1 desc;
select * from test_number order by value * 1 desc;
select * from test_number order by value / 1 desc;
select * from test_number order by 1 + value desc;
select * from test_number order by 1 - value desc;
select * from test_number order by 1 * value desc;
select * from test_number where value <> 0 order by 6000 / value desc;
select * from test_number order by -1 + value desc;
select * from test_number order by -1 - value desc;
select * from test_number order by - 1 * value desc;
select * from test_number where value <> 0 order by - 6000 / value desc;
select * from test_number order by abs(value) desc;
drop table test_number;
create table test_number2(value1 integer,value2 integer);
insert into test_number2(value1,value2) values(-2,2);
insert into test_number2(value1,value2) values(-1,2);
insert into test_number2(value1,value2) values(0,1);
insert into test_number2(value1,value2) values(0,2);
insert into test_number2(value1,value2) values(1,1);
insert into test_number2(value1,value2) values(2,1);
select * from test_number2 order by abs(value1),mod(value2,2);
drop table test_number2;
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

-- test fof using table correlation names 
select * from (values (2),(1)) as t(x) order by t.x;

create table ta(id int);
create table tb(id int,c1 int,c2 int);
insert into ta(id)  values(1);
insert into ta(id)  values(2);
insert into ta(id)  values(3);
insert into ta(id)  values(4);
insert into ta(id)  values(5);
insert into tb(id,c1,c2) values(1,5,3);
insert into tb(id,c1,c2) values(2,4,3);
insert into tb(id,c1,c2) values(3,4,2);
insert into tb(id,c1,c2) values(4,4,1);
insert into tb(id,c1,c2) values(5,4,2);
select t1.id,t2.c1 from ta as t1 join tb as t2 on t1.id = t2.id order by t2.c1,t2.c2,t1.id;

drop table ta;
drop table tb;

-- some investigation of the handling of non-unique columns in the result set
-- related to DERBY-147. The idea with this tests is that it should be
-- acceptable to mention a column in the SELECT statement multiple times and
-- then order by it, so long as the multiple columns truly are equivalent.
-- There are a few cases where there truly is an ambiguity, and in those
-- cases we reject the ORDER BY clause.

create table derby147 (a int, b int, c int, d int);
insert into derby147 values (1, 2, 3, 4);
insert into derby147 values (6, 6, 6, 6);
select t.* from derby147 t;
select t.a,t.b,t.* from derby147 t order by b;
select t.a,t.b,t.b,t.c from derby147 t;
select t.a,t.b,t.b,t.c from derby147 t order by t.b;
-- This one truly is ambiguous, because the two columns named "e" are
-- NOT equivalent. So it should fail:
select a+b as e, c+d as e from derby147 order by e;

create table derby147_a (a int, b int, c int, d int);
insert into derby147_a values (1,2,3,4), (40, 30, 20, 10), (1,50,3,50);
create table derby147_b (a int, b int);
insert into derby147_b values (4, 4), (10, 10), (2, 50);
-- The columns named "a" are NOT equivalent.
select t1.a,t2.a from derby147_a t1, derby147_b t2 where t1.d=t2.b order by a;
select t1.a,t2.a from derby147_a t1, derby147_b t2 where t1.d=t2.b order by t2.a;
select a,a,b,c,d,a from derby147_a order by a;
select c+d as a, t1.a, t1.b+t1.c as a from derby147_a t1 order by 3, 2 desc;
-- The columns named "a" are NOT equivalent.
select c+d as a, t1.a, t1.b+t1.c as a from derby147_a t1 order by a, a desc;
select a, c+d as a from derby147_a;
-- The columns named "a" are NOT equivalent.
select a, c+d as a from derby147_a order by a;
select c+d as a, t1.a, t1.b+t1.c as b_plus_c from derby147_a t1 order by c+d;
-- The columns named "a" are NOT equivalent.
select c+d as a, t1.a, t1.b+t1.c as a from derby147_a t1 order by d-4, a;
select * from derby147_a order by c+2 desc, b asc, a desc;
-- If you introduce a coorelation name for a table, use the correlation
-- name in the order by:
select a, b from derby147_a t order by derby147_a.b;
-- pull expressions from the ORDER BY clause into the implicit area of
-- the SELECT column list, and ensure they don't end up in the result. This
-- statement causes a SanityManager assertion, filed as DERBY-1861
-- select * from derby147_b order by b, a+2;
-- Verify that correlation names match the table names properly:
select t.a, sum(t.a) from derby147_a t group by t.a order by t.a;

-- Tests which verify the handling of expressions in the ORDER BY list
-- related to DERBY-1861. The issue in DERBY-1861 has to do with how the
-- compiler handles combinations of expressions and simple columns in the
-- ORDER BY clause, so we try a number of such combinations

create table derby1861 (a int, b int, c int, d int);
insert into derby1861 values (1, 2, 3, 4);
select * from derby1861 order by a, b, c+2;
select a, c from derby1861 order by a, b, c-4;
select t.* from derby1861 t order by t.a, t.b, t.c+2;
select a, b, a, c, d from derby1861 order by b, c-1, a;
select * from derby1861 order by a, c+2, a;
select * from derby1861 order by c-1, c+1, a, b, c * 6;
select t.*, t.c+2 from derby1861 t order by a, b, c+2;
select * from derby1861 order by 3, 1;
select * from derby1861 order by 2, a-2;

-- Tests which verify the handling of expressions in the ORDER BY list
-- related to DERBY-2459. The issue in DERBY-2459 has to do with handling
-- of ORDER BY in the UNION case. The current Derby implementation has no
-- support for expressions in the ORDER BY clause of a UNION SELECT.
-- These test cases demonstrate some aspects of what works, and what doesn't.

create table d2459_A1 ( id char(1) ,value int ,ref char(1));
create table d2459_A2 ( id char(1) ,value int ,ref char(1));
create table d2459_B1 ( id char(1) ,value int);
create table d2459_B2 ( id char(1) ,value int);
insert into d2459_A1 (id, value, ref) values ('b', 1, null);
insert into d2459_A1 (id, value, ref) values ('a', 12, 'e');
insert into d2459_A2 (id, value, ref) values ('c', 3, 'g');
insert into d2459_A2 (id, value, ref) values ('d', 8, null);
insert into d2459_B1 (id, value) values ('f', 2);
insert into d2459_B1 (id, value) values ('e', 4);
insert into d2459_B2 (id, value) values ('g', 5); 

-- Should work, as the order by expression is against a select, not a union:
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END ;

-- Should work, it's a simple column reference to the first column in UNION:
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by id;

-- Should work, it's a column reference by position number
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by 2;

-- should fail, because qualified column references can't refer to UNIONs
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by t1.id;

-- should fail, because the union's results can't be referenced this way
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END;

-- should fail, because this column is not in the result:
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by value;

-- ought to work, but currently fails, due to implementation restrictions:
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by CASE WHEN id IS NOT NULL THEN id ELSE 2 END;

-- Also ought to work, but currently fails due to implementation restrictions:
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A1 t1 left outer join d2459_B1 t2 ON t2.id = t1.ref
union all
select t1.id, CASE WHEN t2.value IS NOT NULL THEN t2.value ELSE t1.value END
from d2459_A2 t1 left outer join d2459_B2 t2 ON t2.id = t1.ref
order by id || 'abc';

-- A number of simpler test cases investigating how the result set of the
-- UNION is constructed. If both children have identical result column names,
-- then the UNION result set's columns have the same names. Otherwise the
-- UNION result set's columns have generated names, and can only be
-- referred to by column position. Note als othat the matching of columns
-- for the result set of the UNION is done by column position, not by name

select id from D2459_A1 union select ref from D2459_A2;
select id from D2459_A1 union select ref from D2459_A2 order by id;
select id from D2459_A1 union select ref from D2459_A2 order by 1;
select id i from D2459_A1 union select ref i from D2459_A2 order by i;
select id i from D2459_A1 union select ref j from D2459_A2;
select id i from D2459_A1 union select ref j from D2459_A2 order by i;
select id i from D2459_A1 union select ref j from D2459_A2 order by 1;
select id from D2459_A1 union select id from D2459_A2 order by D2459_A1.id;
select id from D2459_A1 union select id from D2459_A2 order by id||'abc';
select * from D2459_A1 union select id, value, ref from D2459_A2 order by value;
select id, value, ref from D2459_A1 union select * from D2459_A2 order by 2;
select id, id i from D2459_A1 union select id j, id from D2459_A2 order by id;
select id, id i from D2459_A1 union select id j, id from D2459_A2 order by 2;
select id, ref from D2459_A1 union select ref, id from D2459_A2;
select id i, ref j from D2459_A1 union select ref i, id j from D2459_A2;

-- Some test cases for DERBY-2351. The issue in DERBY-2351 involves whether
-- pulled-up ORDER BY columns appear in the result set or not, and how
-- DISCTINCT interacts with that decision. The point is that DISTINCT should
-- apply only to the columns specified by the user in the result column list,
-- not to the extra columns pulled up into the result by the ORDER BY. This
-- means that some queries should throw an error, but due to DERBY-2351
-- the queries instead display erroneous results.

create table t1 (c1 int, c2 varchar(10));
create table t2 (t2c1 int);
insert into t1 values (3, 'a'), (4, 'c'), (2, 'b'), (1, 'c');
insert into t2 values (4), (3);
-- This query should return 4 distinct rows, ordered by column c1:
select distinct c1, c2 from t1 order by c1;
-- This statement is legitimate. Even though c1+1 is not distinct, c1 is:
select distinct c1, c2 from t1 order by c1+1;
-- DERBY-2351 causes this statement to return 4 rows, which it should
-- instead show an error. Note that the rows returned are not distinct!
select distinct c2 from t1 order by c1;
-- This query should return 3 distinct rows, ordered by column c2
select distinct c2 from t1 order by c2;
-- This query should work because * will be expanded to include c2:
select distinct * from t1 order by c2;
-- After the * is expanded, the query contains c1, so this is legitimate:
select distinct * from t1 order by c1+1;
-- This query also should not work because the order by col is not in result:
select distinct t1.* from t1, t2 where t1.c1=t2.t2c1 order by t2c1;
-- But without the distinct it should be fine:
select t1.* from t1, t2 where t1.c1=t2.t2c1 order by t2c1;
drop table t1;

create table person (name varchar(10), age int);
insert into person values ('John', 10);
insert into person values ('John', 30);
insert into person values ('Mary', 20);
-- DERBY-2351 causes this statement to display 3 rows, when it should
-- instead show an error. Again, note that the rows returned are not distinct.
SELECT DISTINCT name FROM person ORDER BY age;
-- This query should return two rows, ordered by name.
SELECT DISTINCT name FROM person ORDER BY name;
-- This query should return two rows, ordered by name descending:
SELECT DISTINCT name FROM person ORDER BY name desc;
-- Ordering by an expression involving name is legitimate:
select distinct name from person order by upper(name);
-- Ordering by an expression involving an unselected column is not. However,
-- Derby does not currently enforce this restriction. Note that the answer
-- that Derby returns is incorrect: Derby returns two rows with duplicate
-- 'name' values. This is because Derby currently implicitly includes the
-- 'age' column into the 'distinct' processing due to its presence in the
-- ORDER BY clause. DERBY-2351 and DERBY-3373 discuss this situation in
-- more detail.
select distinct name from person order by age*2;
-- Some test cases involving column aliasing:
select distinct name as first_name from person order by name;
select distinct name as first_name from person order by first_name;
select distinct person.name from person order by name;
select distinct name as first_name from person order by person.name;
select distinct name as age from person order by age;
select distinct name as age from person order by person.age;
select distinct name, name from person order by name;
select distinct name, name as first_name from person order by name;
select distinct name, name as first_name from person order by 2;
-- Some test cases combining column aliasing with table aliasing:
select distinct name nm from person p order by name;
select distinct name nm from person p order by nm;
select distinct name nm from person p order by p.name;
select distinct name nm from person p order by person.name;
select distinct name nm from person p order by person.nm;
select distinct name nm from person p order by p.nm;
create table pets (name varchar(10), age int);
insert into pets values ('Rover', 3), ('Fido', 5), ('Buster', 1);
select distinct name from person union select distinct name from pets order by name;
select distinct name from person, pets order by name;
select distinct person.name as person_name, pets.name as pet_name from person,pets order by name;
select distinct person.name as person_name, pets.name from person,pets order by name;
select distinct person.name as person_name, pets.name from person,pets order by person.name;
select distinct person.name as name, pets.name as pet_name from person,pets order by name;
select distinct person.name as name, pets.name as pet_name from person,pets order by pets.name;
-- Include some of the error cases from above without the DISTINCT
-- specification to investigate how that affects the behavior:
select name as age from person order by person.age;
select name from person, pets order by name;
select person.name as person_name, pets.name as pet_name from person,pets order by name;
select person.name as person_name, pets.name from person,pets order by person.name;
select person.name as person_name, pets.name from person,pets order by name;
select person.name as name, pets.name as pet_name from person,pets order by name;
drop table person;
drop table pets;


create table d2887_types(
   id             int,
   c1_smallint    smallint,
   c2_int         integer,
   c3_bigint      bigint,
   c4_real        real,
   c5_float       float,
   c6_numeric     numeric(10,2),
   c7_char        char(10),
   c8_date        date,
   c9_time        time,
   c10_timestamp  timestamp,
   c11_varchar    varchar(50)
);

-- Tests to demonstrate proper operation of <null ordering> (DERBY-2887)

insert into d2887_types values
  (1, 1, 1, 1, 1.0, 1.0, 1.0, 'one', 
   '1991-01-01', '11:01:01', '1991-01-01 11:01:01',
   'one'),
  (2, 2, 2, 2, 2.0, 2.0, 2.0, 'two', 
   '1992-02-02', '12:02:02', '1992-02-02 12:02:02',
   'two'),
  (3, 3, 3, 3, 3.0, 3.0, 3.0, 'three',
   '1993-03-03', '03:03:03', '1993-03-03 03:03:03',
   'three'),
  (4, null, null, null, null, null, null, null,
   null, null, null,
   null);


-- Demonstrate various combinations of NULLS FIRST, NULLS LAST, and default,
-- with various combinations of ASC, DESC, and default, with various
-- data types. These should all succeed, should all produce output with the
-- non-null values in the proper order, and should all produce output with
-- the null values ordered as specified. If null ordering was not specified,
-- the default Derby behavior is nulls are last if asc, first if desc.

select id, c1_smallint from d2887_types order by c1_smallint nulls first;
select id, c2_int from d2887_types order by c2_int nulls last;
select id, c3_bigint from d2887_types order by c3_bigint asc;
select id, c4_real from d2887_types order by c4_real desc;
select id, c5_float from d2887_types order by c5_float asc nulls last;
select id, c6_numeric from d2887_types order by c6_numeric desc nulls last;
select id, c7_char from d2887_types order by c7_char asc nulls first;
select id, c8_date from d2887_types order by c8_date desc nulls first;

drop table d2887_types;

-- DERBY-2352 involved a mismatch between the return type of the SUBSTR
-- method and the expected type of the result column. During compilation,
-- bind processing was computing that the SUBSTR would return a CHAR, but
-- at execution time it actually returned a VARCHAR, resulting in a type
-- mismatch detected by the sorter. Since the TRIM functions are very
-- closely related to the SUBSTR function, we include a few tests of
-- those functions in the test case.

create table d2352 (c int);
insert into d2352 values (1), (2), (3);
select substr('abc', 1) from d2352 order by substr('abc', 1);
select substr('abc', 1) from d2352 group by substr('abc', 1);
select ltrim('abc') from d2352 order by ltrim('abc');
select ltrim('abc') from d2352 group by ltrim('abc');
select trim(trailing ' ' from 'abc') from d2352
       order by trim(trailing ' ' from 'abc');
select trim(trailing ' ' from 'abc') from d2352
       group by trim(trailing ' ' from 'abc');
drop table d2352;

-- DERBY-3303: Failures in MergeSort when GROUP BY is used with
-- an ORDER BY on an expression (as opposed to an ORDER BY on
-- a column reference).

create table d3303 (i int, j int, k int);
insert into d3303 values (1, 1, 2), (1, 3, 3), (2, 3, 1), (2, 2, 4);
select * from d3303;

-- All of these should execute without error.  Note the variance
-- in expressions and sort order for the ORDER BY clause.

select sum(j) as s from d3303 group by i order by 1;
select sum(j) as s from d3303 group by i order by s;
select sum(j) as s from d3303 group by i order by s desc;
select sum(j) as s from d3303 group by i order by abs(1), s;
select sum(j) as s from d3303 group by i order by sum(k), s desc;
select sum(j) as s from d3303 group by k order by abs(k) desc;
select sum(j) as s from d3303 group by k order by abs(k) asc;
select sum(j) as s from d3303 group by i order by abs(i);
select sum(j) as s from d3303 group by i order by abs(i) desc;

-- Sanity check that a DISTINCT with a GROUP BY is ok, too.
select distinct sum(j) as s from d3303 group by i;

-- Slightly more complex queries, more in line with the query
-- that was reported in DERBY-3303.  Try out various ORDER
-- BY clauses to make sure they are actually being enforced.

select max(i) as m1, max(j) as m2, sum(k) - max(j) as mdiff
  from d3303 group by j order by abs(sum(k) - max(j)) asc;

select max(i) as m1, max(j) as m2, sum(k) - max(j) as mdiff
  from d3303 group by j order by abs(sum(k) - max(j)) desc;

select max(i) as m1, max(j) as m2, sum(k) - max(j) as mdiff
  from d3303 group by j order by abs(sum(k) - max(j)) desc, m2 asc;

select max(i) as m1, max(j) as m2, sum(k) - max(j) as mdiff
  from d3303 group by j order by abs(sum(k) - max(j)) desc, m2 desc;

-- Queries that include a "*" in the SELECT list and have
-- expressions in the ORDER BY.

select d3303.i as old_i, sum(d3303.k), d3303.*
  from d3303 group by k, i, j order by j; 

select d3303.i as old_i, sum(d3303.k), d3303.*
  from d3303 group by k, i, j order by 4; 

select d3303.i as old_i, sum(d3303.k), d3303.*
  from d3303 group by k, i, j order by k+2; 

-- These should all fail with error 42X77 (as opposed to an
-- ASSERT or an IndexOutOfBoundsException or an execution time
-- NPE).

select k as s from d3303 order by 2;
select sum(k) as s from d3303 group by i order by 2;
select k from d3303 group by i,k order by 2;
select k as s from d3303 group by i,k order by 2;

drop table d3303;
