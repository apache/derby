
--
-- this test is for the values clause functionality
--

-- create the tables
create table t1 (i int, j int);
create table t2 (k int, l int);

-- populate t2
insert into t2 values (1, 2);
insert into t2 values (3, 4);

-- negative tests
values(null);
values(1,null);
values(null,1);
values(null),(1);
values(1),(null);
select x from (values(null,1)) as x(x,y);
select x from (values(1,null)) as x(x,y);
select x from (values null) as x(x);

-- empty values clause
values();

-- positive tests

-- single value
values 1;
values (1);
insert into t1 values (1, null);
select * from t1;
delete from t1;

-- multiple values
values (1, 2, 3);

-- values in derived table
select * from (values (1, 2, 3)) a;
select a, b from (values (1, 2, 3)) a (a, b, c);
select * from (values (1, 2, 3)) a, (values (4, 5, 6)) b;
select * from t2, (values (1, 2, 3)) a;
select * from (values (1, 2, 3)) a (a, b, c), t2 where l = b;

-- subquery in values clause
values (select k from t2 where k = 1);
values (2, (select k from t2 where k = 1));
values ((select k from t2 where k = 1), 2);
values ((select k from t2 where k = 1), (select l from t2 where l = 4));
insert into t1 values ((select k from t2 where k = 1), (select l from t2 where l = 4));
select * from t1;
delete from t1;

-- values clause in set clause
update t2 set k = (values 5) where l = 2;
select * from t2;
-- k should be set to null
update t2 set k = (values (select 2 from t2 where l = 5));
select * from t2;

-- table constructor tests

-- negative tests

-- non-matching # of elements
values 1, (2, 3), 4;
values (2, 3), (4, 5, 6);

-- empty element
values 1, , 2;

-- all ? parameters in a column position
prepare v1 as 'values (1, ?, 2), (3, ?, 4), (5, ?, 7)';

-- positive tests

values 1, 2, 3;
values (1, 2, 3), (4, 5, 6);

prepare v2 as 'values (1, 1, ?), (1e0, ?, ''abc''), (?, 0, ''def'')';
execute v2 using 'values (''ghi'', 1, 2)';
execute v2 using 'values (cast(null as char(3)), cast(null as smallint), cast(null as float))';
remove v2;

-- type precedence tests. tinyint not supported by DB2 Cloudscape
values (1 = 1.2);
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (1.2 = 1);
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (1 = cast(1 as bigint));
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (1 = cast(1 as smallint));
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (cast(1 as bigint) = 1);
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (cast(1 as smallint) = 1);

-- inserts
create table insert_test1 (c1 int);
create table insert_test2 (i int, s smallint, d double precision, r real,
						  c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30));

insert into insert_test1 values 1, 2, 3;
select * from insert_test1;
delete from insert_test1;

insert into insert_test1 values 1, null, 3;
select * from insert_test1;
delete from insert_test1;

insert into insert_test2 values (1, 1, 1e1, 1e1, '111', '1111111111', '111', '111111111'),
								(2, 2, 2e2, 2e2, '222', '2222222222', '222', '222222222'),
								(3, 3, 3e3, 3e3, '333', '3333333333', '333', '333333333');

select * from insert_test2;
delete from insert_test2;

insert into insert_test2 values (1, 1, null, null, null, null, null, null),
								(2, 2, null, null, null, null, null, null),
								(3, 3, null, null, null, null, null, null);
select * from insert_test2;
delete from insert_test2;

insert into insert_test2 values (1, null, null, null, null, null, null, null),
								(null, 2, null, null, null, null, null, null),
								(3, null, null, null, null, null, null, null);
select * from insert_test2;
delete from insert_test2;

insert into insert_test2 (r, d) values (1e2, 1e1),
									   (2e2, 2e1),
									   (3e2, 3e1);
select * from insert_test2;
delete from insert_test2;

prepare v3 as 'insert into insert_test2 values (1, 1, ?, 1e1, ''111'', ''1111111111'', ''111'', ''111111111''),
								(2, 2, 2e2, 2e2, ''222'', ?, ''222'', ''222222222''),
								(3, 3, 3e3, ?, ''333'', ''3333333333'', ''333'', ''333333333'')';

execute v3 using 'values (1e1, ''2222222222'', 3e3)';
execute v3 using 'values (cast(null as float), cast(null as char(10)), cast(null as real))';

remove v3;

-- insert with a table constructor with all ?s in one column
prepare v4 as 'insert into insert_test2 values (?, null, null, null, null, null, null, null),
				(?, null, null, null, null, null, null, null),
				(?, null, null, null, null, null, null, null)';
execute v4 using 'values (10, 20, 30)';
select * from insert_test2;
remove v4;

delete from insert_test2;

-- negative test - all ?s in one column
prepare v3 as 'values (1, ?, ?, 1e1, ''111'', ''1111111111'', ''111'', ''111111111''),
								(2, ?, 2e2, 2e2, ''222'', ?, ''222'', ''222222222''),
								(3, ?, 3e3, ?, ''333'', ''3333333333'', ''333'', ''333333333'')';

-- values clause with a subquery in a derived table (bug 2335)
create table x(x int);
insert into x values 1, 2, 3, 4;
select * from (values (1, (select max(x) from x), 1)) c;
select * from x, (values (1, (select max(x) from x), 1)) c(a, b, c) where x = c;
drop table x;

-- drop the tables
drop table t1;
drop table t2;
drop table insert_test1;
drop table insert_test2;

--- supporting <TABLE> in table expression.
create table target (a int, b int);
create index idx on target(b);
insert into target values (1, 2), (2, 3), (0, 2);

create table sub (a int, b int);
insert into sub values (1, 2), (2, 3), (2, 4);

select *
from (select b from sub) as q(b);

select *
from table (select b from sub) as q(b);

select *
from table (select * from table (select b from sub) as q(b)) as p(a);

select *
from table (select b from sub) as q(b), target;

select *
from table (select b from sub) as q(b), target where q.b = target.b;

select *
from target, table (select b from sub) as q(b);

select *
from  (values (1)) as q(a);

select *
from  table (values (1)) as q(a), table (values ('a'), ('b'), ('c')) as p(a);

-- should fail because <TABLE> can appear in front of derived table
select *
from  table target;

select *
from  table (target);

select *
from  table (target as q);

drop table sub;
drop table target;


-- negative tests
create table t1 (c1 int);
insert into t1 values 1;

-- boolean expression IS disallowed in values or select clause
select nullif(c1, 1) is null from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 1 is null;

-- boolean expression =, >, >=, <, <= disallowed in values or select clause
values 1 = 1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select 1 = 1 from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (nullif('abc','a') = 'abc');
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select (nullif('abc','a') = 'abc') from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select c11 = any (select c11 from t1) from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 2 > 1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select 2 > 1 from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 2 >= 1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select 2 >= 1 from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 1 < 2;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select 1 < 2 from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 1 <= 2;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select 1 <= 2 from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (1>1);
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select (c1 < 2) from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values (1 between 2 and 5);
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select (c1 between 1 and 3) from t1;

-- boolean expression LIKE disallowed in values and select clause
prepare ll1 as 'values ''asdf'' like ?';
prepare ll1 as 'select ''asdf'' like ? from t1';
prepare ll15 as 'values ''%foobar'' like ''Z%foobar'' escape ?';
prepare ll15 as 'select ''%foobar'' like ''Z%foobar'' escape ? from t1';
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values '%foobar' like '%%foobar' escape '%';	
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select '_foobar' like '__foobar' escape '_' from t1;	
prepare ll4 as 'values org.apache.derbyTesting.functionTests.tests.lang.CharUTF8::getMaxDefinedCharAsString() like ?';

-- boolean expression INSTANCEOF disallowed in values and select clause
values 1 instanceof int;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 1 instanceof java.lang.Integer between false and true;

-- boolean expression EXISTS disallowed in values and select clause
select exists (values 1) from t1;
values exists (values 2);

-- boolean expression EXISTS diallowed in update set clause too
update t1 set c11 = exists(values 1);

-- ?: not supported anymore
values not true ? false : true;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select not true ? false : true from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values 1 ? 2 : 3;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select c1 is null ? true : false from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select new java.lang.Integer(c1 is null ? 0 : c1) from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select c1, (c1=1? cast(null as int) : c1) is null from t1;

-- try few tests in cloudscape mode for boolean expressions in values or select clause
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values new java.lang.String() = '';
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
values new java.lang.String('asdf') = 'asdf';
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select new java.lang.String() = '' from t1;
-- this test runs in SPS mode too, hence adding a comment line before the sql, so we get correct column number in error message in both SPS and non-SPS mode
select new java.lang.String('asdf') = 'asdf' from t1;

