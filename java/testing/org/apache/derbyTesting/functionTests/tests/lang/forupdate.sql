--
-- tests for the for update/read only and updatable specifications parts
-- of cursors and positioned statements.
--
-- for positioned update/delete see positionedUpdate.jsql and
-- positionedDelete.jsql.
--
-- note that comments that begin '-- .' are test cases from the test plan

-- assumed available in queries at time of initial writing:
-- subqueries.  Additional tests will be needed once we have:
-- union (all), order by, group by, having, aggregates, distinct, views ...

-- setup some tables for use in the tests

create table t1 ( i int, v varchar(10), d double precision, t time );

create table t2 ( s smallint, c char(10), r real, ts timestamp );

-- we need to turn autocommit off so that cursors aren't closed before
-- the positioned statements against them.
autocommit off;

-- . leave out some keywords (for, update, read, only)
-- results: all of these should get syntax errors for missing/inappropriate keywords
select i, v from t1 for;
select i, v from t1 for read;
select i, v from t1 for only;
select i, v from t1 for update of;
select i, v from t1 update;
select i, v from t1 only;
select i, v from t1 read;


-- . for update no columns listed
-- should not complain
select i, v from t1 for update;

-- . implicit update test for read only spec
-- this will end up being read only; we know because the delete is refused
-- with a 'cursor not updatable' message
get cursor c as 'select i, v from t1, t2';
delete from t1 where current of c;

--  cursor with same name already exists
get cursor c as 'select i, v from t1, t2';
close c;

-- . implicit update test for updatable spec
-- this will end up being read only; we know because the delete is refused
get cursor c1 as 'select i, v from t1 where i is not null';
next c1;
-- the delete will get a 'cursor not updatable' execution error, but won't get
-- a compile time error
delete from t1 where current of c1;
close c1;

-- . read only for read only cursor spec
-- we know because the delete is refused with a 'cursor not updatable' message
get cursor c2 as 'select i, v from t1, t2 for read only';
delete from t1 where current of c2;
close c2;

-- . read only for updatable cursor spec
-- we know because the delete is refused with a 'cursor not updatable' message
get cursor c3 as 'select i, v from t1 where i is not null for read only';
delete from t1 where current of c3;
close c3;

-- . for update col not in select list
-- this is allowed:
select i, v from t1 for update of t;

-- . for update col in select list
-- this is allowed:
select i, v from t1 for update of i;

-- . for update col not in sel list or in table
-- this gets a 'no such column' error
select i, v from t1 for update of g;

-- . for update col in select not in table (generated col)
-- this gets a 'no such column' error
select i+10 as iPlus10, v from t1 for update of iPlus10;

-- . for update on read only spec, variety of reasons 
-- these will get cursor not updatable errors:
-- join is not updatable
select i from t1, t2 for update;
-- no subqueries are updatable
select i from t1 where i=(select i from t1) for update;
select i from t1 where i in (select i from t1) for update;
select i from t1 where exists (select i from t1) for update;
select i from t1 where exists (select s from t2) for update;
select i from t1 where exists (select s from t2 where i=s) for update;
-- note subquery in select expr is not updatable
select (select s from t2) from t1 where exists (select i from t1) for update;
select (select s from t2 where i=s) from t1 where exists (select i from t1) for update;
select * from (select i, d from t1) a for update;
select * from (select i+10, d from t1) a for update;
-- constant table not updatable
select * from (values (1, 2, 3)) a for update;
values (1, 2, 3) for update;

-- unions are not updatable
select * from t1 union all select * from t1 for update;

-- . table with/without correlation name
-- the idea is that the delete is against the table name, not the correlation name
-- we've already seen the without correlation name case in previous tests
get cursor c4 as 'select i from t1 s1 for update';
next c4;
-- this will get a target table mismatch error, it uses the correlation name:
delete from s1 where current of c4;
-- this will compile and get a 'no current row' error, it uses the table name:
delete from t1 where current of c4;
close c4;

-- . list columns in order same/different from appearance in table
-- the columns are 'found' regardless of their order.
-- none of these should get errors:
select i from t1 for update of i, v, d, t;
select i from t1 for update of v, i, t, d;

-- . list some, not all, columns in table, not contiguous
-- the columns are 'found' regardless of their order or contiguity
-- none of these should get errors:
select i from t1 for update of i, d;
select i from t1 for update of t, v;
select i from t1 for update of d;

-- . use column as named in as clause of select v. as named in base table
-- the column name must be the table's column name, not the select list name
select i as z from t1 for update of z;

-- . use column as named in as clause that matches underlying column name
-- this uses the select list name which *is* an underlying column name
-- note that the column updated is the underlying column, *not* the
-- selected column (we can see this from the type error)

get cursor c5 as 'select i as v from t1 for update of v';


-- i (renamed v in the select) is an integer; but v is still the
-- varchar column, so this compiles (gets a no current row error):

update t1 set v='hello' where current of c5;
close c5;

-- . include duplicate column name
-- expect an error:
select i from t1 for update of i, v, v, t;

-- . try using qualified column name
-- expect an error, only unqualified names are expected (SQL92 spec):
select i from t1 for update of t1.v, t1.i, t1.d;

-- . for update when select list has expressions and correlation name in use,
--   and column is repeated
-- this is allowed:
select a.i+10, d, d from t1 a for update;


-- for update is used by applications to control locking behaviour
-- without ever doing a positioned update. We test here to see
-- that is some situations we can use an index even when no
-- columns are specified in the for update case.

create table t3 (i int not null constraint t3pk primary key, b char(10));
create index t3bi on t3(b);

insert into t3 values (1, 'hhhh'), (2, 'uuuu'), (3, 'yyyy'), (4, 'aaaa'), (5, 'jjjj'), (6, 'rrrr');
insert into t3 values (7, 'iiii'), (8, 'wwww'), (9, 'rrrr'), (10, 'cccc'), (11, 'hhhh'), (12, 'rrrr');

commit;

maximumdisplaywidth 5000;
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);

select i, b from t3 FOR UPDATE;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
commit;

prepare T3PK as 'select i, b from t3  where i = ? FOR UPDATE';
execute T3PK using 'values (7)';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
remove T3PK;
commit;

prepare T3PKFORCE as 'select i, b from t3 where i = ? FOR UPDATE';

prepare T3PK as 'select i, b from t3 where i < ? FOR UPDATE';
execute T3PK using 'values (7)';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
remove T3PK;
commit;

-- non-unique index

prepare T3BI as 'select i, b from t3  where b = ? FOR UPDATE';
execute T3BI using 'values (''cccc'')';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
remove T3BI;
commit;

prepare T3BIFORCE as 'select i, b from t3 where b = ? FOR UPDATE';
commit;

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);


-- see what happens to a cursor that updates the primary key.
-- first case - no update;
get cursor T3C1 as 'select i,b from t3 where i = 4 for update';
next T3C1;
next T3C1;
close T3C1;
commit;

-- second case - simple update;
get cursor T3C1 as 'select i,b from t3 where i = 4 for update';
next T3C1;
update t3 set i = 13 where current of T3C1;
next T3C1;
close T3C1;
commit;

-- third (evil) case - update to change key value and insert a new value;
get cursor T3C1 as 'select i,b from t3 where i = 6 for update';
next T3C1;
update t3 set i = 14 where current of T3C1;
insert into t3 values (6, 'new!');
-- We will not see the newly inserted row because we are now using index scan on the
-- updateable cursor and we already get a row with that key from the unique index.
-- We would get the new row if the index were not unique.  Beetle 3865.
next T3C1;
close T3C1;
commit;

-- reset autocomiit
autocommit on;

-- drop the tables
drop table t1;
drop table t2;

-- bug 5643
-- JCC throws NPE when trying to execute a cursor after the resultset is closed
autocommit off;
create table t1 (c1 int);
insert into t1 (c1) values (1),(2),(3);
get cursor curs1 as 'select * from t1 for update of c1';
prepare curs1 as 'update t1 set c1=c1 where current of curs1';
next curs1;
close curs1;
execute curs1;

-- clean up
drop table t1;
