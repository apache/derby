-- ** insert positionedDelete.sql
--
-- tests for positioned delete
--
-- note that comments that begin '-- .' are test cases from the test plan

-- assumed available in queries at time of initial writing:
-- subqueries.  Additional tests will be needed once we have:
-- order by, group by, having, aggregates, distinct, views ...

-- setup some tables for use in the tests

create table t1 ( i int, v varchar(10), d double precision, t time );
create table t1_copy ( i int, v varchar(10), d double precision, t time );

create table t2 ( s smallint, c char(10), r real, ts timestamp );

-- populate the first table and copy
insert into t1 values (1, '1111111111', 11e11, time('11:11:11'));
insert into t1_copy select * from t1;

-- we need to turn autocommit off so that cursors aren't closed before
-- the positioned statements against them.
autocommit off;

-- empty table tests

-- .no table name given
-- this should fail with a syntax error
delete;

-- this should succeed
get cursor c0 as 'select * from t1 for update';
next c0;
delete from t1 where current of c0;
select * from t1;
close c0;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .same table name
-- .cursor before 1st row
get cursor c1 as 'select * from t2 for update';
-- 'cursor not on a row' expected
delete from t2 where current of c1;

-- .different table name
delete from t1 where current of c1;
-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .non-existant table
delete from not_exists where current of c1;
close c1;

-- .delete from  base table, not exposed table name
-- (this one should work, since base table)
get cursor c2 as 'select * from t2 asdf for update';
delete from t2 where current of c2;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .match correlation name
-- (this one should fail, since correlation name)
delete from asdf where current of c2;
close c2;

-- .non-updatable cursor
-- NOTE - forupdate responsible for extensive tests
get cursor c3 as 'select * from t2 for read only';
delete from t2 where current of c3;
close c3;

-- .target cursor does not exist
delete from t2 where current of c44;

-- .target cursor after last row
get cursor c4 as 'select * from t1 for update';
next c4;
next c4;
next c4;
delete from t1 where current of c4;
close c4;

-- .target cursor exists, closed
get cursor c5 as 'select * from t1';
close c5;
delete from t1 where current of c5;

-- .target cursor on row
get cursor c6 as 'select * from t1 for update';
next c6;
delete from t1 where current of c6;
select * from t1;
close c6;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .target cursor on row deleted by another cursor
get cursor c7 as 'select * from t1 for update';
next c7;
get cursor c8 as 'select * from t1 for update';
next c8;
delete from t1 where current of c7;
delete from t1 where current of c8;
select * from t1;
close c7;
close c8;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .target cursor on already deleted row
get cursor c9 as 'select * from t1 for update';
next c9;
delete from t1 where current of c9;
delete from t1 where current of c9;
select * from t1;
close c9;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- delete to row which was subject to searched update
-- (row still within cursor qualification)
get cursor c10 as 'select * from t1 for update';
next c10;
update t1 set i = i + 1;
select * from t1;
delete from t1 where current of c10;
select * from t1;
close c10;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- delete to row which was subject to searched update
-- (row becomes outside of cursor qualification)
get cursor c10a as 'select * from t1 where i = 1 for update';
next c10a;
update t1 set i = i + 1;
select * from t1;
delete from t1 where current of c10a;
select * from t1;
close c10a;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- delete to row which was subject to positioned update
-- (row becomes outside of cursor qualification)
get cursor c11 as 'select * from t1 where i = 1 for update';
next c11;
update t1 set i = i + 1 where current of c11;
select * from t1;
delete from t1 where current of c11;
select * from t1;
close c11;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- delete to row which was subject to 2 searched updates
-- (1st puts row outside of cursor qualification, 2nd restores it)
get cursor c12 as 'select * from t1 where i = 1 for update';
next c12;
update t1 set i = i + 1;
update t1 set i = 1;
select * from t1;
delete from t1 where current of c12;
select * from t1;
close c12;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- positioned delete on table with index (#724)
create table t5 (c1 int, c2 int);
insert into t5 values (1, 1), (2, 2), (3, 3), (4, 4);
commit;
create index i5 on t5(c1);
get cursor c1 as 'select * from t5 where c1 > 1 for update of c2';
next c1;
delete from t5 where current of c1;
next c1;
next c1;
delete from t5 where current of c1;
select * from t5;
close c1;
rollback;
create index i5 on t5(c2);
get cursor c1 as 'select * from t5 where c1 > 1 for update of c2';
next c1;
delete from t5 where current of c1;
next c1;
next c1;
delete from t5 where current of c1;
select * from t5;
close c1;
rollback;

-- reset autocommit
autocommit on;

-- drop the tables
drop table t1;
drop table t2;
drop table t5;
drop table t1_copy;

-- ** insert positionedUpdate.sql
--
-- tests for positioned update
--
-- note that comments that begin '-- .' are test cases from the test plan

-- assumed available in queries at time of initial writing:
-- subqueries.  Additional tests will be needed once we have:
-- order by, group by, having, aggregates, distinct, views ...

-- setup some tables for use in the tests

create table t1 ( i int, v varchar(10), d double precision, t time );
create table t1_copy ( i int, v varchar(10), d double precision, t time );

create table t2 ( s smallint, c char(10), r real, ts timestamp );

-- populate the first table and copy
insert into t1 values (1, '1111111111', 11e11, time('11:11:11'));
insert into t1_copy select * from t1;

-- we need to turn autocommit off so that cursors aren't closed before
-- the positioned statements against them.
autocommit off;

-- empty table tests

-- .no table name given
-- this should fail with a syntax error
update set c1 = c1;

-- this should succeed
get cursor c0 as 'select * from t1 for update';
next c0;
update t1 set i = 999 where current of c0;
select * from t1;
update t1 set i = 1 where current of c0;
select * from t1;
close c0;

-- .same table name
-- .cursor before 1st row
get cursor c1 as 'select * from t2 for update';
-- 'cursor not on a row' expected
update t2 set s = s where current of c1;

-- .different table name
update t1 set i = i where current of c1;

-- .non-existant table
update not_exists set i = i where current of c1;
close c1;

-- .update base table, not exposed table name
-- (this one should work, since base table)
get cursor c2 as 'select * from t2 asdf for update';
update t2 set s = s where current of c2;

-- .match correlation name
-- (this one should fail, since correlation name)
update asdf set s = s where current of c2;
close c2;

-- .non-updatable cursor
-- NOTE - forupdate responsible for extensive tests
get cursor c3 as 'select * from t2 for read only';
update t2 set s = s where current of c3;
close c3;

-- .target cursor does not exist
update t2 set s = s where current of c44;

-- .target cursor after last row
get cursor c4 as 'select * from t1 for update';
next c4;
next c4;
next c4;
update t1 set i = i where current of c4;
close c4;

-- .target cursor exists, closed
get cursor c5 as 'select * from t1';
close c5;
update t1 set i = i where current of c5;

-- .target cursor on row
get cursor c6 as 'select * from t1 for update';
next c6;
update t1 set i = i + 1 where current of c6;
select * from t1;
-- .consecutive updates to same row in cursor, keeping it in the cursor qual
update t1 set i = i + 1 where current of c6;
select * from t1;
close c6;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .target cursor on row deleted by another cursor
get cursor c7 as 'select * from t1 for update';
next c7;
get cursor c8 as 'select * from t1 for update';
next c8;
delete from t1 where current of c7;
update t1 set i = i + 1 where current of c8;
select * from t1;
close c7;
close c8;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .target cursor on already deleted row
get cursor c9 as 'select * from t1 for update';
next c9;
delete from t1 where current of c9;
update t1 set i = i + 1 where current of c9;
select * from t1;
close c9;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- update to row which was subject to searched update
-- (row still within cursor qualification)
get cursor c10 as 'select * from t1 for update';
next c10;
update t1 set i = i + 1;
select * from t1;
update t1 set i = i + 2 where current of c10;
select * from t1;
close c10;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- update to row which was subject to searched update
-- (row becomes outside of cursor qualification)
get cursor c10a as 'select * from t1 where i = 1 for update';
next c10a;
update t1 set i = i + 1;
select * from t1;
update t1 set i = i + 2 where current of c10a;
select * from t1;
close c10a;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- update to row which was subject to positioned update
-- (row becomes outside of cursor qualification)
get cursor c11 as 'select * from t1 where i = 1 for update';
next c11;
update t1 set i = i + 1 where current of c11;
select * from t1;
update t1 set i = i + 2 where current of c11;
select * from t1;
close c11;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- update to row which was subject to 2 searched updates
-- (1st puts row outside of cursor qualification, 2nd restores it)
get cursor c12 as 'select * from t1 where i = 1 for update';
next c12;
update t1 set i = i + 1;
update t1 set i = 1;
select * from t1;
update t1 set i = i + 2 where current of c12;
select * from t1;

-- negative test - try to update a non-existant column
update t1 set notacolumn = i + 1 where current of c12;

close c12;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- update column not in SELECT list, but in FOR UPDATE OF list
get cursor c13 as 'select i from t1 for update of v';
next c13;
update t1 set v = '999' where current of c13;
select * from t1;

-- update column not in FOR UPDATE OF list (negative test)
update t1 set i = 999 where current of c13;
select * from t1;
close c13;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- update a non-referenced column
get cursor c14 as 'select i from t1 for update';
next c14;
update t1 set v = '999' where current of c14;
select * from t1;
close c14;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .update columns in list in order different from the list's
get cursor c15 as 'select i, v from t1 for update of i, v';
next c15;
update t1 set v = '999', i = 888 where current of c15;
select * from t1;

-- . show that target table name must be used as qualifier, other names not allowed
update t1 set t1.v = '998' where current of c15;
update t1 set t2.v = '997' where current of c15;
select * from t1;
close c15;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .update only 1 column in the list
get cursor c16 as 'select i, v from t1 for update of i, v';
next c16;
update t1 set v = '999' where current of c16;
select * from t1;
close c16;

-- restore t1
delete from t1;
insert into t1 select * from t1_copy;
select * from t1;

-- .try to update through a closed cursor
get cursor c17 as 'select i, v from t1 for update of i, v';
next c17;
close c17;
update t1 set v = '999' where current of c17;
select * from t1;

-- a positioned update requires a named target table.
-- if we prepare the positioned update, close the underlying cursor
-- and reopen it on a different table, then the positioned update
-- should fail 

create table t3(c1 int, c2 int);
insert into t3 values (1,1), (2, 1), (3,3);
create table t4(c1 int, c2 int);
insert into t4 select * from t3;

get cursor c1 as 'select c1 from t3 for update of c1';
next c1;
prepare u1 as 'update t3 set c1 = c1 + 1 where current of c1';
execute u1;
next c1;
select * from t3;

close c1;

get cursor c1 as 'select c1 from t4 for update of c1';
next c1;
execute u1;
select * from t4;
select * from t3;

close c1;

-- now, reopen c1 on a table without column c1 and see
-- what happens on an attempted positioned update
get cursor c1 as 'select * from t1 for update';
next c1;
execute u1;

close c1;

-- now, reopen c1 on t3, but as a read only cursor
select * from t3;
get cursor c1 as 'select c1 from t3 ';
next c1;
execute u1;
select * from t3;

close c1;

-- positioned update on table with index (#724)
create table t5 (c1 int, c2 int);
insert into t5 values (1, 1), (2, 2), (3, 3), (4, 4);
commit;
create index i5 on t5(c1);
get cursor c1 as 'select * from t5 where c1 > 1 for update of c2';
next c1;
update t5 set c2 = 9 where current of c1;
next c1;
next c1;
update t5 set c2 = 9 where current of c1;
select * from t5;
close c1;
rollback;
create index i5 on t5(c2);
get cursor c1 as 'select * from t5 where c1 > 1 for update of c2';
next c1;
update t5 set c2 = 9 where current of c1;
next c1;
next c1;
update t5 set c2 = 9 where current of c1;
select * from t5;
close c1;
rollback;


-- reset autocommit
autocommit on;

-- drop the tables
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t1_copy;

-- tests for beetle 4417, schema and correlation name not working with
-- current of
create schema ejb;
create table ejb.test1
	(primarykey varchar(41) not null primary key,
	name varchar(200),
	parentkey varchar(41));
insert into ejb.test1 values('0','jack','jill');
autocommit off;

-- test update with schema name
get cursor c1 as 'select primarykey, parentkey, name from ejb.test1 where primarykey = ''0'' for update';
next c1;
prepare p1 as 'update ejb.test1 set name = ''john'' where current of c1';
execute p1;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- test update with schema name and correlation name
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from ejb.test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p1 as 'update ejb.test1 set name = ''joe'' where current of c1';
execute p1;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- test update with set schema
set schema ejb;
get cursor c1 as 'select primarykey, parentkey, name from test1 where primarykey = ''0'' for update';
next c1;
prepare p1 as 'update test1 set name = ''john'' where current of c1';
execute p1;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- test update with set schema and correlation name
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p1 as 'update test1 set name = ''joe'' where current of c1';
execute p1;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- test update with set schema and correlation name and schema name
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from ejb.test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p1 as 'update ejb.test1 set name = ''joe'' where current of c1';
execute p1;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- 
-- reset schema name
set schema app;

-- test delete with schema name 
get cursor c1 as 'select primarykey, parentkey, name from ejb.test1 where primarykey = ''0'' for update';
next c1;
prepare p2 as 'delete from ejb.test1 where current of c1';
execute p2;
select primarykey, parentkey, name from ejb.test1;
close c1;
-- test delete with schema name and correlation name
insert into ejb.test1 values('0','jack','jill');
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from ejb.test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p2 as 'delete from ejb.test1 where current of c1';
execute p2;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- test delete with set schema 
set schema ejb;
insert into test1 values('0','jack','jill');
get cursor c1 as 'select primarykey, parentkey, name from test1 where primarykey = ''0'' for update';
next c1;
prepare p2 as 'delete from test1 where current of c1';
execute p2;
select primarykey, parentkey, name from ejb.test1;
close c1;

-- test delete with set schema and correlation name
insert into test1 values('0','jack','jill');
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p2 as 'delete from test1 where current of c1';
execute p2;
select primarykey, parentkey, name from ejb.test1;
close c1;
-- test delete with set schema and correlation name and schema name
insert into test1 values('0','jack','jill');
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from ejb.test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p2 as 'delete from ejb.test1 where current of c1';
execute p2;
select primarykey, parentkey, name from ejb.test1;
close c1;
commit;

-- clean up
autocommit on;
set schema app;
drop table ejb.test1;
--drop schema ejb restrict; - can't drop this because it will fail SPS tests since
--statements are created and would need to be dropped

-- test correlation on select in current of cursor in current schema
-- this was also broken
create table test1
	(primarykey varchar(41) not null primary key,
	name varchar(200),
	parentkey varchar(41));
-- make sure a cursor will work fine in this situation
insert into test1 values('0','jack','jill');
autocommit off;
get cursor c1 as 'select t1.primarykey, t1.parentkey, t1.name from test1 t1 where t1.primarykey = ''0'' for update';
next c1;
prepare p2 as 'delete from test1 where current of c1';
execute p2;
select primarykey, parentkey, name from test1;
close c1;
commit;

-- clean up
autocommit on;
drop table test1;
