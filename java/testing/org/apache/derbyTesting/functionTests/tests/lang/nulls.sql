--
-- this test shows the current supported null value functionality
--
autocommit off;

-- trying to define null and not null for a column
create table a(a1 int null not null);

-- same as above, except that it's in reverse order
create table a(a1 int not null null);

-- defining null constraint for a column now does not work
create table a(a1 int not null , a2 int not null);

-- alter table adding explicitly nullable column and primary key column
-- constraint on it fails
alter table a add column a3 int null constraint ap1 primary key;

-- alter table table level primary key constraint on nullable column
-- doesn't give an error
alter table a add constraint ap1 primary key(a1,a2);

drop table a;

-- create table with not null column and unique key should work
create table a (a int not null unique );
insert into a values (1);
-- second insert should fail
insert into a values (1);

drop table a;

-- alter nullability on a unique column should fail
create table a ( a int not null unique);
alter table a modify a null;

drop table a;

-- try adding a primary key where there is null data
-- this should error
create table a (a1 int not null, a2 int);
insert into a values(1, NULL);

alter table a add constraint ap1 primary key(a1, a2);

drop table a;

-- try with multiple columns
create table a (a1 int, a2 int, a3 int);

-- This is an error in DB2 compatibility mode
alter table a add constraint ap1 primary key(a1, a2, a3);

drop table a;

-- try with multiple null columns
create table a (a1 int not null, a2 int, a3 int);
insert into a values(1,1,1);

-- table with no null data should work
alter table a add constraint ap1 primary key(a1, a2, a3);

-- insert a null into one of the primary key columns should fail
insert into a values(1, NULL, 1);

drop table a;

-- try with multiple null columns
create table a (a1 int not null, a2 int default null, a3 int default null);
insert into a values(1,NULL,1);

-- table with some null data should fail
alter table a add constraint ap1 primary key(a1, a2, a3);

-- defining primarykey column constraint for explicitly nullable column
-- gives error
create table a1(ac1 int null primary key);

-- defining primarykey table constraint on explicitly nullable columns
-- give error
create table a1(ac1 int null, ac2 int not null, primary key(ac1,ac2));

-- should fail because
-- not null must explicitly be specified for columns that have primary keys
create table a1(ac1 int, ac2 int not null, primary key(ac1,ac2));

-- say null twice should fail
create table a2(ac1 int null null);

-- say not null, null and no null for a column. This is to make sure the flags
-- stay proper for a column
create table a3(ac1 int not null null not null);

-- first statement says null and second one says not null. This is to make sure
-- the flag for the first one doesn't affect the second one
create table a3(ac1 int default null);
create table a4(ac1 int not null);

-- one column says null and second one says not null
create table a5(ac1 int default null, ac2 int not null);

-- statement1 says null, 2nd says nothing but says primary key
create table a6(ac1 int default null);
create table a7(ac1 int not null primary key);

-- create a table with null and non-null columns
create table t (i int, i_d int default null, i_n int not null,
		s smallint, s_d smallint default null, s_n smallint not null);

-- insert non-nulls into null and non-null columns
insert into t (i, i_d, i_n, s, s_d, s_n) values (1, 1, 1, 1, 1, 1);

-- insert nulls into those columns that take nulls
insert into t values (null, null, 2, null, null, 2);

-- insert a null as a default value into the first default null column
insert into t (i, i_n, s, s_d, s_n) values (3, 3, 3, 3, 3);

-- insert a null as a default value into the other default null columns
insert into t (i, i_d, i_n, s, s_n) values (4, 4, 4, 4, 4);

-- insert nulls as default values into all default null columns
insert into t (i, i_n, s, s_n) values (5, 5, 5, 5);

-- attempt to insert default values into the columns that don't accept nulls
insert into t (i, i_d, s, s_d) values (6, 6, 6, 6);

-- insert default nulls into nullable columns that have no explicit defaults
insert into t (i_d, i_n, s_d, s_n) values (7, 7, 7, 7);

-- attempt to insert an explicit null into a column that doesn't accept nulls
insert into t values (8, 8, null, 8, 8, 8);

-- attempt to insert an explicit null into the other columns
-- that doesn't accept nulls
insert into t values (9, 9, 9, 9, 9, null);

-- select all the successfully inserted rows
select * from t;

-- create a table with a non-null column with a default value of null
-- and verify that nulls are not allowed
create table s (x int default null not null, y int);
insert into s (y) values(1);
select * from s;

-- is null/is not null on an integer type
create table u (c1 integer);
insert into u values null;
insert into u values 1;
insert into u values null;
insert into u values 2;
select * from u where c1 is null;
select * from u where c1 is not null;

-- is [not] null and parameters
prepare p1 as 'select * from u where cast (? as varchar(1)) is null';
execute p1 using 'values (''a'')';
prepare p2 as 'select * from u where cast (? as varchar(1)) is not null';
execute p2 using 'values (''a'')';

select count(*) from u where c1 is null;
insert into u select * from (values null) as X;
select count(*) from u where c1 is null;

-- cleanup
drop table t;
drop table s;
drop table u;
drop table a;
drop table a3;
drop table a4;
drop table a5;
drop table a6;
drop table a7;
