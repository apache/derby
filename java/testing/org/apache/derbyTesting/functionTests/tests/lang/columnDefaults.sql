-- tests for column defaults

-- negative

-- ? in default
create table neg(c1 int default ?);

-- column reference in default
create table neg(c1 int, c2 int default c1);

-- subquery in default
create table neg(c1 int default (values 1));

-- type incompatibility at compile time
create table neg(c1 date default 1);

-- type incompatibility at execution time
-- bug 5585 - should fail at create table statement
-- because the default value '1' is not valid
create table neg(c1 int, c2 date default '1');
insert into neg (c1) values 1;
drop table neg;

-- bug 5203 - built-in functions are not be allowed in a constantExpression
-- because DB2 UDB returns SQLSTATE 42894
CREATE FUNCTION ASDF (DATA DOUBLE) RETURNS DOUBLE EXTERNAL NAME 'java.lang.Math.sin' LANGUAGE JAVA PARAMETER STYLE JAVA;
create table neg(c1 int default asdf(0));
drop table neg;

-- DEFAULT only valid in VALUES within an insert
values default;
values 1, default;

-- alter table modify default
create table neg(c1 date);
alter table neg modify x default null;
alter table neg add column x date default 1;
-- bug 5585 - should fail at alter table statement
-- because the default value '1' is not valid
alter table neg add column x date default '1';
insert into neg (c1) values default;
drop table neg;

-- too many values in values clause
create table neg(c1 int default 10);
insert into neg values (1, default);
insert into neg values (default, 1);
drop table neg;


-- positive

-- create tables
create table t1(c1 int, c2 int with default 5, c3 date default current_date, c4 int);

-- verify that defaults work
insert into t1 (c1) values 1;
insert into t1 (c4) values 4;
select c1, c2, c4 from t1;
select c1, c2, c4 from t1 where c3 = current_date;

-- update
-- default for column whose default is null
update t1 set c1 = default;
select c1, c2, c4 from t1 where c3 = current_date;
-- default for column that has explicit default
update t1 set c2 = 7;
select c2 from t1;
update t1 set c2 = default;
select c2 from t1;

-- insert default
delete from t1;
insert into t1 values (5, default, '1999-09-09', default);
insert into t1 values (default, 6, default, 5);
insert into t1 values (default, 6, default, 5), (7, default, '1997-07-07', 3);
select c1, c2, c4 from t1 where c3 = current_date;
select c1, c2, c4 from t1 where c3 <> current_date;
delete from t1;
insert into t1 (c1, c3, c4) values (5, '1999-09-09', default);
insert into t1 (c1, c3, c4) values (default, default, 5);
insert into t1 (c1, c3, c4) values (default, default, default);
insert into t1 (c1, c3, c4) values (default, default, 5), (7, '1997-07-07', 3);
select c1, c2, c4 from t1 where c3 = current_date;
select c1, c2, c4 from t1 where c3 <> current_date;

-- delimited identifiers
-- this schema
create table "x1" ("c1" int);
insert into "x1" values 1;
alter table "x1" add column "c2" char(1) default 'x';
select * from "x1";

-- another schema
create schema "otherschema";
create table "otherschema"."y1" ("c11" int);
insert into "otherschema"."y1" values 2;
alter table "otherschema"."y1" add column "c22" char(1) default 'y';
select * from "otherschema"."y1";

-- bug 3433
create table t7(c1 int default 10);
insert into t7 values (default);
select * from t7;

-- clean up
drop function asdf;
drop table t1;
drop table t7;
drop table "x1";
drop table "otherschema"."y1";
drop schema "otherschema" restrict;
