
--
-- this test is for identifiers and delimited idenifiers
-- identifiers get converted to upper case
-- delimited identifiers have their surrounding double quotes removed and
-- any pair of adjacent double quotes is converted to a single double quote
-- max identifier length is 128
--

-- trailing blank not trimmed
create table t1("       " int);

-- duplicate identifiers
create table t1 (c1 int, C1 int);

-- duplicate identifier/delimited identifier
create table t1 (c1 int, "C1" int);

-- duplicate delimited identifier/identifier
create table t1 ("C1" int, C1 int);

-- duplicate delimited identifiers
create table t1 ("C1" int, "C1" int);

-- verify preservation of spaces
create table success1 (c1 int, " C1" int, "  C1  " int);

-- verify correct handling of case
create table success2 ("c1" int, "C1" int);

create table success3 (c1 int, "c1" int);

-- verify correct handling of double quotes
create table success4 ("C1""" int, "C1""""" int);

-- verify correct handling in an insert
insert into success1 (c1, " C1", "  C1  ") 
values (1, 2, 3);

insert into success1 (C1, " C1", "  C1  ") 
values (6, 7, 8);

-- negative testing for an insert
-- "c1 " is not in success1
insert into success1 (c1, "c1 ", " C1", " C1 ", "  C1  ") 
values (11, 12, 13, 14, 15);

-- C1 appears twice in the column list - C1 and "C1"
insert into success1 (C1, "C1", " C1", " C1 ", "  C1  ") 
values (16, 17, 18, 19, 20);


-- verify correct handling in a select
select C1, " C1", " C1", "  C1  " from success1;

-- following should fail for "C1 "
select c1, "C1 ", " C1", " C1 ", "  C1  " from success1;

-- negative testing for an insert
-- "c1 " should not match
select c1, "c1 ", " C1", " C1 ", "  C1  "  from success1;

-- negative test for max identifier width
-- 4567890123456789012345678901234567890123456789012345678901234567890
create table
asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast6
(c1 int);
create table
"asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast7"
(c1 int);

-- positive test for max identifier width
create table
asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast
(c1 int);
insert into 
asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast
values (1);
select * from 
asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast;

create table
"delimitedsdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast"
(c1 int);
insert into 
"delimitedsdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast"
values (2);
select * from 
"delimitedsdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast";

-- drop the tables
drop table success1;
drop table success2;
drop table success3;
drop table success4;
drop table
asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast;
drop table
"delimitedsdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfaslast";


-- 2003-04-14 14:04:38 
-- new testcases for SQL92 reserved keywords as identifiers

CREATE TABLE WHEN (WHEN INT, A INT);
INSERT INTO WHEN (WHEN) VALUES (1);
INSERT INTO WHEN VALUES (2, 2);
SELECT * FROM WHEN;
SELECT WHEN.WHEN, WHEN FROM WHEN;
SELECT WHEN.WHEN, WHEN FROM WHEN WHEN;
DROP TABLE WHEN;

CREATE TABLE THEN (THEN INT, A INT);
INSERT INTO THEN (THEN) VALUES (1);
INSERT INTO THEN VALUES (2, 2);
SELECT * FROM THEN;
SELECT THEN.THEN, THEN FROM THEN;
SELECT THEN.THEN, THEN FROM THEN THEN;
DROP TABLE THEN;

CREATE TABLE SIZE (SIZE INT, A INT);
INSERT INTO SIZE (SIZE) VALUES (1);
INSERT INTO SIZE VALUES (2, 2);
SELECT * FROM SIZE;
SELECT SIZE.SIZE, SIZE FROM SIZE;
SELECT SIZE.SIZE, SIZE FROM SIZE SIZE;
DROP TABLE SIZE;

CREATE TABLE LEVEL (LEVEL INT, A INT);
INSERT INTO LEVEL (LEVEL) VALUES (1);
INSERT INTO LEVEL VALUES (2, 2);
SELECT * FROM LEVEL;
SELECT LEVEL.LEVEL, LEVEL FROM LEVEL;
SELECT LEVEL.LEVEL, LEVEL FROM LEVEL LEVEL;
DROP TABLE LEVEL;

CREATE TABLE DOMAIN (DOMAIN INT, A INT);
INSERT INTO DOMAIN (DOMAIN) VALUES (1);
INSERT INTO DOMAIN VALUES (2, 2);
SELECT * FROM DOMAIN;
SELECT DOMAIN.DOMAIN, DOMAIN FROM DOMAIN;
SELECT DOMAIN.DOMAIN, DOMAIN FROM DOMAIN DOMAIN;
DROP TABLE DOMAIN;

CREATE TABLE ZONE (ZONE INT, A INT);
INSERT INTO ZONE (ZONE) VALUES (1);
INSERT INTO ZONE VALUES (2, 2);
SELECT * FROM ZONE;
SELECT ZONE.ZONE, ZONE FROM ZONE;
SELECT ZONE.ZONE, ZONE FROM ZONE ZONE;
DROP TABLE ZONE;


-- Negative tests
-- Novera wanted 0-length delimited identifiers but for db2-compatibility, we are going to stop supporting 0-length delimited identifiers
-- test1
create table "" (c1 int);
-- test2
create table t1111 ("" int);
-- test3
create schema "";
-- identifiers can not start with "_"
-- test4
create table _t1(_c1 int);
-- test5
create table t1(_c1 int);
-- test6
create view _v1 (c1) as select * from t1;
-- test7
create view v1 (__c1) as select * from t1;
-- test8
create index _i1 on t1(c1);
-- test9
create table "_"."_"(c1 int);
-- test10
create table "".""(c1 int);
