
--
-- Script for creating the 'wombat' test database
-- that will be used for testing the 'dblook'
-- utility.  Basically, we just create a database
-- that has one or more of every possible type
-- of database object (tables, indexes, keys,
-- etc.) and then run dblook on the database
-- to check that all of the objects make it into
-- the final DDL script.  After this initial
-- (full) test, dblook is run with a series
-- of parameters, thus returning only a subset
-- of objects it contains; this allows us to
-- make sure that said parameters are working
-- as expected.
--
-- NOTE: For purposes of this test, any object
-- names (ex table, index, constraint, etc)
-- which start with the letters 'SQL' are
-- assumed by dblook_test to be system-
-- generated, and so are filtered out of the
-- output (they are replaced with "systemname"
-- or something similar).

-- ----------------------------------------------
-- Schemas
-- ----------------------------------------------

create schema "FOO";
create schema "Foo Bar";
create schema bar;

-- ----------------------------------------------
-- Jars:
-- Note: a jar file called 'dblook_test.jar'
-- must exist in the current directory (it is
-- put there by the harness).
-- ----------------------------------------------

call sqlj.install_jar('file:dblook_test.jar', 'foo.foojar', 0);

-- ----------------------------------------------
-- Stored Procedures.
-- ----------------------------------------------

create procedure proc1 (INOUT a CHAR(10), IN b int) language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams3' parameter style java dynamic result sets 4 contains sql;

create procedure bar.OP4(OUT a DECIMAL(4,2), IN b VARCHAR(255)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams4';

create procedure foo.sqqlcontrol_1 (OUT E1 VARCHAR(128), OUT E2 VARCHAR(128), OUT E3 VARCHAR(128), OUT E4 VARCHAR(128), OUT E5 VARCHAR(128), OUT E6 VARCHAR(128), OUT E7 VARCHAR(128)) no sql PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.sqlControl';

create procedure "Foo Bar".proc2 (OUT E1 VARCHAR(128), OUT E2 VARCHAR(128), OUT E3 VARCHAR(128), OUT E4 VARCHAR(128), OUT E5 VARCHAR(128), OUT E6 VARCHAR(128), OUT E7 VARCHAR(128)) reads sql data PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.sqlControl';

create procedure "procTwo" (INOUT a CHAR(10), IN b int) language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams3' parameter style java dynamic result sets 2 modifies sql data;

create procedure """proc ""In Quotes with spaces""" (INOUT a CHAR(10), IN b int) language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams3' parameter style java dynamic result sets 2 modifies sql data;

-- ----------------------------------------------
-- Tables
-- ----------------------------------------------

-- Basic.
create table t1 (i int, c char(8), d date, f float not null);
create table t2 (p1 varchar(10), b blob(20), c clob(15));
create table t8t1t4 (c1 char (4) default 'okie', c2 char(4) default 'doki');
create table bar."MULTI WORD NAME" (c char(2));

-- auto increment/defaults.
create table bar.t3 (p1 varchar(10) default 'okie', b blob(20), id int generated always as identity (start with 2, increment by 4));
create table bar.t4 (i int default 2, j int not null, k int generated always as identity);

create table "Foo Bar".t5 (cost double);
create table "Foo Bar".t6 (num integer, letter char(1));
create table "Foo Bar".t7 (un int, deux int);

-- Keys/checks.
create table bar."tWithKeys" (c char(5) not null PRIMARY KEY, i int check (i > 0), vc varchar(10) constraint notevil check (vc != 'evil'));
create table bar.t8 (someInt int constraint "pkOne" not null primary key, fkChar char(5) references bar."tWithKeys" (c) on delete set null);
create table foo.t10 (vach varchar(12), k int not null primary key, uk char(3) not null unique);
create table foo.t9 (fkInt int not null, constraint "fkOne" foreign key (fkInt) references foo.t10 (k) on update restrict on delete no action, ch char(8) not null);
create table bar.t1 (p1 varchar(10) not null constraint unq unique, c clob(15));
alter table foo.t10 add constraint "chkOne" check (k < 0);
alter table foo.t9 add constraint "pkTwo" primary key (ch, fkInt);
create table t11 (myChar char(8), lola int, foreign key (myChar, lola) references foo.t9 (ch, fkInt));

-- Quoted names, checks, keys... (start, middle, and end of the name).
create table "tquote""One" ("i""q1" int not null constraint "pee""kay1" primary key, "i""q2" int default 8 constraint "c""k1" check ("i""q2" > 4));
create table """tquoteTwo" ("""iq1" int constraint """effkay1" references "tquote""One" ("i""q1"), """iq2" int constraint """ck2" check ("""iq2" > 0));
create table "tquoteThree""" ("iq1""" int not null constraint "unqkay1""" unique, "iq2""" int constraint "ck2""" check ("iq2""" > 4));
create table """Quoted""Schema"""."tee""""Hee" (n char not null primary key);

-- ----------------------------------------------
-- Indexes.
-- ----------------------------------------------

create index ix1 on t1 (f, i);
create index bar.ix2 on t3 (p1 desc, id desc);
create index bar.ix3 on bar."tWithKeys" (c desc);
create unique index "Foo Bar"."CostIndex" on "Foo Bar".t5 (cost);
create unique index ix4 on bar.t4 (k asc);
create index """Quoted""Schema"""."Ix""5" on "tee""""Hee" (n desc);

-- ----------------------------------------------
-- Views
-- ----------------------------------------------

create view v1 (dum, dee, dokie) as select a.d, a.c, b.p1 from t1 as a, bar.t3 as b;

set schema foo;
create view v1 (doo, dwa) as select num, letter from "Foo Bar".t6;
set schema app;

create view bar."viewTwo" as select * from app.t11;

create view v2 as select * from bar.t1;

create view "V""3"(i) as values (8), (28), (78);

-- ----------------------------------------------
-- Triggers
-- ----------------------------------------------

create trigger trigOne after insert on bar.t3 for each row mode db2sql update bar.t4 set j=8 where i=2;
create trigger "Foo Bar".trig2 no cascade before delete on bar.t1 for each statement mode db2sql values (1), (2);
create trigger "TrigThree" after update of i, c on bar."tWithKeys" for each row mode db2sql select c from bar."tWithKeys";
create trigger bar.reftrig after delete on bar.t8 referencing old_table as oldtable for each statement mode db2sql select * from oldtable;
create trigger """Quoted""Schema"""."""trig""One""" after insert on """Quoted""Schema"""."tee""""Hee" for each row mode db2sql values(8);

-- Test trigger with new AND old referencing names (beetle 5725).
create table x (x int);
create table removed (x int);
create trigger trigFour after update of x on x referencing old_table as old new_table as new for each statement mode db2sql insert into removed select * from old where x not in (select x from new where x < 10);
