
--
-- Script for creating the 'wombat' test database
-- that will be used for testing the 'dblook'
-- messages. Basically, we just create a database
-- that has one of every possible type of object
-- (table, index, key, etc) and then run dblook
-- to check that all of the messages related to
-- those objects are correct.
--
-- NOTE: Because order of DDL statements is NOT
-- guaranteed within a specific category of
-- objects (ex. tables), this test should ONLY
-- have ONE of each kind of object; otherwise,
-- intermittent diffs might occur.

-- ----------------------------------------------
-- Schemas
-- ----------------------------------------------

create schema bar;

-- ----------------------------------------------
-- Jars:
-- Note: a jar file called 'dblook_test.jar'
-- must exist in the current directory (it is
-- put there by the harness).
-- ----------------------------------------------

call sqlj.install_jar('file:dblook_test.jar', 'bar.barjar', 0);

-- ----------------------------------------------
-- Stored Procedures.
-- ----------------------------------------------

create procedure proc1 (INOUT a CHAR(10), IN b int) language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams3' parameter style java dynamic result sets 4 contains sql;

-- ----------------------------------------------
-- Functions.
-- ----------------------------------------------

create function gatp(SCH VARCHAR(128), TBL VARCHAR(128)) RETURNS VARCHAR(1000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestPropertyInfo.getAllTableProperties' LANGUAGE JAVA PARAMETER STYLE JAVA CONTAINS SQL;

-- ----------------------------------------------
-- Tables
-- ----------------------------------------------

-- Includes one primary key, foreign key, and check constraint.
create table bar.t1 (c char(5) not null PRIMARY KEY, i int, vc varchar(10) constraint notevil check (vc != 'evil'), fkChar char(5) references bar.t1 (c) on delete no action);

-- ----------------------------------------------
-- Indexes.
-- ----------------------------------------------

create index ix1 on bar.t1 (i desc);

-- ----------------------------------------------
-- Views
-- ----------------------------------------------

create view v1 (dum, dee, dokie) as select a.c, a.i, a.vc from bar.t1 as a;

-- ----------------------------------------------
-- Synonyms
-- ----------------------------------------------

create synonym syn1 for bar.t1;

-- ----------------------------------------------
-- Triggers
-- ----------------------------------------------

create trigger trigOne after insert on bar.t1 for each row mode db2sql update bar.t1 set i = 4 where i = 2;
