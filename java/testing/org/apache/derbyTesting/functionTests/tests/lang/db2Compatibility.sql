
-- With DB2 current schema is equal to the user name on login.
CREATE TABLE DST.DEF_SCHEMA_TEST(NAME_USER VARCHAR(128), NAME_SCHEMA VARCHAR(128));
INSERT INTO DST.DEF_SCHEMA_TEST VALUES(USER, CURRENT SCHEMA);
SELECT COUNT(*) FROM DST.DEF_SCHEMA_TEST WHERE NAME_USER = NAME_SCHEMA;
SET SCHEMA DILBERT;

connect 'jdbc:derby:wombat;user=dilbert';
INSERT INTO DST.DEF_SCHEMA_TEST VALUES(USER, CURRENT SCHEMA);
SELECT COUNT(*) FROM DST.DEF_SCHEMA_TEST WHERE NAME_USER = NAME_SCHEMA;
VALUES CURRENT SCHEMA;
disconnect;

SET CONNECTION CONNECTION0;

-- still should not be created
SET SCHEMA DILBERT;

connect 'jdbc:derby:wombat;user=dilbert';
INSERT INTO DST.DEF_SCHEMA_TEST VALUES(USER, CURRENT SCHEMA);
SELECT COUNT(*) FROM DST.DEF_SCHEMA_TEST WHERE NAME_USER = NAME_SCHEMA;
VALUES CURRENT SCHEMA;
CREATE TABLE SCOTT(i int);
insert into SCOTT VALUES(4);
disconnect;

SET CONNECTION CONNECTION0;

SELECT * FROM DILBERT.SCOTT;
DROP TABLE DILBERT.SCOTT;
DROP TABLE DST.DEF_SCHEMA_TEST;
DROP SCHEMA DST RESTRICT;
DROP SCHEMA DILBERT RESTRICT;


-- Simple Cloudscape specific features.

-- CLASS ALIAS;

create class alias MyMath for java.lang.Math;
drop class alias MyMath;
create class alias for java.lang.Math;
drop class alias Math;

-- METHOD ALIAS;

create method alias myabs for java.lang.Math.abs;
drop method alias myabs;

-- STORED PREPARED STATEMENTS 
-- create statement no more supported both in db2 and cloudscpae mode. -ve test for that
create statement s1 as values 1,2;
-- alter, drop and execute statements are still supported for existing stored prepared statements for customers
alter statement recompile all;
-- following will give error because there is no stored prepared statement s1 in the database
drop statement s1;

-- clean up
DROP TABLE t1;
DROP TABLE t2;
DROP CLASS ALIAS ExternalInsert;
DROP STATEMENT insert1;

-- Primary key constraint, DB2 requires NOT null on the columns.

create table customer (id int primary key, name char(100));
drop table customer;

create table customer (id  int NOT NULL, id2 int, name char(100), primary key (id, id2));
drop table customer;

-- Unique key constraint, DB2 requires NOT null on the columns.

create table customer (id int unique, name char(100));

create table customer (id  int NOT NULL, id2 int, constraint custCon unique(id, id2));

-- check they actually work!
create table customer (id int NOT NULL primary key, name char(100));
drop table customer;

create table customer (id  int NOT NULL, id2 int NOT NULL, name char(100), primary key (id, id2));
drop table customer;


-- drop schema requires restrict
create schema fred;
drop schema fred;
drop schema fred restrict;

-- create schema not supported for schemas that start with SYS

create schema SYS;
create schema SYSDJD;
create schema "SYSNO";
create schema "sys";
create schema "sysok";
drop schema "sys" restrict;
drop schema "sysok" restrict;

-- data types not supported
create table NOTYPE(i int, b BOOLEAN);
create table NOTYPE(i int, b TINYINT);
create table NOTYPE(i int, b java.lang.String);
create table NOTYPE(i int, b com.acme.Address);
create table NOTYPE(i int, b org.apache.derby.vti.VTIEnvironment);

-- VTI in the DELETE statement
-- beetle 5234
CREATE TABLE testCS (col1 int, col2 char(30), col3 int);
INSERT INTO testCS VALUES (100, 'asdf', 732);
DELETE FROM NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') WHERE col1 = 100 and col3 = 732;

-- VTI in the INSERT statement
-- beetle 5234
INSERT INTO NEW org.apache.derbyTesting.functionTests.util.serializabletypes.ExternalTable('jdbc:derby:wombat', 'testCS') VALUES (100, 'asdf', 732);

-- VTI in the SELECT statement
-- beetle 5234
select * from testCS, new org.apache.derbyTesting.functionTests.util.VTIClasses.PositiveInteger_VTICosting_SI(col1, 1) a;
select * from new com.acme.myVTI() as T;
select * from new org.apache.derbyTesting.not.myVTI() as T;
select * from new org.apache.derby.diag.LockTable() as T;

-- VTI in CREATE TRIGGER statement
-- beetle 5234
CREATE TABLE tb1(a int);
CREATE TRIGGER testtrig1 AFTER DELETE ON tb1 FOR EACH ROW MODE DB2SQL INSERT INTO NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') VALUES (1000);

-- VTI in CREATE TRIGGER statement
-- beetle 5234
CREATE TRIGGER testtrig2 AFTER DELETE ON tb1 FOR EACH ROW MODE DB2SQL DELETE FROM NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') WHERE col1 = 100 and col3 = 732;

-- VTI in CREATE TRIGGER statement
-- beetle 5234
CREATE TRIGGER testtrig3 AFTER DELETE ON tb1 FOR EACH ROW MODE DB2SQL SELECT * FROM testCS, NEW org.apache.derbyTesting.functionTests.util.VTIClasses.PositiveInteger_VTICosting_SI(col1, 1) a;

-- clean up
DROP TABLE tb1;
DROP TABLE testCS;

-- PROPERTIES in DB2 mode
create table maps (country_ISO_code char(2)) PROPERTIES derby.storage.pageSize=262144;

-- PROPERTIES in DB2 mode
-- beetle 5177
create table maps2 (country_ISO_code char(2));
create index map_idx1 on maps2(country_ISO_code) properties derby.storage.pageSize = 2048;
-- BTREE not supported in both Cloudscape and DB2 mode and that is why rather than getting feature not implemented, we will get syntax error in DB2 mode
create btree index map_idx2 on maps2(country_ISO_code);
create unique btree index map_idx2 on maps2(country_ISO_code);
drop table maps2;

-- SET LOCKING clause in DB2 mode
-- beetle 5208
create table maps1 (country_ISO_code char(2)) set locking = table;
create table maps2 (country_ISO_code char(2)) set locking = row;
drop table maps1;
drop table maps2;

-- ALTER TABLE statement
-- beetle 5201

-- Locking syntax
-- negative tests
create table tb1 (country_ISO_code char(2));
alter table tb1 set locking = table;
alter table tb1 set locking = row;

-- Locking syntax 
-- positive tests
-- beetle 5201
create table tb2 (country_ISO_code char(2));
alter table tb2 locksize table;
alter table tb2 locksize row;

-- clean up
drop table tb1;
drop table tb2;

-- VTI in the DELETE statement
-- beetle 5234
CREATE TABLE testCS (col1 int, col2 char(30), col3 int);
INSERT INTO testCS VALUES (100, 'asdf', 732);
DELETE FROM NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') WHERE col1 = 100 and col3 = 732;

-- VTI in the INSERT statement
-- beetle 5234
INSERT INTO NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') VALUES (100, 'asdf', 732);

-- VTI in the SELECT statement
-- beetle 5234
select * from testCS, new org.apache.derbyTesting.functionTests.util.VTIClasses.PositiveInteger_VTICosting_SI(col1, 1) a;

-- VTI in CREATE TRIGGER statement
-- beetle 5234
CREATE TABLE tb1(a int);
CREATE TRIGGER testtrig1 AFTER DELETE ON tb1 FOR EACH ROW MODE DB2SQL INSERT INTO NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') VALUES (1000);

-- VTI in CREATE TRIGGER statement
-- beetle 5234
CREATE TRIGGER testtrig2 AFTER DELETE ON tb1 FOR EACH ROW MODE DB2SQL DELETE FROM NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable('jdbc:derby:wombat', 'testCS') WHERE col1 = 100 and col3 = 732;

-- VTI in CREATE TRIGGER statement
-- beetle 5234
CREATE TRIGGER testtrig3 AFTER DELETE ON tb1 FOR EACH ROW MODE DB2SQL SELECT * FROM testCS, NEW org.apache.derbyTesting.functionTests.util.VTIClasses.PositiveInteger_VTICosting_SI(col1, 1) a;

-- clean up
DROP TABLE tb1;
DROP TABLE testCS;

-- RENAME/DROP COLUMN
-- ALTER RENAME TABLE/COLUMN
-- beetle 5205
create table table tt (a int, b int, c int);
alter table tt drop column b;
alter table tt rename to ttnew;
alter table tt rename c to d;
rename column tt.c to tt.d;
drop table tt;

-- CASCADE/RESTRICT on DROP CONSTRAINT
-- beetle 5204
ALTER TABLE TT DROP CONSTRAINT ABC CASCADE;
ALTER TABLE TT DROP CONSTRAINT ABC2 RESTRICT;

-- CASCADE/RESTRICT on DROP TABLE
-- beetle 5206
DROP TABLE TT CASCADE;
DROP TABLE TT RESTRICT;

-- beetle 5216
-- there should only be one autoincrement column per table 
CREATE TABLE T1 (C1 INT GENERATED ALWAYS AS IDENTITY 
(START WITH 1, INCREMENT BY 1));
-- this statement should raise an error because it has more than one auto increment column in a table
CREATE TABLE T2 (C1 INT GENERATED ALWAYS AS IDENTITY 
(START WITH 1, INCREMENT BY 1), C2 INT GENERATED ALWAYS AS 
IDENTITY (START WITH 1, INCREMENT BY 1));
-- clean up
DROP TABLE t1;
DROP TABLE t2;

-- limit to 16 columns in an index key
-- beetle 5181
-- this create index statement should be successful in db2 compat mode because ix2 specifies 16 columns
create table testindex1 (a int,b int,c int,d int ,e int ,f int,g int,h int,i int,j int,k int,l int,m int,n int,o int,p int);
create unique index ix1 on testindex1(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p);

-- this create index statement should fail in db2 compat mode because ix2 specifies more than 16 columns
create table testindex2 (a int,b int,c int,d int ,e int ,f int,g int,h int,i int,j int,k int,l int,m int,n int,o int,p int,q int);
create unique index ix2 on testindex2(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q);
--clean up
drop table testindex1;
drop table testindex2;

-- insert into a lob column using explicit cast
-- positive test
-- beetle 5221
CREATE TABLE testblob(col1 BLOB(1M));
INSERT INTO testblob (col1) VALUES cast(X'11' as blob(1M));
CREATE TABLE testclob(col1 CLOB(1M));
INSERT INTO testclob (col1) VALUES cast('asdf' as clob(1M));

-- ALTER INDEX
-- beetle 5222
CREATE TABLE TT (A INT);
CREATE INDEX TTIDX ON TT(A);
ALTER INDEX TTIDX RENAME TTIDXNEW;
-- clean up
drop table tt;

-- CREATE and DROP AGGREGATE
-- beetle 5222
CREATE AGGREGATE STDEV FOR org.apache.derbyTesting.functionTests.util.aggregates.StandardDeviation;
DROP AGGREGATE STDEV;
CREATE AGGREGATE MAXBUTONE FOR org.apache.derbyTesting.functionTests.util.aggregates.MaxButOneDef;
DROP AGGREGATE MAXBUTONE;

-- CREATE and DROP CLASS ALIAS
-- beetle 5222
create class alias for java.util.Hashtable;
drop class alias Hashtable;

-- CREATE and DROP METHOD ALIAS
-- beetle 5222
create method alias hashtable for java.lang.Math.sin;
drop method alias hashtable;

-- RENAME COLUMN
-- beetle 5222
create table TT(col1 int, col2 int);
rename column TT.col2 to newcolumn2;
drop table TT;

-- SET TRIGGERS
-- beetle 5222
CREATE TABLE tb1 (col1 int, col2 int, col3 int, constraint chk1 check (col1 > 0));
CREATE TABLE tb2 (col1 char(30), c2 int, c3 int);
CREATE TRIGGER testtrig2 AFTER UPDATE on tb1
REFERENCING OLD as oldtable FOR EACH ROW MODE DB2SQL INSERT INTO tb2 VALUES ('tb', oldtable.col1, oldtable.col2);
SET TRIGGERS FOR tb1 ENABLED;
SET TRIGGERS FOR tb1 DISABLED;
SET TRIGGERS testtrig2 ENABLED;
SET TRIGGERS testtrig2 DISABLED;
-- clean up
DROP TRIGGER testtrig1;
DROP TRIGGER testtrig2;
DROP TRIGGER testtrig3;
DROP TABLE tb1;
DROP TABLE tb2;

-- INSTANCEOF in where clause of select, delete, update,
-- beetle 5224
create table t1 (i int, s smallint, c10 char(10), vc30 varchar(30), b boolean);
create table mm (x org.apache.derbyTesting.functionTests.util.ManyMethods);
create table sc (x org.apache.derbyTesting.functionTests.util.SubClass);
select i from t1 where i instanceof java.lang.Integer;
select i from t1 where i instanceof java.lang.Number;
select i from t1 where i instanceof java.lang.Object;
select s from t1 where s instanceof java.lang.Integer;
select b from t1 where b instanceof java.lang.Boolean;
select c10 from t1 where c10 instanceof java.lang.String;
select vc30 from t1 where vc30 instanceof java.lang.String;
-- following are negative test cases because boolean values disallowed in select clause
select x instanceof org.apache.derbyTesting.functionTests.util.ManyMethods from mm; 
select x instanceof org.apache.derbyTesting.functionTests.util.SubClass from mm; 
select x instanceof org.apache.derbyTesting.functionTests.util.SubSubClass from mm; 
select (i + i) instanceof java.lang.Integer from t1;
select (i instanceof java.lang.Integer) = true from t1;
DELETE FROM t1 where i INSTANCEOF 
org.apache.derbyTesting.functionTests.util.serializabletypes.City;
UPDATE t1 SET s = NULL WHERE i INSTANCEOF 
org.apache.derbyTesting.functionTests.util.serializabletypes.City;
-- clean up
drop table t1;
drop table mm;
drop table sc;

-- datatypes
-- beetle 5233
create table testtype1(col1 bit);
create table testtype2(col1 bit varying(10));
-- boolean datatype already disabled
create table testtype3(col1 boolean);
create table testtype4(col1 LONG NVARCHAR);
create table testtype5(col1 LONG VARBINARY);
create table testtype6(col1 LONG BIT VARYING);
create table testtype7(col1 LONG BINARY);
create table testtype8(col1 NCHAR);
create table testtype9(col1 NVARCHAR(10));
-- tinyint datatype already disabled
create table testtype10(col1 TINYINT);
create table testtype11 (a national character large object (1000));
-- beetle5426
-- disable nclob
create table beetle5426 (a nclob (1M));
create table testtype12 (a national char(100));
CREATE CLASS ALIAS FOR org.apache.derbyTesting.functionTests.util.serializabletypes.Tour;
create table testtype13 (a Tour);
-- clean up
drop table testtype1;
drop table testtype2;
drop table testtype3;
drop table testtype4;
drop table testtype5;
drop table testtype6;
drop table testtype7;
drop table testtype8;
drop table testtype9;
drop table testtype10;
drop table testtype11;
drop table beetle5426;
drop table testtype12;
drop class alias Tours;
drop table testtype13;

-- limit char to 254 and varchar to 32672 columns in db2 mode
-- beetle 5552
-- following will fail because char length > 254
create table test1(col1 char(255));
-- following will pass because char length <= 254
create table test1(col1 char(254), col2 char(23));
-- try truncation error with the 2 chars
-- the trailing blanks will not give error
insert into test1 values('a','abcdefghijklmnopqrstuvw   ');
-- the trailing non-blank characters will give error
insert into test1 values('a','abcdefghijklmnopqrstuvwxyz');
insert into test1 values('12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890','a');
drop table test1;
-- following will fail because varchar length > 32672
create table test1(col1 varchar(32673));
-- following will pass because varchar length <= 32672
create table test1(col1 varchar(32672), col2 varchar(1234));
drop table test1;

-- SET CONSTRAINTS statement
-- beetle 5251
CREATE TABLE testsetconst1 (col1 CHAR(7) NOT NULL, PRIMARY KEY(col1));
CREATE TABLE testsetconst2 (col1 char(7) NOT NULL, CONSTRAINT fk FOREIGN KEY(col1) REFERENCES testsetconst1(col1));
SET CONSTRAINTS fk DISABLED;
SELECT STATE FROM SYS.SYSCONSTRAINTS;
SET CONSTRAINTS fk ENABLED;
SELECT STATE FROM SYS.SYSCONSTRAINTS;
SET CONSTRAINTS ALL DISABLED;
SELECT STATE FROM SYS.SYSCONSTRAINTS;
SET CONSTRAINTS FOR testsetconst1 ENABLED;
SELECT STATE FROM SYS.SYSCONSTRAINTS;

-- clean up
DROP TABLE testsetconst1;
DROP TABLE testsetconst2;

-- CALL statement
-- beetle 5252
call org.apache.derby.iapi.db.Factory::getDatabaseOfConnection().dropAllJDBCMetaDataSPSes();

-- Beetle 5203: DB2 restricts what can be used for default clauses, and enforces
-- constraints on the default clause that Cloudscape does not.
-- Following should be okay:
create table deftest1 (i int default 1);
create table deftest2 (vc varchar(30) default 'howdy');
create table deftest21 (vc clob(10) default 'okie');
create table deftest3 (d date default current date);
create table deftest31 (d date default '2004-02-08');
create table deftest5 (vc char(130) default current schema);
create table deftest4 (c char(130) default user);
create table deftest6 (d decimal(5,2) default null);
create table deftest7 (d decimal(5,2) default 123.450);
create table deftest8 (f float default 1.234);
-- make sure they actually work @ insertion.
insert into deftest1 values (default);
insert into deftest2 values (default);
insert into deftest21 values (default);
insert into deftest3 values (default);
insert into deftest31 values (default);
insert into deftest4 values (default);
insert into deftest5 values (default);
insert into deftest6 values (default);
insert into deftest7 values (default);
insert into deftest8 values (default);
-- cleanup.
drop table deftest1;
drop table deftest2;
drop table deftest21;
drop table deftest3;
drop table deftest31;
drop table deftest4;
drop table deftest5;
drop table deftest6;
drop table deftest7;
drop table deftest8;
-- Beetle 5203, con't: following should all fail (though they'd pass in Cloudscape mode).
-- expressions:
create table deftest1 (vc varchar(30) default java.lang.Integer::toBinaryString(3));
create table deftest2 (i int default 3+4);
-- floating point assignment to non-float column.
create table deftest3 (i int default 1.234);
-- decimal value with too much precision.
create table deftest4 (d decimal(5,2) default 1.2234);
-- char constant longer than 254.
create table deftest5 (vc varchar(300) default 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
-- function calls (built-in and other) should fail with error 42894 (NOT with 42X01), to match DB2.
create table t1 (i int default abs(0));
create table t1 (i int default someFunc('hi'));
-- Type mismatches should fail with 42894 (NOT with 42821), to match DB2.
create table t1 (i int default 'hi');

-- Beetle 5281: <cast-function> for a default.
-- Date-time functions (DATE, TIME, and TIMESTAMP)
create table t1a (d date default date(current date));
create table t1b (d date default date('1978-03-22'));
create table t2a (t time default time(current time));
create table t2b (t time default time('08:28:08'));
create table t3a (ch timestamp default timestamp(current timestamp));
create table t3b (ts timestamp default timestamp('2004-04-27 08:59:02.91'));
-- BLOB function (not yet supported).
create table t4 (b blob default blob('nope'));
-- cleanup.
drop table t1a;
drop table t1b;
drop table t2a;
drop table t2b;
drop table t3a;
drop table t3b;

-- DROP constraint syntax that should be supported in db2 compat mode:
-- beetle 5204
CREATE TABLE testconst1 (col1 CHAR(7) NOT NULL, col2 int CONSTRAINT cc CHECK(col2 > 1), PRIMARY KEY(col1));
CREATE TABLE testconst2 (col1 char(7) NOT NULL, col2 char(7) NOT NULL, col3 int, CONSTRAINT fk FOREIGN KEY(col1) REFERENCES testconst1(col1), CONSTRAINT uk UNIQUE (col2));
-- DROP FOREIGN KEY syntax should be supported in DB2 compat mode
insert into testconst1( col1, col2) values( 'a', 2);
insert into testconst1( col1, col2) values( 'a', 2);
insert into testconst1( col1, col2) values( 'b', 0);

insert into testconst2( col1, col2, col3) values( 'a', 'a', 1);
insert into testconst2( col1, col2, col3) values( 'z', 'b', 1);
insert into testconst2( col1, col2, col3) values( 'a', 'a', 1);

-- beetle 5204
ALTER TABLE testconst1 DROP FOREIGN KEY cc;
ALTER TABLE testconst2 DROP UNIQUE fk;
ALTER TABLE testconst2 DROP CHECK fk;

ALTER TABLE testconst2 DROP FOREIGN KEY fk;
-- DROP PRIMARY KEY syntax should be supported in DB2 compat mode
-- beetle 5204
ALTER TABLE testconst1 DROP PRIMARY KEY;
-- DROP UNIQUE KEY syntax should be supported in DB2 compat mode
-- beetle 5204
ALTER TABLE testconst2 DROP UNIQUE uk;
-- DROP CHECK condition syntax should be supported in DB2 compat mode
-- beetle 5204
ALTER TABLE testconst1 DROP CHECK cc;

insert into testconst1( col1, col2) values( 'a', 2);
insert into testconst1( col1, col2) values( 'b', 0);

insert into testconst2( col1, col2, col3) values( 'z', 'b', 1);
insert into testconst2( col1, col2, col3) values( 'a', 'a', 1);

ALTER TABLE testconst2 DROP FOREIGN KEY noSuchConstraint;
ALTER TABLE testconst2 DROP CHECK noSuchConstraint;
ALTER TABLE testconst2 DROP UNIQUE noSuchConstraint;
ALTER TABLE testconst1 DROP PRIMARY KEY;

-- clean up
DROP TABLE testconst1;
DROP TABLE testconst2;

-- CREATE TRIGGERS
-- beetle 5253
CREATE TABLE tb1 (col1 int, col2 int, col3 int, constraint chk1 check (col1 > 0));
CREATE TABLE tb2 (col1 char(30), c2 int, c3 int);
-- change syntax of before to "NO CASCADE BEFORE"
CREATE TRIGGER testtrig1 NO CASCADE BEFORE UPDATE OF col1,col2 on tb1 FOR EACH ROW MODE DB2SQL VALUES 1;
CREATE TRIGGER testtrig2 AFTER UPDATE on tb1
REFERENCING OLD as oldtable FOR EACH ROW MODE DB2SQL INSERT INTO tb2 VALUES ('tb', oldtable.col1, oldtable.col2);
CREATE TRIGGER testtrig3 AFTER UPDATE on tb1
REFERENCING OLD as oldtable FOR EACH ROW MODE DB2SQL INSERT INTO tb2 VALUES ('tb', oldtable.col1, oldtable.col2);
-- clean up
DROP TRIGGER testtrig1;
DROP TRIGGER testtrig2;
DROP TRIGGER testtrig3;
DROP TABLE tb1;
DROP TABLE tb2;

-- SET TRANSACTION ISOLATION LEVEL
-- beetle 5254
-- these SET TRANSACTION ISOLATION statements fail in db2 compat mode because it has cloudscape specific syntax
create table t1(c1 int not null constraint asdf primary key);
insert into t1 values 1;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
-- clean up
drop table t1;

-- statements should pass in db2 compat mode
-- beetle 5260
autocommit off;
create table t1(c1 int not null constraint asdf primary key);
commit;
insert into t1 values 1;
-- verify SET TRANSACTION ISOLATION commits and changes isolation level
set isolation serializable;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- verify SET TRANSACTION ISOLATION commits and changes isolation level
set isolation read committed;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- verify SET TRANSACTION ISOLATION commits and changes isolation level
set isolation repeatable read;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- verify SET TRANSACTION ISOLATION commits and changes isolation level
set isolation read uncommitted;
-- rollback should find nothing to undo
rollback;
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
drop table t1;

-- SET ISOLATION statement
-- beetle 5260
-- set isolation statement that are supported in db2
create table t1(c1 int not null constraint asdf primary key);
insert into t1 values 1;
set isolation serializable;
set isolation read committed;
set isolation repeatable read;
set isolation read uncommitted;
-- clean up
drop table t1;

-- SELECT statement testing
-- beetle 5255

CREATE TABLE t1(col1 int, col2 int);
CREATE TABLE t2(col1 int, col2 int);
INSERT INTO t1 VALUES(3,4);
INSERT INTO t2 VALUES(3,4);

-- CROSS JOIN not supported in both Cloudscape and DB2 mode and that is why rather than getting feature not implemented, we will get syntax error
-- (1) CROSS JOIN should be disabled in FROM clause of SELECT statement
SELECT * FROM t1 CROSS JOIN t2;

-- (2) USING should be disabled in INNER JOIN of SELECT statement
SELECT * FROM t1 INNER JOIN t2 USING (col1);

-- (3) USING should be disabled in INNER JOIN of SELECT statement
SELECT * FROM t1 LEFT OUTER JOIN t2 USING (col1);

-- (4) USING should be disabled in INNER JOIN of SELECT statement
SELECT * FROM t1 RIGHT OUTER JOIN t2 USING (col1);

-- (5) TRUE and FALSE constants should be disabled in WHERE clause of SELECT statement
SELECT * FROM t1 INNER JOIN t2 ON t1.col1 = t2.col1 WHERE true;
SELECT * FROM t1 INNER JOIN t2 ON t1.col1 = t2.col1 WHERE false;

-- (5) TRUE and FALSE constants should be disabled in WHERE clause of DELETE statement
DELETE FROM t1 where true;
DELETE FROM t1 where false;

-- (5) TRUE and FALSE constants should be disabled in WHERE clause of DELETE statement
UPDATE t2 SET col1 = NULL WHERE true;
UPDATE t2 SET col1 = NULL WHERE false;

-- (6) AT ISOLATION clause should be disabled in SELECT statement
-- AT ISOLATION not supported in both Cloudscape and DB2 mode and that is why rather than getting feature not implemented, we will get syntax error
SELECT * FROM t1 AT ISOLATION READ UNCOMMITTED;
SELECT * FROM t1 AT ISOLATION READ COMMITTED;
SELECT * FROM t1 AT ISOLATION SERIALIZABLE;
SELECT * FROM t1 AT ISOLATION REPEATABLE READ;

-- clean up
DROP TABLE t1;
DROP TABLE t2;

-- DEFAULT CAST not supported in both Cloudscape and DB2 mode and that is why rather than getting feature not implemented, we will get syntax error
create table testuser(col1 BLOB(3K) default cast(user as blob(3k)));
create table testsessionuser(col1 BLOB(3K) default cast(session_user as blob(3k)));
create table testcurrentuser(col1 BLOB(3K) default cast(current_user as blob(3k)));
create table testschema(col1 BLOB(3K) default cast(current schema as blob(3k)));

-- alter table syntax that should be supported in db2 compat mode
-- beetle 5267
create table testmodify (col1 varchar(30), col2 int generated always as identity);
-- increasing the length of the varchar column
alter table testmodify alter col1 set data type varchar(60);
-- specifying the interval between consecutive values of col2, the identity column 
alter table testmodify alter col2 set increment by 2;
-- clean up
drop table testmodify;

-- (1) adding more than one column 
-- beetle 5268
-- db2 compat mode should support the following statements
create table testaddcol (col1 int);
alter table testaddcol add column col2 int add col3 int;
drop table testaddcol;

-- (2) adding more than one unique, referential, or check constraint 
-- beetle 5268
-- db2 compat mode should support the following statements
create table testaddconst1 (col1 int not null primary key, col2 int not null unique);
create table testaddconst2 (col1 int not null primary key, col2 int not null unique);
create table testaddconst3 (col1 int not null, col2 int not null, col3 int not null, col4 int not null, col5 int, col6 int);
create table testaddconst4 (col1 int not null, col2 int not null, col3 int not null, col4 int not null, col5 int, col6 int);
-- adding more than one unique-constraint 
alter table testaddconst3 add primary key (col1) add unique (col2);
alter table testaddconst3 add unique (col3) add unique (col4);
-- adding more than one referential-constraint 
alter table testaddconst3 add foreign key (col1) references testaddconst1(col1) add foreign key (col2) references testaddconst2(col2);
-- adding more than one check-constraint 
alter table testaddconst3 add check (col5 is null) add check (col6 is null);
-- adding a primary, unique, foreign key, and  check-constraint 
alter table testaddconst4 add primary key(col1) add unique(col2) add foreign key (col1) references testaddconst1(col1) add check (col2 is null);
-- clean up
drop table testaddconst1;
drop table testaddconst2;
drop table testaddconst3;
drop table testaddconst4;

-- (3) adding more than one unique, referential, or check constraints 
-- beetle 5268
-- syntax that will be supported in db2 compat mode (beetle 5204)
CREATE TABLE testdropconst1 (col1 CHAR(7) NOT NULL, col2 int not null CONSTRAINT uk1 UNIQUE , PRIMARY KEY(col1));
CREATE TABLE testdropconst2 (col1 CHAR(7) NOT NULL, col2 int not null CONSTRAINT uk2 UNIQUE, col3 CHAR(5) not null CONSTRAINT uk3 UNIQUE, PRIMARY KEY(col1));
CREATE TABLE testdropconst3 (col1 CHAR(7) NOT NULL, col2 int not null CONSTRAINT uk4 UNIQUE , PRIMARY KEY(col1));
CREATE TABLE testdropconst4 (col1 CHAR(7) NOT NULL, col2 int not null CONSTRAINT uk5 UNIQUE , PRIMARY KEY(col1));
CREATE TABLE testdropconst5 (col1 CHAR(7) NOT NULL, col2 int, col3 CHAR(5) not null, CONSTRAINT fk1 FOREIGN KEY (col1) REFERENCES testdropconst3(col1), CONSTRAINT fk2 FOREIGN KEY (col1) REFERENCES testdropconst4(col1));
CREATE TABLE testdropconst6 (col1 CHAR(7) CONSTRAINT ck1 CHECK (col1 is null), col2 int CONSTRAINT ck2 CHECK (col2 is null));
-- dropping more than one unique-constraint 
alter table testdropconst1 drop primary key drop constraint uk1;
alter table testdropconst2 drop primary key drop constraint uk2 drop constraint uk3;
-- dropping more than one foreign key constraint
alter table testdropconst5 drop constraint fk1 drop constraint fk2;
-- dropping more than one check constraint
alter table testdropconst6 drop constraint ck1 drop constraint ck2;
--clean up
drop table testdropconst1;
drop table testdropconst2;
drop table testdropconst3;
drop table testdropconst4;
drop table testdropconst5;
drop table testdropconst6;

-- (4) altering more than one column
-- beetle 5268
-- syntax that will be supported in db2 compat mode (beetle 5267)
-- db2 compat mode should support 
create table testmodify (col1 varchar(30), col2 varchar(30));
alter table testmodify alter col1 set data type varchar(60) alter col2 set data type varchar(60);
-- clean up
drop table testmodify;

-- number of values assigned in an INSERT statement should be the same as the number of specified or implied columns 
-- beetle 5269
create table t1(a int, b int, c char(10));
-- this statement should throw an error in db2 compat mode, but it does not
insert into t1 values(1);

-- clean up
drop table t1;

-- beetle 5281
-- These statements are successful in DB2 UDB v8, but not in Cloudscape
-- Cloudscape does not support cast-functions such as blob, timestamp, time, and date
-- DB2 does support cast functions such as these below:
create table t1 (ch blob(10));
insert into t1 values (blob('hmm'));
create table t2 (ch timestamp);
insert into t2 values (timestamp(current timestamp));
create table t3 (ch time);
insert into t3 values (time(current time));
create table t4 (ch date);
insert into t4 values (date(current date));
drop table t1;
drop table t2;
drop table t3;
drop table t4;

-- test operands
-- beetle 5282
-- <,> =, !=, <=, >= operands are not supported in db2 but supported in cloudscape
CREATE TABLE testoperatorclob (colone clob(1K));
INSERT INTO testoperatorclob VALUES (CAST('50' AS CLOB(1K)));
select * from testoperatorclob;
-- these select statements should raise an error but are successful in cloudscape
select * from testoperatorclob where colone > 10;
select * from testoperatorclob where colone < 70;
select * from testoperatorclob where colone = 50;
select * from testoperatorclob where colone != 10;
select * from testoperatorclob where colone <= 70;
select * from testoperatorclob where colone >= 10;
select * from testoperatorclob where colone <> 10;
drop table testoperatorclob;

-- beetle 5282
CREATE TABLE testoperatorblob (colone clob(1K));
INSERT INTO testoperatorblob VALUES (CAST('50' AS BLOB(1K)));
select * from testoperatorblob;
-- these select statements should raise an error but are successful in cloudscape
select * from testoperatorblob where colone > 10;
select * from testoperatorblob where colone < 999999;
select * from testoperatorblob where colone = 00350030;
select * from testoperatorblob where colone != 10;
select * from testoperatorblob where colone <= 999999;
select * from testoperatorblob where colone >= 10;
select * from testoperatorblob where colone <> 10;
drop table testoperatorblob;

-- beetle 5283
-- casting using "X" for hex constant, "B" literal is not allowed in DB2
-- db2 raises ERROR 56098, cloudscape should raise error msg?a
values cast(B'1' as char(100));
values cast(B'1' as clob(1M));
values cast(B'1' as blob(1M));
values cast(X'11' as char(100));
values cast(X'11' as clob(1M));
values cast(X'11' as blob(1M));

-- beetle 5284
-- minor difference in outputs when casting to blob in Cloudscape and DB2. 
values cast('   ' as blob(1M));
values cast('a' as blob(1M));

-- beetle 5294
-- diable column names in the characterExpression and escape clause of a LIKE predicate
create table likeable (match_me varchar(10), pattern varchar(10), esc varchar(1));
insert into likeable values ('foo%bar3', 'fooZ%bar3', 'Z');
select match_me from likeable where match_me like pattern escape esc;
select match_me from likeable where match_me like pattern escape 'Z';
drop table likeable;

-- beetle 5298 
-- disable Field Access
VALUES java.lang.Integer::MAX_VALUE;
VALUES (1)->noSuchField;


-- beetle 5299
-- disable Method Invocations 
VALUES (1)->toString();
VALUES 1.->toString();
VALUES 1..getClass()->toString();
create table m5299 (i int, s varchar(10));
insert into m5299 values(1, 'hello');
select i.hashCode(), s.indexOf('ll') from m5299;
select s.indexOf('ll') from m5299;
drop table m5299;

-- beetle 5307
-- scale of the resulting data type for division 
values(11.0/1111.33);
values (11111111111111111111111111111.10/1.11);
values (11111111111111111111111111111.10/1.1);

-- beetle 5346
-- positive test
-- NULLs sort low in Cloudscape, but sort high in DB2 
create table testOrderBy(c1 int);
insert into testOrderBy values (1);
insert into testOrderBy values (2);
insert into testOrderBy values (null);
select * from testOrderBy order by c1;
drop table testOrderBy;

create table likeable (match_me varchar(10), pattern varchar(10), esc varchar(1), e varchar(1));
insert into likeable values ('foo%bar3', 'fooZ%bar3', 'Z', 'Z');

select match_me from likeable where match_me like 'fooZ%bar3' escape 'Z';
select match_me from likeable where 'foo%bar3' like 'fooZ%bar3' escape 'Z';
select match_me from likeable where 'foo%bar3' like 'foo%';


-- SQLSTATE=42824
select match_me from likeable where match_me like pattern escape esc;
select match_me from likeable where match_me like pattern escape e;
select match_me from likeable where match_me like pattern escape 'Z';
select match_me from likeable where match_me like pattern;
select match_me from likeable where match_me like e;

-- SQLSTATE=22019
select match_me from likeable where match_me like 'fooZ%bar3' escape esc;
select match_me from likeable where match_me like 'fooZ%bar3' escape e;

-- SQLSTATE=42884
select match_me from likeable where match_me like 'fooZ%bar3' escape 1;
select match_me from likeable where match_me like 'fooZ%bar3' escape 1;
select match_me from likeable where 'foo%bar3' like 1;
select match_me from likeable where 1 like 1;
select match_me from likeable where match_me like 1;
-- beetle 5845
select match_me from likeable where match_me like CURRENT_DATE;
create table likes (dt date, tm time, ts timestamp);
insert into likes values (current_date, current_time, current_timestamp);
insert into likes values ('2004-03-03', current_time, current_timestamp);
select * from likes where dt like '2004-03-0_';
select * from likes where tm like '_8:%:1%';
select * from likes where ts like '2004-04-09 08:5%';

drop table likeable;
drop table likes;

-- no schema names in constraint names (beetle 5143)
CREATE TABLE S5143.T5143_1 (C1 int CONSTRAINT S5143.CPK1 PRIMARY KEY);
CREATE TABLE S5143.T5143_2 (C1 int, C2 int, CONSTRAINT S5143.CPK1  PRIMARY KEY(C1,C2));
CREATE TABLE S5143.T5143_3 (C1 int, C2 int, CONSTRAINT S5143.C3 CHECK(C1 > C2));


-- READ ONLY not allowed in "FOR" clause of a select.
create table roTable (i int);
insert into roTable values (8);
select * from roTable for update;
select * from roTable for update of i;
select * from roTable for fetch only;
select * from roTable for read only;
drop table roTable;


-- No support for Java types in CAST statements;

values CAST (NULL AS CLASS java.lang.Integer);
values CAST (NULL AS CLASS com.acme.SomeClass);
values CAST (NULL AS CLASS java.sql.Date);

values CAST (NULL AS java.lang.Integer);
values CAST (NULL AS com.acme.SomeClass);
values CAST (NULL AS java.sql.Date);

values CAST (? AS CLASS java.lang.Integer);
values CAST (? AS CLASS com.acme.SomeClass);
values CAST (? AS CLASS java.sql.Date);

values CAST (? AS java.lang.Integer);
values CAST (? AS com.acme.SomeClass);
values CAST (? AS java.sql.Date);

-- No support for BIT_LENGTH, OCTET_LENGTH, TRIP and SUBSTRING in DB2 compatibility mode

values BIT_LENGTH(X'55');
values OCTET_LENGTH('asdfasdfasdf');
values TRIM('x' FROM 'xasdf x');
values SUBSTRING('12345' FROM 3 FOR 2);

-- Tests for explicit nulls. Not allowed in DB2, defect 5589 
-- Should fail.
create table t1 ( i int null);

-- Should pass.
create table t1 (i int);
insert into t1 values null;

-- Alter table add explict null column should also fail.
alter table t1 add column j int null;

-- Should pass
alter table t1 add column j int;
insert into t1 values (null, null);

drop table t1;

-- Beetle 5538: Match DB2 trigger restrictions.

-- Part I) SQL-Procedure-Statement restrictions:

-- 1) BEFORE triggers: can't have CALL, INSERT, UPDATE, or DELETE as action; when beetle 5253 is resolved, thsese should be changed to "no cascade before", instead of just "before".
create table t1 (i int, j int);
create table t2 (i int);
create trigger trig1a NO CASCADE before insert on t1 for each row mode db2sql insert into t2 values(1);
create trigger trig1b NO CASCADE before insert on t1 for each row mode db2sql update t2 set i=1 where i=2;
create trigger trig1c NO CASCADE before insert on t1 for each row mode db2sql delete from t2 where i=8;
create trigger trig1d NO CASCADE before insert on t1 for each row mode db2sql call procOne();

-- 2) AFTER triggers: can't have CALL as action, but others should still work.
create trigger trig2 after insert on t1 for each row mode db2sql call procOne();
create trigger trig2a after insert on t1 for each row mode db2sql insert into t2 values(1);
create trigger trig2b after insert on t1 for each row mode db2sql update t2 set i=1 where i=2;
create trigger trig2c after insert on t1 for each row mode db2sql delete from t2 where i=8;

-- Part II) Verify applicable restrictions on the "REFERENCES" clause (should be the same as in DB2).

-- 3) NEW, NEW_TABLE only valid with insert and update triggers; OLD, OLD_TABLE
--  only valid with delete and update triggers.

-- Next 8 should succeed.
create trigger trig3a after insert on t1 referencing new as ooga for each row mode db2sql values(1);
create trigger trig3b after update on t1 referencing old as ooga for each row mode db2sql values(1);
create trigger trig3c after update on t1 referencing new as ooga for each row mode db2sql values(1);
create trigger trig3d after delete on t1 referencing old as ooga for each row mode db2sql values(1);
create trigger trig3e after insert on t1 referencing new_table as ooga for each statement mode db2sql values(1);
create trigger trig3f after update on t1 referencing old_table as ooga for each statement mode db2sql values(1);
create trigger trig3g after update on t1 referencing new_table as ooga for each statement mode db2sql values(1);
create trigger trig3h after delete on t1 referencing old_table as ooga for each statement mode db2sql values(1);

-- Next 4 should fail.
create trigger trig3i after insert on t1 referencing old as ooga for each row mode db2sql values(1);
create trigger trig3j after delete on t1 referencing new as ooga for each row mode db2sql values(1);
create trigger trig3k after insert on t1 referencing old_table as ooga for each statement mode db2sql values(1);
create trigger trig3m after delete on t1 referencing new_table as ooga for each statement mode db2sql values(1);

-- 4) NEW_TABLE, OLD_TABLE not valid with BEFORE triggers (these will throw syntax errors until beetle 5253 is resolved).
create trigger trig4a no cascade before update on t1 referencing old_table as ooga for each statement mode db2sql values(1);
create trigger trig4b no cascade before update on t1 referencing new_table as ooga for each statement mode db2sql values(1);

-- 5) OLD, NEW not valid with FOR EACH STATEMENT.
create trigger trig5a after update on t1 referencing old as ooga for each statement mode db2sql values(1);
create trigger trig5b after update on t1 referencing new as ooga for each statement mode db2sql values(1);

-- cleanup for 5538:
drop table t1;
drop table t2;

-- Beetle 5637: Require FOR EACH clause in DB2 mode. Optional in Cloudscape mode. 

create table t1(i int);

-- Should fail
create trigger trig1 after insert on t1 mode db2sql values (8);

-- Should pass
create trigger trig1 after insert on t1 for each row mode db2sql values (8);
create trigger trig2 after insert on t1 for each statement mode db2sql values (8);

drop table t1;

-- match SUBSTR builtin function out of range handling (5570).

create table x1 (c char(10));
insert into x1 values ('foo');

-- DB2: Raises ERROR 22011: out of range, Cloudscape doesn't
select substr('foo', -2,1) from x1;

-- DB2: Raises ERROR 22011: out of range, Cloudscape return NULL
select substr('foo', 1,-1) from x1;
select substr('foo', 2,-1) from x1;
select substr('foo', 3,-2) from x1;
select substr('foo', -2,-3) from x1;

-- DB2: ERROR 22011 out of range, Cloudscape returns empty string
select substr('foo', 5) from x1;
select substr('foo', 6,3) from x1;

-- DB2: Raises ERROR 22011: out of range, Cloudscape returns 'f'
select substr('foo', 0,1) from x1;

-- DB2: ERROR 22011 out of range, Cloudscape return 'foo'
select substr('foo', 1,4) from x1;

-- DB2: ERROR 22011 out of range, Cloudscape return 'foo'
select substr('foo', -5) from x1;

-- DB2: ERROR 22011 out of range
select substr('foo', -6,3) from x1;

-- DB2: Returns an empty value, Cloudscape returns NULL
select substr('foo', 1,0) from x1;
select substr('foo', 2,0) from x1;
select substr('foo', 3,0) from x1;

-- DB2: Raises ERROR 22011: out of range, Cloudscape returns NULL
select substr('foo', 4,0) from x1;
select substr('foo', 5,0) from x1;
select substr('foo', 6,0) from x1;

-- Beetle 5630: A column check constraint can only refer to that column in DB2

create table t1(c1 int, c2 int check (c1 > 5));
-- check constraint ck1 in the column-definition of c2 can not refer to column c1
create table t1(c1 int, c2 int constraint ck1 check(c1 > c2));

-- Same test with alter table
create table t1(c1 int);
alter table t1 add column c2 int constraint ck2 check(c2 > c1);

-- These should pass, uses table constraints
create table t2(c1 int, c2 int, check (c1 > 5));
create table t3(i int, j int, check (j > 5));
alter table t1 add column c2 int;
alter table t1 add constraint t1con check(c2 > c1);

drop table t1;
drop table t2;
drop table t3;

-- Beetle 5638: DB2 requires matching target and result columns for insert

create table t1 ( i int, j int);
create table t2 ( i int, j int);

insert into t1 values (1, 1);
insert into t2 values (2, 2);

-- negative tests, mismatch of columns
insert into t1 select i from t2;
insert into t1(i) select * from t2;
insert into t1(i, j) select j from t2;

insert into t1 select * from t2 union select i from t2;
insert into t1 select j from t2 union select j from t2;
insert into t1(i) select * from t2 union all select * from t2;
insert into t1(i, j) select i, j from t2 union all i from t2;

-- positive cases
insert into t1 select * from t2;
select * from t1;
insert into t1(i,j) select * from t2 union select i, j from t2;
insert into t1(i) select i from t2 union all select j from t2;
select * from t1;

drop table t1;
drop table t2;

-- Beetle 5667: DB2 requires non-nullable columns to have a default in ALTER TABLE

create table t1( i int);

-- Negative cases
alter table t1 add column j int not null;
alter table t1 add column j int not null default null;

-- positive cases
alter table t1 add column j int;
alter table t1 add column k int not null default 5;

drop table t1;

-- IS [NOT] TRUE/FALSE/UNKNOWN not supported in both Cloudscape and DB2 mode and that is why rather than getting feature not implemented, we will get syntax error
-- 
create table t1( i int);
select * from t1 where ((1=1) IS TRUE);
select * from t1 where ((1=1) IS NOT TRUE);
select * from t1 where ((1=0) IS FALSE);
select * from t1 where ((1=0) IS NOT FALSE);
select * from t1 where (null IS UNKNOWN);
drop table t1;

-- Beetle 5635, 5645 and 5633: Generated column name issues

create table t1(i int, j int);
create table t2(c1 int, c2 int);

insert into t1 values (1, 1);
insert into t2 values (2, 2);

-- Cloudscape should generate column names when both sides of union don't match
 
select i,j from t1
union all
select c1,c2 from t2
order by 1;

select i as c1, j as c2 from t1
union all
select c1, c2 from t2
order by 1;

-- Prevent Cloudscape from using generated column names for ordering

select i+1 from t1 order by "SQLCol1";
select i+1 from t1 order by SQLCol1;

values (1,2,3),(4,5,6),(7,8,9) order by "SQLCol1";

-- Column names for a CREATE VIEW should be specified when result table has unnamed columns. 

create view v1 as values 1;
create view v1 as select i+1 from t1;
create view v1 as select i+1 as i from t1;
create view v2(c) as select i+1 from t1;

drop view v1;
drop view v2;
drop table t1;
drop table t2;


-- ALTER TABLE COMPRESS statement is cloudscape specific, disable in db2 mode
-- beetle 5553
--   TODO - not working yet

-- negative tests
create table tb1 (country_ISO_code char(2));
alter table tb1 compress;
alter table tb1 compress sequential;

-- clean up
drop table tb1;

-- Beetle 5717: Disable adding primary or unique constraints on non-nullable columns
 
-- negative tests
create table t1 (c1 int, c2 int);
alter table t1 add constraint pk1 primary key (c1);
alter table t1 add constraint uc1 unique (c2);

-- positive tests
create table t2 (c1 int not null, c2 char(10) not null);
alter table t2 add constraint pk2 primary key (c1);
alter table t2 add constraint uc2 unique (c2);

drop table t1;
drop table t2;


-- SET STATISTICS TIMING ON stmt is cloudscape specific, disabled in db2 mode
-- Once we have rewritten our functions to not use following sql, SET STATISTICS TIMING can be completely removed from the parser.
set statistics timing on;

-- SET RUNTIMESTATISTICS ON stmt is cloudscape specific, disabled in db2 mode
-- Once we have rewritten our functions to not use following sql, SET RUNTIMESTATISTICS can be completely removed from the parser.
set runtimestatistics on;

-- following runtime statistics related sql will fail in db2 mode but will run fine in Cloudscape mode
create table t1 (c1 int, c2 int);
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
select * from t1;
values runtimestatistics()->getScanStatisticsText();
values runtimestatistics()->toString();
-- following runtime statistics related sql is not supported anymore and will not run in any mode
UPDATE STATISTICS FOR TABLE T1;
DROP STATISTICS FOR TABLE T1;
drop table t1;
