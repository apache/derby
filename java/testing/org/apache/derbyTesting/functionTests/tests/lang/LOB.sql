-- This test lang/LOB.sql still includes tests for 
-- DB2 UDB incompatible datatype NCLOB.
-- Still waiting for DB2 UDB compatible functionality for NCLOB to be implemented

-- Also note that in DB2 UDB, to create BLOB and CLOB strings greater than 1 gigabyte,
-- the NOT LOGGED option must be specified (SQLSTATE 42993).


-- test that BLOB/CLOB are not reserved words
create table blob(a int);
insert into blob values(3);
select blob.a from blob;

create table clob(a int);
insert into clob values(3);
select clob.a from clob;

create table nclob(a int);
insert into nclob values(3);
select nclob.a from nclob;

create table a(blob int, clob int, nclob int);
insert into a values(1,2,3);
insert into a(blob, clob, nclob) values(1,2,3);
select a.blob, a.clob, a.nclob from a;
select a.blob, a.clob, a.nclob from a where a.blob = 1;
select a.blob, a.clob, a.nclob from a where a.clob = 2;
select a.blob, a.clob, a.nclob from a where a.nclob = 3;
select a.blob, a.clob, a.nclob from a where a.blob = 1 and a.clob = 2 and a.nclob = 3;

create table b(blob blob(3K), clob clob(2M));
insert into b values(cast(X'0031' as blob(3K)),cast('2' as clob(2M)));
insert into b(blob, clob, nclob) values(cast(X'0031' as blob(3K)),cast('2' as clob(2M)));
select b.blob, b.clob, b.nclob from b;

-- equal tests are not allowed
select 1 from b where cast(X'e0' as blob(5))=cast(X'e0' as blob(5));
select 1 from b where cast(X'e0' as blob(5))=cast(X'e0' as blob(7));
select 1 from b where cast(X'e0' as blob(5))=cast(X'e0' as blob(7));
select 1 from b where cast(X'e0' as blob(5))=cast(X'e0' as blob(7));
select 1 from b where cast(X'e0' as blob(5))=cast(X'e000' as blob(7));

select 1 from b where X'80' = cast(X'80' as blob(1));
select 1 from b where cast(X'80' as blob(1)) = X'80';
select 1 from b where cast(X'80' as blob(1)) = cast(X'80' as blob(1));

select 1 from b where '1' = cast('1' as clob(1));
select 1 from b where cast('1' as clob(1)) = '1';
select 1 from b where cast('1' as clob(1)) = cast('1' as clob(1));

select 1 from b where '1' = cast('1' as nclob(1));
select 1 from b where cast('1' as nclob(1)) = '1';
select 1 from b where cast('1' as nclob(1)) = cast('1' as nclob(1));

-- NCLOB is comparable with CLOB

select 1 from b where cast('1' as nclob(10)) = cast('1' as clob(10));
select 1 from b where cast('1' as clob(10)) = cast('1' as nclob(10));

-- comparsion using tables
select * from b as b1, b as b2 where b1.blob=b2.blob;
select * from b as b1, b as b2 where b1.blob!=b2.blob;

select * from b as b1, b as b2 where b1.blob=X'20';
select * from b as b1, b as b2 where X'20'=b1.blob;
select * from b as b1, b as b2 where X'20'!=b1.blob;

select * from b as b1, b as b2 where b1.blob=X'7575';
select * from b as b1, b as b2 where X'7575'=b1.blob;

select b.blob, b.clob, b.nclob from b where b.blob = '1' and b.clob = '2' and b.nclob = '3';
select b.blob from b where b.blob = '1';
-- however it works for types which cloudscape autocasts to char
select b.clob from b where b.clob = '2';
select b.nclob from b where b.nclob = '3';

-- test insert of NULL
insert into b values(null, null, null);
select * from b;

-- cleanup
drop table blob;
drop table clob;
drop table nclob;
drop table a;
drop table b;

-- test insert limitations
create table b(b blob(5));
create table c(c clob(5));
create table n(n nclob(5));

insert into b values(cast(X'01020304' as blob(10)));
insert into b values(cast(X'0102030405' as blob(10)));
insert into b values(cast(X'010203040506' as blob(10)));

-- truncate before insert, no errors
insert into b values(cast(X'01020304' as blob(5)));
insert into b values(cast(X'0102030405' as blob(5)));
insert into b values(cast(X'010203040506' as blob(5)));

-- clob/nclob
--   ok in spite of not being cast
insert into c values('1234');
insert into c values('12345');
insert into c values('123456');

insert into n values('1234');
insert into n values('12345');
insert into n values('123456');

--   ok
insert into c values(cast('1234' as clob(5)));
insert into c values(cast('12345' as clob(5)));
insert into c values(cast('123456' as clob(5)));

insert into n values(cast('1234' as nclob(5)));
insert into n values(cast('12345' as nclob(5)));
insert into n values(cast('123456' as nclob(5)));

select * from b;
select * from c;
select * from n;

-- concatenate
values cast('12' as clob(2)) || cast('34' as clob(2));
values cast('12' as nclob(2)) || cast('34' as nclob(2));
select 1 from b where cast('12' as clob(2)) || cast('34' as clob(2)) = '1234';
select 1 from b where cast('12' as nclob(2)) || cast('34' as nclob(2)) = '1234';
select 1 from b where cast('12' as clob(2)) || cast('34' as clob(2)) = cast('1234' as clob(4));
select 1 from b where cast('12' as nclob(2)) || cast('34' as nclob(2)) = cast('1234' as clob(4));

-- like
select * from b where b like '0102%';
select * from c where c like '12%';
select * from n where n like '12%';

select * from b where b like cast('0102%' as blob(10));
select * from c where c like cast('12%' as clob(10));
select * from n where n like cast('12%' as nclob(10));

-- cleanup
drop table b;
drop table c;
drop table n;

-- test syntax of using long type names
create table a(a binary large object(3K));
create table b(a character large object(3K));
create table c(a national character large object(3K));
create table d(a char large object(204K));

-- create index (not allowed)
create index ia on a(a);
create index ib on b(a);
create index ic on c(a);
create index id on d(a);

-- cleanup
drop table a;
drop table c;
drop table d;

-- ORDER tests on LOB types (not allowed)
select 1 from b where cast(X'e0' as blob(5))=cast(X'e0' as blob(5));
select 1 from b where cast(X'e0' as blob(5))!=cast(X'e0' as blob(5));
select 1 from b where cast(X'e0' as blob(5))<cast(X'e0' as blob(5));
select 1 from b where cast(X'e0' as blob(5))>cast(X'e0' as blob(7));
select 1 from b where cast(X'e0' as blob(5))<=cast(X'e0' as blob(7));
select 1 from b where cast(X'e0' as blob(5))>=cast(X'e0' as blob(7));

select 1 from b where cast('fish' as clob(5))=cast('fish' as clob(5));
select 1 from b where cast('fish' as clob(5))!=cast('fish' as clob(5));
select 1 from b where cast('fish' as clob(5))<cast('fish' as clob(5));
select 1 from b where cast('fish' as clob(5))>cast('fish' as clob(7));
select 1 from b where cast('fish' as clob(5))<=cast('fish' as clob(7));
select 1 from b where cast('fish' as clob(5))>=cast('fish' as clob(7));

select 1 from b where cast('fish' as nclob(5))=cast('fish' as nclob(5));
select 1 from b where cast('fish' as nclob(5))!=cast('fish' as nclob(5));
select 1 from b where cast('fish' as nclob(5))<cast('fish' as nclob(5));
select 1 from b where cast('fish' as nclob(5))>cast('fish' as nclob(7));
select 1 from b where cast('fish' as nclob(5))<=cast('fish' as nclob(7));
select 1 from b where cast('fish' as nclob(5))>=cast('fish' as nclob(7));

-- test operands on autocast
-- beetle 5282
-- <,> <=, >= operands are not supported in db2 but supported in cloudscape
-- compare w. integer/char types are also not ok

-- CLOB testing
CREATE TABLE testoperatorclob (colone clob(1K));
INSERT INTO testoperatorclob VALUES (CAST('50' AS CLOB(1K)));
INSERT INTO testoperatorclob VALUES (CAST(cast('50' as varchar(80)) AS CLOB(1K)));
select * from testoperatorclob;

-- these select statements should raise an error but are successful in cloudscape
select * from testoperatorclob where colone > 10;
select * from testoperatorclob where colone > 5;
select * from testoperatorclob where colone < 70;
select * from testoperatorclob where colone = 50;
select * from testoperatorclob where colone != 10;
select * from testoperatorclob where colone <= 70;
select * from testoperatorclob where colone >= 10;
select * from testoperatorclob where colone <> 10;

select * from testoperatorclob where colone > '10';
select * from testoperatorclob where colone > '5';
select * from testoperatorclob where colone < '70';
select * from testoperatorclob where colone = '50';
select * from testoperatorclob where colone != '10';
select * from testoperatorclob where colone <= '70';
select * from testoperatorclob where colone >= '10';
select * from testoperatorclob where colone <> '10';

drop table testoperatorclob;

-- BLOB testing
CREATE TABLE testoperatorblob (colone blob(1K));
INSERT INTO testoperatorblob VALUES (CAST('50' AS BLOB(1K)));
INSERT INTO testoperatorblob VALUES (CAST(cast('50' as varchar(80)) AS BLOB(1K)));
select * from testoperatorblob;

-- these select statements should raise an error but are successful in cloudscape
select * from testoperatorblob where colone > 10;
select * from testoperatorblob where colone > 5;
select * from testoperatorblob where colone < 999999;
select * from testoperatorblob where colone = 00350030;
select * from testoperatorblob where colone != 10;
select * from testoperatorblob where colone <= 999999;
select * from testoperatorblob where colone >= 10;
select * from testoperatorblob where colone <> 10;

select * from testoperatorblob where colone > '10';
select * from testoperatorblob where colone > '5';
select * from testoperatorblob where colone < '70';
select * from testoperatorblob where colone = '50';
select * from testoperatorblob where colone != '10';
select * from testoperatorblob where colone <= '70';
select * from testoperatorblob where colone >= '10';
select * from testoperatorblob where colone <> '10';

drop table testoperatorblob;

-- NCLOB testing
CREATE TABLE testoperatornclob (colone nclob(1K));
INSERT INTO testoperatornclob VALUES (CAST('50' AS NCLOB(1K)));
INSERT INTO testoperatornclob VALUES (CAST(cast('50' as varchar(80)) AS NCLOB(1K)));
select * from testoperatornclob;

-- these select statements should raise an error but are successful in cloudscape
select * from testoperatornclob where colone > 10;
select * from testoperatornclob where colone > 5;
select * from testoperatornclob where colone < 70;
select * from testoperatornclob where colone = 50;
select * from testoperatornclob where colone != 10;
select * from testoperatornclob where colone <= 70;
select * from testoperatornclob where colone >= 10;
select * from testoperatornclob where colone <> 10;

select * from testoperatornclob where colone > '10';
select * from testoperatornclob where colone > '5';
select * from testoperatornclob where colone < '70';
select * from testoperatornclob where colone = '50';
select * from testoperatornclob where colone != '10';
select * from testoperatornclob where colone <= '70';
select * from testoperatornclob where colone >= '10';
select * from testoperatornclob where colone <> '10';

drop table testoperatornclob;

----- test method invocations on LOB objects (should disallow)
-- setup
drop table b;
create table b(b blob(77));
insert into b values(cast('33' as blob(77)));

create table c(c clob(77));
insert into c values(cast('33' as clob(77)));

-- LOB as main object for method invocation not allowed
values (cast('1' as blob(1M)))->toString();
values (cast('1' as clob(1M)))->toString();
values (cast('1' as nclob(1M)))->toString();

-- LOB column as parameter not allowed
select b->equals('3') from b;
select c->equals('3') from c;

-- explicit LOB as parameter not allowed
values '3'->equals(cast('3' as blob(7)));
values '3'->equals(cast('3' as clob(7)));

-- LOB column as parameter not allowed
select '3'->equals(b) from b;
select '3'->equals(c) from c;

drop table b;
drop table c;

------ TEST length functions on LOBs
---- BLOB
values length(cast('foo' as blob(10)));
values {fn length(cast('foo' as blob(10)))};

---- CHAR
values length(cast('foo' as char(10)));
values {fn length(cast('foo' as char(10)))};

---- CLOB
values length(cast('foo' as clob(10)));
values {fn length(cast('foo' as clob(10)))};

---- NCLOB
values length(cast('foo' as nclob(10)));
values {fn length(cast('foo' as nclob(10)))};

-- Longvarchar negative tests

create table testPredicate1 (c1 long varchar);
create table testPredicate2 (c1 long varchar);
insert into testPredicate1 (c1) values 'a';
insert into testPredicate2 (c1) values 'a';

-- UNION
select * from testPredicate1 union select * from testPredicate2;

-- IN predicate
select c1 from testPredicate1 where c1 IN (select c1 from testPredicate2);


-- NOT IN predicate
select c1 from testPredicate1 where c1 NOT IN (select c1 from testPredicate2);

-- ORDER BY clause
select * from testPredicate1 order by c1;


-- GROUP BY clause
select substr(c1,1,2) from testPredicate1 group by c1;

-- JOIN
select * from testPredicate1 t1, testPredicate2 t2 where t1.c1=t2.c1;
select * from testPredicate1 LEFT OUTER JOIN testPredicate2 on testPredicate1.c1=testPredicate2.c1;

-- PRIMARY KEY
create table testConst1(c1 long varchar not null primary key);

-- UNIQUE KEY constraints
CREATE TABLE testconst2 (col1 long varchar not null, CONSTRAINT uk UNIQUE (col1));

-- FOREIGN KEY constraints
create table testConst3 (c1 char(10) not null, primary key (c1));
create table testConst4 (c1 long varchar not null, constraint fk foreign key (c1) references testConst3 (c1));
drop table testConst3;

-- MAX aggregate function
select max(c1) from testPredicate1;

-- MIN aggregate function
select min(c1) from testPredicate1;

drop table testpredicate1;
drop table testpredicate2;

-- CLOB/BLOB limits and sizes

-- FAIL - bigger than 2G or 2Gb with no modifier
create table DB2LIM.FB1(FB1C BLOB(3G));
create table DB2LIM.FB2(FB2C BLOB(2049M));
create table DB2LIM.FB3(FB3C BLOB(2097153K));
create table DB2LIM.FB4(FB4C BLOB(2147483648));

-- OK 2G and end up as 2GB - 1 (with modifier)
create table DB2LIM.GB1(GB1C BLOB(2G));
create table DB2LIM.GB2(GB2C BLOB(2048M));
create table DB2LIM.GB3(GB3C BLOB(2097152K));
create table DB2LIM.GB4(GB4C BLOB(2147483647));

-- next lower value
create table DB2LIM.GB5(GB5C BLOB(1G));
create table DB2LIM.GB6(GB6C BLOB(2047M));
create table DB2LIM.GB7(GB7C BLOB(2097151K));
create table DB2LIM.GB8(GB8C BLOB(2147483646));
drop table DB2LIM.GB5;
drop table DB2LIM.GB6;
drop table DB2LIM.GB7;
drop table DB2LIM.GB8;

-- no length (default to 1Mb)
create table DB2LIM.GB9(GB9C BLOB);
create table DB2LIM.GB10(GB10C BINARY LARGE OBJECT);
drop table DB2LIM.GB9;
drop table DB2LIM.GB10;

-- FAIL - bigger than 2G or 2Gb with no modifier
create table DB2LIM.FC1(FC1C CLOB(3G));
create table DB2LIM.FC2(FC2C CLOB(2049M));
create table DB2LIM.FC3(FC3C CLOB(2097153K));
create table DB2LIM.FC4(FC4C CLOB(2147483648));

-- OK 2G and end up as 2GC - 1 (with modifier)
create table DB2LIM.GC1(GC1C CLOB(2G));
create table DB2LIM.GC2(GC2C CLOB(2048M));
create table DB2LIM.GC3(GC3C CLOB(2097152K));
create table DB2LIM.GC4(GC4C CLOB(2147483647));

-- next lower value
create table DB2LIM.GC5(GC5C CLOB(1G));
create table DB2LIM.GC6(GC6C CLOB(2047M));
create table DB2LIM.GC7(GC7C CLOB(2097151K));
create table DB2LIM.GC8(GC8C CLOB(2147483646));
drop table DB2LIM.GC5;
drop table DB2LIM.GC6;
drop table DB2LIM.GC7;
drop table DB2LIM.GC8;

-- no length (default to 1Mb)
create table DB2LIM.GC9(GC9C CLOB);
create table DB2LIM.GC10(GC10C CHARACTER LARGE OBJECT);
create table DB2LIM.GC11(GC11C CHAR LARGE OBJECT);
drop table DB2LIM.GC9;
drop table DB2LIM.GC10;
drop table DB2LIM.GC11;

SELECT CAST (TABLENAME AS CHAR(10)) AS T, CAST (COLUMNNAME AS CHAR(10)) AS C, CAST (COLUMNDATATYPE AS CHAR(30)) AS Y
	FROM SYS.SYSTABLES T, SYS.SYSSCHEMAS S, SYS.SYSCOLUMNS C
	WHERE S.SCHEMAID = T.SCHEMAID AND S.SCHEMANAME = 'DB2LIM'
	AND C.REFERENCEID = T.TABLEID
	ORDER BY 1;

--- CHAR/VARCHAR and LOBs. (beetle 5741)
--- test that we can insert CHAR/VARCHAR directly

CREATE TABLE b (colone blob(1K));

VALUES '50';
INSERT INTO b VALUES '50';

VALUES cast('50' as varchar(80));
INSERT INTO b VALUES cast('50' as varchar(80));

VALUES (CAST('50' AS BLOB(1K)));
INSERT INTO b VALUES (CAST('50' AS BLOB(1K)));

VALUES (CAST(cast('50' as varchar(80)) AS BLOB(1K)));
INSERT INTO b VALUES (CAST(cast('50' as varchar(80)) AS BLOB(1K)));

VALUES cast('50' as long varchar);
INSERT INTO b VALUES cast('50' as long varchar);

-- test w LOBs
VALUES (CAST('50' AS BLOB(1K)));
INSERT INTO b VALUES (CAST('50' AS BLOB(1K)));

VALUES (CAST('50' AS CLOB(1K)));
INSERT INTO b VALUES (CAST('50' AS CLOB(1K)));

VALUES (CAST('50' AS NCLOB(1K)));
INSERT INTO b VALUES (CAST('50' AS NCLOB(1K)));

DROP TABLE b;



CREATE TABLE c (colone clob(1K));

VALUES '50';
INSERT INTO c VALUES '50';

VALUES cast('50' as varchar(80));
INSERT INTO c VALUES cast('50' as varchar(80));

VALUES (CAST('50' AS CLOB(1K)));
INSERT INTO c VALUES (CAST('50' AS CLOB(1K)));

VALUES (CAST(cast('50' as varchar(80)) AS CLOB(1K)));
INSERT INTO c VALUES (CAST(cast('50' as varchar(80)) AS CLOB(1K)));

VALUES cast('50' as long varchar);
INSERT INTO c VALUES cast('50' as long varchar);

-- test w LOBs
VALUES (CAST('50' AS BLOB(1K)));
INSERT INTO c VALUES (CAST('50' AS BLOB(1K)));

VALUES (CAST('50' AS CLOB(1K)));
INSERT INTO c VALUES (CAST('50' AS CLOB(1K)));

VALUES (CAST('50' AS NCLOB(1K)));
INSERT INTO c VALUES (CAST('50' AS NCLOB(1K)));

DROP TABLE c;



CREATE TABLE n (colone clob(1K));

VALUES '50';
INSERT INTO n VALUES '50';

VALUES cast('50' as varchar(80));
INSERT INTO n VALUES cast('50' as varchar(80));

VALUES (CAST('50' AS CLOB(1K)));
INSERT INTO n VALUES (CAST('50' AS CLOB(1K)));

VALUES (CAST(cast('50' as varchar(80)) AS CLOB(1K)));
INSERT INTO n VALUES (CAST(cast('50' as varchar(80)) AS CLOB(1K)));

VALUES cast('50' as long varchar);
INSERT INTO n VALUES cast('50' as long varchar);

-- test w LOBs
VALUES (CAST('50' AS BLOB(1K)));
INSERT INTO n VALUES (CAST('50' AS BLOB(1K)));

VALUES (CAST('50' AS CLOB(1K)));
INSERT INTO n VALUES (CAST('50' AS CLOB(1K)));

VALUES (CAST('50' AS NCLOB(1K)));
INSERT INTO n VALUES (CAST('50' AS NCLOB(1K)));

DROP TABLE n;
