-- Test various functions

create table alltypes(
  id int not null primary key,
  smallIntCol smallint,
  intCol int,
  bigIntCol bigint,
  floatCol float,
  float1Col float(1),
  float26Col float(26),
  realCol real,
  doubleCol double,
  decimalCol decimal,
  decimal10Col decimal(10),
  decimal11Col decimal(11),
  numeric10d2Col numeric(10,2),
  charCol char,
  char32Col char(32),
  charForBitCol char(16) for bit data,
  varcharCol varchar(64),
  varcharForBitCol varchar(64) for bit data,
  longVarcharCol long varchar,
  blobCol blob(10k),
  clobCol clob(10k),
  dateCol date,
  timeCol time,
  timestampCol timestamp);
insert into allTypes(id) values(1),(2);
update allTypes set smallIntCol = 2 where id = 1;
update allTypes set intCol = 2 where id = 1;
update allTypes set bigIntCol = 3 where id = 1;
update allTypes set floatCol = 4.1 where id = 1;
update allTypes set float1Col = 5 where id = 1;
update allTypes set float26Col = 6.1234567890123456 where id = 1;
update allTypes set realCol = 7.2 where id = 1;
update allTypes set doubleCol = 8.2 where id = 1;
update allTypes set decimalCol = 9 where id = 1;
update allTypes set decimal10Col = 1234 where id = 1;
update allTypes set decimal11Col = 1234 where id = 1;
update allTypes set numeric10d2Col = 11.12 where id = 1;
update allTypes set charCol = 'a' where id = 1;
update allTypes set char32Col = 'abc' where id = 1;
update allTypes set charForBitCol = X'ABCD' where id = 1;
update allTypes set varcharCol = 'abcde' where id = 1;
update allTypes set varcharForBitCol = X'ABCDEF' where id = 1;
update allTypes set longVarcharCol = 'abcdefg' where id = 1;
update allTypes set blobCol = cast( X'0031' as blob(10k)) where id = 1;
update allTypes set clobCol = 'clob data' where id = 1;
update allTypes set dateCol = date( '2004-3-13') where id = 1;
update allTypes set timeCol = time( '16:07:21') where id = 1;
update allTypes set timestampCol = timestamp( '2004-3-14 17:08:22.123456') where id = 1;

select id, length(smallIntCol) from allTypes order by id;
select id, length(intCol) from allTypes order by id;
select id, length(bigIntCol) from allTypes order by id;
select id, length(floatCol) from allTypes order by id;
select id, length(float1Col) from allTypes order by id;
select id, length(float26Col) from allTypes order by id;
select id, length(realCol) from allTypes order by id;
select id, length(doubleCol) from allTypes order by id;
select id, length(decimalCol) from allTypes order by id;
select id, length(decimal10Col) from allTypes order by id;
select id, length(decimal11Col) from allTypes order by id;
select id, length(numeric10d2Col) from allTypes order by id;
select id, length(charCol) from allTypes order by id;
select id, length(char32Col) from allTypes order by id;
select id, length(charForBitCol) from allTypes order by id;
select id, length(varcharCol) from allTypes order by id;
select id, length(varcharForBitCol) from allTypes order by id;
select id, length(longVarcharCol) from allTypes order by id;
select id, length(blobCol) from allTypes order by id;
select id, length(clobCol) from allTypes order by id;
select id, length(dateCol) from allTypes order by id;
select id, length(timeCol) from allTypes order by id;
select id, length(timestampCol) from allTypes order by id;

-- try length of constants
values( length( 1), length( 720176), length( 12345678901));
values( length( 2.2E-1));
values( length( 1.), length( 12.3), length( 123.4), length( 123.45));
values( length( '1'), length( '12'));
values( length( X'00'), length( X'FF'), length( X'FFFF'));
values( length( date('0001-1-1')), length( time('0:00:00')), length( timestamp( '0001-1-1 0:00:00')));

-- try a length in the where clause
select id from allTypes where length(smallIntCol) > 5 order by id;
select id from allTypes where length(intCol) > 5 order by id;
select id from allTypes where length(bigIntCol) > 5 order by id;
select id from allTypes where length(floatCol) > 5 order by id;
select id from allTypes where length(float1Col) > 5 order by id;
select id from allTypes where length(float26Col) > 5 order by id;
select id from allTypes where length(realCol) > 5 order by id;
select id from allTypes where length(doubleCol) > 5 order by id;
select id from allTypes where length(decimalCol) > 5 order by id;
select id from allTypes where length(decimal10Col) > 5 order by id;
select id from allTypes where length(decimal11Col) > 5 order by id;
select id from allTypes where length(numeric10d2Col) > 5 order by id;
select id from allTypes where length(charCol) > 5 order by id;
select id from allTypes where length(char32Col) > 5 order by id;
select id from allTypes where length(charForBitCol) > 5 order by id;
select id from allTypes where length(varcharCol) > 5 order by id;
select id from allTypes where length(varcharForBitCol) > 5 order by id;
select id from allTypes where length(longVarcharCol) > 5 order by id;
select id from allTypes where length(blobCol) > 5 order by id;
select id from allTypes where length(clobCol) > 5 order by id;
select id from allTypes where length(dateCol) > 5 order by id;
select id from allTypes where length(timeCol) > 5 order by id;
select id from allTypes where length(timestampCol) > 5 order by id;

-- try an expression
select id, length( charCol || 'abc') from allTypes order by id;


-- bug 5761 & 5627
-- JDBC escape length function has the following behavior
-- LENGTH (RTRIM (xxxx))
values {FN LENGTH('xxxx                    ')};
values {FN LENGTH(' xxxx                    ')};
values {FN LENGTH('  xxxx                    ')};
values {FN LENGTH('   xxxx                    ')};


CREATE FUNCTION SV_RNNI(P1 INT) RETURNS VARCHAR(10) RETURNS NULL ON NULL INPUT EXTERNAL NAME 'java.lang.String.valueOf' LANGUAGE JAVA PARAMETER STYLE JAVA;
CREATE FUNCTION SV_CNI(P1 INT) RETURNS VARCHAR(10) CALLED ON NULL INPUT EXTERNAL NAME 'java.lang.String.valueOf' LANGUAGE JAVA PARAMETER STYLE JAVA;
CREATE FUNCTION SV_DEF(P1 INT) RETURNS VARCHAR(10) EXTERNAL NAME 'java.lang.String.valueOf' LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE FUNCTION MAX_RNNI(P1 INT, P2 INT) RETURNS INT RETURNS NULL ON NULL INPUT EXTERNAL NAME 'java.lang.Math.max' LANGUAGE JAVA PARAMETER STYLE JAVA;
CREATE FUNCTION MAX_CNI(P1 INT, P2 INT) RETURNS INT CALLED ON NULL INPUT EXTERNAL NAME 'java.lang.Math.max' LANGUAGE JAVA PARAMETER STYLE JAVA;
CREATE FUNCTION MAX_DEF(P1 INT, P2 INT) RETURNS INT EXTERNAL NAME 'java.lang.Math.max' LANGUAGE JAVA PARAMETER STYLE JAVA;


VALUES SV_RNNI(3);
VALUES SV_CNI(4);
VALUES SV_DEF(5);

create table SV_TAB(I INT);
insert into SV_TAB values(null);
insert into SV_TAB values(7);

select SV_RNNI(I) from SV_TAB where I = 7;
select SV_CNI(I) from SV_TAB where I = 7;
select SV_DEF(I) from SV_TAB where I = 7;

select SV_RNNI(I) from SV_TAB where I IS NULL;
select SV_CNI(I) from SV_TAB where I IS NULL;
select SV_DEF(I) from SV_TAB where I IS NULL;

VALUES MAX_RNNI(67, 12);
VALUES MAX_RNNI(-3, -98);
VALUES MAX_CNI(5, 3);
VALUES MAX_DEF(99, -45);

select MAX_RNNI(5, I) from SV_TAB where I = 7;
select MAX_CNI(6, I) from SV_TAB where I = 7;
select MAX_DEF(2, I) from SV_TAB where I = 7;

select MAX_RNNI(I, 34) from SV_TAB where I = 7;
select MAX_CNI(I, 24) from SV_TAB where I = 7;
select MAX_DEF(I, 14) from SV_TAB where I = 7;

select MAX_RNNI(5, I) from SV_TAB where I IS NULL;
select MAX_CNI(6, I) from SV_TAB where I IS NULL;
select MAX_DEF(2, I) from SV_TAB where I IS NULL;

select MAX_RNNI(I, 34) from SV_TAB where I IS NULL;
select MAX_CNI(I, 24) from SV_TAB where I IS NULL;
select MAX_DEF(I, 14) from SV_TAB where I IS NULL;

CREATE FUNCTION COUNT_ROWS(P1 VARCHAR(128), P2 VARCHAR(128)) RETURNS INT
READS SQL DATA
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.countRows'
LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE FUNCTION FN_ABS(P1 INT) RETURNS INT
NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

select FN_ABS(i) FROM SV_TAB;
select COUNT_ROWS(CURRENT SCHEMA, 'SV_TAB') from SV_TAB;
select FN_ABS(i), COUNT_ROWS(CURRENT SCHEMA, 'SV_TAB') from SV_TAB;

DROP FUNCTION SV_RNNI;
DROP FUNCTION SV_CNI;
DROP FUNCTION SV_DEF;
DROP FUNCTION MAX_RNNI;
DROP FUNCTION MAX_CNI;
DROP FUNCTION MAX_DEF;

DROP FUNCTION FN_ABS;
DROP FUNCTION COUNT_ROWS;

DROP TABLE SV_TAB;

-- check MODIFIES SQL DATA not allowed with FUNCTION
CREATE FUNCTION COUNT_ROWS(P1 VARCHAR(128), P2 VARCHAR(128)) RETURNS INT
MODIFIES SQL DATA
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.countRows'
LANGUAGE JAVA PARAMETER STYLE JAVA;

