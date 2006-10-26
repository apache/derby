AUTOCOMMIT OFF;

-- MODULE  DML106  

-- SQL Test Suite, V6.0, Interactive SQL, dml106.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;
--N temporarily changed authorization to HU, to simplify schema name usage until
--N we have permissions; table USIG requires explicit schema name FLATER until
--N this is changed back

--0   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:0599 UNION in views (feature 8) (static)!

   CREATE VIEW UUSIG (U1) AS
  SELECT C1 FROM USIG UNION SELECT C_1 FROM USIG;
-- PASS:0599 If view is created?

   COMMIT WORK;

   SELECT COUNT(*) FROM UUSIG;
-- PASS:0599 If count = 4?

   SELECT COUNT(DISTINCT U1) FROM UUSIG;
-- PASS:0599 If count = 4?

   SELECT COUNT(*) FROM UUSIG WHERE U1 < 0
  OR U1 > 3 OR U1 IS NULL;
-- PASS:0599 If count = 0?

   COMMIT WORK;

   CREATE VIEW ABOVE_AVERAGE (COLUMN_1, COLUMN_2, COLUMN_3) AS
      SELECT PNUM, BUDGET, CITY
        FROM HU.PROJ OUTER_REF
        WHERE BUDGET >= (SELECT AVG(BUDGET) FROM HU.PROJ INNER_REF
                          WHERE OUTER_REF.CITY = INNER_REF.CITY)
     UNION
      SELECT 'MAX', MAX(BUDGET), MIN(CITY)
        FROM HU.PROJ 
        WHERE CITY > 'Deale';
-- PASS:0599 If view is created?

   COMMIT WORK;

   SELECT * FROM ABOVE_AVERAGE ORDER BY COLUMN_1;
-- PASS:0599 If 4 rows selected with ordered rows and column values: ?
-- PASS:0599    MAX  30000  Tampa  ?
-- PASS:0599    P2   30000  Vienna ?
-- PASS:0599    P3   30000  Tampa  ?
-- PASS:0599    P6   50000  Deale  ?

   COMMIT WORK;

   CREATE VIEW STAFF_DUP AS
      SELECT EMPNUM, EMPNAME, GRADE, CITY
        FROM HU.STAFF
     UNION ALL
      SELECT * FROM HU.STAFF3;
-- PASS:0599 If view is created?

   COMMIT WORK;

   SELECT * FROM STAFF_DUP ORDER BY CITY;
-- PASS:0599 If 10 rows selected ?
-- PASS:0599 If first row contains EMPNUM/CITY values E5 / Akron ?
-- PASS:0599 If second row contains EMPNUM/CITY values E5 / Akron ?

   SELECT COUNT(*) FROM STAFF_DUP;
-- PASS:0599 If count = 10 ?

   COMMIT WORK;

   CREATE VIEW FOUR_CITIES (C1, C2, C3) AS
      SELECT 'P', CITY, 666
        FROM HU.PROJ
        WHERE BUDGET <> 30000
     UNION
      SELECT 'S', CITY, 777
        FROM HU.STAFF
        WHERE EMPNAME <> 'Ed'
     UNION
      SELECT 'T', CITY, -999
        FROM HU.STAFF3
        WHERE CITY NOT LIKE 'V%'
     UNION
      SELECT 'X', CITY, -1
        FROM HU.STAFF3
        WHERE CITY = 'Vienna';
-- PASS:0599 If view is created?

   COMMIT WORK;

   SELECT C2, C1, C3 FROM FOUR_CITIES ORDER BY C3, C2;
-- PASS:0599 If 7 rows selected with ordered rows and column values ?
-- PASS:0599    Akron   T     -999  ?
-- PASS:0599    Deale   T     -999  ?
-- PASS:0599    Vienna  X       -1  ?
-- PASS:0599    Deale   P      666  ?
-- PASS:0599    Vienna  P      666  ?
-- PASS:0599    Deale   S      777  ?
-- PASS:0599    Vienna  S      777  ?

   SELECT COUNT (*) FROM FOUR_CITIES;
-- PASS:0599 If count = 7 ?

   SELECT COUNT(*) FROM FOUR_CITIES WHERE C3 > 0;
-- PASS:0599 If count = 4 ?

   SELECT COUNT(*) FROM FOUR_CITIES WHERE C2 = 'Vienna';
-- PASS:0599 If count = 3 ?

   COMMIT WORK;

--0   DROP VIEW ABOVE_AVERAGE CASCADE;
   DROP VIEW ABOVE_AVERAGE ;

   COMMIT WORK;

--0   DROP VIEW STAFF_DUP CASCADE;
   DROP VIEW STAFF_DUP ;

   COMMIT WORK;

--0   DROP VIEW FOUR_CITIES CASCADE;
   DROP VIEW FOUR_CITIES ;

   COMMIT WORK;

--0   DROP VIEW UUSIG CASCADE;
   DROP VIEW UUSIG ;

   COMMIT WORK;

-- END TEST >>> 0599 <<< END TEST

-- *********************************************

-- TEST:0601 DATETIME data types (feature 5) (static)!

   CREATE TABLE TEMPUS (TDATE DATE, TTIME TIME,
--0  TTIMESTAMP TIMESTAMP, TINT1 INTERVAL YEAR TO MONTH,
  TTIMESTAMP TIMESTAMP);
--0  TINT2 INTERVAL DAY TO SECOND);
-- PASS:0601 If table is created?

   COMMIT WORK;

   INSERT INTO TEMPUS VALUES (
  DATE( '1993-08-24'),
  TIME( '16:03:00'),
  TIMESTAMP( '1993-08-24 16:03:00')
--0  , INTERVAL -'1-6' YEAR TO MONTH,
--0  INTERVAL '13 0:10' DAY TO SECOND);
  );
-- PASS:0601 If 1 row is inserted?

   SELECT DAY( TDATE)
  FROM TEMPUS;
-- PASS:0601 If 1 row selected and value is 24?

--0   SELECT COUNT(*) FROM TEMPUS
--0  WHERE (TTIMESTAMP - TIMESTAMP( '1995-02-24 16:03:00'))
--0  YEAR TO MONTH = TINT1;
-- PASS:0601 If count = 1?

--0   SELECT COUNT(*) FROM TEMPUS
--0  WHERE (TTIMESTAMP, TINT1) OVERLAPS
--0  (TIMESTAMP( '1995-02-24 16:03:00'), INTERVAL '1-6' YEAR TO MONTH);
-- PASS:0601 If count = 0?

   ROLLBACK WORK;

--0   DROP TABLE TEMPUS CASCADE;
   DROP TABLE TEMPUS ;

   COMMIT WORK;

-- END TEST >>> 0601 <<< END TEST

-- *********************************************

-- TEST:0611 FIPS sizing, DATETIME data types (static)!

   CREATE TABLE TSFIPS (
  FIPS1 TIME,
  FIPS2 TIMESTAMP
--0  , FIPS3 INTERVAL YEAR (2) TO MONTH,
--0  FIPS4 INTERVAL DAY (2) TO SECOND (6));
  );
-- PASS:0611 If table is created?

   COMMIT WORK;

   INSERT INTO TSFIPS VALUES (
  TIME( '16:03:00'),
  TIMESTAMP( '1996-08-24 16:03:00.999999')
--0  ,INTERVAL -'99-6' YEAR (2) TO MONTH,
--0  INTERVAL '99 0:10:00.999999' DAY (2) TO SECOND (6));
  );
-- PASS:0611 If 1 row is inserted?

   SELECT SECOND( FIPS2)
  * 1000000 - 999990 FROM TSFIPS;
-- PASS:0611 If 1 row selected and value is 9?

--0   SELECT YEAR( FIPS3),
--0  MONTH( FIPS3)
--0  FROM TSFIPS;
-- PASS:0611 If 1 row selected and values are -99 and -6?

--0   SELECT DAY( FIPS4),
--0  SECOND( FIPS4) * 1000000 - 999990
--0  FROM TSFIPS;
-- PASS:0611 If 1 row selected and values are 99 and 9?

   ROLLBACK WORK;

--0   DROP TABLE TSFIPS CASCADE;
   DROP TABLE TSFIPS ;

   COMMIT WORK;

-- END TEST >>> 0611 <<< END TEST

-- *********************************************

-- TEST:0613 <datetime value function> (static)!

   CREATE TABLE TSSMALL (
  SMALLD DATE,
  SMALLT TIME,
  SMALLTS TIMESTAMP);
-- PASS:0613 If table is created?

   COMMIT WORK;

   INSERT INTO TSSMALL VALUES (
  CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP);
-- PASS:0613 If 1 row is inserted?

   SELECT COUNT(*)
FROM TSSMALL WHERE
YEAR( SMALLD) = YEAR( SMALLTS);
-- PASS:0613 If count = 1?

   SELECT COUNT(*)
FROM TSSMALL WHERE
MONTH( SMALLD) = MONTH( SMALLTS);
-- PASS:0613 If count = 1?

   SELECT COUNT(*)
FROM TSSMALL WHERE
DAY( SMALLD) = DAY( SMALLTS);
-- PASS:0613 If count = 1?

   SELECT COUNT(*)
FROM TSSMALL WHERE
HOUR( SMALLT) = HOUR( SMALLTS);
-- PASS:0613 If count = 1?

   SELECT COUNT(*)
FROM TSSMALL WHERE
MINUTE( SMALLT) = MINUTE( SMALLTS);
-- PASS:0613 If count = 1?

   SELECT COUNT(*)
FROM TSSMALL WHERE
SECOND( SMALLT) -
SECOND( SMALLTS) > -1
AND SECOND( SMALLT) -
SECOND( SMALLTS) < 1;
-- PASS:0613 If count = 1?

   ROLLBACK WORK;

--0   DROP TABLE TSSMALL CASCADE;
   DROP TABLE TSSMALL ;

   COMMIT WORK;

-- END TEST >>> 0613 <<< END TEST

-- *********************************************

-- TEST:0615 DATETIME-related SQLSTATE codes (static)!

   CREATE TABLE TSERR (
--0  BADINT INTERVAL YEAR (2) TO MONTH,
  BADDATE DATE);
-- PASS:0615 If table is created?

   COMMIT WORK;

--0   INSERT INTO TSERR VALUES (
--0  INTERVAL '0-11' YEAR TO MONTH,
--0  DATE( '9999-01-01') + INTERVAL '1-00' YEAR TO MONTH);
-- PASS:0615 If ERROR, datetime field overflow, 0 rows inserted?

   INSERT INTO TSERR VALUES (
--0  INTERVAL '9999-11' YEAR TO MONTH,
  DATE( '1984-01-01'));
-- PASS:0615 If ERROR, interval field overflow, 0 rows inserted?

--0   INSERT INTO TSERR VALUES (
--0  INTERVAL '1-11' YEAR TO MONTH,
--0  CAST ('DATE ''1993-02-30''' AS DATE));
-- PASS:0615 If ERROR, invalid datetime format, 0 rows inserted?

--0   INSERT INTO TSERR VALUES (
--0  INTERVAL '1-11' YEAR TO MONTH,
--0  CAST ('1993-02-30' AS DATE));
-- PASS:0615 If ERROR, invalid datetime format, 0 rows inserted?

   ROLLBACK WORK;

--0   DROP TABLE TSERR CASCADE;
   DROP TABLE TSERR ;

   COMMIT WORK;

-- END TEST >>> 0615 <<< END TEST
-- *************************************************////END-OF-MODULE
