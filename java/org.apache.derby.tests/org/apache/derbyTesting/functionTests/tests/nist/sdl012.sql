AUTOCOMMIT OFF;

-- MODULE SDL012

-- SQL Test Suite, V6.0, Interactive SQL, sdl012.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0148 CREATE Table with NOT NULL!

     INSERT INTO STAFF1(EMPNAME,GRADE,CITY)
            VALUES('Carmen',40,'Boston');
-- PASS:0148 If ERROR, NOT NULL constraint, 0 rows inserted?
-- NOTE:0148 Not Null Column EMPNUM is missing.

--O      SELECT COUNT(*)
      SELECT *
           FROM STAFF1;
-- PASS:0148 If count = 0?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0148 <<< END TEST
-- *************************************************////END-OF-MODULE
