AUTOCOMMIT OFF;

-- MODULE CDR030  

-- SQL Test Suite, V6.0, Interactive SQL, cdr030.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION SUN
   set schema SUN;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- NOTE Direct support for SQLCODE or SQLSTATE is not required
-- NOTE    in Interactive Direct SQL, as defined in FIPS 127-2.
-- NOTE   ********************* instead ***************************
-- NOTE If a statement raises an exception condition,
-- NOTE    then the system shall display a message indicating that
-- NOTE    the statement failed, giving a textual description
-- NOTE    of the failure.
-- NOTE If a statement raises a completion condition that is a
-- NOTE    "warning" or "no data", then the system shall display
-- NOTE    a message indicating that the statement completed,
-- NOTE    giving a textual description of the "warning" or "no data."

-- TEST:0516 SQLSTATE 23502: integrity constraint violation!

--O   INSERT INTO EMP 
--O         VALUES (41,'Tom','China Architecture',
--O                 20,'Architecture',040553);
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows inserted?
-- PASS:0516 OR RI ERROR, parent missing, 0 rows inserted?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

--O   DELETE FROM EMP
--O         WHERE ENO = 21;
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows deleted?
-- PASS:0516 OR RI ERROR, children exist, 0 rows deleted?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

--O   UPDATE EMP
--O         SET ENAME = 'Thomas'
--O         WHERE ENO = 21;
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows updated?
-- PASS:0516 OR RI ERROR, chldren exist, 0 rows updated?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

-- setup 
   DELETE FROM STAFF7;

-- PRIMARY KEY (EMPNUM)
   INSERT INTO STAFF7 (EMPNUM) 
         VALUES ('XXX');
-- PASS:0516 If 1 row inserted?

   INSERT INTO STAFF7 (EMPNUM) 
         VALUES ('XXX');
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows inserted?
-- PASS:0516 OR ERROR, unique constraint, 0 rows inserted?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

-- setup
   DELETE FROM PROJ3;

-- UNIQUE (PNUM)
   INSERT INTO PROJ3 (PNUM) VALUES ('787');

   INSERT INTO PROJ3 (PNUM) VALUES ('789');
-- PASS:0516 If 1 row inserted?

   UPDATE PROJ3          SET PNUM = '787' 
                       WHERE PNUM = '789';
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows updated?
-- PASS:0516 OR ERROR, unique constraint, 0 rows updated?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

-- setup
   DELETE FROM STAFF11;

   INSERT INTO STAFF11
         VALUES('E3','Susan',11,'Hawaii');
-- PASS:0516 If 1 row inserted?

-- (CHECK GRADE NOT IN (5,22))
   UPDATE STAFF11
         SET GRADE = 5
         WHERE EMPNUM = 'E3';
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows updated?
-- PASS:0516 OR ERROR, check constraint, 0 rows updated?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

-- (CHECK NOT EMPNAME LIKE 'T%')
   UPDATE STAFF11
         SET EMPNAME = 'Tom'
         WHERE EMPNUM = 'E3';
-- PASS:0516 If ERROR, integrity constraint violation, 0 rows updated?
-- PASS:0516 OR ERROR, check constraint, 0 rows updated?
-- PASS:0516 OR SQLSTATE = 23502 OR SQLCODE < 0?

-- restore
   ROLLBACK WORK;

-- END TEST >>> 0516 <<< END TEST
-- *************************************************////END-OF-MODULE
