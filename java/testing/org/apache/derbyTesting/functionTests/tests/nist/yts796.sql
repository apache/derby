AUTOCOMMIT OFF;

-- MODULE  YTS796  

-- SQL Test Suite, V6.0, Interactive SQL, yts796.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:7530 <scalar subquery> as first operand in <comp pred>!

--O   SELECT EMPNAME FROM STAFF WHERE
--O    (SELECT EMPNUM FROM WORKS WHERE PNUM = 'P3')
   SELECT EMPNAME FROM HU.STAFF WHERE
    (SELECT EMPNUM FROM HU.WORKS WHERE PNUM = 'P3')
     = EMPNUM;
-- PASS:7530 If empname = 'Alice'?

--O   SELECT EMPNAME FROM STAFF WHERE 
--O     (SELECT EMPNUM FROM WORKS WHERE PNUM = 'P4')
   SELECT EMPNAME FROM HU.STAFF WHERE 
     (SELECT EMPNUM FROM HU.WORKS WHERE PNUM = 'P4')
     = EMPNUM;
-- PASS:7530 If ERROR - cardinality violation?

   ROLLBACK WORK;

-- END TEST >>> 7530 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
