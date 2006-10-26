AUTOCOMMIT OFF;

-- MODULE  DML141  

-- SQL Test Suite, V6.0, Interactive SQL, dml141.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;

--0   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

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

-- TEST:0514 SQLSTATE 23502: integrity constraint violation!

-- NOT NULL constraint violated
   INSERT INTO HU.STAFF VALUES (NULL, NULL, NULL, NULL);
-- PASS:0514 If ERROR, integrity constraint violation, 0 rows inserted?

-- UNIQUE constraint violated
   INSERT INTO HU.STAFF VALUES ('E1', 'Agnetha', 12, 'Paris');
-- PASS:0514 If ERROR, integrity constraint violation, 0 rows inserted?

   ROLLBACK WORK;

-- END TEST >>> 0514 <<< END TEST
-- *************************************************////END-OF-MODULE
