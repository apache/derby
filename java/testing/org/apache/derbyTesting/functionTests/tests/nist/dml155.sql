AUTOCOMMIT OFF;

-- MODULE  DML155  

-- SQL Test Suite, V6.0, Interactive SQL, dml155.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:0850 Comparing fixed vs. variable length caracter strings!

   CREATE TABLE T850 (
--O     T850KEY INT NOT NULL UNIQUE,
     T850KEY INT NOT NULL constraint t850_unique UNIQUE,
     T850C   CHAR (11),
     T850VC  VARCHAR (10),
     T850LVC VARCHAR (20));
-- PASS:0850 If table created successfully?

   COMMIT WORK;

   INSERT INTO T850 VALUES (
     10, '1234567890', '1234567890', '1234567890   ');
-- PASS:0850 If 1 row inserted successfully?

   INSERT INTO T850 VALUES (
     5, '12345     ', '12345', '12345');
-- PASS:0850 If 1 row inserted successfully?

   INSERT INTO T850 VALUES (
     0, '          ', '', '                    ');
-- PASS:0850 If 1 row inserted successfully?

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850C = T850VC;
-- PASS:0850 If COUNT = 3?

   SELECT T850KEY 
     FROM T850 WHERE T850VC = '1234567890     ';
-- PASS:0850 If T850KEY = 10?

   SELECT T850KEY 
     FROM T850 WHERE T850VC = '12345  ';
-- PASS:0850 If T850KEY = 5?

   SELECT T850KEY 
--N     FROM T850 WHERE T850VC = '1234567890     '; error in NIST test
     FROM T850 WHERE T850VC = '               '; --N this corresponds to the esql/c test
-- PASS:0850 If T850KEY = 0?

--O   SELECT COUNT(*)
   SELECT * 
     FROM T850 WHERE T850C = '1234567890';
-- INFORMATIONAL

--O   SELECT COUNT(*) 
   SELECT *
     FROM T850 WHERE T850C = '12345';
-- INFORMATIONAL

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850VC = T850LVC;
-- PASS:0850 If COUNT = 3?

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850VC = '12345          ';
-- PASS:0850 If COUNT = 1?

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850VC = '12345  ';
-- PASS:0850 If COUNT = 1?

--O   SELECT COUNT(*)
   SELECT * 
     FROM T850 WHERE T850LVC = '12345          ';
-- PASS:0850 If COUNT = 1?

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850LVC = '12345  ';
-- PASS:0850 If COUNT = 1?

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850C = '12345          ';
-- PASS:0850 If COUNT = 1?

--O   SELECT COUNT(*) 
   SELECT * 
     FROM T850 WHERE T850C = '12345  ';
-- PASS:0850 If COUNT = 1?

   ROLLBACK WORK;

--O   DROP TABLE T850 CASCADE;
   DROP TABLE T850 ;

   COMMIT WORK;

-- END TEST >>> 0850 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
