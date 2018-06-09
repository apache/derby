AUTOCOMMIT OFF;

-- MODULE  DML144  

-- SQL Test Suite, V6.0, Interactive SQL, dml144.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
--O   ROLLBACK WORK;

-- date_time print

-- TEST:0834 <length expression> (static)!

   CREATE TABLE GRUB (C1 VARCHAR (10));
-- PASS:0834 If table is created?

   COMMIT WORK;

   SELECT LENGTH (EMPNAME)
   FROM HU.STAFF WHERE GRADE = 10;
-- PASS:0834 If 1 row selected and value is 20?

   SELECT LENGTH ('HI' || 'THERE')
   FROM HU.ECCO;
-- PASS:0834 If 1 row selected and value is 7?

   INSERT INTO GRUB VALUES ('Hi  ');
-- PASS:0834 If 1 row is inserted?

   SELECT LENGTH (C1)
   FROM GRUB;
-- PASS:0834 If 1 row selected and value is 4?

-- following is not supported in derby
--   SELECT OCTET_LENGTH (C1)
--  FROM GRUB;
-- PASS:0834 If 1 row selected and value is > 2?

   UPDATE GRUB SET C1 = NULL;
-- PASS:0834 If 1 row is updated?

   SELECT LENGTH (C1)
  FROM GRUB;
-- PASS:0834 If 1 row selected and value is NULL?

-- following is not supported in derby
--   SELECT OCTET_LENGTH (C1)
--  FROM GRUB;
-- PASS:0834 If 1 row selected and value is NULL?

   ROLLBACK WORK;

--O   DROP TABLE GRUB CASCADE;
   DROP TABLE GRUB ;

   COMMIT WORK;

-- END TEST >>> 0834 <<< END TEST

-- *********************************************

-- TEST:0835 <character substring function> (static)!

   CREATE TABLE MOREGRUB (C1 VARCHAR (10), ID INT);
-- PASS:0835 If table is created?

   COMMIT WORK;

   CREATE VIEW X4 (S1, S2, ID) AS
  SELECT SUBSTR (C1, 6),
         SUBSTR (C1, 2, 4), ID
  FROM MOREGRUB;
-- PASS:0835 If view is created?

   COMMIT WORK;

   SELECT SUBSTR (CITY, 4, 10)
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0835 If 1 row selected and value is 'on        '?

-- NOTE:0835 Right truncation subtest deleted.

   SELECT SUBSTR (CITY, 4, -1)
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0835 If ERROR, substring error, 0 rows selected?

   SELECT SUBSTR (CITY, 0, 10)
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0835 If 1 row selected and value is 'Akron     '?

-- NOTE:0835 Host language variable subtest deleted.

   SELECT SUBSTR (CITY, 1, 1)
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0835 If 1 row selected and value is 'A'?

   SELECT SUBSTR (CITY, 1, 0)
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0835 If 1 row selected and value is ''?

   SELECT SUBSTR (CITY, 12, 1)
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0835 If 1 row selected and value is ''?

   INSERT INTO MOREGRUB VALUES ('Pretzels', 1);
-- PASS:0835 If 1 row is inserted?

   INSERT INTO MOREGRUB VALUES (NULL, 2);
-- PASS:0835 If 1 row is inserted?

   INSERT INTO MOREGRUB VALUES ('Chips', 3);
-- PASS:0835 If 1 row is inserted?

   SELECT S1 FROM X4 WHERE ID = 1;
-- PASS:0835 If 1 row selected and S1 = 'els'?

   SELECT S1 FROM X4 WHERE ID = 3;
-- PASS:0835 If 1 row selected and S1 =  ''?

   SELECT S2 FROM X4 WHERE ID = 1;
-- PASS:0835 If 1 row selected and S2 = 'retz'?

   SELECT S2 FROM X4 WHERE ID = 3;
-- PASS:0835 If 1 row selected and S2 = 'hips'?

   SELECT SUBSTR (C1, ID)
  FROM MOREGRUB
  WHERE C1 LIKE 'Ch%';
-- PASS:0835 If 1 row selected and value is 'ips'?

   SELECT SUBSTR (C1, 1, ID)
  FROM MOREGRUB
  WHERE C1 LIKE 'Ch%';
-- PASS:0835 If 1 row selected and value is 'Chi'?

-- NOTE:0835 Host language variable subtest deleted.

   SELECT S1 FROM X4 WHERE ID = 2;
-- PASS:0835 If 1 row selected and S1 is NULL?

   DELETE FROM MOREGRUB;

   INSERT INTO MOREGRUB VALUES ('Tacos', NULL);
-- PASS:0835 If 1 row is inserted?

   SELECT SUBSTR (C1, 1, ID)
   FROM MOREGRUB;
-- PASS:0835 If 1 row selected and value is NULL?

   SELECT SUBSTR (C1, ID, 1)
   FROM MOREGRUB;
-- PASS:0835 If 1 row selected and value is NULL?

   UPDATE MOREGRUB SET C1 = NULL;

   SELECT SUBSTR (C1, ID, ID)
   FROM MOREGRUB;
-- PASS:0835 If 1 row selected and value is NULL?

   ROLLBACK WORK;

--O   DROP TABLE MOREGRUB CASCADE;
   drop view x4;
   DROP TABLE MOREGRUB ;

   COMMIT WORK;

-- END TEST >>> 0835 <<< END TEST

-- *********************************************

-- TEST:0839 Composed <length expression> and SUBSTR!

   SELECT LENGTH (SUBSTR
  (CITY, 4, 4))
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0839 If 1 row selected and value is 4?

   SELECT LENGTH (SUBSTR
  (EMPNUM, 1))
  FROM HU.STAFF WHERE EMPNAME = 'Ed';
-- PASS:0839 If 1 row selected and value is 3?

   COMMIT WORK;

-- END TEST >>> 0839 <<< END TEST
-- *************************************************////END-OF-MODULE
