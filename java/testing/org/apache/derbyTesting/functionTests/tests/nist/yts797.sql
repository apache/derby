AUTOCOMMIT OFF;

-- MODULE  YTS797  

-- SQL Test Suite, V6.0, Interactive SQL, yts797.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1              
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:7558 <scalar subquery> in SET of searched update!

   DELETE FROM TV;

   INSERT INTO TV VALUES (1,'a');

   INSERT INTO TV VALUES (2,'b');

   INSERT INTO TV VALUES (3,'c');

   INSERT INTO TV VALUES (4,'d');

   INSERT INTO TV VALUES (5,'e');

   DELETE FROM TW;

   INSERT INTO TW VALUES ('b',2);

   INSERT INTO TW VALUES ('g',1);

   INSERT INTO TW VALUES ('f',2);

   INSERT INTO TW VALUES ('h',4);

   INSERT INTO TW VALUES ('i',5);

--O   UPDATE TV AS X
   UPDATE TV 
     SET B =
--O         (SELECT D FROM TV AS Y, TW AS Z
         (SELECT D FROM TV  Y, TW  Z
              WHERE Y.A = Z.E
              AND TV.A = Y.A);
-- PASS:7558 If ERROR - cardinality violation?
--N new error messages are temporarily valid, till we implement this kind of update properly

--O   UPDATE TV AS X
   UPDATE TV  
     SET B =
--O         (SELECT D FROM TV AS Y, TW AS Z
         (SELECT D FROM TV  Y, TW  Z
              WHERE Y.A = Z.E AND Z.E <> 2
              AND TV.A = Y.A);
-- PASS:7558 If UPDATE completed successfully?

   SELECT B 
     FROM CTS1.TV
     WHERE A = 1;
-- PASS:7558 If B = 'g'?

   SELECT B 
     FROM CTS1.TV
     WHERE A = 2;
-- PASS:7558 If B = NULL?

   SELECT B 
     FROM CTS1.TV
     WHERE A = 3;
-- PASS:7558 If B = NULL?

   SELECT B 
     FROM CTS1.TV
     WHERE A = 4;
-- PASS:7558 If B = 'h'?

   SELECT B 
     FROM CTS1.TV
     WHERE A = 5;
-- PASS:7558 If B = 'i'?

   ROLLBACK WORK;

-- END TEST >>> 7558 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
