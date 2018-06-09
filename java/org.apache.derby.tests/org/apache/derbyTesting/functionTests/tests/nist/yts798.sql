AUTOCOMMIT OFF;

-- MODULE  YTS798  

-- SQL Test Suite, V6.0, Interactive SQL, yts798.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1              
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

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

-- date_time print

-- TEST:7559 <scalar subquery> in <select list> of single-row select!

   SELECT DISTINCT A,
            (SELECT D FROM TW
             WHERE E = X.A)
--O             FROM TV AS X, TW AS Y
             FROM TV  X, TW  Y
--O             WHERE 1 <
--O                     (SELECT COUNT (*) FROM TV, TW
             WHERE exists
                     (SELECT * FROM TV, TW
                      WHERE A = X.A
                      AND A = E);
-- PASS:7559 If ERROR - cardinality violation?

   SELECT DISTINCT A,
              (SELECT D FROM TW
               WHERE E = X.A)
--O         FROM TV AS X, TW AS Y
         FROM TV  X, TW  Y
         WHERE A = 1;
-- PASS:7559 If A = 1 and D = 'g'?

   SELECT DISTINCT A,
             (SELECT D FROM TW
              WHERE E = X.A)
--O         FROM TV AS X, TW AS Y
         FROM TV  X, TW  Y
         WHERE A = 3;
-- PASS:7559 If A = 3 and D = NULL?

   ROLLBACK WORK;

-- END TEST >>> 7559 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
