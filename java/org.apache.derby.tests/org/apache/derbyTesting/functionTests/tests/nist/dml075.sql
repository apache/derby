AUTOCOMMIT OFF;

-- MODULE DML075

-- SQL Test Suite, V6.0, Interactive SQL, dml075.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0431 Redundant rows in IN subquery!

--O   SELECT COUNT (*) FROM STAFF
   SELECT * FROM STAFF
       WHERE EMPNUM IN
            (SELECT EMPNUM FROM WORKS);
-- PASS:0431 If count = 4?

   INSERT INTO STAFF1
       SELECT * FROM STAFF;

--O   SELECT COUNT (*) FROM STAFF1
   SELECT * FROM STAFF1
       WHERE EMPNUM IN
            (SELECT EMPNUM FROM WORKS);
-- PASS:0431 If count = 4?

   ROLLBACK WORK;

-- END TEST >>> 0431 <<< END TEST
-- *************************************************************

-- TEST:0432 Unknown comparison predicate in ALL, SOME, ANY!

-- setup

UPDATE PROJ SET CITY = NULL 
  WHERE PNUM = 'P3';

--OSELECT COUNT(*)
SELECT *
  FROM STAFF
  WHERE CITY = ALL (SELECT CITY
                   FROM PROJ
                   WHERE PNAME = 'SDP');
-- PASS:0432 If count = 0?

--OSELECT COUNT(*)
SELECT *
  FROM STAFF
  WHERE CITY <> ALL (SELECT CITY 
                    FROM PROJ
                    WHERE PNAME = 'SDP');
-- PASS:0432 If count = 0?

--OSELECT COUNT(*)
SELECT *
  FROM STAFF
  WHERE CITY = ANY (SELECT CITY    
                   FROM PROJ
                   WHERE PNAME = 'SDP');
-- PASS:0432 If count = 2?

--OSELECT COUNT(*)
SELECT *
  FROM STAFF
  WHERE CITY <> ANY (SELECT CITY
                     FROM PROJ
                     WHERE PNAME = 'SDP');
-- PASS:0432 If count = 3?

--OSELECT COUNT(*)
SELECT *
  FROM STAFF
  WHERE CITY = SOME (SELECT CITY
                     FROM PROJ
                     WHERE PNAME = 'SDP');
-- PASS:0432 If count = 2?

--OSELECT COUNT(*)
SELECT *
  FROM STAFF
  WHERE CITY <> SOME (SELECT CITY
                      FROM PROJ
                      WHERE PNAME = 'SDP');
-- PASS:0432 If count = 3?

   ROLLBACK WORK;

-- END TEST >>> 0432 <<< END TEST
-- *************************************************************

-- TEST:0433 Empty subquery in ALL, SOME, ANY!

--O   SELECT COUNT(*) FROM PROJ
   SELECT * FROM PROJ
        WHERE PNUM = ALL (SELECT PNUM
                          FROM WORKS WHERE EMPNUM = 'E8');
-- PASS:0433 If count = 6?

--O   SELECT COUNT(*) FROM PROJ
   SELECT * FROM PROJ
        WHERE PNUM <> ALL (SELECT PNUM
                          FROM WORKS WHERE EMPNUM = 'E8');

-- PASS:0433 If count = 6?

--O   SELECT COUNT(*) FROM PROJ
   SELECT * FROM PROJ
        WHERE PNUM = ANY (SELECT PNUM
                          FROM WORKS WHERE EMPNUM = 'E8');
-- PASS:0433 If count = 0?

--O   SELECT COUNT(*) FROM PROJ
   SELECT * FROM PROJ
        WHERE PNUM <> ANY (SELECT PNUM
                          FROM WORKS WHERE EMPNUM = 'E8');
-- PASS:0433 If count = 0?

--O   SELECT COUNT(*) FROM PROJ
   SELECT * FROM PROJ
        WHERE PNUM = SOME (SELECT PNUM
                          FROM WORKS WHERE EMPNUM = 'E8');
-- PASS:0433 If count = 0?

--O   SELECT COUNT(*) FROM PROJ
   SELECT * FROM PROJ
        WHERE PNUM <> SOME (SELECT PNUM
                          FROM WORKS WHERE EMPNUM = 'E8');
-- PASS:0433 If count = 0?

-- END TEST >>> 0433 <<< END TEST
-- *************************************************************

-- TEST:0434 GROUP BY with HAVING EXISTS-correlated set function!

   SELECT PNUM, SUM(HOURS) FROM WORKS c
          GROUP BY PNUM
--O          HAVING EXISTS (SELECT PNAME FROM PROJ
--O                         WHERE PROJ.PNUM = WORKS.PNUM AND
          HAVING EXISTS (SELECT PNAME FROM PROJ, works a
                         WHERE PROJ.PNUM = a.PNUM AND
--O                               SUM(WORKS.HOURS) > PROJ.BUDGET / 200);
                         PROJ.BUDGET / 200 < (select sum(hours) from works b
			  where a.pnum = b.pnum
			  and a.pnum = c.pnum));

-- PASS:0434 If 2 rows selected with values (in any order):?
-- PASS:0434 PNUM = 'P1', SUM(HOURS) = 80?
-- PASS:0434 PNUM = 'P5', SUM(HOURS) = 92?

-- END TEST >>> 0434 <<< END TEST
-- *************************************************************

-- TEST:0442 DISTINCT with GROUP BY, HAVING!

   SELECT PTYPE, CITY FROM PROJ
          GROUP BY PTYPE, CITY
          HAVING AVG(BUDGET) > 21000;
-- PASS:0442 If 3 rows selected with PTYPE/CITY values(in any order):?
-- PASS:0442 Code/Vienna, Design/Deale, Test/Tampa?

   SELECT DISTINCT PTYPE, CITY FROM PROJ
          GROUP BY PTYPE, CITY
          HAVING AVG(BUDGET) > 21000;
-- PASS:0442 If 3 rows selected with PTYPE/CITY values(in any order):?
-- PASS:0442 Code/Vienna, Design/Deale, Test/Tampa?

   SELECT DISTINCT SUM(BUDGET) FROM PROJ
          GROUP BY PTYPE, CITY
          HAVING AVG(BUDGET) > 21000;
-- PASS:0442 If 2 rows selected (in any order):?
-- PASS:0442 with SUM(BUDGET) values 30000 and 80000?

-- END TEST >>> 0442 <<< END TEST
-- *************************************************////END-OF-MODULE
