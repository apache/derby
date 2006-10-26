AUTOCOMMIT OFF;

-- MODULE  DML147  

-- SQL Test Suite, V6.0, Interactive SQL, dml147.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER            

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
--O   ROLLBACK WORK;

-- date_time print

-- TEST:0840 Roll back schema manipulation !

   CREATE TABLE NOT_THERE (C1 CHAR (10));
-- PASS:0840 If table is created?

   ROLLBACK WORK;

   INSERT INTO NOT_THERE VALUES ('1234567890');
-- PASS:0840 If ERROR, syntax error/access violation, 0 rows selected?

   ROLLBACK WORK;

   CREATE VIEW NOT_HERE AS
      SELECT * FROM USIG;
-- PASS:0840 If view is created?

   ROLLBACK WORK;

   SELECT COUNT (*) FROM NOT_HERE;
-- PASS:0840 If ERROR, syntax error/access violation, 0 rows selected?

   ROLLBACK WORK;

  ALTER TABLE USIG
  ADD COLUMN NUL INT;
-- PASS:0840 If column is added?

   ROLLBACK WORK;

  SELECT COUNT (*)
  FROM USIG WHERE NUL IS NULL;
-- PASS:0840 If ERROR, syntax error/access violation, 0 rows selected?

   ROLLBACK WORK;

--O   DROP TABLE USIG CASCADE;
   DROP TABLE USIG ;
-- PASS:0840 If table is dropped?

   ROLLBACK WORK;

  SELECT COUNT(*)
  FROM U_SIG;
-- PASS:0840 If count = 2?

   ROLLBACK WORK;

  SELECT COUNT(*)
  FROM USIG;
-- PASS:0840 If count = 2?

   ROLLBACK WORK;

-- END TEST >>> 0840 <<< END TEST

-- *********************************************

-- TEST:0841 Multiple-join and default order of joins !

-- setup
   DELETE FROM HU.STAFF4;

   INSERT INTO HU.STAFF4
      SELECT * FROM HU.STAFF3
         WHERE EMPNUM > 'E3';

--O   SELECT EMPNUM FROM
   SELECT a.EMPNUM FROM
--O      HU.STAFF3 NATURAL LEFT JOIN HU.STAFF NATURAL INNER JOIN HU.STAFF4
      HU.STAFF3 a, HU.staff b, HU.staff4 c 
	where a.empnum = b.empnum and b.empnum = c.empnum
      ORDER BY EMPNUM DESC;
-- PASS:0841 If 2 rows selected?
-- PASS:0841 If ordered EMPNUM values are: E5, E4 ?

--O   SELECT EMPNUM FROM
--O      (HU.STAFF3 NATURAL LEFT JOIN HU.STAFF) NATURAL INNER JOIN HU.STAFF4
--O       ORDER BY EMPNUM ASC;
-- PASS:0841 If 2 rows selected?
-- PASS:0841 If ordered EMPNUM values are: E4, E5 ?

--O   SELECT EMPNUM FROM
--O      HU.STAFF3 NATURAL LEFT JOIN (HU.STAFF NATURAL INNER JOIN HU.STAFF4)
--O      ORDER BY EMPNUM;
--O      ;
-- PASS:0841 If 5 rows selected?
-- PASS:0841 If ordered EMPNUM values are: E1, E2, E3, E4, E5 ?

   ROLLBACK WORK;

-- END TEST >>> 0841 <<< END TEST

-- *********************************************

-- TEST:0842 Multi-column joins !

-- setup
   CREATE TABLE STAFF66 (
     SALARY   INTEGER,
     EMPNAME CHAR(20),
     GRADE   DECIMAL,
     EMPNUM  CHAR(3));

   COMMIT WORK;

-- setup
   INSERT INTO STAFF66
      SELECT GRADE*1000, EMPNAME, GRADE, EMPNUM
         FROM HU.STAFF3 WHERE EMPNUM > 'E2';
-- PASS:0842 If 3 rows inserted ?

   UPDATE HU.STAFF3 SET EMPNUM = 'E6' WHERE EMPNUM = 'E5';
-- PASS:0842 If 1 row updated ?

   UPDATE HU.STAFF3 SET EMPNAME = 'Ali' WHERE GRADE = 12;
-- PASS:0842 If 2 rows updated ?

-- FULL OUTER JOIN of tables with unique data in the joined column
--O   SELECT EMPNUM, CITY, SALARY
--O      FROM HU.STAFF3 LEFT JOIN STAFF66 USING (EMPNUM)
--O   UNION
--O   SELECT EMPNUM, CITY, SALARY
--O      FROM HU.STAFF3 RIGHT JOIN STAFF66 USING (EMPNUM)
--O      ORDER BY EMPNUM;
-- PASS:0842 If 6 rows selected with ordered rows and column values ?
-- PASS:0842    E1   Deale   NULL   ?
-- PASS:0842    E2   Vienna  NULL   ?
-- PASS:0842    E3   Vienna  13000  ?
-- PASS:0842    E4   Deale   12000  ?
-- PASS:0842    E5   NULL    13000  ?
-- PASS:0842    E6   Akron   NULL   ?

-- 7.5 SR 6 d
-- table STAFF66 has 3 rows, only 1 matching on all columns
-- this is a 3-column join:
   SELECT * FROM
--O      STAFF66 NATURAL INNER JOIN HU.STAFF3;
      STAFF66 a, HU.staff3 b where a.empnum = b.empnum
	and a.grade = b.grade
	and a.empname = b.empname;
-- PASS:0842 If 1 row selected?
-- PASS:0842 If column values are in the exact order: ?
-- PASS:0842 EMPNAME=Carmen,GRADE=13,EMPNUM=E3,SALARY=13000,CITY=Vienna?


-- table STAFF66 has 3 rows, only 1 matching on all columns
-- this is a 3-column join, preserving HU.STAFF3:
--O   SELECT EMPNUM, EMPNAME, SALARY FROM
--O      HU.STAFF3 NATURAL LEFT OUTER JOIN STAFF66
--O      WHERE EMPNUM > 'E1'
--O      ORDER BY EMPNUM ASC;
-- PASS:0842 If 4 rows selected with ordered rows and column values ?
-- PASS:0842    E2   Betty    NULL  ?
-- PASS:0842    E3   Carmen   13000 ?
-- PASS:0842    E4   Ali      NULL  ?
-- PASS:0842    E6   Ed       NULL  ?


-- table HU.STAFF has 5 rows, only 3 matching on all columns
-- this is a 3-column join, preserving HU.STAFF:
--O   SELECT EMPNUM, EMPNAME, SALARY FROM
--O      STAFF66 NATURAL RIGHT OUTER JOIN HU.STAFF
--O      WHERE EMPNUM > 'E1'
--O      ORDER BY EMPNUM DESC;
-- PASS:0842 If 4 rows selected with ordered rows and column values ?
-- PASS:0842    E5  Ed      13000  ?
-- PASS:0842    E4  Don     12000  ?
-- PASS:0842    E3  Carmen  13000  ?
-- PASS:0842    E2  Betty   NULL   ?


-- table HU.STAFF has 5 rows, only 3 matching on all columns
-- ordinal position is determined by order in T1, not USING list
-- REF:  7.5 SR 6 d
-- this is a 3-column join, preserving HU.STAFF:
--O   SELECT * FROM
--O      STAFF66 RIGHT JOIN HU.STAFF USING ( GRADE, EMPNUM, EMPNAME)
--O      WHERE EMPNUM > 'E1'
--O      ORDER BY EMPNUM;
-- PASS:0842 If 4 rows selected with ordered rows and column values ?
-- PASS:0842    Betty    10   E2   NULL    Vienna ?
-- PASS:0842    Carmen   13   E3   13000   Vienna ?
-- PASS:0842    Don      12   E4   12000   Deale  ?
-- PASS:0842    Ed       13   E5   13000   Akron  ?


-- table STAFF66 has 3 rows, with 2 matching on named columns
-- this is a 2-column join, preserving HU.STAFF3:
--O   SELECT * FROM
--O      HU.STAFF3 LEFT JOIN STAFF66 USING (GRADE, EMPNUM)
--O      WHERE EMPNUM > 'E1'
--O      ORDER BY EMPNUM ASC;
-- PASS:0842 If 4 rows selected with ordered rows and column values ?
-- PASS:0842    E2  10  Betty   Vienna    NULL    NULL   ?
-- PASS:0842    E3  13  Carmen  Vienna    13000   Carmen ?
-- PASS:0842    E4  12  Ali     Deale     12000   Don    ?
-- PASS:0842    E6  13  Ed      Akron     NULL    NULL   ?


-- similar to above, except for explicit names of columns
--O   SELECT staff3.EMPNUM, staff3.GRADE, HU.STAFF3.EMPNAME, CITY,
   SELECT HU.staff3.EMPNUM, HU.staff3.GRADE, HU.STAFF3.EMPNAME, CITY,
     SALARY, STAFF66.EMPNAME FROM
--O      HU.STAFF3 LEFT JOIN STAFF66 USING (GRADE, EMPNUM)
--O      WHERE EMPNUM = 'E3';
      HU.STAFF3, STAFF66 where HU.staff3.GRADE = staff66.grade and  HU.staff3.EMPNUM = staff66.empnum
      and HU.staff3.EMPNUM = 'E3';
-- PASS:0842 If 1 row selected with ordered column values?
-- PASS:0842    E3  13  Carmen  Vienna  13000  Carmen    ?

-- REF: 7.5 GR 1 d ii
-- this is a cartesian product
--O   SELECT COUNT (*) FROM STAFF66 NATURAL RIGHT JOIN HU.PROJ;
   SELECT count (*) FROM STAFF66 , HU.PROJ;
-- PASS:0842 If count = 18?


   ROLLBACK WORK;

--O   DROP TABLE STAFF66 CASCADE;
   DROP TABLE STAFF66 ;

   COMMIT WORK;

-- END TEST >>> 0842 <<< END TEST

-- *************************************************////END-OF-MODULE
