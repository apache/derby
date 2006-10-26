AUTOCOMMIT OFF;

-- MODULE DML009

-- SQL Test Suite, V6.0, Interactive SQL, dml009.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0022 INSERT(column list) VALUES(literals and NULL)!

-- setup
     INSERT INTO WORKS(PNUM,EMPNUM,HOURS)
            VALUES ('P22','E22',NULL);
-- PASS:0022 If 1 row inserted?

      SELECT EMPNUM,PNUM
           FROM   WORKS
           WHERE  HOURS IS NULL;
-- PASS:0022 If EMPNUM = 'E22'?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0022 <<< END TEST
-- **************************************************************

-- TEST:0023 DEC precision >= col.def.: ERROR if left-truncate!

-- setup
     DELETE FROM TEMP_S;
   
     COMMIT WORK;

-- setup
     INSERT INTO TEMP_S(EMPNUM,GRADE,CITY)
          VALUES('E23',2323.4,'China');
-- PASS:0023 If 1 row inserted?

--O     SELECT COUNT(*)
     SELECT *
          FROM TEMP_S;
-- PASS:0023 If count = 1?

-- setup
     INSERT INTO TEMP_S
            VALUES('E23',23234,'China');
-- PASS:0023 If 1 row inserted or ?
-- PASS:0023 insert fails due to precision of 23234?

--O      SELECT COUNT(*)
      SELECT *
           FROM TEMP_S;
-- PASS:0023 If count = 1 or 2 (depending on previous insertion)?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0023 <<< END TEST
-- ***************************************************************

-- TEST:0024 INSERT:<query spec.> is empty: SQLCODE = 100!

-- setup
     INSERT INTO TEMP_S
            SELECT EMPNUM,GRADE,CITY
                 FROM STAFF
                 WHERE GRADE > 13;
-- PASS:0024 If 0 rows selected, SQLCODE = 100, end of data?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0024 <<< END TEST
-- *************************************************************

-- TEST:0025 INSERT:<query spec.> is not empty!
     DELETE FROM TEMP_S;

-- setup
     INSERT INTO TEMP_S(EMPNUM,GRADE,CITY)
            SELECT EMPNUM,GRADE,CITY
                 FROM STAFF
                 WHERE GRADE > 12;
-- PASS:0025 If 2 rows are inserted?

--O      SELECT COUNT(*)
      SELECT *
           FROM TEMP_S;
-- PASS:0025 If count = 2?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0025 <<< END TEST
-- *************************************************************

-- TEST:0026 INSERT into view with check option and unique violation!

--0     SELECT COUNT(*) FROM STAFF;
-- PASS:0026 If count = 5?

-- setup
--0     INSERT INTO TEMP_SS
--0            SELECT EMPNUM,GRADE,CITY
--0                 FROM STAFF3
--0                 WHERE GRADE = 10;
-- PASS:0026 If ERROR, view check constraint, 0 rows inserted  OR ?
-- PASS:0026 If ERROR, unique constraint, 0 rows inserted?

--0     SELECT COUNT(*) FROM STAFF;
-- PASS:0026 If count = 5?

-- restore
--0     ROLLBACK WORK;

-- END TEST >>> 0026 <<< END TEST
-- *************************************************////END-OF-MODULE
