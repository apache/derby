AUTOCOMMIT OFF;

-- MODULE  XTS729

-- SQL Test Suite, V6.0, Interactive SQL, xts729.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

   ROLLBACK WORK;

-- TEST:7029 Column name with 19, 72 and 128 characters!
-- NOTE:  If long lines are not supported by the ISQL interfac, an
--        implementation defined line continuation format may be used
-- Begin 19 character column names

   CREATE TABLE TESTA6439
        (COLUMNOFCHARACTERSA CHARACTER(10),
         columnofcharactersb CHARACTER(10),
         cOlUmNoFNUMERICss_0 NUMERIC(5),
         cOlUmNoFNUMERICss_1 NUMERIC(5));
-- PASS:7029 If table created successfully?

   COMMIT WORK;

--O   INSERT INTO CTS1.TESTA6439
   INSERT INTO TESTA6439
         VALUES('ABCD','DCBA',1,9999);
-- PASS:7029 If 1 row inserted successfully?

   COMMIT WORK;

   SELECT COLUMNOFCHARACTERSA, columnofcharactersb,
                cOlUmNoFNUMERICss_0, cOlUmNoFNUMERICss_1
--O                FROM CTS1.TESTA6439;
                FROM TESTA6439;
-- PASS:7029 If COLUMNOFCHARACTERSA = ABCD?
-- PASS:7029 If columnofcharactersb = DCBA?
-- PASS:7029 If cOlUmNoFNUMERICss_0 = 1?
-- PASS:7029 If cOlUmNoFNUMERICss_1 = 9999?
   
   COMMIT WORK;

--O   DROP TABLE TESTA6439 CASCADE;
   DROP TABLE TESTA6439 ;
-- PASS:7029 If table dropped successfully?

   COMMIT WORK;

-- End 19 character column names

-- Begin 30 character column names
   CREATE TABLE TESTB6439
(COLUMNOFCHARACTERDATATYPE123a CHARACTER(3),
columnofcharacterdatatype123b CHARACTER(3),
cOlUmNoFNUMERIC123456789012_0 NUMERIC(5),
CoLuMnOfNUMERIC123456789012_1 NUMERIC(5));
-- PASS:7029 If table created successfully?

   COMMIT WORK;

--O   INSERT INTO CTS1.TESTB6439
   INSERT INTO TESTB6439
         VALUES('AB','BB',1,2);
-- PASS:7029 If 1 row inserted successfully?

--O   INSERT INTO CTS1.TESTB6439
   INSERT INTO TESTB6439
         VALUES('CC','DD',3,4);
-- PASS:7029 If 1 row inserted successfully?

--O   INSERT INTO CTS1.TESTB6439
   INSERT INTO TESTB6439
         VALUES('EE','FF',5,6);
-- PASS:7029 If 1 row inserted successfully?

--O   INSERT INTO CTS1.TESTB6439
   INSERT INTO TESTB6439
         VALUES('GG','HH',7,8);
-- PASS:7029 If 1 row inserted successfully?

--O   INSERT INTO CTS1.TESTB6439
   INSERT INTO TESTB6439
         VALUES('II','KK',9,0);
-- PASS:7029 If 1 row inserted successfully?

--O   SELECT * FROM CTS1.TESTB6439
   SELECT * FROM TESTB6439
          ORDER BY cOlUmNoFNUMERIC123456789012_0;
-- PASS:7029 If 5 rows selected in the following order?
--                    ===  ===  === ===
-- PASS:7029   If      AB   BB   1   2?
-- PASS:7029   If      CC   DD   3   4?
-- PASS:7029   If      EE   FF   5   6?
-- PASS:7029   If      GG   HH   7   8?
-- PASS:7029   If      II   KK   9   0?

--O   SELECT  COLUMN_NAME, ORDINAL_POSITION
--O         FROM INFORMATION_SCHEMA.COLUMNS
--O         WHERE TABLE_SCHEMA = 'CTS1' AND TABLE_NAME = 'TESTB6439'
--O         ORDER BY ORDINAL_POSITION;
-- PASS:7029 If 4 rows are selected in the following order?
--
-- PASS:7029 If r1,c1 = COLUMNOFCHARACTERDATATYPE12345678901234567890
--                         1234567890123456789012345678901234567890
--                         1234567890123456789012345678901234567890123?
-- PASS:7029 If row1,col2 = 1?

-- PASS:7029 If r2,c1 = COLUMNOFCHARACTERDATATYPE12345678901234567890
--                      1234567890123456789012345678901234567890
--                      123456789012345678901234567890123456789012B?
-- PASS:7029 If row2,col2 = 2?

-- PASS:7029 If r3,c1 = COLUMNOFNUMERIC123456789012345678901234567890
--                      1234567890123456789012345678901234567890
--                      12345678901234567890123456789012345678901_0?
-- PASS:7029 If row3,col2 = 3?

-- PASS:7029 If r4,c1 = COLUMNOFNUMERIC123456789012345678901234567890
--                      1234567890123456789012345678901234567890
--                      12345678901234567890123456789012345678901_1?
-- PASS:7029 If row4,col2 = 4?

   COMMIT WORK;

--O   ALTER TABLE CTS1.TESTB6439
   ALTER TABLE TESTB6439
         ADD COLUMN 
columnofcharacterdatatype123C CHAR(3);
-- PASS:7029 If table altered successfully?

   COMMIT WORK;

--O   INSERT INTO CTS1.TESTB6439
   INSERT INTO TESTB6439
         VALUES('TTT','TTT',100,100,'ADD');
-- PASS:7029 If 1 row inserted successfully?

--O   SELECT * FROM CTS1.TESTB6439
   SELECT * FROM TESTB6439
         WHERE columnofcharacterdatatype123C = 'ADD';
-- PASS:7029 If 5 values =  TTT  TTT  100  100  ADD?

   ROLLBACK WORK;

--O   DROP TABLE TESTB6439 CASCADE;
   DROP TABLE TESTB6439 ;
-- PASS:7029 If table dropped successfully?

   COMMIT WORK;

-- End 128 character column names

-- Begin 72 character column names

   CREATE TABLE TESTC6439 (COLUMNOFCHARACTERSA CHAR(3),
columnofcharacterdatatype123a
CHAR(3));
-- PASS:7029 If table created successfully?

   COMMIT WORK;

--O   INSERT INTO CTS1.TESTC6439
   INSERT INTO TESTC6439
         VALUES('aba','bbb');
-- PASS:7029 If 1 row inserted successfully?

--O   INSERT INTO CTS1.TESTC6439
   INSERT INTO TESTC6439
         VALUES  ('ccc','ddd');
-- PASS:7029 If 1 row inserted successfully?

--O   INSERT INTO CTS1.TESTC6439
   INSERT INTO TESTC6439
         VALUES('eee','fff');
-- PASS:7029 If 1 row inserted successfully?

--O   SELECT * FROM CTS1.TESTC6439
   SELECT * FROM TESTC6439
         ORDER BY COLUMNOFCHARACTERSA;
-- PASS:7029 If 3 rows selected in the following order?
--                 ===    ===
-- PASS:7029 If    aba    bbb?
-- PASS:7029 If    ccc    ddd?
-- PASS:7029 If    eee    fff? 

   COMMIT WORK;

--O   DROP TABLE TESTC6439 CASCADE;
   DROP TABLE TESTC6439 ;
-- PASS:7029 If table dropped successfully?

   COMMIT WORK;

-- End 72 character column names

-- END TEST >>> 7029 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
