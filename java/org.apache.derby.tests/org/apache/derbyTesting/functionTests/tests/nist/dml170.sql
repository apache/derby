AUTOCOMMIT OFF;

-- MODULE  DML170  

-- SQL Test Suite, V6.0, Interactive SQL, dml170.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
--O   ROLLBACK WORK;

-- date_time print

-- TEST:0880 Long constraint names, cursor names!

   CREATE TABLE T0880 (
     C1 INT NOT NULL, C2 INT NOT NULL,
     CONSTRAINT
     "It was the best of"
     PRIMARY KEY (C1, C2));
-- PASS:0880 If table created successfully?

   COMMIT WORK;

   INSERT INTO T0880 VALUES (0, 1);
-- PASS:0880 If 1 row inserted successfully?

   INSERT INTO T0880 VALUES (1, 2);
-- PASS:0880 If 1 row inserted successfully?

   INSERT INTO T0880 VALUES (1, 2);
-- PASS:0880 If ERROR - integrity constraint violation?

   SELECT C1 FROM T0880 ORDER BY C1;
-- PASS:0880 If 2 rows are returned in the following order?
--                c1
--                ==
-- PASS:0880 If   0 ?
-- PASS:0880 If   1 ?

   COMMIT WORK;

   ALTER TABLE T0880
     DROP CONSTRAINT
     "It was the best of"
--O     CASCADE;
    ;
-- PASS:0880 If table altered successfully?

   COMMIT WORK;

   INSERT INTO T0880 VALUES (0, 1);
-- PASS:0880 If 1 row inserted successfully?

   SELECT COUNT (*) FROM T0880;
-- PASS:0880 If COUNT = 3?

   COMMIT WORK;

--O   DROP TABLE T0880 CASCADE;
   DROP TABLE T0880 ;
-- PASS:0880 If table dropped successfully?

   COMMIT WORK;

-- END TEST >>> 0880 <<< END TEST
-- *********************************************

-- TEST:0881 Long character set names, domain names!

--O   CREATE CHARACTER SET
--O     "Little boxes on the hillside, Little boxes made of ticky-tacky"
--O     GET SQL_TEXT;
-- PASS:0881 If character set created successfully?

--O   COMMIT WORK;

--O   CREATE DOMAIN
--O     "Little boxes on the hillside, Little boxes all the same."
--O     CHAR (4) CHARACTER SET
--O     "Little boxes on the hillside, Little boxes made of ticky-tacky";
-- PASS:0881 If domain created successfully?

--O   COMMIT WORK;

--O   CREATE TABLE T0881 ( C1
--O     "Little boxes on the hillside, Little boxes all the same.");
-- PASS:0881 If table created successfully?

--O   COMMIT WORK;

--O   INSERT INTO T0881 VALUES ('ABCD');
-- PASS:0881 If insert completed successfully?

--O   SELECT COUNT(*) FROM T0881
--O     WHERE C1 = 'ABCD';
-- PASS:0881 If COUNT = 1?

--O   COMMIT WORK;

--O   DROP TABLE T0881 CASCADE;
-- PASS:0881 if table dropped successfully?

--O   COMMIT WORK;

--O   DROP DOMAIN
--O     "Little boxes on the hillside, Little boxes all the same."
--O     CASCADE;
-- PASS:0881 If domain dropped successfully?

--O   COMMIT WORK;

--O   DROP CHARACTER SET
--O   "Little boxes on the hillside, Little boxes made of ticky-tacky";
-- PASS:0881 If character set dropped successfully?

--O   COMMIT WORK;

-- END TEST >>> 0881 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
