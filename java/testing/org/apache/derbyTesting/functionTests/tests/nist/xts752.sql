AUTOCOMMIT OFF;

-- MODULE   XTS752

-- SQL Test Suite, V6.0, Interactive SQL, xts752.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
--O   ROLLBACK WORK;

-- date_time print

-- TEST:7052 ALTER TABLE ADD TABLE CONSTRAINT!

   CREATE TABLE TAB752a
         (COL1 NUMERIC(5) NOT NULL,
          COL2 CHAR(15) NOT NULL UNIQUE,
          COL3 CHAR(15));
-- PASS:7052 If table created successfully?

   COMMIT WORK;

   CREATE TABLE TAB752b
         (C1 NUMERIC(5) NOT NULL PRIMARY KEY,
          C2 CHAR(15),
          C3 CHAR(15));
-- PASS:7052 If table created successfully?

   COMMIT WORK;

--O   ALTER TABLE CTS1.TAB752a 
   ALTER TABLE TAB752a 
         ADD CONSTRAINT TA752a_PRKEY PRIMARY KEY(COL1);
-- PASS:7052 If table altered successfully?

   COMMIT WORK;

--O   SELECT COUNT(*) 
--O         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
--O         WHERE TABLE_SCHEMA = 'CTS1' AND TABLE_NAME = 'TA752A'
--O         AND CONSTRAINT_NAME = 'TA752A_PRKEY' AND COLUMN_NAME = 'COL1';
-- PASS:7052 If COUNT = 1?

--O   COMMIT WORK;

   ALTER TABLE TAB752b
      ADD CONSTRAINT TA752b_FKEY FOREIGN KEY(C2) 
      REFERENCES TAB752a(COL2);
-- PASS:7052 If table altered successfully?

--O   COMMIT WORK;

--O   SELECT COUNT(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
--O       WHERE TABLE_SCHEMA = 'CTS1'
--O       AND TABLE_NAME = 'TAB752B' 
--O       AND CONSTRAINT_NAME = 'TA752B_FKEY' 
--O       AND COLUMN_NAME = 'C2';
-- PASS:7052 If COUNT = 1?

--O   COMMIT WORK;

   ALTER TABLE TAB752a
     ADD CONSTRAINT COL3_CHECK CHECK 
     (COL3 IN ('ATHENS','CORFU','PYLOS'));
-- PASS:7052 If table altered successfully?

   COMMIT WORK;

   INSERT INTO TAB752a VALUES(1000,'KILLER','PAROS');
-- PASS:7052 If ERROR - integrity constraint violation?

   ROLLBACK WORK;

--O   DROP TABLE TAB752a CASCADE;

--
--HACK: we need to drop b before a since
-- we don't support cascade on drop table
--

-- PASS:7052 If table dropped successfully?

   COMMIT WORK;

--O   DROP TABLE TAB752b CASCADE;
   DROP TABLE TAB752b ;
   DROP TABLE TAB752a ;
-- PASS:7052 If table dropped successfully?

   COMMIT WORK;

-- END TEST >>> 7052 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
