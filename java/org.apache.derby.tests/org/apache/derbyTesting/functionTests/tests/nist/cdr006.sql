--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
AUTOCOMMIT OFF;

-- MODULE CDR006

-- SQL Test Suite, V6.0, Interactive SQL, cdr006.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION SUN
   set schema SUN;

--O   SELECT USER FROM SUN.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0316 CHECK <null predicate> in <tab. cons.>, update!

-- setup
  DELETE FROM STAFF8;

  INSERT INTO STAFF8
        VALUES('E1','Alice',34,'Deale');

  UPDATE STAFF8
        SET EMPNAME = NULL
        WHERE EMPNUM = 'E1';
-- PASS:0316 If ERROR, check constraint, 0 rows updated?

  SELECT COUNT(*) FROM STAFF8
        WHERE EMPNAME = 'Alice';
-- PASS:0316 If count = 1?

-- END TEST >>> 0316 <<< END TEST

-- *************************************************


-- TEST:0317 CHECK X IS NOT NULL, NOT X IS NULL same, by update!

-- setup
  DELETE FROM STAFF13;

  INSERT INTO STAFF13
        VALUES('E1','Alice',36,'Deale');

  UPDATE STAFF13
        SET EMPNAME = NULL
        WHERE EMPNUM = 'E1';
-- PASS:0317 If ERROR, check constraint, 0 rows updated?

  SELECT COUNT(*)
        FROM STAFF13
        WHERE EMPNAME = 'Alice';
-- PASS:0317 If count = 1?


-- END TEST >>> 0317 <<< END TEST

-- *************************************************


-- TEST:0318 CHECK <like predicate> in <tab. cons.>, update!

-- setup
  DELETE FROM STAFF9;

  INSERT INTO STAFF9
        VALUES('E3','Susan',11,'Hawaii');

  UPDATE STAFF9
        SET EMPNAME = 'Thomas'
        WHERE EMPNUM = 'E3';
-- PASS:0318 If ERROR, check constraint, 0 rows updated?

  SELECT COUNT(*)
        FROM STAFF9
        WHERE EMPNAME = 'Susan';
-- PASS:0318 If count = 1?

  COMMIT WORK;
 
-- END TEST >>> 0318 <<< END TEST

-- *************************************************////END-OF-MODULE

