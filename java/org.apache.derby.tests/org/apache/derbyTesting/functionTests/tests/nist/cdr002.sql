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

-- MODULE CDR002

-- SQL Test Suite, V6.0, Interactive SQL, cdr002.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION SUN
   set schema SUN;

--O   SELECT USER FROM SUN.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0302 CHECK <comp. predicate> in <tab. cons.>, insert!

-- setup
  DELETE FROM SUN.STAFF5;

  INSERT INTO SUN.STAFF5
        VALUES('E1','Alice',0,'Deale');
-- PASS:0302 If ERROR, check constraint, 0 rows inserted?

  INSERT INTO SUN.STAFF5
        VALUES('E3','Susan',11,'Hawaii');

  INSERT INTO SUN.STAFF5
        VALUES('E2','Tom',22,'Newyork');
-- PASS:0302 If ERROR, check constraint, 0 rows inserted?

  SELECT COUNT(*) FROM SUN.STAFF5;
-- PASS:0302 If count = 1?

-- restore
  ROLLBACK WORK;

-- END TEST >>> 0302 <<< END TEST

-- *************************************************


-- TEST:0303 CHECK <comp. predicate> in <col. cons.>, insert!

-- setup
  DELETE FROM SUN.STAFF6;

  INSERT INTO SUN.STAFF6
        VALUES('E1','Alice',0,'Deale');
-- PASS:0303 If ERROR, check constraint, 0 rows inserted?

  INSERT INTO SUN.STAFF6
        VALUES('E2','Tom',22,'Newyork');
-- PASS:0303 If ERROR, check constraint, 0 rows inserted?

  INSERT INTO SUN.STAFF6
        VALUES('E3','Susan',11,'Hawaii');

  SELECT GRADE FROM SUN.STAFF6
        WHERE GRADE > 10;
-- PASS:0303 If 1 row selected and GRADE = 11?

-- restore
  ROLLBACK WORK;

-- END TEST >>> 0303 <<< END TEST

-- *************************************************


-- TEST:0304 CHECK <between predicate> in <tab. cons.>, insert!

-- setup
  DELETE FROM SUN.STAFF7;

  INSERT INTO SUN.STAFF7
        VALUES('E1','Alice',0,'Deale');
-- PASS:0304 If ERROR, check constraint, 0 rows inserted?

  INSERT INTO SUN.STAFF7
        VALUES('E2','Tom',22,'Newyork');
-- PASS:0304 If ERROR, check constraint, 0 rows inserted?

  INSERT INTO SUN.STAFF7
        VALUES('E3','Susan',11,'Hawaii');

  SELECT COUNT(*)
        FROM SUN.STAFF7;
-- PASS:0304 If count = 1?

-- restore
  ROLLBACK WORK;

-- END TEST >>> 0304 <<< END TEST

-- *************************************************


-- TEST:0305 CHECK <null predicate> in <tab. cons.>, insert!

-- setup
  DELETE FROM SUN.STAFF8;

  INSERT INTO SUN.STAFF8
        VALUES('E1','Alice',34,'Deale');

  SELECT COUNT(*) FROM SUN.STAFF8;
-- PASS:0305 If count = 1?

  INSERT INTO SUN.STAFF8
        VALUES('E2',NULL,34,'Newyork');
-- PASS:0305 If ERROR, check constraint, 0 rows inserted?

  SELECT COUNT(*) FROM SUN.STAFF8;
-- PASS:0305 If count = 1?


  COMMIT WORK;
 
-- END TEST >>> 0305 <<< END TEST

-- *************************************************////END-OF-MODULE
