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

-- MODULE DML012

-- SQL Test Suite, V6.0, Interactive SQL, dml012.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--0   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0037 DELETE without WHERE clause!
     SELECT COUNT(*)
          FROM STAFF;
-- PASS:0037 If count = 5?

      DELETE FROM STAFF;
-- PASS:0037 If 5 rows deleted?

      SELECT COUNT(*)
           FROM STAFF;
-- PASS:0037 If count = 0?

-- restore
     ROLLBACK WORK;

-- Testing Rollback
      SELECT COUNT(*)
           FROM STAFF;
-- PASS:0037 If count = 5?

-- END TEST >>> 0037 <<< END TEST
-- **************************************************************

-- TEST:0038 DELETE with correlated subquery in WHERE clause!
     SELECT COUNT(*)
          FROM WORKS;
-- PASS:0038 If count = 12?

     DELETE FROM WORKS
           WHERE WORKS.PNUM IN
                 (SELECT PROJ.PNUM
                       FROM PROJ
                       WHERE PROJ.PNUM=WORKS.PNUM
                       AND PROJ.CITY='Tampa');
-- PASS:0038 If 1 row deleted?

      SELECT COUNT(*)
           FROM WORKS;
-- PASS:0038 If count = 11?

-- restore
      ROLLBACK WORK;

-- Testing Rollback
      SELECT COUNT(*)
           FROM WORKS;
-- PASS:0038 If count = 12?

-- END TEST >>> 0038 <<< END TEST
-- *************************************************////END-OF-MODULE
