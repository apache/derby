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

-- MODULE DML015

-- SQL Test Suite, V6.0, Interactive SQL, dml015.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- NO_TEST:0060 COMMIT work closes CURSORs!

-- Testing cursors
  
-- ************************************************************

-- TEST:0061 COMMIT work keeps changes to database!

     INSERT INTO TEMP_S
           SELECT EMPNUM, GRADE, CITY
                FROM STAFF;
-- PASS:0061 If 5 rows are inserted?

     COMMIT WORK;
 
-- verify previous COMMIT keeps changes
     ROLLBACK WORK;

--O     SELECT COUNT(*)
     SELECT *
          FROM TEMP_S;
-- PASS:0061 If count = 5?

-- END TEST >>> 0061 <<< END TEST
-- ************************************************************

-- TEST:0062 ROLLBACK work cancels changes to database!
-- NOTE:0062 uses data created by TEST 0061

     DELETE FROM TEMP_S
           WHERE EMPNUM = 'E5';
-- PASS:0062 If 1 row is deleted?

--O        SELECT COUNT(*)
        SELECT *
             FROM TEMP_S;
-- PASS:0062 If count = 4?

-- restore
     ROLLBACK WORK;
 
--O     SELECT COUNT(*)
        SELECT *
          FROM TEMP_S;
-- PASS:0062 If count = 5?

-- restore
     DELETE FROM TEMP_S;
     COMMIT WORK;

-- END TEST >>> 0062 <<< END TEST
-- ***********************************************************

-- NO_TEST:0063 ROLLBACK work closes CURSORs!

-- Testing cursors
-- *************************************************////END-OF-MODULE
