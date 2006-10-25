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

-- MODULE DML016

-- SQL Test Suite, V6.0, Interactive SQL, dml016.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION SULLIVAN
   create schema SULLIVAN;
   set schema SULLIVAN;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0064 SELECT USER!

     SELECT USER, PNAME
          FROM HU.PROJ;
-- PASS:0064 If 6 rows are selected and each USER = 'SULLIVAN' ?

-- END TEST >>> 0064 <<< END TEST
-- ***********************************************************

-- NO_TEST:0172 SELECT USER into short variable!
-- Tests Host Variable

-- **********************************************************

-- TEST:0065 SELECT CHAR literal and term with numeric literal!

     SELECT 'USER',PNAME
          FROM HU.PROJ;
-- PASS:0065 If 6 rows are selected and first column is value 'USER'?

     SELECT PNUM,'BUDGET IN GRAMS IS ',BUDGET * 5
          FROM HU.PROJ
          WHERE PNUM = 'P1';
-- PASS:0065 If values are 'P1', 'BUDGET IN GRAMS IS ', 50000?

-- END TEST >>> 0065 <<< END TEST
-- ************************************************************

-- TEST:0066 SELECT numeric literal!
     SELECT EMPNUM,10
          FROM HU.STAFF
          WHERE GRADE = 10;
-- PASS:0066 If 1 row with values 'E2' and 10?

     SELECT EMPNUM, 10
          FROM HU.STAFF;
-- PASS:0066 If 5 rows are selected with second value always = 10?
-- PASS:0066 and EMPNUMs are 'E1', 'E2', 'E3', 'E4', 'E5'?

-- END TEST >>> 0066 <<< END TEST
-- *************************************************////END-OF-MODULE
