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
-- MODULE DML051

-- SQL Test Suite, V6.0, Interactive SQL, dml051.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--0   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0227 BETWEEN predicate with character string values!
      SELECT PNUM
           FROM   PROJ
           WHERE  PNAME BETWEEN 'A' AND 'F';
-- PASS:0227 If PNUM = 'P2'?

      SELECT PNUM
           FROM   PROJ
           WHERE PNAME >= 'A' AND PNAME <= 'F';
-- PASS:0227 If PNUM = 'P2'?

-- END TEST >>> 0227 <<< END TEST
-- ***********************************************************

-- TEST:0228 NOT BETWEEN predicate with character string values!
      SELECT CITY
           FROM   STAFF
           WHERE  EMPNAME NOT BETWEEN 'A' AND 'E';
-- PASS:0228 If CITY = 'Akron'?

      SELECT CITY
           FROM   STAFF
           WHERE  NOT( EMPNAME BETWEEN 'A' AND 'E' );
-- PASS:0228 If CITY = 'Akron'?

-- END TEST >>> 0228 <<< END TEST
-- *************************************************////END-OF-MODULE
