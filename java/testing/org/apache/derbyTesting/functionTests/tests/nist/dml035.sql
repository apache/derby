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

-- MODULE DML035

-- SQL Test Suite, V6.0, Interactive SQL, dml035.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0157 ORDER BY approximate numeric!

-- setup
     INSERT INTO JJ VALUES(66.2);
-- PASS:0157 If 1 row is inserted?
     INSERT INTO JJ VALUES(-44.5);
-- PASS:0157 If 1 row is inserted?
     INSERT INTO JJ VALUES(0.2222);
-- PASS:0157 If 1 row is inserted?
     INSERT INTO JJ VALUES(66.3);
-- PASS:0157 If 1 row is inserted?
     INSERT INTO JJ VALUES(-87);
-- PASS:0157 If 1 row is inserted?
     INSERT INTO JJ VALUES(-66.25);
-- PASS:0157 If 1 row is inserted?

     SELECT FLOATTEST
          FROM JJ
          ORDER BY FLOATTEST DESC;
-- PASS:0157 If 6 rows are selected ?
-- PASS:0157 If last FLOATTEST = -87 OR  is between -87.5 and -86.5 ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0157 <<< END TEST
-- *************************************************////END-OF-MODULE
