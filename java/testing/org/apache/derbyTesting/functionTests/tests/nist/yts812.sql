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

-- MODULE  YTS812  

-- SQL Test Suite, V6.0, Interactive SQL, yts812.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1              
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:7569 <null predicate> with concatenation in <row value constructor>!

--O   SELECT COUNT (*)
   SELECT *
     FROM TX
     WHERE TX2 || TX3 IS NOT NULL;
-- PASS:7569 If COUNT = 3?

   SELECT TX1 FROM TX
     WHERE TX3 || TX2 IS NULL;
-- PASS:7569 If 2 rows returned in any order?
-- PASS:7569 If TX1 = 1?
-- PASS:7569 If TX1 = 2?

   ROLLBACK WORK;

-- END TEST >>> 7569 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
