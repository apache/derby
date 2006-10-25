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

-- MODULE DML042

-- SQL Test Suite, V6.0, Interactive SQL, dml042.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0213 FIPS sizing -- 100 columns in a row!
-- FIPS sizing TEST

-- setup
     INSERT INTO T100(C1,C21,C41,C61,C81,C100)
            VALUES(' 1','21','41','61','81','00');
-- PASS:0213 If 1 row is inserted?

      SELECT C1,C21,C41,C61,C81,C100
           FROM T100;
-- PASS:0213 If C1 = ' 1' and C100 = '00' ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0213 <<< END TEST

-- *************************************************////END-OF-MODULE
