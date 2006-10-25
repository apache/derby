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

-- MODULE DML043

-- SQL Test Suite, V6.0, Interactive SQL, dml043.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0214 FIPS sizing -- 2000-byte row!
-- FIPS sizing TEST

-- setup
     INSERT INTO T2000(STR110,STR200,STR216)
            VALUES
               ('STR11111111111111111111111111111111111111111111111',
                'STR22222222222222222222222222222222222222222222222',
                'STR66666666666666666666666666666666666666666666666');

-- PASS:0214 If 1 row is inserted?

      UPDATE T2000
           SET STR140 =
           'STR44444444444444444444444444444444444444444444444';
-- PASS:0214 If 1 row is updated?

      UPDATE T2000
           SET STR180 =
           'STR88888888888888888888888888888888888888888888888';
-- PASS:0214 If 1 row is updated?

      SELECT STR110,STR180,STR216
           FROM T2000;
-- PASS:0214 If STR180 = ?
-- PASS:0214   'STR88888888888888888888888888888888888888888888888'?
-- PASS:0214 If STR216 = ?
-- PASS:0214   'STR66666666666666666666666666666666666666666666666'?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0214 <<< END TEST
-- *************************************************////END-OF-MODULE
