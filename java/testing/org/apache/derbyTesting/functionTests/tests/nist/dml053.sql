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

-- MODULE DML053

-- SQL Test Suite, V6.0, Interactive SQL, dml053.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0233 Table as multiset of rows - INSERT duplicate VALUES()!
 
-- setup
     INSERT INTO TEMP_S
            VALUES('E1',11,'Deale');
-- PASS:0233 If 1 row is inserted?

     INSERT INTO TEMP_S
            VALUES('E1',11,'Deale');
-- PASS:0233 If 1 row is inserted?

--O     SELECT COUNT(*)
     SELECT empnum
                FROM TEMP_S
                WHERE EMPNUM='E1' AND GRADE=11 AND CITY='Deale';
-- PASS:0233 If count = 2?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0233 <<< END TEST
-- *************************************************////END-OF-MODULE
