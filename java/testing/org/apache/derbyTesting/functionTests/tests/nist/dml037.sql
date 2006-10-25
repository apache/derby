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
-- MODULE DML037

-- SQL Test Suite, V6.0, Interactive SQL, dml037.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- NO_TEST:0202 Host variable names same as column name!

-- Testing host identifier

-- ***********************************************************

-- TEST:0234 SQL-style comments with SQL statements!
-- OPTIONAL TEST

    DELETE  -- we empty the table  
        FROM TEXT240;

     INSERT INTO TEXT240   -- This is the test for the rules  
            VALUES         -- for the placement            
       ('SQL-STYLE COMMENTS') -- of
                              -- SQL-style comments 
      ;
-- PASS:0234 If 1 row is inserted?

    SELECT * 
            FROM TEXT240;
-- PASS:0234 If TEXXT = 'SQL-STYLE COMMENTS'?
     
-- restore
     ROLLBACK WORK;

-- END TEST >>> 0234 <<< END TEST
-- *************************************************////END-OF-MODULE
