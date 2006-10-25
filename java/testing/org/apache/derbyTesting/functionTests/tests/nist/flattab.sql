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

-- ***************************************************************
-- ****** THIS FILE SHOULD BE RUN UNDER AUTHORIZATION ID FLATER **
-- ***************************************************************
-- MODULE  FLATTAB  

-- SQL Test Suite, V6.0, Interactive SQL, flattab.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;

--0   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- This routine initializes the contents of tables:
--      BASE_VS1, USIG and U_SIG
-- This routine may be run at any time to re-initialize tables.

   DELETE FROM BASE_VS1;
   INSERT INTO BASE_VS1 VALUES (0,1);
   INSERT INTO BASE_VS1 VALUES (1,0);
   INSERT INTO BASE_VS1 VALUES (0,0);
   INSERT INTO BASE_VS1 VALUES (1,1);

   SELECT COUNT(*) FROM BASE_VS1;
-- PASS:Setup If count = 4?

   DELETE FROM USIG;
   INSERT INTO USIG VALUES (0,2);
   INSERT INTO USIG VALUES (1,3);

   DELETE FROM U_SIG;
   INSERT INTO U_SIG VALUES (4,6);
   INSERT INTO U_SIG VALUES (5,7);

   SELECT COUNT(*) FROM USIG;
-- PASS:Setup If count = 2?

   SELECT COUNT(*) FROM U_SIG;
-- PASS:Setup If count = 2?

   COMMIT WORK;
-- *************************************************////END-OF-MODULE

