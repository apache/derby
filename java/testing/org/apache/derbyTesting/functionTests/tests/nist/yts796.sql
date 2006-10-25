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

-- MODULE  YTS796  

-- SQL Test Suite, V6.0, Interactive SQL, yts796.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:7530 <scalar subquery> as first operand in <comp pred>!

--O   SELECT EMPNAME FROM STAFF WHERE
--O    (SELECT EMPNUM FROM WORKS WHERE PNUM = 'P3')
   SELECT EMPNAME FROM HU.STAFF WHERE
    (SELECT EMPNUM FROM HU.WORKS WHERE PNUM = 'P3')
     = EMPNUM;
-- PASS:7530 If empname = 'Alice'?

--O   SELECT EMPNAME FROM STAFF WHERE 
--O     (SELECT EMPNUM FROM WORKS WHERE PNUM = 'P4')
   SELECT EMPNAME FROM HU.STAFF WHERE 
     (SELECT EMPNUM FROM HU.WORKS WHERE PNUM = 'P4')
     = EMPNUM;
-- PASS:7530 If ERROR - cardinality violation?

   ROLLBACK WORK;

-- END TEST >>> 7530 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
