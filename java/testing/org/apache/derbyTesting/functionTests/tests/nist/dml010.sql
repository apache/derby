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

-- MODULE DML010

-- SQL Test Suite, V6.0, Interactive SQL, dml010.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0027 INSERT short string in long col -- space padding !

-- setup 
     INSERT INTO TMP (T1, T2, T3) 
            VALUES ( 'xxxx',23,'xxxx');
-- PASS:0027 If 1 row inserted?
 
      SELECT *
           FROM TMP
           WHERE T2 = 23 AND T3 = 'xxxx      ';
-- PASS:0027 If T1 = 'xxxx      ' ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0027 <<< END TEST
-- ************************************************************* 

-- TEST:0028 Insert String that fits Exactly in Column!

-- setup
     INSERT INTO TMP (T1, T2, T3) 
            VALUES ('xxxxxxxxxx', 23,'xxxxxxxxxx'); 
-- PASS:0028 If 1 row inserted?

      SELECT *
           FROM TMP
           WHERE T2 = 23;
-- PASS:0028 If T1 = 'xxxxxxxxxx'?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0028 <<< END TEST
-- ***********************************************************

-- TEST:0031 INSERT(column list) VALUES(NULL and literals)!

-- setup
     INSERT INTO TMP (T2, T3, T1) 
            VALUES (NULL,'zz','z'); 
-- PASS:0031 If 1 row inserted?

      SELECT *
           FROM   TMP
           WHERE  T2 IS NULL;
-- PASS:0031 If T1 = 'z         '?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0031 <<< END TEST

-- *************************************************////END-OF-MODULE
