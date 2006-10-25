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

-- MODULE DML076

-- SQL Test Suite, V6.0, Interactive SQL, dml076.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- NO_TEST:0435 Host variables in UPDATE WHERE CURRENT!

-- Testing cursors <update statement:positioned>

-- *************************************************************

-- TEST:0436 NULL values for various SQL data types!

    INSERT INTO BB VALUES(NULL);
    INSERT INTO EE VALUES(NULL);
    INSERT INTO GG VALUES(NULL);
    INSERT INTO HH VALUES(NULL);
    INSERT INTO II VALUES(NULL);
    INSERT INTO JJ VALUES(NULL);
    INSERT INTO MM VALUES(NULL);
    INSERT INTO SS VALUES(NULL);
 

    SELECT CHARTEST 
      FROM BB;
-- PASS:0436 If CHARTEST is NULL (Implementor defined print format)?


    SELECT INTTEST
      FROM EE;
-- PASS:0436 If INTTEST is NULL (Implementor defined print format)?


    SELECT REALTEST 
      FROM GG;
-- PASS:0436 If REALTEST is NULL (Implementor defined print format)?


--O    SELECT COUNT(*)
    SELECT *
      FROM GG 
      WHERE REALTEST IS NULL;
-- PASS:0436 If count = 1?


    SELECT SMALLTEST 
      FROM HH;
-- PASS:0436 If SMALLTEST is NULL (Implementor defined print format)?


    SELECT DOUBLETEST 
      FROM II;
-- PASS:0436 If DOUBLETEST is NULL (Implementor defined print format)?


--O    SELECT COUNT(*) 
    SELECT * 
      FROM II 
      WHERE DOUBLETEST IS NULL;
-- PASS:0436 If count = 1?


    SELECT FLOATTEST 
      FROM JJ;
-- PASS:0436 If FLOATTEST is NULL (Implementor defined print format)?


--O    SELECT COUNT(*) 
    SELECT * 
      FROM JJ 
      WHERE FLOATTEST IS NULL;
-- PASS:0436 If count = 1?


    SELECT NUMTEST  
      FROM MM;
-- PASS:0436 If NUMTEST is NULL (Implementor defined print format)?


    SELECT NUMTEST 
      FROM SS;
-- PASS:0436 If NUMTEST is NULL (Implementor defined print format)?


-- restore
    ROLLBACK WORK;

-- END TEST >>> 0436 <<< END TEST
-- *************************************************************

-- NO_TEST:0437 NULL values for various host variable types!

-- Testing Host Variables & Indicator Variables

-- *************************************************************

-- NO_TEST:0410 NULL value in OPEN CURSOR!

-- Testing Cursors & Indicator Variables

-- *************************************************************

-- NO_TEST:0441 NULL value for various predicates!

-- Testing Indicator Variables

-- *************************************************////END-OF-MODULE
