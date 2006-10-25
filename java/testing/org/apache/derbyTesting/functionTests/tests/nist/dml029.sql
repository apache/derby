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

-- MODULE DML029

-- SQL Test Suite, V6.0, Interactive SQL, dml029.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0129 Double quote work in character string literal!

-- setup
     INSERT INTO STAFF
            VALUES('E8','Yang Ling',15,'Xi''an');
-- PASS:0129 If 1 row is inserted?

      SELECT GRADE,CITY
           FROM STAFF
           WHERE EMPNUM = 'E8';
-- PASS:0129 If GRADE = 15 and CITY = 'Xi'an'?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0129 <<< END TEST
-- ************************************************************

-- TEST:0130 Approximate numeric literal <mantissa>E<exponent>!

-- setup
     INSERT INTO JJ
            VALUES(123.456E3);
-- PASS:0130 If 1 row is inserted?

--O      SELECT COUNT(*)
      SELECT *
           FROM JJ
           WHERE FLOATTEST > 123455 AND FLOATTEST < 123457;
-- PASS:0130 If count = 1 ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0130 <<< END TEST
-- ***************************************************************

-- TEST:0131 Approximate numeric literal with negative exponent!

-- setup
     INSERT INTO JJ
            VALUES(123456E-3);
-- PASS:0131 If 1 row is inserted?

--O      SELECT COUNT(*)
      SELECT *
           FROM JJ
           WHERE FLOATTEST > 122 AND FLOATTEST < 124;
-- PASS:0131 If count = 1 ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0131 <<< END TEST
-- ********************************************************

-- TEST:0182 Approx numeric literal with negative mantissa & exponent!

-- setup
     INSERT INTO JJ
            VALUES(-123456E-3);
-- PASS:0182 If 1 row is inserted?

--O     SELECT COUNT(*)
     SELECT *
           FROM JJ
           WHERE FLOATTEST > -124 AND FLOATTEST < -122;
-- PASS:0182 If count = 1 ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0182 <<< END TEST
-- *************************************************////END-OF-MODULE
