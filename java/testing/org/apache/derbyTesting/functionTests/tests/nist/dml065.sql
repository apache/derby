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

-- MODULE DML065

-- SQL Test Suite, V6.0, Interactive SQL, dml065.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0284 INSERT, SELECT char. strings with blank!

  INSERT INTO STAFF(EMPNUM,EMPNAME)
         VALUES ('E6','Ed');

  INSERT INTO STAFF(EMPNUM,EMPNAME)
         VALUES ('E7','Ed ');

  INSERT INTO STAFF(EMPNUM,EMPNAME)
         VALUES ('E8','Ed                  ');

--O  SELECT COUNT(*) FROM STAFF
  SELECT * FROM STAFF
                  WHERE EMPNAME = 'Ed';

-- PASS:0284 If count = 4?

--O  SELECT COUNT(*) FROM STAFF
  SELECT * FROM STAFF
                  WHERE EMPNAME = 'Ed ';

-- PASS:0284 If count = 4?

--O  SELECT COUNT(*) FROM STAFF
  SELECT * FROM STAFF
                  WHERE EMPNAME = 'Ed                ';

-- PASS:0284 If count = 4?

-- restore
  ROLLBACK WORK;

-- END TEST >>> 0284 <<< END TEST

-- *************************************************


-- TEST:0285 INSERT, SELECT integer with various formats!

  INSERT INTO STAFF(EMPNUM,GRADE)
         VALUES ('E6',25);

  INSERT INTO STAFF(EMPNUM,GRADE)
         VALUES ('E7',25.0);

  INSERT INTO STAFF(EMPNUM,GRADE)
         VALUES ('E8',-25);

  INSERT INTO STAFF(EMPNUM,GRADE)
         VALUES ('E9',25.000);

  UPDATE STAFF
         SET GRADE = -GRADE
         WHERE GRADE < 0;

--O  SELECT COUNT(*) FROM STAFF
  SELECT * FROM STAFF
                  WHERE GRADE = 25;

-- PASS:0285 If count = 4?

-- restore
  ROLLBACK WORK;


-- END TEST >>> 0285 <<< END TEST

-- *************************************************


-- NO_TEST:0286 Compatibility of structures and host variables!

-- Testing host identifiers

-- *************************************************////END-OF-MODULE
