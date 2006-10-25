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

-- MODULE DML068 

-- SQL Test Suite, V6.0, Interactive SQL, dml068.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU 
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0389 95-character graphic subset of ASCII!
-- NOTE:  OPTIONAL test
-- NOTE:0389 Collating sequence is implementor defined

     DELETE FROM AA;
-- Making sure the table is empty

-- setup
     INSERT INTO AA VALUES('@ at');
     INSERT INTO AA VALUES('`-qt');
     INSERT INTO AA VALUES('!exc');
     INSERT INTO AA VALUES('"dqt');
     INSERT INTO AA VALUES('#pou');
     INSERT INTO AA VALUES('$dol');
     INSERT INTO AA VALUES('%pct');
     INSERT INTO AA VALUES('&amp');
     INSERT INTO AA VALUES('''+qt');
     INSERT INTO AA VALUES('(lpr');
     INSERT INTO AA VALUES(')rpr');
     INSERT INTO AA VALUES('*ast');
     INSERT INTO AA VALUES('aaaa');
     INSERT INTO AA VALUES(':col');
     INSERT INTO AA VALUES('+plu');
     INSERT INTO AA VALUES(';sem');
     INSERT INTO AA VALUES('[lbk');
     INSERT INTO AA VALUES('{lbc');
     INSERT INTO AA VALUES(',com');
     INSERT INTO AA VALUES('< lt');
     INSERT INTO AA VALUES('\bsl');
     INSERT INTO AA VALUES('|dvt');
     INSERT INTO AA VALUES('-hyp');
     INSERT INTO AA VALUES('=equ');
     INSERT INTO AA VALUES(']rbk');
     INSERT INTO AA VALUES('}rbc');
     INSERT INTO AA VALUES('.per');
     INSERT INTO AA VALUES('> gt');
     INSERT INTO AA VALUES('^hat');
     INSERT INTO AA VALUES('~til');
     INSERT INTO AA VALUES('/ sl');
     INSERT INTO AA VALUES('?que');
     INSERT INTO AA VALUES('_und');
     INSERT INTO AA VALUES('AAAA');
     INSERT INTO AA VALUES('0000');
     INSERT INTO AA VALUES('9999');
     INSERT INTO AA VALUES('zzzz');
     INSERT INTO AA VALUES('  sp');
     INSERT INTO AA VALUES('ZZZZ');

     SELECT * FROM AA
          ORDER BY CHARTEST;
-- PASS:0389 If character in 1st position matches                 ?
-- PASS:0389    description in positions 2-4                      ?
-- PASS:0389 If ASCII, then ORDER is: space followed by characters?
-- PASS:0389    !"#$%&'()*+,-./09:;<=>?@AZ[\]^_`az{|}~            ?


--O     SELECT COUNT(*)
     SELECT *
          FROM AA;
-- PASS:0389 If count = 39? 

-- restore
     ROLLBACK WORK;


-- END TEST >>> 0389 <<< END TEST
-- *************************************************////END-OF-MODULE
