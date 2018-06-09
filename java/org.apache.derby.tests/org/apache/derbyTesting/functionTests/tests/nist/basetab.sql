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
-- ****** THIS FILE SHOULD BE RUN UNDER AUTHORIZATION ID HU ******
-- ***************************************************************
-- MODULE BASETAB

-- SQL Test Suite, V6.0, Interactive SQL, basetab.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU

--0   SELECT USER FROM HU.ECCO;
   VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

--   This routine initializes the contents of tables:
--        STAFF, PROJ, WORKS, STAFF3, VTABLE, and UPUNIQ
--   This routine may be run at any time to re-initialize tables.

   DELETE FROM HU.ECCO;
   INSERT INTO HU.ECCO VALUES ('NL');
      DELETE FROM HU.STAFF;
      DELETE FROM HU.PROJ;
      DELETE FROM HU.WORKS;

      INSERT INTO HU.STAFF VALUES ('E1','Alice',12,'Deale');
      INSERT INTO HU.STAFF VALUES ('E2','Betty',10,'Vienna');
      INSERT INTO HU.STAFF VALUES ('E3','Carmen',13,'Vienna');
      INSERT INTO HU.STAFF VALUES ('E4','Don',12,'Deale');
      INSERT INTO HU.STAFF VALUES ('E5','Ed',13,'Akron');

      INSERT INTO HU.PROJ VALUES  ('P1','MXSS','Design',10000,'Deale');
      INSERT INTO HU.PROJ VALUES  ('P2','CALM','Code',30000,'Vienna');
      INSERT INTO HU.PROJ VALUES  ('P3','SDP','Test',30000,'Tampa');
      INSERT INTO HU.PROJ VALUES  ('P4','SDP','Design',20000,'Deale');
      INSERT INTO HU.PROJ VALUES  ('P5','IRM','Test',10000,'Vienna');
      INSERT INTO HU.PROJ VALUES  ('P6','PAYR','Design',50000,'Deale');

      INSERT INTO HU.WORKS VALUES  ('E1','P1',40);
      INSERT INTO HU.WORKS VALUES  ('E1','P2',20);
      INSERT INTO HU.WORKS VALUES  ('E1','P3',80);
      INSERT INTO HU.WORKS VALUES  ('E1','P4',20);
      INSERT INTO HU.WORKS VALUES  ('E1','P5',12);
      INSERT INTO HU.WORKS VALUES  ('E1','P6',12);
      INSERT INTO HU.WORKS VALUES  ('E2','P1',40);
      INSERT INTO HU.WORKS VALUES  ('E2','P2',80);
      INSERT INTO HU.WORKS VALUES  ('E3','P2',20);
      INSERT INTO HU.WORKS VALUES  ('E4','P2',20);
      INSERT INTO HU.WORKS VALUES  ('E4','P4',40);
      INSERT INTO HU.WORKS VALUES  ('E4','P5',80);

      COMMIT WORK;

--O      SELECT COUNT(*) FROM HU.PROJ;
      SELECT * FROM HU.PROJ;
-- PASS:Setup if count = 6?

--O      SELECT COUNT(*) FROM HU.STAFF;
      SELECT * FROM HU.STAFF;
-- PASS:Setup if count = 5?

--O      SELECT COUNT(*) FROM HU.WORKS;
      SELECT * FROM HU.WORKS;
-- PASS:Setup if count = 12?


      DELETE FROM HU.STAFF3;
      DELETE FROM HU.VTABLE;
      DELETE FROM HU.UPUNIQ;

      INSERT INTO HU.STAFF3
              SELECT * 
              FROM HU.STAFF;

      INSERT INTO HU.VTABLE VALUES(10,+20,30,40,10.50);
      INSERT INTO HU.VTABLE VALUES(0,1,2,3,4.25);
      INSERT INTO HU.VTABLE VALUES(100,200,300,400,500.01);
      INSERT INTO HU.VTABLE VALUES(1000,-2000,3000,NULL,4000.00);

      INSERT INTO HU.UPUNIQ VALUES(1,'A');
      INSERT INTO HU.UPUNIQ VALUES(2,'B');
      INSERT INTO HU.UPUNIQ VALUES(3,'C');
      INSERT INTO HU.UPUNIQ VALUES(4,'D');
      INSERT INTO HU.UPUNIQ VALUES(6,'F');
      INSERT INTO HU.UPUNIQ VALUES(8,'H');

      COMMIT WORK;

--O      SELECT COUNT(*) FROM HU.STAFF3;
      SELECT * FROM HU.STAFF3;
-- PASS:Setup if count = 5?

--O      SELECT COUNT(*) FROM HU.VTABLE;
-- PASS:Setup if count = 4?

--O      SELECT COUNT(*) FROM HU.UPUNIQ;
      SELECT * FROM HU.UPUNIQ;
-- PASS:Setup if count = 6?
-- *************************************************////END-OF-MODULE
disconnect;
