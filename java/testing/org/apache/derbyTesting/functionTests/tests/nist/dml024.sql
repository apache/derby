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

-- MODULE DML024

-- SQL Test Suite, V6.0, Interactive SQL, dml024.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
 
-- date_time print

-- TEST:0108 Search condition true OR NOT(true)!
     SELECT EMPNUM,CITY                                                 
          FROM   STAFF                                                        
          WHERE  EMPNUM='E1' OR NOT(EMPNUM='E1');
-- PASS:0108 If 5 rows are selected ?

-- END TEST >>> 0108 <<< END TEST
-- ****************************************************************

-- TEST:0109 Search condition true AND NOT(true)!
     SELECT EMPNUM,CITY                                                  
          FROM   STAFF                                                       
          WHERE  EMPNUM='E1' AND NOT(EMPNUM='E1');
-- PASS:0109 If 0 rows are selected ?

-- END TEST >>> 0109 <<< END TEST
-- **************************************************************

-- TEST:0110 Search condition unknown OR NOT(unknown)!

-- setup
     INSERT INTO WORKS
            VALUES('E8','P8',NULL);
-- PASS:0110 If 1 row is inserted?
                                                   
     SELECT EMPNUM,PNUM                                                  
          FROM   WORKS                                                       
          WHERE HOURS < (SELECT HOURS FROM WORKS                              
                    WHERE EMPNUM = 'E8')                                     
          OR NOT(HOURS < (SELECT HOURS FROM WORKS                              
                    WHERE EMPNUM = 'E8'));
-- PASS:0110 If 0 rows are selected ?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0110 <<< END TEST
-- *************************************************************

-- TEST:0111 Search condition unknown AND NOT(unknown)!

-- setup
     INSERT INTO WORKS
            VALUES('E8','P8',NULL);
-- PASS:0111 If 1 row is inserted?
                                                   
     SELECT EMPNUM,PNUM                                                
          FROM   WORKS                                                       
          WHERE HOURS < (SELECT HOURS FROM WORKS                            
                    WHERE EMPNUM = 'E8')                                     
          AND NOT(HOURS< (SELECT HOURS FROM WORKS                              
                    WHERE EMPNUM = 'E8'));

-- PASS:0111 If 0 rows are selected?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0111 <<< END TEST
-- ***************************************************************

-- TEST:0112 Search condition unknown AND true!

-- setup
     INSERT INTO WORKS
            VALUES('E8','P8',NULL);
-- PASS:0112 If 1 row is inserted?
                                                           
     SELECT EMPNUM,PNUM                                                 
          FROM   WORKS                                                      
          WHERE HOURS < (SELECT HOURS FROM WORKS                              
                    WHERE EMPNUM = 'E8')                                      
          AND   HOURS IN (SELECT HOURS FROM WORKS);

-- PASS:0112 If 0 rows are selected?

-- restore
     ROLLBACK WORK;

-- END TEST >>> 0112 <<< END TEST
-- *************************************************************

-- TEST:0113 Search condition unknown OR true!

-- setup
     INSERT INTO WORKS
            VALUES('E8','P8',NULL);
-- PASS:0113 If 1 row is inserted?
                                                  
     SELECT EMPNUM,PNUM                                                 
          FROM   WORKS                                                        
          WHERE HOURS < (SELECT HOURS FROM WORKS                              
                    WHERE EMPNUM = 'E8')                                     
          OR    HOURS IN (SELECT HOURS FROM WORKS)
          ORDER BY EMPNUM;

-- PASS:0113 If 12 rows are selected?
-- PASS:0113 If first EMPNUM = 'E1'?

-- restore
     ROLLBACK WORK;                                                  

-- END TEST >>> 0113 <<< END TEST
-- *************************************************////END-OF-MODULE
