-- MODULE DML020

-- SQL Test Suite, V6.0, Interactive SQL, dml020.sql
-- 59-byte ID
-- TEd Version #
                                                                  
-- AUTHORIZATION HU
   set schema HU;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment

-- date_time print

-- TEST:0080 Simple two-table join!
     SELECT EMPNUM,EMPNAME,GRADE,STAFF.CITY, PNAME, PROJ.CITY          
          FROM STAFF, PROJ                                                   
          WHERE STAFF.CITY = PROJ.CITY
		ORDER BY EMPNUM, EMPNAME, GRADE, STAFF.CITY, PNAME;
-- PASS:0080 If 10 rows are selected with EMPNAMEs:'Alice', 'Betty', ?
-- PASS:0080 'Carmen', and 'Don' but not 'Ed'?

-- END TEST >>> 0080 <<< END TEST
-- **************************************************************

-- TEST:0081 Simple two-table join with filter!
--
-- Added ORDER BY clause to get stable results across platforms - Jeff
     SELECT EMPNUM,EMPNAME,GRADE,STAFF.CITY,PNUM,PNAME,         
                                PTYPE,BUDGET,PROJ.CITY                        
          FROM STAFF, PROJ                                                  
          WHERE STAFF.CITY = PROJ.CITY                                       
          AND GRADE <> 12
		ORDER BY EMPNUM, EMPNAME, GRADE, STAFF.CITY, PNUM, PNAME;
-- PASS:0081 If 4 rows selected with EMPNAMEs 'Betty' and 'Carmen' ?

-- END TEST >>> 0081 <<< END TEST
-- ************************************************************** 

-- TEST:0082 Join 3 tables!
     SELECT DISTINCT STAFF.CITY, PROJ.CITY                                
          FROM STAFF, WORKS, PROJ                                            
          WHERE STAFF.EMPNUM = WORKS.EMPNUM                                 
          AND WORKS.PNUM = PROJ.PNUM
		ORDER BY STAFF.CITY, PROJ.CITY;
-- PASS:0082 If 5 distinct rows are selected ?

-- END TEST >>> 0082 <<< END TEST
-- ************************************************************

-- TEST:0083 Join a table with itself!
     SELECT FIRST1.EMPNUM, SECOND2.EMPNUM                               
          FROM STAFF FIRST1, STAFF SECOND2                                   
          WHERE FIRST1.CITY = SECOND2.CITY                                  
          AND FIRST1.EMPNUM < SECOND2.EMPNUM
		ORDER BY FIRST1.EMPNUM, SECOND2.EMPNUM;
-- PASS:0083 If 2 rows are selected and ?
-- PASS:0083 If EMPNUM pairs are 'E1'/'E4' and 'E2'/'E3'?

-- END TEST >>> 0083 <<< END TEST
-- *************************************************////END-OF-MODULE
