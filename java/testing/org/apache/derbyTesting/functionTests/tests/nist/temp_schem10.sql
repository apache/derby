AUTOCOMMIT OFF;

-- SQL Test Suite, V6.0, Schema Definition, schem10.std
-- 59-byte ID
-- TEd Version #
-- date_time print
-- ***************************************************************
-- ****** THIS FILE SHOULD BE RUN UNDER AUTHORIZATION ID SCHANZLE 
-- ***************************************************************

-- This file defines base tables used in the CDR tests.

-- This is a standard schema definition.

-- Constraints:  column vs. column

   create schema SCHANZLE;
   set schema SCHANZLE;

   CREATE TABLE RET_CATALOG (
     VENDOR_ID INT,
     PRODUCT_ID INT,
     WHOLESALE NUMERIC (10,2),
     RETAIL NUMERIC (10,2),
     MARKUP NUMERIC (10,2),
     EXPORT_CODE CHAR(2),
     EXPORT_LICNSE_DATE CHAR(20),
     CHECK (EXPORT_LICNSE_DATE IS NULL OR (
       EXPORT_CODE = 'F1' OR
       EXPORT_CODE = 'F2' OR
       EXPORT_CODE = 'F3'                  )),
     CHECK (EXPORT_CODE <> 'F2' OR WHOLESALE > 10000.00),
     CHECK (RETAIL >= WHOLESALE),
     CHECK (RETAIL = WHOLESALE + MARKUP));

   CREATE TABLE CPBASE
      (KC INT NOT NULL,
       JUNK1 CHAR (10),
       PRIMARY KEY (KC));

   CREATE TABLE FOUR_TYPES
      (T_INT     INTEGER,
       T_CHAR    CHAR(10),
       T_DECIMAL DECIMAL(10,2),
       T_REAL    REAL);

commit;
-- ************* End of Schema *************
