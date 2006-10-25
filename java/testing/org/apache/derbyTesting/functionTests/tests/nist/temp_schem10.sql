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
