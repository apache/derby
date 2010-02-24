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
-- This test will cover SHOW TABLES, SHOW SCHEMAS, etc.
-- and the DESCRIBE command.
-- first, set schema to sys and demonstrate that we can see the system tables.
SET SCHEMA SYS;
SHOW TABLES;

SET SCHEMA APP;
CREATE TABLE t1 (i int generated always as identity, d DECIMAL(5,2), test VARCHAR(20));

CREATE SCHEMA USER1;
SET SCHEMA = USER1;
CREATE TABLE t2 (i int);

CREATE SYNONYM USER1.T3 FOR USER1.T2;
CREATE VIEW v1 AS SELECT * from app.t1;
CREATE INDEX idx1 ON APP.t1 (test ASC);
CREATE PROCEDURE APP.PROCTEST(IN A INTEGER, OUT B DECIMAL(10,2))
PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA 
EXTERNAL NAME 'a.b.c.d.e';
CREATE FUNCTION APP.FUNCTTEST(A INTEGER)
RETURNS INTEGER
PARAMETER STYLE JAVA
LANGUAGE JAVA
NO SQL
EXTERNAL NAME 'a.b.c.d.e.f';


-- first display all tables, then display tables in one schema
SHOW TABLES;
SHOW TABLES IN APP;
SHOW TABLES IN app;

-- 'describe t1' will give error, as not in current schema
DESCRIBE t1;
DESCRIBE APP.t1;
DESCRIBE app.t1;
DESCRIBE v1;

SHOW SCHEMAS;
SHOW VIEWS IN USER1;
SHOW PROCEDURES IN APP;
SHOW FUNCTIONS IN APP;
SHOW FUNCTIONS;
SHOW SYNONYMS IN USER1;

--
-- DERBY-4553
--
GET SCROLL INSENSITIVE CURSOR CURS AS 'SELECT * FROM APP.T1';
GETCURRENTROWNUMBER CURS;
CLOSE CURS;

-- DERBY-2019: ensure that tables with mixed-case names can be described:
SET SCHEMA APP;
create table "CamelCaseTable" (c1 int, c2 varchar(20));
-- should fail, as unquoted stirng is treated as case-insensitive upper case:
describe CamelCaseTable;
describe APP.CamelCaseTable;
-- should find the table, as quoted string case is preserved.
describe 'CamelCaseTable';
-- should fail, as case is wrong:
describe 'CAMELCaseTable';
-- should work, note that schema name must be upper case:
describe 'APP.CamelCaseTable';
set SCHEMA USER1;
-- should work, even after changing default schema, so long as schema is right
describe 'APP.CamelCaseTable';
-- should fail, since table is in the other schema
describe 'CamelCaseTable';
-- Can use * as a wildcard for table name:
describe '*';
describe 'APP.*';
-- Observe behavior with empty string:
describe '';

--
-- DERBY-4550: qualified identifiers
--

-- setup source db
connect 'jdbc:derby:wombat;user=fred' as DERBY4550_1;
create table t1(a int, b int);
insert into t1(a,b) values (1,100), (2,200), (3,300);
prepare fred_select as 'select a from t1';

-- setup destination db
connect 'jdbc:derby:wombat;user=alice' as DERBY4550_2;
create table t2(a int);

-- execute prepared statements
autocommit off;
execute fred_select@DERBY4550_1;
execute 'insert into t2(a) values(?)' using fred_select@DERBY4550_1;
commit;
remove fred_select@DERBY4550_1;

-- check result
select a from t2;

-- prepare in a different connection/switch/execute
prepare fred_select2@DERBY4550_1 as 'select b from t1';
set connection DERBY4550_1;
execute fred_select2;
remove fred_select2;

-- setup cursor/switch connection/use
get scroll insensitive cursor fred_cursor as 'select b from t1';
set connection DERBY4550_2;
next fred_cursor@DERBY4550_1;
-- getcurrentrownumber fred_cursor@DERBY4550_1;
last fred_cursor@DERBY4550_1;
previous fred_cursor@DERBY4550_1;
first fred_cursor@DERBY4550_1;
after last fred_cursor@DERBY4550_1;
before first fred_cursor@DERBY4550_1;
relative 2 fred_cursor@DERBY4550_1;
absolute 1 fred_cursor@DERBY4550_1;
close fred_cursor@DERBY4550_1;

-- non-existant connection
prepare fred_select@XXXX as 'values(1)';

-- async statements
async a@DERBY4550_1 'select a from t1';
wait for a@DERBY4550_1;

-- non existant statement
wait for xxxx@DERBY4550_1;

disconnect DERBY4550_2;
disconnect DERBY4550_1;
