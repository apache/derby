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
run resource '/org/apache/derbyTesting/functionTests/util/testRoutines.sql';
CREATE FUNCTION GET_TABLE_PROPERTY (SCHEMA_NAME VARCHAR(128), TABLE_NAME VARCHAR(128), PROP_KEY VARCHAR(1000)) RETURNS VARCHAR(1000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestPropertyInfo.getTableProperty' LANGUAGE JAVA PARAMETER STYLE JAVA;


-- Get a property that hasn't been set yet - should return null
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('key1');

-- Set a couple of properties
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('key1', 'one, two, three');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('key2', 'eins, zwei, drei');

-- and fetch them
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('key1');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('key2');


-- and delete one of theme
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('key2', null);

-- and fetch them
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('key1');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('key2');


-- Now check some explicit properties

-- ************ derby.storage.pageSize

-- See what the default is first
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageSize');
drop table T;

-- set the per-database value
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '16384');

-- this create table should pick up the per-database
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageSize');
drop table T;


-- ************ derby.storage.minimumRecordSize

-- See what the default is first
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.minimumRecordSize');
drop table T;

-- set the per-database value
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.minimumRecordSize', '42');

-- this create table should pick up the per-database
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.minimumRecordSize');
drop table T;


-- ************ derby.storage.pageReservedSpace

-- See what the default is first
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageReservedSpace');
drop table T;

-- set the per-database value
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace', '17');

-- this create table should pick up the per-database
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageReservedSpace');
drop table T;




-- ************ derby.database.noAutoBoot
-- should be set in service.properties, not the conglomerate, but that's transparent here ... 

values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.noAutoBoot');

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.noAutoBoot', 'true');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.noAutoBoot');


call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.noAutoBoot', 'false');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.noAutoBoot');

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.noAutoBoot', null);
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.noAutoBoot');
-- Now check some explicit properties


-- Now check with derby.storage.pageSize if derby.database.propertiesOnly
-- ensures that system wide properties are ignored

-- See is currently set, should be 16384
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageSize');

drop table T;

-- set system value
CALL TESTROUTINE.SET_SYSTEM_PROPERTY('derby.storage.pageSize', '8192');

-- this create table should pick up the system value - 8192
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageSize');
drop table T;

-- 
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.propertiesOnly', 'true');


-- this create table should pick up the database value - 16384
create table T (i int);
values GET_TABLE_PROPERTY('APP', 'T', 'derby.storage.pageSize');
drop table T;

-- verify that creation time only properties may not be set.
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.engineType', '9');
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.engineType');

drop function GET_TABLE_PROPERTY;
