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
--
-- test java.sql.Connection.DatabaseMetaData.getBestRowIdentifier()
--

-- create a java procedure to do the metadata call
create procedure getBestRowID(in schema_param Char(10), in tableName_param Char(10), in scope_param int, in nullable_param Char(5)) parameter style java reads sql data dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.metadataHelperProcs.getBestRowId'; 
prepare bestrow as 'call getBestRowID(?,?,?,?)';

autocommit off;

-- each one of these have only one choice
create table t1 (i int not null primary key, j int);
create table t2 (i int not null unique, j int);
-- adding not null unique to j - otherwise t2 & t3 would be same.
create table t3 (i int not null unique, j int not null unique);
create table t4 (i int, j int);
create unique index t4i on t4(i);
create table t5 (i int, j int);

-- result: column i
execute bestrow using 'values(''APP'',''T1'',0,''true'')';

-- result: column i
execute bestrow using 'values(''APP'',''T2'',0,''true'')';

-- result: column i
execute bestrow using 'values(''APP'',''T3'',0,''true'')';

-- result: column i
execute bestrow using 'values(''APP'',''T4'',0,''true'')';

-- result: columns i and j
execute bestrow using 'values(''APP'',''T5'',0,''true'')';

rollback work;

-- PK preferred to unique
create table t6 (i int not null unique, j int not null primary key);

-- result: column j
execute bestrow using 'values(''APP'',''T6'',0,''true'')';

-- PK preferred to unique index
create table t7 (i int not null, j int not null primary key);
create unique index t7i_index on t7(i);

-- result: column j
execute bestrow using 'values(''APP'',''T7'',0,''true'')';

-- unique con preferred to unique index
create table t8 (i int not null, j int not null unique);
create unique index t8i_index on t8(i);

-- result: column j
execute bestrow using 'values(''APP'',''T8'',0,''true'')';

-- non-unique index just ignored
create table t9 (i int, j int);
create index t9i_index on t9(i);

-- result: columns i,j
execute bestrow using 'values(''APP'',''T9'',0,''true'')';

rollback work;

-- fewer cols unique con still ignored over primary key
create table t10 (i int unique not null , j int not null , primary key (i,j));

-- result: columns i,j
execute bestrow using 'values(''APP'',''T10'',0,''true'')';

-- fewer cols unique index still ignored over primary key
create table t11 (i int not null, j int not null, primary key (i,j));
create unique index t11i_index on t11(i);

-- result: columns i,j
execute bestrow using 'values(''APP'',''T11'',0,''true'')';

-- fewer cols unique index still ignored over unique con
create table t12 (i int not null, j int not null, unique (i,j));
create unique index t12i_index on t12(i);

-- result: columns i,j
execute bestrow using 'values(''APP'',''T12'',0,''true'')';

rollback work;

-- REMIND: we aren't handling nullOk flag correctly
-- we just drop nullable cols, we should skip an answer
-- that has nullable cols in it instead and look for another one.
create table t13 (i int not null, j int not null, k int, unique (i,j));

-- result: columns i, j (WRONG) 
-- the correct answer is k: the non-null columns of the table
execute bestrow using 'values(''APP'',''T13'',0,''false'')';

-- fewest cols unique con is the one picked of several
create table t14 (i int not null unique, j int not null, k int, unique (i,j));

-- result: columns i
execute bestrow using 'values(''APP'',''T14'',0,''true'')';

-- fewest cols unique index is the one picked of several
create table t15 (i int not null, j int not null, k int);
create unique index t15ij on t15(i,j);
create unique index t15i on t15(i);

-- result: columns i
execute bestrow using 'values(''APP'',''T15'',0,''true'')';

-- we don't do anything with SCOPE except detect bad values
create table t16 (i int not null primary key, j int);

-- result: columns i
execute bestrow using 'values(''APP'',''T16'',1,''true'')';

-- result: columns i
execute bestrow using 'values(''APP'',''T16'',2,''true'')';

-- result: no rows
execute bestrow using 'values(''APP'',''T16'',-1,''true'')';

-- result: no rows
execute bestrow using 'values(''APP'',''T16'',3,''true'')';

rollback work;

drop procedure getBestRowID;
commit;
