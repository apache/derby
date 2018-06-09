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
-- Test cases to test booting of encrypted databases using encryptionKey 
-- when using jar, classpath subprotocol.

--------------------------------------------------------------------
-- Case: create encrypted database using encryptionKey, jar it up and then test 
-- using the jar protocol.

-- create encrypted database.
connect 'jdbc:derby:encdb;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
create table t1(a int ) ;
insert into t1 values(1) ;
insert into t1 values(2) ;
insert into t1 values(3) ;
insert into t1 values(4) ;
insert into t1 values(5) ;
connect 'jdbc:derby:encdb;shutdown=true';

-- now create archive of encrypted database.
connect 'jdbc:derby:wombat;create=true';
create procedure CREATEARCHIVE(jarName VARCHAR(20), path VARCHAR(20), dbName VARCHAR(20))
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.createArchive';

-- archive the encdb and put in ina.jar with dbname as db1 and ina2.jar as db2.
call CREATEARCHIVE('ina.jar', 'encdb', 'db1');
call CREATEARCHIVE('ina2.jar','encdb','db2');
disconnect;

-- now that we have the database in a jar file,
-- test using the jar protocol to connect to the db1 that is in ina.jar
-- Should pass: ( DERBY-1373)
connect 'jdbc:derby:jar:(ina.jar)db1;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768' AS DB1;
select * from t1 order by a;
connect 'jdbc:derby:;shutdown=true';

-- NEGATIVE CASE: Test with wrong encryption key. This should fail.
connect 'jdbc:derby:jar:(ina.jar)db1;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666760' AS DB1;
disconnect;

-- test reading a database from a jar file and loading
-- classes etc. from the jars within the database.
-- first using the jar protocol and then the classpath option.

connect 'jdbc:derby:jar:(ina.jar)db1;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768' AS DB1;
connect 'jdbc:derby:;shutdown=true';

-- connect to database in jar file using classpath subprotocol.
-- should fail as it is not on the classpath yet.
connect 'jdbc:derby:classpath:db2;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768' AS DB2CL;

-- create a class loader for this current thread
connect 'jdbc:derby:encdb;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
create procedure setDBContextClassLoader(JARNAME VARCHAR(20))
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.setDBContextClassLoader';

call setDBContextClassLoader('ina2.jar');
disconnect;

-- connect using classpath option with correct encryption key.
connect 'jdbc:derby:classpath:db2;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768' AS DB2CL;
select * from t1 order by a;
connect 'jdbc:derby:;shutdown=true';
-- try with wrong encryption key, this should fail.
connect 'jdbc:derby:classpath:db2;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666760' AS DB2CL;
exit;