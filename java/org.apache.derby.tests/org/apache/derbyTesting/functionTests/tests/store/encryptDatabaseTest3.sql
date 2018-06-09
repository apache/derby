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
-- This script tests error cases where encryption of an un-encryped database 
-- or re-encrption of an encrypted databases with new password/key should fail 
-- when 
--   1) the database is booted read-only mode using jar subprotocol.
--   2) the databases with log archive mode enabled. It should 
---     succeed after disabling the log archive mode.
--   3) when restoring from backup.

--------------------------------------------------------------------
-- Case : create a plain database, jar it up and then attempt 
-- to encrypt using the jar protocol 

connect 'jdbc:derby:endb;create=true';
create table t1(a int ) ;
insert into t1 values(1) ;
insert into t1 values(2) ;
insert into t1 values(3) ;
insert into t1 values(4) ;
insert into t1 values(5) ;
disconnect;
connect 'jdbc:derby:endb;shutdown=true';

-- now create archive of the  database.
connect 'jdbc:derby:wombat;create=true';
create procedure CREATEARCHIVE(jarName VARCHAR(20), path VARCHAR(20), dbName VARCHAR(20))
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.createArchive';

-- archive the "endb" and put in "ina.jar" with dbname as "jdb1".
call CREATEARCHIVE('ina.jar', 'endb', 'jdb1');
disconnect;

-- try encrypting the database 'jdb1' using the jar protocol.
-- should fail 
connect 'jdbc:derby:jar:(ina.jar)jdb1;dataEncryption=true;bootPassword=xyz1234abc';
connect 'jdbc:derby:jar:(ina.jar)jdb1;dataEncryption=true;encryptionKey=6162636465666768';

-- Case: create a a jar file of an encrypted database and  
-- try  re-encrypting it while boot it with the jar sub protocol 

-- encrypt the databases.
connect 'jdbc:derby:endb;dataEncryption=true;bootPassword=xyz1234abc';
insert into t1 values(6);
insert into t1 values(7);
disconnect;
connect 'jdbc:derby:endb;shutdown=true';

-- create archive of encrypted  database.
connect 'jdbc:derby:wombat';
call CREATEARCHIVE('ina.jar', 'endb', 'jdb1');
disconnect;

-- test the encrypted jar db 
connect 'jdbc:derby:jar:(ina.jar)jdb1;dataEncryption=true;bootPassword=xyz1234abc;';
select * from t1;
disconnect;
connect 'jdbc:derby:;shutdown=true';

-- now finally attempt to re-encrypt the encrypted jar db with 
-- a new boot password, it should fail.
connect 'jdbc:derby:jar:(ina.jar)jdb1;dataEncryption=true;bootPassword=xyz1234abc;newBootPassword=new1234xyz';

-- Decrypting a read-only db should also fail.
connect 'jdbc:derby:jar:(ina.jar)jdb1;bootPassword=xyz1234abc;decryptDatabase=true';

-- testing (re) encryption of a database 
-- when the log arhive mode enabled -----

-- Case : configuring a un-encrypted database for 
-- encryption should fail, when log archive mode is enabled.
connect 'jdbc:derby:wombat';
create table emp(id int, name char (200)); 
insert into emp values (1, 'john');
insert into emp values(2 , 'mike');
insert into emp values(3 , 'robert');
-- take a backup , this is used later. 
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup');
-- enable the log archive mode and perform backup.
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                           'extinout/mybackup1', 0);
insert into emp select * from emp ;
insert into emp select * from emp ;
insert into emp select * from emp ;
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- attempt to configure the database for encryption using password.
connect 'jdbc:derby:wombat;dataEncryption=true;bootPassword=xyz1234abc;';
-- attempt to configure the database for encryption using key.
connect 'jdbc:derby:wombat;dataEncryption=true;encryptionKey=6162636465666768';

-- disable log archive mode and then reattempt encryption on 
-- next boot.
connect 'jdbc:derby:wombat';
select count(*) from emp ;
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- Case: encrypt the database, with log archive mode disabled.  
connect 'jdbc:derby:wombat;dataEncryption=true;bootPassword=xyz1234abc;';
select count(*) from emp;
create table t1(a int ) ;
insert into t1 values(1);
-- enable log archive mode and perform backup.
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                           'extinout/mybackup2', 0);
insert into t1 values(2);
insert into t1 values(3);
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- attempt to re-encrypt the database , with log archive mode enabled.
-- it should fail.
connect 'jdbc:derby:wombat;dataEncryption=true;bootPassword=xyz1234abc;newBootPassword=new1234xyz';

-- Attempt to decrypt the database with log archive mode enabled.
-- It should fail.
connect 'jdbc:derby:wombat;bootPassword=xyz1234abc;decryptDatabase=true';

-- reboot the db and disable the log archive mode
connect 'jdbc:derby:wombat;bootPassword=xyz1234abc';
select * from t1;
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- re-encrypt the database, with the log archive mode disabled. 
-- it should pass. 
connect 'jdbc:derby:wombat;dataEncryption=true;bootPassword=xyz1234abc;newBootPassword=new1234xyz';
select * from t1;
select count(*) from emp;
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- testing re-encryption with external key on a log archived database.

-- restore from the backup orignal un-encrypted database and
-- encrypt with a key. 
connect 'jdbc:derby:wombat;restoreFrom=extinout/mybackup1/wombat';
select count(*) from emp;
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- encrypt with a key and enable the log archive mode.
connect 'jdbc:derby:wombat;dataEncryption=true;encryptionKey=6162636465666768';
select count(*) from emp;
create table t1(a int ) ;
insert into t1 values(1);
-- enable log archive mode and perform backup.
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                           'extinout/mybackup2', 0);
insert into t1 values(2);
insert into t1 values(3);
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- attempt to re-encrypt the database with external key, with log archive mode enabled.
-- it should fail.
connect 'jdbc:derby:wombat;encryptionKey=6162636465666768;newEncryptionKey=5666768616263646';

-- reboot the db and disable the log archive mode
connect 'jdbc:derby:wombat;encryptionKey=6162636465666768';
select * from t1;
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup1');
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- now re-encrypt the database, with the log archive mode disbaled.
-- it should pass. 
connect 'jdbc:derby:wombat;encryptionKey=6162636465666768;newEncryptionKey=5666768616263646';
select * from t1;
select count(*) from emp;
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- Finally, decrypt the database with log archive mode disabled.
-- It should pass.
connect 'jdbc:derby:wombat;encryptionKey=5666768616263646;decryptDatabase=true';
select * from t1;
select count(*) from emp;
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

-- restore from backup and attempt to configure database for encryption.
-- it shoud fail.
connect 'jdbc:derby:wombat;restoreFrom=extinout/mybackup/wombat;dataEncryption=true;bootPassword=xyz1234abc';

-- creating database from backup and attempting to configure database for encryption.
-- it shoud fail.
connect 'jdbc:derby:wombat_new;createFrom=extinout/mybackup/wombat;dataEncryption=true;bootPassword=xyz1234abc';

-- restore from backup and attempt to reEncrypt
-- it should fail.
connect 'jdbc:derby:wombat;restoreFrom=extinout/mybackup1/wombat;encryptionKey=6162636465666768;newEncryptionKey=5666768616263646';

-- restore from backup without re-encryption
-- it shoud boot. 
connect 'jdbc:derby:wombat;restoreFrom=extinout/mybackup1/wombat;encryptionKey=6162636465666768';
select count(*) from emp;
disconnect;
connect 'jdbc:derby:wombat;shutdown=true';

