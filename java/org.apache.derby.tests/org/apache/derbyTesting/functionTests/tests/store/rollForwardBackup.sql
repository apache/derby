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

CREATE PROCEDURE RENAME_FILE(LOCATION VARCHAR(32000), NAME VARCHAR(32000), NEW_NAME  VARCHAR(32000)) DYNAMIC RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.FTFileUtil.renameFile' PARAMETER STYLE JAVA;

values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');
--check whether log archive mode  enabling method is working
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 0);
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');
--check whether the logArchive Mode is persistent across boots
disconnect;
connect 'wombat;shutdown=true';
connect 'wombat';
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');

--check whether log archive mode  disabling method is working
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');
--check whether the logArchive Mode disabling persistent across boots
disconnect;
connect 'wombat;shutdown=true';
connect 'wombat';
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');
-- reenable the log archive mode again to see whether the 
-- disabling has any side effects.
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 0);
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');
disconnect;
connect 'wombat;shutdown=true';
connect 'wombat';
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storage.logArchiveMode');
--END OF PROPERTY ARCHIVE CHECKS

---PERFORM DIFFERENT TYPES OF RESTORE
create table t1(a int ) ;
insert into t1 values(1) ;
insert into t1 values(2) ;
insert into t1 values(3 ) ;
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 0);
insert into t1 values(4) ;
insert into t1 values(5);
insert into t1 values(6);
connect 'wombat;shutdown=true';
disconnect;
--performa rollforward recovery
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t1 ;
insert into t1 values(7);
insert into t1 values(8);
insert into t1 values(9);
--take a backup again
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
insert into t1 values(10);
insert into t1 values(11);
insert into t1 values(12);
connect 'wombat;shutdown=true';
disconnect;
--perform complete version restore
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
select * from t1 ;
insert into t1 values(10);
insert into t1 values(11);
insert into t1 values(12);
insert into t1 values(13);
insert into t1 values(14);
insert into t1 values(15);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'wombat;shutdown=true';
disconnect;
--create a new database using wombat db backup copy with a different database name
connect 'wombat1;createFrom=extinout/mybackup/wombat';
select * from t1;
insert into t1 values(16);
insert into t1 values(17);
insert into t1 values(18);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'wombat1;shutdown=true';
disconnect;
---BACKUP AND RESTORE USING LOGDEVICE.
connect 'crwombat;createFrom=extinout/mybackup/wombat;logDevice=extinout/crwombatlog';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('crwombatlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),11);
select * from t1;
insert into t1 values(19);
insert into t1 values(20);
insert into t1 values(21);
connect 'crwombat;shutdown=true';
disconnect;
--do a plain boot , we should have the log device specified earlier.
connect 'crwombat';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('crwombatlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),11);

select * from t1;
insert into t1 values(22);
insert into t1 values(23);
insert into t1 values(24);
connect 'crwombat;shutdown=true';
disconnect;
---check the error case of log device only existing when
-- we try to do createFrom .
--following connection shoul fail.
connect 'erwombat;createFrom=extinout/mybackup/wombat;logDevice=extinout/crwombatlog';

connect 'wombat;restoreFrom=extinout/mybackup/wombat;logDevice=extinout/wombatlog';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),9);

select * from t1 ;
insert into t1 values(19);
insert into t1 values(20);
insert into t1 values(21);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'wombat;shutdown=true';
disconnect;
--restore again from backup case to make sure
--backups are getting the log device property.	
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),9);
select * from t1;
insert into t1 values(22);
insert into t1 values(23);
insert into t1 values(24);
connect 'wombat;shutdown=true';
disconnect;
--do a vannila boot and see the device to make sure the log device is still intact.
connect 'wombat';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),9);

select * from t1;
autocommit off;
insert into t1 values(25);
insert into t1 values(26);
insert into t1 values(27);
rollback;
connect 'wombat;shutdown=true';
disconnect;

--performa rollforward recovery with logDevice specified at backup
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),9);
select * from t1 ;
insert into t1 values(25);
insert into t1 values(26);
insert into t1 values(27);
connect 'wombat;shutdown=true';
disconnect;

--perform a rollforward recovery with log device is moved
--to some other place than what it was when backup was taken.
--move the log to different dir name.
connect 'dummycondb;createFrom=extinout/mybackup/wombat;logDevice=extinout/wombatlog1';
call RENAME_FILE(null,'extinout/wombatlog','extinout/wombatlogmoved');
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat;logDevice=extinout/wombatlogmoved';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlogmoved',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),14);

select * from t1 ;
insert into t1 values(30);
insert into t1 values(31);
insert into t1 values(32);
connect 'wombat;shutdown=true';
disconnect;
--do a plain boot and verify the log device.
connect 'wombat';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlogmoved',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),14);
select * from t1 ;
insert into t1 values(33);
insert into t1 values(34);
insert into t1 values(35);
--take a fresh backup again with moved log device.
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'wombat;shutdown=true';
disconnect;
--restore and check the results;
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wombatlogmoved',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),14);
select * from t1;
insert into t1 values(36);
insert into t1 values(37);
insert into t1 values(38);
connect 'wombat;shutdown=true';
disconnect;
--simulate OS type copy and then boot(Commented because it does not work in nightlies)
--connect 'dummycondb';
--call RENAME_FILE('rollForwardBackup', 'wombat', 'wombat.old');
--call RENAME_FILE(null, 'extinout/mybackup/wombat', 'rollForwardBackup/wombat');
---disconnect;
---connect 'wombat';
--Following SHOULD SHOW NULL value.
--values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice');
--select * from t1;
--call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
--     'extinout/mybackup', 1);
--connect 'wombat;shutdown=true';
--disconnect;
---createFrom without logDevice specified on URL should have null value.
connect 'tempwombat;createFrom=extinout/mybackup/wombat';
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice');
select * from t1;
insert into t1 values(39);
insert into t1 values(40);
insert into t1 values(41);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'tempwombat;shutdown=true';
disconnect;
connect 'wombat;restoreFrom=extinout/mybackup/tempwombat';
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice');
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'wombat;shutdown=true';
disconnect;

---Using plain backup mechanism rstore/recreate db using  restoreFrom/createFrom
connect 'wombat';
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
select * from t1;
insert into t1 values(42);
insert into t1 values(43);
insert into t1 values(44);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup');
--following inserted values should not be there
--when we do restore from the above backup.
insert into t1 values(45);
insert into t1 values(46);
insert into t1 values(47);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
select * from t1;
insert into t1 values(45);
insert into t1 values(46);
insert into t1 values(47);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup');
connect 'wombat;shutdown=true';
disconnect;
connect 'wombatnew;createFrom=extinout/mybackup/wombat';
select * from t1;
insert into t1 values(48);
insert into t1 values(49);
insert into t1 values(50);
connect 'wombatnew;shutdown=true';
disconnect;
--enable the log archive mode again.
connect 'wombat';
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 1);
connect 'wombat;shutdown=true';
disconnect;

--NEGATIVE TEST with  RESTORE FLAGS
-- with createFrom option should give erro on existing database
connect 'wombat;createFrom=extinout/mybackup/wombat';
-- specify conflictint attributes; it should fail.
connect 'wombat;create=true;createFrom=extinout/mybackup/wombat';
connect 'wombat;create=true;rollForwardRecoveryFrom=extinout/mybackup/wombat';
connect 'wombat;create=true;restoreFrom=extinout/mybackup/wombat';
connect 'wombat;restoreFrom=extinout/mybackup/wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
connect 'wombat;createFrom=extinout/mybackup/wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- With wrong back up path name it shoud fail.
connect 'wombat;rollForwardRecoveryFrom=nobackup/wombat';
connect 'wombat;restoreFrom=nobackup/wombat';
connect 'wombat2;createFrom=nobackup/wombat';
--Simulate missing files by renaming some files in backup(like a corrupted backup and check 
--whether we get proper error messages
--Get a connection because it is required to make any calls in ij 
connect 'wombat2;restoreFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','service.properties','service.properties.old');
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','service.properties.old','service.properties');
call RENAME_FILE('extinout/mybackup/wombat/','log','log.old');
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','log.old','log');
call RENAME_FILE('extinout/mybackup/wombat/','seg0','data.old');
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','data.old','seg0');
--try error cases with createFrom;if root created is not getting cleaned up,
--next createFrom call will fail with DBLOCATION/wombat exist error.
call RENAME_FILE('extinout/mybackup/wombat/','service.properties','service.properties.old');
connect 'wombat;createFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','service.properties.old','service.properties');
call RENAME_FILE('extinout/mybackup/wombat/','log','log.old');
connect 'wombat;createFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','log.old','log');
call RENAME_FILE('extinout/mybackup/wombat/','seg0','data.old');
connect 'wombat;createFrom=extinout/mybackup/wombat';
call RENAME_FILE('extinout/mybackup/wombat/','data.old','seg0');

drop procedure RENAME_FILE;






