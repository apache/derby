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
-- This script tests online backup functionality 
-- 1) in a non-idle tranaction.
-- 2) mutiple backup calls on the same connection. 
-- 3) when unlogged operations are running in parallel 
--      to the  backup thread. 

connect 'wombat' as c1 ;
create procedure sleep(t INTEGER) dynamic result sets 0  
language java external name 'java.lang.Thread.sleep' 
parameter style java;

create function fileExists(fileName varchar(128))
returns VARCHAR(100) external name
 'org.apache.derbyTesting.functionTests.util.FTFileUtil.fileExists' 
language java parameter style java;

create function removeDirectory(fileName varchar(128))
returns VARCHAR(100) external name
 'org.apache.derbyTesting.functionTests.util.FTFileUtil.removeDirectory' 
language java parameter style java;


autocommit off;
create table t1(a int ) ;
insert into t1 values(1) ;
insert into t1 values(2) ; 
commit ;
-- make sure backup calls are not allowed in a transaction that
-- has executed unlogged operations before the backup calls. 
insert into t1 values(3); 
create index idx1 on t1(a);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup') ;
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT('extinout/mybackup') ;
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                              'extinout/mybackup', 1);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT(
                                              'extinout/mybackup', 1);
--backup failures should not rollback/commit the transaction. 
select * from t1 ;
insert into t1 values(4) ;
commit;
drop index idx1;
commit;
--- make sure backup calls can be run one after another.
insert into t1 values(5) ;
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup') ;
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT('extinout/mybackup');
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                           'extinout/mybackup', 1);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT(
                                               'extinout/mybackup', 1);
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
commit;
-- make sure backup is not allowed when non-logged 
-- operations are pending
connect 'wombat' as c2 ;
autocommit off ;
-- index creaton is a non-logged ops, backup should not run 
-- until it is committed
create index idx1 on t1(a) ;

set connection c1 ;
-- following two backup calls should fail , because they are not waiting
-- for the unlogged index creation in anothere transaction to commit/rollback.

call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT('extinout/mybackup') ;

call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT(
                                               'extinout/mybackup', 1);

set connection c2;
rollback ;

-- make sure backup call waits, if wait parameter value is non-zero or 
-- the procedures used from before 10.2( old backup procedures wait by 
-- default for  unlogged operation to finish.)

-- This testing is done by starting backup in a different thread and then 
-- wait for few seconds and check if the backup dir is created ? 
-- If backup dir is not created , the backup thread is waiting for unlogged
-- op to finih.

-- Note: Not a 100% foolproof approach because checking for backupdir 
-- might occur before backup thread gets into action. But I think 
-- test  will fail  atleast on some systems, if  backup is not waiting
-- for unlogged ops to complete.

-- case1 : simple database backup with unlogged ops pending.
set connection c2;
-- index is a non-logged operation
create index idx1 on t1(a) ;

set connection c1;
-- make sure backup does not already exists at the backup location.
values removeDirectory('extinout/ulbackup1');
values fileExists('extinout/ulbackup1'); 
async bthread1 'call SYSCS_UTIL.SYSCS_BACKUP_DATABASE(
                                    ''extinout/ulbackup1'')' ;
set connection c2;
-- sleep for a while for the backup thread to 
-- really get into the wait state
call sleep(1000);
-- make sure backup did not really proceed, backup dir should not exist
values fileExists('extinout/ulbackup1'); 

-- rollback the unlogged op for backup to proceed.
rollback;

set connection c1;
-- wait for backup thread to finish the work.
wait for bthread1;
-- check if backup is created.
values fileExists('extinout/ulbackup1');
commit;

-- case2: simple backup call with the default wait for ulogged ops
set connection c2;
create index idx1 on t1(a) ;

set connection c1;
-- make sure backup does not already exists at the backup location.
values removeDirectory('extinout/ulbackup2');
values fileExists('extinout/ulbackup2'); 
async bthread1 
  'call SYSCS_UTIL.SYSCS_BACKUP_DATABASE(''extinout/ulbackup2'')';
set connection c2;
-- sleep for a while for the backup thread to 
-- really get into the wait state
call sleep(1000);
-- make sure backup did not really proceed, backup dir should not exist
values fileExists('extinout/ulbackup2'); 
-- rollback the unlogged op for backup to proceed.
rollback;

set connection c1;
-- wait for backup thread to finish the work.
wait for bthread1;
-- check if backup is created.
values fileExists('extinout/ulbackup2');
commit;
 
--- case 3: log archive backup with with unlogged ops pending.
set connection c2;
create index idx1 on t1(a) ;

set connection c1;
--make sure backup does not already exists at the backup location.
values removeDirectory('extinout/ulbackup3');
values fileExists('extinout/ulbackup3'); 
async bthread1 
  'call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                    ''extinout/ulbackup3'' , 1)' ;
set connection c2;
-- sleep for a while for the backup thread to 
-- really get into the wait state
call sleep(1000);
-- make sure backup did not really proceed, backup dir should not exist
values fileExists('extinout/ulbackup3'); 
-- rollback the unlogged op for backup to proceed.
rollback;
set connection c1;
-- wait for backup thread to finish the work.
wait for bthread1;
-- check if backup is created.
values fileExists('extinout/ulbackup3');
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);

-- case4 : log archive backup with the defailt wait for unlogged ops.
set connection c2;
create index idx1 on t1(a) ;

set connection c1;
--make sure backup does not already exists at the backup location.
values removeDirectory('extinout/ulbackup4');
values fileExists('extinout/ulbackup4'); 
async bthread1 
  'call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
                                    ''extinout/ulbackup4'' , 1)' ;
set connection c2;
-- sleep for a while for the backup thread to 
-- really get into the wait state
call sleep(1000);
-- make sure backup did not really proceed, backup dir should not exist
values fileExists('extinout/ulbackup4'); 
-- commit the unlogged op for backup to proceed.
commit;
set connection c1;
-- wait for backup thread to finish the work.
wait for bthread1;
-- check if backup is created.
values fileExists('extinout/ulbackup4');
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1); 
