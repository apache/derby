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
-- simple SYSCS procedures test - just test if we can call them
-- other tests will check that they work, this is just a quick test
-- that they can be called.  For the purpose of this test "works", 
-- means function called with no errors.  Other tests will check if
-- function did the right thing.
autocommit off;
maximumdisplaywidth 9000;

create table foo(a int);

-- check if unqualified schema call works.
set schema SYSCS_UTIL;

call SYSCS_FREEZE_DATABASE();
call SYSCS_UNFREEZE_DATABASE();
call SYSCS_CHECKPOINT_DATABASE();
call SYSCS_SET_DATABASE_PROPERTY('foo', 'bar');
commit;
call SYSCS_BACKUP_DATABASE('extinout/mybackup');
call SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE('extinout/mybackup2', 0);
call SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE('extinout/mybackup2', 1);
call SYSCS_DISABLE_LOG_ARCHIVE_MODE(0);
call SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);

call SYSCS_COMPRESS_TABLE('APP', 'FOO', 0);
call SYSCS_COMPRESS_TABLE('APP', 'FOO', 1);

-- system funtions
set schema SYSCS_UTIL;
values SYSCS_GET_DATABASE_PROPERTY('foo');
values SYSCS_CHECK_TABLE('APP', 'FOO');

call SYSCS_SET_RUNTIMESTATISTICS(1);
call SYSCS_SET_STATISTICS_TIMING(1);
select * from APP.foo;
-- TODO, figure out how to test with a master file, that timings are non-zero,
-- without having them diff everytime.  Ran this once and hand checked that
-- it was working.

-- values SYSCS_GET_RUNTIMESTATISTICS();

call SYSCS_SET_STATISTICS_TIMING(0);
select * from APP.foo;
values SYSCS_GET_RUNTIMESTATISTICS();

call SYSCS_SET_RUNTIMESTATISTICS(0);
call SYSCS_SET_STATISTICS_TIMING(0);
select * from APP.foo;
values SYSCS_GET_RUNTIMESTATISTICS();

-- check if qualified schema call works.
set schema APP;

call SYSCS_UTIL.SYSCS_FREEZE_DATABASE();
call SYSCS_UTIL.SYSCS_UNFREEZE_DATABASE();
call SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('foo', 'bar');
-- backup procedures will work only in new transaction, commit the work so far.
commit;  
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup');
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE('extinout/mybackup3', 0);
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE('extinout/mybackup3', 1);
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(0);
call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);

call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'FOO', 0);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'FOO', 1);

-- the following does not work yet.
values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('foo');
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'FOO');

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1);
select * from APP.foo;

-- TODO, figure out how to test with a master file, that timings are non-zero,
-- without having them diff everytime.  Ran this once and hand checked that
-- it was working.

-- values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0);
select * from APP.foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);
call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0);
select * from APP.foo;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

drop table foo;
commit;
