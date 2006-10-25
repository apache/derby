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
-- multiuser lock table tests
disconnect;

connect 'wombat;user=U1' AS C1;
autocommit off;
connect 'wombat;user=U2' AS C2;
autocommit off;

set connection C1;

-- create a table and populate it
create table t1 (c1 int);
insert into t1 values 1;
commit;

-- test TX vs TX locks
lock table u1.t1 in exclusive mode;

set connection C2;
lock table u1.t1 in exclusive mode;

set connection C1;
-- verify that we still have the lock
run resource 'LockTableQuery.subsql';

-- verify that we can insert into the table
insert into t1 values 2;
select * from t1;
commit;

-- test TX vs TS locks
lock table t1 in exclusive mode;

set connection C2;
lock table u1.t1 in share mode;

set connection C1;
-- verify that we still have the lock
run resource 'LockTableQuery.subsql';

-- verify that we can insert into the table
insert into t1 values 3;
select * from t1;
commit;

-- test TS vs TX locks
lock table t1 in share mode;

set connection C2;
lock table u1.t1 in exclusive mode;

set connection C1;
-- verify that we still have the lock
run resource 'LockTableQuery.subsql';

-- verify that we can insert into the table
insert into t1 values 4;
select * from t1;
commit;

-- test TS vs TS locks
lock table t1 in share mode;

set connection C2;
lock table u1.t1 in share mode;

set connection C1;
-- verify that we still have the lock
run resource 'LockTableQuery.subsql';

-- verify that we cannot insert into the table
insert into t1 values 5;
select * from t1;
commit;

set connection C2;
commit;

set connection C1;
-- create another table
create table t2(c1 int);
commit;
-- verify that user getting error on lock table doesn't get rolled back
lock table t1 in share mode;

set connection C2;
lock table u1.t2 in share mode;
lock table u1.t1 in exclusive mode;

set connection C1;
-- verify that other user still has the lock
run resource 'LockTableQuery.subsql';
commit;
disconnect;

set connection C2;
disconnect;
