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
drop table t1;
create table t1 (i integer primary key, j integer, c char(200));
insert into t1 values (1, 1, 'a');
insert into t1 (select t1.i + 2,    t1.j + 2,    t1.c from t1);
insert into t1 (select t1.i + 4,    t1.j + 4,    t1.c from t1);
insert into t1 (select t1.i + 8,    t1.j + 8,    t1.c from t1);
insert into t1 (select t1.i + 16,   t1.j + 16,   t1.c from t1);
insert into t1 (select t1.i + 32,   t1.j + 32,   t1.c from t1);
insert into t1 (select t1.i + 64,   t1.j + 64,   t1.c from t1);
insert into t1 (select t1.i + 128,  t1.j + 128,  t1.c from t1);
insert into t1 (select t1.i + 256,  t1.j + 256,  t1.c from t1);
insert into t1 (select t1.i + 512,  t1.j + 512,  t1.c from t1);
insert into t1 (select t1.i + 1024, t1.j + 1024, t1.c from t1);

delete from t1 where j=1;

CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);

delete from t1 where j=2;

CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);

delete from t1 where i > 1024;

CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);

delete from t1 where i < 512;

-- prior to the fix the following compress would result in a deadlock
CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);
