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
--test to make sure WAIT state is displayed when lock table is printed
connect 'wombat;user=c1' AS C1;
create procedure c1.sleep(t INTEGER) dynamic result sets 0  language java external name 'java.lang.Thread.sleep' parameter style java;
create table c1.account (a int primary key not null, b int);
autocommit off;
insert into c1.account values (0, 1);
insert into c1.account values (1, 1);
insert into c1.account values (2, 1);
--setting to -1 (wait for ever to handle timing issues in the test)
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.locks.waitTimeout', '-1');
commit ;
-- call sleep once now  we don't have a timing problem later
call c1.sleep(200);

update c1.account set b = b + 11;
connect 'wombat;user=c2' AS C2;
autocommit off;
async C2S1 'update c1.account set b = b + 11';
set connection C1;
call c1.sleep(200);
select state from syscs_diag.lock_table order by state;
commit;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.locks.waitTimeout', '180');
commit;
set connection c2 ;
wait for C2S1;
select state from syscs_diag.lock_table order by state;
commit;


