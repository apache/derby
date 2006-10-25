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
-- Test whether the RllRAMAccessmanager is working right (ie. forcing row 
-- level locking). 
run resource 'LockTableQuery.subsql';

autocommit off;

create table heap_only (a int);

commit;

--------------------------------------------------------------------------------
-- Test insert into empty heap, should just get row lock 
--------------------------------------------------------------------------------
insert into heap_only values (1);

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
