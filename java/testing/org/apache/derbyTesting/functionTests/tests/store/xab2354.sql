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
xa_datasource 'wombat' create;
xa_connect;
xa_start xa_noflags 1;
xa_getconnection;
create table foo (a int);
insert into foo values (1);
xa_end xa_success 1;
xa_commit xa_1phase 1;

xa_start xa_noflags 1;
insert into foo values (2);
select cast(global_xid as char(2)) as gxid, status from syscs_diag.transaction_table where gxid is not null order by gxid, status;
xa_end xa_success 1;
xa_prepare 1;

xa_getconnection ;
select cast(global_xid as char(2)) as gxid, status from syscs_diag.transaction_table where gxid is not null order by gxid, status;
xa_datasource 'wombat' shutdown;

xa_datasource 'wombat';
xa_connect;

-- this works correctly
xa_start xa_noflags 1;

xa_getconnection;

-- this was the bug, this statement should also get DUPID error
xa_start xa_noflags 1;

-- should see two transactions, one global transaction and one local
select cast(global_xid as char(2)) as gxid, status from syscs_diag.transaction_table where gxid is not null order by gxid, status;
