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
-- testing Transaction table
maximumdisplaywidth 9000;

connect 'wombat' as c1;
set isolation to rr;

-- Only look at user transactions.  Depending on timing of background 
-- threads for post commit and checkpoint there may be system and 
-- and internal transactions that vary from machine to machine.
create view xactTable as
select username, type, status,
case when first_instant is NULL then 'readonly' else 'not readonly' end as readOnly, cast(sql_text as varchar(512)) sql_text
  from syscs_diag.transaction_table
    where type = 'UserTransaction';
commit;
select * from xactTable order by username, sql_text, status, type;

create table foo (a int);
create index fooi on foo (a);

select * from xactTable order by username, sql_text, status, type;

autocommit off;
select * from foo;

select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S' 
order by lockname, mode, cnt, state;
commit;
select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table ;


insert into foo values (1), (3), (5), (7), (9);
select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;

commit;
select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table;


insert into foo values (6), (10);

-- make another connection
connect 'wombat' as c2;
set isolation to rr;


autocommit off;

select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;

select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;


autocommit off;

select * from foo where a < 2;

select * from xactTable order by username, sql_text, status, type;


select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;

insert into foo values (2), (4);
select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;

-- this should block and result in a timeout

select * from foo;

select * from xactTable order by username, sql_text, status, type;

-- when last statement finished rolling back, this transaction should be IDLE;
select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;


-- this should also block

drop table foo;

select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;

commit;
disconnect;

set connection c1;
select * from xactTable order by username, sql_text, status, type;

select type, lockcount as cnt, mode, tablename, lockname, state
from syscs_diag.lock_table
where tableType <> 'S'
order by lockname, mode, cnt, state;

drop table foo;

commit;
select * from xactTable order by username, sql_text, status, type;

select l.type, lockcount as cnt, mode, tablename, lockname, state
from   syscs_diag.lock_table l right outer join syscs_diag.transaction_table t
       on l.xid = t.xid where l.tableType <> 'S' and t.type='UserTransaction'
order by lockname, mode, cnt, state;

commit;
-- ensure the system vti can not be modified.
drop table syscs_diag.transaction_table;
alter table syscs_diag.transaction_table add column x int;
update syscs_diag.transaction_table set xid = NULL;
delete from syscs_diag.transaction_table where 1 = 1;
insert into syscs_diag.transaction_table(xid) values('bad');

call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('SYSCS_DIAG', 'TRANSACTION_TABLE', 1);
call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('SYSCS_DIAG', 'TRANSACTION_TABLE', 1, 1, 1);

-- ensure the old syntax still works until it is deprecated
select xid from new org.apache.derby.diag.TransactionTable() AS t where 1 = 0;
update new org.apache.derby.diag.TransactionTable() set xid = NULL;
delete from new org.apache.derby.diag.TransactionTable() where 1 = 0;

disconnect;






