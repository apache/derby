-- testing Transaction table
maximumdisplaywidth 9000;

connect 'wombat' as c1;
set isolation to rr;
create view xactTable as
select username, type, status,
case when first_instant is NULL then 'readonly' else 'not readonly' end as readOnly, cast(sql_text as varchar(512)) sql_text
  from syscs_diag.transaction_table
    where type != 'InternalTransaction';
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
disconnect;






