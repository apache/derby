-- Test whether the RllRAMAccessmanager is working right.  The property file
-- for this test sets the lock level to table so make sure we get a table lock.
-- level locking). 
run resource 'LockTableQuery.subsql';

autocommit off;

create table heap_only (a int);

commit;

--------------------------------------------------------------------------------
-- Test insert into empty heap, should just get table lock 
--------------------------------------------------------------------------------
insert into heap_only values (1);

select * from lock_table order by tabname, type desc, mode, cnt, lockname;

commit;
