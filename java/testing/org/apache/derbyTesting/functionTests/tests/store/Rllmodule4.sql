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
