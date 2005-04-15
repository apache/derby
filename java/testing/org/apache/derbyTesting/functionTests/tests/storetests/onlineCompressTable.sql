autocommit off;
-- start with simple test, does the call work?
create table test1 (a int);
-- call SYSCS_UTIL.SYSCS_ONLINE_COMPRESS_TABLE('APP', 'TEST1');

-- expect failures schema/table does not exist
-- call SYSCS_UTIL.SYSCS_ONLINE_COMPRESS_TABLE(null, 'test2');
-- call SYSCS_UTIL.SYSCS_ONLINE_COMPRESS_TABLE('APP', 'test2');

-- non existent schema
-- call SYSCS_UTIL.SYSCS_ONLINE_COMPRESS_TABLE('doesnotexist', 'a');

-- cleanup
drop table test1;


-- load up a table, delete most of it's rows and then see what compress does.
create table test1 (keycol int, a char(250), b char(250), c char(250), d char(250));
insert into test1 values (1, 'a', 'b', 'c', 'd');
insert into test1 (select keycol + 1, a, b, c, d from test1);
insert into test1 (select keycol + 2, a, b, c, d from test1);
insert into test1 (select keycol + 4, a, b, c, d from test1);
insert into test1 (select keycol + 8, a, b, c, d from test1);
insert into test1 (select keycol + 16, a, b, c, d from test1);
insert into test1 (select keycol + 32, a, b, c, d from test1);
insert into test1 (select keycol + 64, a, b, c, d from test1);
insert into test1 (select keycol + 128, a, b, c, d from test1);
insert into test1 (select keycol + 256, a, b, c, d from test1);

create index test1_idx on test1(keycol);
commit;

select 
    conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, 
    estimspacesaving
        from new org.apache.derby.diag.SpaceTable('TEST1') t
                order by conglomeratename;

delete from test1 where keycol > 300;
commit;
delete from test1 where keycol < 100;
commit;


call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'TEST1', 1, 0, 0);

select 
    conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, 
    estimspacesaving
        from new org.apache.derby.diag.SpaceTable('TEST1') t
                order by conglomeratename;
commit;

-- call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'TEST1', 0, 1, 0);

select 
    conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, 
    estimspacesaving
        from new org.apache.derby.diag.SpaceTable('TEST1') t
                order by conglomeratename;

call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'TEST1', 0, 0, 1);

select 
    conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, 
    estimspacesaving
        from new org.apache.derby.diag.SpaceTable('TEST1') t
                order by conglomeratename;
