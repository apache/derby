-- tests for system procedure SYSCS_COMPRESS_TABLE
-- that reclaims disk space to the OS

run resource 'createTestProcedures.subsql';
maximumdisplaywidth 512;
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ConsistencyChecker.runConsistencyChecker'
LANGUAGE JAVA PARAMETER STYLE JAVA;


-- create tables
create table noindexes(c1 int, c2 char(30), c3 decimal(5,2));
create table indexes(c1 int, c2 char(30), c3 decimal(5,2));
create index i_c1 on indexes(c1);
create index i_c2 on indexes(c2);
create index i_c3 on indexes(c3);
create index i_c3c1 on indexes(c3, c1);
create index i_c2c1 on indexes(c2, c1);

create table oldconglom(o_cnum bigint, o_cname long varchar);
create table newconglom(n_cnum bigint, n_cname long varchar);

create view v_noindexes as select * from noindexes;

autocommit off;

-- test with heap only
-- test with empty table
insert into oldconglom
select conglomeratenumber, conglomeratename 
from sys.systables t, sys.sysconglomerates c
where t.tablename = 'NOINDEXES' and t.tableid = c.tableid;
select count(*) from oldconglom;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 0);
insert into newconglom
select conglomeratenumber, conglomeratename 
from sys.systables t, sys.sysconglomerates c
where t.tablename = 'NOINDEXES' and t.tableid = c.tableid;
select * from oldconglom, newconglom where o_cnum = n_cnum;
select count(*) from newconglom;
select * from noindexes;
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- test with various sizes as we use bulk fetch
insert into noindexes values (1, '1', 1.1), (2, '2', 2.2), (3, '3', 3.3),
     (4, '4', 4.4), (5, '5', 5.5), (6, '6', 6.6), (7, '7', 7.7);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 0);
select * from noindexes;
insert into noindexes values (8, '8', 8.8), (8, '8', 8.8), (9, '9', 9.9),
     (10, '10', 10.10), (11, '11', 11.11), (12, '12', 12.12), (13, '13', 13.13),
     (14, '14', 14.14), (15, '15', 15.15), (16, '16', 16.16);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 0);
select * from noindexes;
insert into noindexes values (17, '17', 17.17), (18, '18', 18.18);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 0);
select * from noindexes;
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- test with some indexes
-- test with empty table
insert into oldconglom
select conglomeratenumber, conglomeratename 
from sys.systables t, sys.sysconglomerates c
where t.tablename = 'INDEXES' and t.tableid = c.tableid;
select count(*) from oldconglom;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 0);
insert into newconglom
select conglomeratenumber, conglomeratename 
from sys.systables t, sys.sysconglomerates c
where t.tablename = 'INDEXES' and t.tableid = c.tableid;
select * from oldconglom, newconglom where o_cnum = n_cnum;
select count(*) from newconglom;
select * from indexes;
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- test with various sizes as we use bulk fetch
insert into indexes values (1, '1', 1.1), (2, '2', 2.2), (3, '3', 3.3),
     (4, '4', 4.4), (5, '5', 5.5), (6, '6', 6.6), (7, '7', 7.7);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 0);
select * from indexes;
insert into indexes values (8, '8', 8.8), (8, '8', 8.8), (9, '9', 9.9),
     (10, '10', 10.10), (11, '11', 11.11), (12, '12', 12.12), (13, '13', 13.13),
     (14, '14', 14.14), (15, '15', 15.15), (16, '16', 16.16);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 0);
select * from indexes;
insert into indexes values (17, '17', 17.17), (18, '18', 18.18);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 0);
select * from indexes;
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- primary/foreign keys
create table p (c1 char(1), y int not null, c2 char(1) not null, x int not null, constraint pk primary key(x,y));
create table f (x int, t int, y int, constraint fk foreign key (x,y) references p);
insert into p values ('1', 1, '1', 1);
insert into f values (1, 1, 1), (1, 1, null);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'P', 0);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'F', 0);
insert into f values (1, 1, 1);
insert into f values (2, 2, 2);
insert into p values ('2', 2, '2', 2);
insert into f values (2, 2, 2);
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- self referencing table
create table pf (x int not null constraint p primary key, y int constraint f references pf);
insert into pf values (1,1), (2, 2);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'PF', 0);
insert into pf values (3,1), (4, 2);
insert into pf values (3,1);
insert into pf values (5,6);
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- multiple indexes on same column
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096');
create table t (i int, s varchar(1500));
create index t_s on t(s);
create index t_si on t(s, i);
insert into t values (1, '1'), (2, '2');
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T', 0);
select * from t;
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- verify statements get re-prepared
create table t(c1 int, c2 int);
insert into t values (1, 2), (3, 4), (5, 6);
prepare p1 as 'select * from t where c2 = 4';
execute p1;
prepare s as 'select * from t where c2 = 6';
execute s;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T', 0);
execute p1;
execute s;
remove p1;
remove s;
-- do consistency check on scans, etc.
values ConsistencyChecker();
rollback;

-- verify that space getting reclaimed
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096');
create table t(c1 int, c2 varchar(1500));
insert into t values (1,PADSTRING('1', 1500)), (2,PADSTRING('2', 1500)), (3,PADSTRING('3', 1500)), (4, PADSTRING('4', 1500)),
	(5, PADSTRING('5', 1500)), (6, PADSTRING('6', 1500)), (7, PADSTRING('7', 1500)), (8, PADSTRING('8', 1500));
create table oldinfo (cname varchar(128), nap bigint);
insert into oldinfo select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('T') t;
delete from t where c1 in (1, 3, 5, 7);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T', 0);
create table newinfo (cname varchar(128), nap bigint);
insert into newinfo select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('T') t;
-- verify space reclaimed, this query should return 'compressed!'
-- if nothing is returned from this query, then the table was not compressed
select 'compressed!' from oldinfo o, newinfo n where o.cname = n.cname and o.nap > n.nap;
rollback;

-- sequential
-- no indexes
-- empty table
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 1);
select * from v_noindexes;
-- full table
insert into noindexes values (1, '1', 1.1), (2, '2', 2.2), (3, '3', 3.3),
     (4, '4', 4.4), (5, '5', 5.5), (6, '6', 6.6), (7, '7', 7.7);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 1);
select * from v_noindexes;
insert into noindexes values (8, '8', 8.8), (8, '8', 8.8), (9, '9', 9.9),
     (10, '10', 10.10), (11, '11', 11.11), (12, '12', 12.12), (13, '13', 13.13),
     (14, '14', 14.14), (15, '15', 15.15), (16, '16', 16.16);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 1);
select * from v_noindexes;
insert into noindexes values (17, '17', 17.17), (18, '18', 18.18);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'NOINDEXES', 1);
select * from v_noindexes;
rollback;

-- 1 index
drop index i_c2;
drop index i_c3;
drop index i_c2c1;
drop index i_c3c1;
-- empty table
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
-- full table
insert into indexes values (1, '1', 1.1), (2, '2', 2.2), (3, '3', 3.3),
     (4, '4', 4.4), (5, '5', 5.5), (6, '6', 6.6), (7, '7', 7.7);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
insert into indexes values (8, '8', 8.8), (8, '8', 8.8), (9, '9', 9.9),
     (10, '10', 10.10), (11, '11', 11.11), (12, '12', 12.12), (13, '13', 13.13),
     (14, '14', 14.14), (15, '15', 15.15), (16, '16', 16.16);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
insert into indexes values (17, '17', 17.17), (18, '18', 18.18);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
rollback;

-- multiple indexes
-- empty table
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
-- full table
insert into indexes values (1, '1', 1.1), (2, '2', 2.2), (3, '3', 3.3),
     (4, '4', 4.4), (5, '5', 5.5), (6, '6', 6.6), (7, '7', 7.7);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
insert into indexes values (8, '8', 8.8), (8, '8', 8.8), (9, '9', 9.9),
     (10, '10', 10.10), (11, '11', 11.11), (12, '12', 12.12), (13, '13', 13.13),
     (14, '14', 14.14), (15, '15', 15.15), (16, '16', 16.16);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
insert into indexes values (17, '17', 17.17), (18, '18', 18.18);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'INDEXES', 1);
select * from indexes;
rollback;

--table with multiple indexes, indexes share columns
--table has more than 4 rows
-- multiple indexes on same column
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096');
create table tab (a int, b int, s varchar(1500));
create index i_a on tab(a);
create index i_s on tab(s);
create index i_ab on tab(a, b);
insert into tab values (1, 1, 'abc'), (2, 2,  'bcd');
insert into tab values (3, 3, 'abc'), (4, 4,  'bcd');
insert into tab values (5, 5, 'abc'), (6, 6,  'bcd');
insert into tab values (7, 7, 'abc'), (8, 8,  'bcd');
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'TAB', 1);
select * from tab;
-- do consistency check on scans, etc.
values ConsistencyChecker();
--record the number of rows
create table oldstat(rowCount int);
insert into oldstat select count(*) from tab;
commit;
--double the size of the table
select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('TAB') tab;
insert into tab values (1, 1, 'abc'), (2, 2,  'bcd');
insert into tab values (3, 3, 'abc'), (4, 4,  'bcd');
insert into tab values (5, 5, 'abc'), (6, 6,  'bcd');
insert into tab values (7, 7, 'abc'), (8, 8,  'bcd');
select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('TAB') tab;
delete from tab;
select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('TAB') tab;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'TAB', 0);
-- verify space reclaimed
select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('TAB') tab;
-- do consistency check on scans, etc.
values  ConsistencyChecker();
rollback;
--record the number of rows
create table newstat(rowCount int);
insert into newstat select count(*) from tab;
--make sure the number of rows are the same
select o.rowCount, n.rowCount from oldstat o, newstat n where o.rowCount = n.rowCount;
--show old space usage
select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('TAB') tab;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'TAB', 0);
--show new space usage
select conglomeratename, numallocatedpages from new org.apache.derby.diag.SpaceTable('TAB') tab;
rollback;
drop table tab;
drop table oldstat;

-- test that many levels of aborts of compress table still work
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096');
create table xena (a int, b int, c varchar(1000), d varchar(8000));
create index xena_idx1 on xena (a, c);
create unique index xena_idx2 on xena (b, c);
insert into xena values (1, 1, 'argo', 'horse');
insert into xena values (1, -1, 'argo', 'horse');
insert into xena values (2, 2, 'ares', 'god of war');
insert into xena values (2, -2, 'ares', 'god of war');
insert into xena values (3, 3, 'joxer', 'the mighty');
insert into xena values (4, -4, 'gabrielle', 'side kick');
insert into xena values (4, 4, 'gabrielle', 'side kick');

select 
    conglomeratename, isindex, 
    numallocatedpages, numfreepages, 
    pagesize, estimspacesaving

    from new org.apache.derby.diag.SpaceTable('XENA') t
        order by conglomeratename;

commit;

delete from xena where b = 1;

call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;

create table xena2(a int);

delete from xena where b = 2;
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;

create table xena3(a int);

delete from xena where b = 3;
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;

create table xena4(a int);

delete from xena where b = 4;
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;

create table xena5(a int);

rollback;

-- should all fail
drop table xena2;
drop table xena3;

select a, b from xena;

-- read every row and value in the table, including overflow pages.
insert into xena values (select a + 4, b - 4, c, d from xena);
insert into xena values (select (a + 4, b - 4, c, d from xena);

select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;

-- delete all but 1 row (the sidekick)
delete from xena where a <> 4 or b <> -4;
select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);

select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;

rollback;

select a, b from xena;


drop table xena;

-- bug 2940
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096');
create table xena (a int, b int, c varchar(1000), d varchar(8000));
insert into xena values (1, 1, 'argo', 'horse');
insert into xena values (2, 2, 'beta', 'mule');
insert into xena values (3, 3, 'comma', 'horse');
insert into xena values (4, 4, 'delta', 'goat');
insert into xena values (1, 1, 'x_argo', 'x_horse');
insert into xena values (2, 2, 'x_beta', 'x_mule');
insert into xena values (3, 3, 'x_comma', 'x_horse');
insert into xena values (4, 4, 'x_delta', 'x_goat');
autocommit off;

call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
commit;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4000');
create unique index xena1 on xena (a, c);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','20000');
create unique index xena2 on xena (a, d);
create unique index xena3 on xena (c, d);
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
select * from xena;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'XENA', 0);
select * from xena;
rollback;

select 
     cast (conglomeratename as char(10)) as name, 
     cast (numallocatedpages as char(4)) as aloc, 
     cast (numfreepages as char(4))      as free, 
     cast (estimspacesaving as char(10)) as est
        from new org.apache.derby.diag.SpaceTable('XENA') t order by name;
select schemaname, tablename, 
SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename)
 from sys.systables a,  sys.sysschemas b where a.schemaid = b.schemaid
order by schemaname, tablename;

select a, b from xena;

-- clean up
drop function padstring;
drop view v_noindexes;
drop table noindexes;
drop table indexes;
drop table oldconglom;
drop table newconglom;
