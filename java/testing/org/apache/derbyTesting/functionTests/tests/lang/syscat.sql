--
-- this test shows the system catalogs
--
-- It is a goal of this test not to display information that
-- can (and will!) change from run to run, such as fields
-- that may eventually be UUIDs or UUID-like.
--

maximumdisplaywidth 500;

-- negative tests
-- verify no user ddl allowed on system tables
-- drop table
drop table sys.systables;

-- drop index
drop index sys.sysaliases_index2;

-- create index
create index trash on sys.systables(tableid);

-- system tables are not updateable
autocommit off;
delete from sys.systables;
update sys.systables set tablename = tablename || 'trash';
insert into sys.systables select * from sys.systables;

get cursor c as 'select tablename from sys.systables for update of tablename';

-- users not allowed to do ddl in sys schema
create table sys.usertable(c1 int);
create view sys.userview as values 1;

rollback work;
autocommit on;

-- positive tests
create function gatp(SCH VARCHAR(128), TBL VARCHAR(128)) RETURNS VARCHAR(1000)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestPropertyInfo.getAllTableProperties'
LANGUAGE JAVA PARAMETER STYLE JAVA;
create function gaip(SCH VARCHAR(128), TBL VARCHAR(128)) RETURNS VARCHAR(1000)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestPropertyInfo.getAllIndexProperties'
LANGUAGE JAVA PARAMETER STYLE JAVA;

-- get the properties for the heaps
select tablename,gatp('SYS',
	tablename) from sys.systables
order by tablename;

-- get the properties for the indexes
select conglomeratename, gaip('SYS',
	conglomeratename) from sys.sysconglomerates
where isindex
order by conglomeratename;

select TABLENAME, TABLETYPE from sys.systables;

select TABLENAME,
	COLUMNNAME, COLUMNNUMBER,
	columndatatype
from sys.systables t, sys.syscolumns c
where t.TABLEID=c.REFERENCEID
order by TABLENAME, COLUMNNAME;

select TABLENAME, ISINDEX 
from sys.systables t, sys.sysconglomerates c
where t.TABLEID=c.TABLEID
order by TABLENAME, ISINDEX;

create table t (i int, s smallint);

select TABLENAME, TABLETYPE from sys.systables;

select TABLENAME,
	COLUMNNAME, COLUMNNUMBER,
	columndatatype
from sys.systables t, sys.syscolumns c
where t.TABLEID=c.REFERENCEID
order by TABLENAME, COLUMNNAME;

select TABLENAME, ISINDEX 
from sys.systables t, sys.sysconglomerates c
where t.TABLEID=c.TABLEID
order by TABLENAME, ISINDEX;

-- > 30 char table and column names
create table t234567890123456789012345678901234567890
(c23456789012345678901234567890 int);

select TABLENAME from sys.systables where length(TABLENAME) > 30;
select COLUMNNAME from sys.syscolumns where {fn length(COLUMNNAME)} > 30;

-- primary key
create table primkey1 (c1 int not null constraint prim1 primary key);
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t
where c.tableid = t.tableid and not t.tablename like 'UNNAMED%';

create table unnamed_primkey2 (c1 int not null primary key);
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t
where c.tableid = t.tableid and not t.tablename like 'UNNAMED%';

create table primkey3 (c1 int not null, c2 int not null, constraint prim3 primary key(c2, c1));
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t
where c.tableid = t.tableid and not t.tablename like 'UNNAMED%';

create table uniquekey1 (c1 int not null constraint uniq1 unique);
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t
where c.tableid = t.tableid and not t.tablename like 'UNNAMED%';

create table unnamed_uniquekey2 (c1 int not null unique);
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t
where c.tableid = t.tableid and not t.tablename like 'UNNAMED%';

create table uniquekey3 (c1 int not null, c2 int not null, constraint uniq3 unique(c2, c1));
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t
where c.tableid = t.tableid and not t.tablename like 'UNNAMED%';

-- views
create view dummyview as select * from t, uniquekey3;

select tablename from sys.systables t, sys.sysviews v
where t.tableid = v.tableid;

-- RESOLVE - add selects from sysdepends when simplified

-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

-- drop views
drop view dummyview;

-- added for bug 3544. make sure we can ship columndatatype across the wire.
-- (this test is also part of the rjconnmats suite and will run under rmijdbc).

create table decimal_tab (dcol decimal(5,2), ncol numeric(5,2) default 1.0);

select columnname, columnnumber, columndatatype
from sys.syscolumns
where columnname IN ('DCOL', 'NCOL');

-- now just for fun lets select some other stuff from the system catalogs
-- which is used by Cloudview and make sure we can ship it over the wire.

create index decimal_tab_idx on decimal_tab(dcol);

-- index descriptor.
select conglomeratename, descriptor
from sys.sysconglomerates 
where conglomeratename = 'DECIMAL_TAB_IDX';

create trigger t1 after update on decimal_tab for each row mode db2sql values 1;

-- referenced columns.
select triggername, referencedcolumns
from sys.systriggers;


-- drop tables
drop table t;
drop table t234567890123456789012345678901234567890;
drop table primkey1;
drop table unnamed_primkey2;
drop table primkey3;
drop table uniquekey1;
drop table unnamed_uniquekey2;
drop table uniquekey3;

-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

