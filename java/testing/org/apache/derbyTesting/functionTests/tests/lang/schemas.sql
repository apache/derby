--
-- this test shows the current supported schema functionality, which
-- isn't much.  Currently, we have no CREATE SCHEMA statement, though
-- we do understand schema names in table names
--
-- Catalog names are not supported, and result in syntax errors when used.
--

create table myschem.t(c int);

insert into t values (1);
insert into blah.t values (2);
insert into blah.blah.t values (3);
insert into blah.blah.blah.t values (3);

select "goofy name".t.c  from "goofy name".t;

-- catalog name not supported:
create table mycat.myschem.s(c int);

-- name too long:
create table myworld.mycat.myschem.s(c int);

create table myschem.s(c int);

insert into s values (1);
insert into honk.s values (2);
insert into honk.blat.s values (3);
insert into loud.honk.blat.s values (4);

-- Catalog names in column expressions cause syntax errors.  Rather than
-- fix this, I am checking it in this way, considering that no client we
-- know of uses catalogs.
--							-	Jeff
--
-- select honk.blat.s.c from honk.blat.s;

drop table xyzzy.t;

-- catalog name not supported:
drop table goodness.gosh.s;

-- finds s, schema name ignored:
drop table gosh.s;

-- tests for qualified names in select, relative to method invocations
create table mytab (i int);
create table APP.mytab2 (i int);

insert into mytab values 1,2,3;
insert into APP.mytab2 values 1,2,3;

-- plain and just table names match up fine
select i, mytab.i from mytab;

-- schema names on columns
select APP.mytab2.i from APP.mytab2;
select APP.mytab2.i from mytab2;
select mytab2.i from APP.mytab2;

-- schema names correlation names:
select m.i from APP.mytab2 m;

-- syntax errors on catalog names
select nocatalogs.APP.mytab.i from mytab2;

drop table mytab;
drop table APP.mytab2;

------------------------------------------------
--
-- Now, we'll try to create and drop some schemas
--
------------------------------------------------
create schema app;
create schema sys;

-- negative drop test
drop schema does_not_exist RESTRICT;

-- negative create test - should not be able to create existing system schemas;
create schema app;
create schema APP;
create schema sys;
create schema SYS;
create schema sysibm;
create schema SYSIBM;
create schema syscat;
create schema SYSCAT;
create schema sysfun;
create schema SYSFUN;
create schema sysproc;
create schema SYSPROC;
create schema sysstat;
create schema SYSSTAT;
create schema syscs_diag;
create schema SYSCS_DIAG;
create schema syscs_util;
create schema SYSCS_UTIL;
create schema nullid;
create schema NULLID;
create schema sqlj;
create schema SQLJ;

-- negative create test - should not be able to objects in system schemas
create table syscat.foo1 (a int);
create table sysfun.foo2 (a int);
create table sysproc.foo3 (a int);
create table sysstat.foo4 (a int);
create table syscs_diag.foo6 (a int);
create table nullid.foo7 (a int);
create table sysibm.foo8 (a int);
create table sqlj.foo8 (a int);
create table syscs_util.foo9 (a int);
create table SYSCAT.foo1 (a int);
create table SYSFUN.foo2 (a int);
create table SYSPROC.foo3 (a int);
create table SYSSTAT.foo4 (a int);
create table SYSCS_DIAG.foo6 (a int);
create table SYSIBM.foo8 (a int);
create table SQLJ.foo8 (a int);
create table SYSCS_UTIL.foo9 (a int);

-- negative drop test - should not be able to drop system schema's
drop schema app RESTRICT;
drop schema APP RESTRICT;
drop schema sys RESTRICT;
drop schema SYS RESTRICT;
drop schema sysibm RESTRICT;
drop schema SYSIBM RESTRICT;
drop schema syscat RESTRICT;
drop schema SYSCAT RESTRICT;
drop schema sysfun RESTRICT;
drop schema SYSFUN RESTRICT;
drop schema sysproc RESTRICT;
drop schema SYSPROC RESTRICT;
drop schema sysstat RESTRICT;
drop schema SYSSTAT RESTRICT;
drop schema syscs_diag RESTRICT;
drop schema SYSCS_DIAG RESTRICT;
drop schema syscs_util RESTRICT;
drop schema SYSCS_UTIL RESTRICT;
drop schema nullid RESTRICT;
drop schema NULLID RESTRICT;
drop schema sqlj RESTRICT;
drop schema SQLJ RESTRICT;

create schema app;
set schema app;
create table test (a int);
-- negative create test - should not be able to objects in system schemas
set schema syscat;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema sysfun;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema sysproc;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema sysstat;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema sysstat;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema syscs_diag;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema syscs_util;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema nullid;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema sysibm;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema sqlj;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSCAT;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSFUN;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSPROC;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSSTAT;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSSTAT;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSCS_DIAG;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSCS_UTIL;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema NULLID;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SYSIBM;
create table foo1 (a int);
create view foo1 as select * from app.test;
set schema SQLJ;
create table foo1 (a int);
create view foo1 as select * from app.test;

-- Negative tests. Disable use of schemas starting with SYS
set schema app;
create table t1 (c1 int);
create trigger sysblah.trig1 after update of c1 on t1 for each row mode db2sql insert into t1 values 1;
create procedure sysblah.dummy() language java external name 'NotReallyThere.NoMethod' parameter style java;
drop table t1;

set schema app;

-- create a schema
create schema test;

-- create it again, should fail
create schema test;

-- verify it
select schemaname, authorizationid 
	from sys.sysschemas 
	where schemaname = 'TEST';

-- create a table in test
set schema test;
create table sampletab (c1 int check (c1 > 1), c2 char(20));
insert into sampletab values (1,'in schema: TEST');
insert into sampletab values (2,'in schema: TEST');

-- verify it
select schemaname, tablename, descriptor
	from sys.sysschemas s, sys.sysconglomerates c , sys.systables t
	where t.tablename = 'SAMPLETAB' 
		and s.schemaid = c.schemaid
		and c.tableid = t.tableid;

-- do some ddl on said table
create index ixsampletab on sampletab(c1);
create index ix2sampletab on test.sampletab(c1);
create view vsampletab as select * from sampletab;
create view v2sampletab as select * from test.sampletab;
alter table sampletab add column c3 int;

-- switch schemas
set schema APP;

-- create table with same name in APP
create table sampletab (c1 int check(c1 > 1), c2 char(20));
insert into sampletab values (2,'in schema: APP');

-- verify it
--
select schemaname, tablename, descriptor as descr
from sys.sysschemas s, sys.sysconglomerates c , sys.systables t
where t.tablename = 'SAMPLETAB' 
	and s.schemaid = c.schemaid
	and c.tableid = t.tableid
order by schemaname, tablename;

-- select from both the tables
select * from sampletab;
select * from test.sampletab;

-- switch to the test schema
set schema test;

select * from sampletab;
select * from app.sampletab;

-- try a drop, should fail since we haven't
-- cleaned out everything in the schema
drop schema test RESTRICT;

-- make sure use the correct schema for various ddl
drop view vsampletab;
drop view v2sampletab;
drop index ixsampletab;
drop index ix2sampletab;
alter table sampletab add column c4 int;
select * from sampletab;

-- get rid of last object in test
drop table sampletab;

-- try a drop now, should be ok
drop schema test RESTRICT;

-- use quoted id
create schema "heLLo";
create schema "sys";

-- should fail
drop schema "hello" RESTRICT;

-- ok
drop schema "heLLo" RESTRICT;
drop schema "sys" RESTRICT;

-- try prepared statements, should fail
prepare createSchema as 'create schema ?';
prepare dropSchema as 'drop schema ? RESTRICT';


--
-- specific drop schema tests, all should fail
--
create schema x;
set schema x;

create view vx as select * from sys.sysschemas;
drop schema x RESTRICT;
drop view x.vx;

create table x (x int);
drop schema x restrict;
drop table x.x;

-- syntax not supported yet (but is in the parser)
drop schema x cascade;

set schema app;
drop schema x restrict;
--
-- test using schema names and correlation names
-- first test simple use of schema names
create schema test;
set schema test;
autocommit off;

-- create the all type tables
create table s (i int, s smallint, c char(30), vc char(30));
create table t (i int, s smallint, c char(30), vc char(30));
create table tt (ii int, ss smallint, cc char(30), vcvc char(30));
create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30));

-- populate the tables
insert into s values (null, null, null, null);
insert into s values (0, 0, '0', '0');
insert into s values (1, 1, '1', '1');

insert into t values (null, null, null, null);
insert into t values (0, 0, '0', '0');
insert into t values (1, 1, '1', '1');
insert into t values (1, 1, '1', '1');

insert into tt values (null, null, null, null);
insert into tt values (0, 0, '0', '0');
insert into tt values (1, 1, '1', '1');
insert into tt values (1, 1, '1', '1');
insert into tt values (2, 2, '2', '2');

insert into ttt values (null, null, null, null);
insert into ttt values (11, 11, '11', '11');
insert into ttt values (11, 11, '11', '11');
insert into ttt values (22, 22, '22', '22');

commit;
set schema app;

-- test simple statements which use schema names
insert into test.t values (2, 2, '2', '2');
update test.t set s = 2 where i = 2;
update test.t set s = 2 where test.t.i = 2;
delete from test.t where i = 1;
select * from test.t;
insert into test.t values (1, 1, '1', '1');
insert into test.t values (1, 1, '1', '1');

-- test correlated names with tables and schema names
select * from test.t t1;

-- test subqueries

select * from test.s where exists (select test.s.* from test.t);
select * from test.s t where exists (select t.* from test.t);
select * from test.s u where exists (select u.* from test.t);

-- column reference in select list
select * from test.s where exists (select i from test.t);
select * from test.s where exists (select test.t.i from test.t);

-- derived table in the from list
select 1 from test.s where exists (select * from (select * from test.t) x);
select 1 from test.s where exists (select * from (select * from test.t) x (i, s, c, vc) );

-- subquery in derived table
select * from 
(select * from test.s where exists (select * from test.t) and i = 0) a;

-- exists under an OR
select * from test.s where (1=2) or exists (select * from test.t);
select * from test.s where (1=1) or exists (select * from test.t where (1=2));

-- expression subqueries
-- non-correlated
select * from test.s where i = (select i from test.t where i = 0);

-- ? parameter on left hand side of expression subquery
prepare subq1 as 'select * from test.s where ? = (select i from test.t where i = 0)';
execute subq1 using 'values (0)';
remove subq1;


-- subquery = subquery
select * from test.s where
(select i from test.t where i = 0) = (select s from test.t where s = 0);

select * from test.s t1 where
(select i from test.t t2 where i = 0) = (select s from test.t t3 where s = 0);

-- multiple subqueries at the same level
select * from test.s 
where i = (select s from test.t where s = 0) and
	  s = (select i from test.t where i = 2);

-- nested subqueries
select * from test.s 
where i = (select i from test.t where s = (select i from test.t where s = 2));

select * from test.s  t1
where i = (select i from test.t t2 where s = (select i from test.t t3 where s = 2));
-- correlated subqueries

-- negative tests

-- exists disallowed in select clause
select (exists (select * from test.ttt 
				where iii = (select 11 from test.tt where ii = i and ii <> 1)) ) from test.s;

-- multiple matches at parent level
select * from test.s, test.t where exists (select i from test.tt);
-- match is against base table, but not derived column list
select * from test.s ss (c1, c2, c3, c4) where exists (select i from test.tt);
select * from test.s ss (c1, c2, c3, c4) where exists (select ss.i from test.tt);
-- correlation name exists at both levels, but only column match is at
-- parent level
select * from test.s where exists (select s.i from test.tt s);
-- only match is at peer level
select * from test.s where exists (select * from test.tt) and exists (select ii from test.t);
-- correlated column in a derived table
select * from test.s, (select * from test.tt where test.s.i = ii) a;

-- positive tests

-- skip levels to find match
select * from test.s where exists (select * from test.ttt where iii = 
								(select 11 from test.tt where ii = i and ii <> 1)); 

-- join in subquery
select * from test.s where i in (select i from test.t, test.tt where test.s.i <> i and i = ii);
select * from test.s t1 where i in (select i from test.t t2, test.tt t3 where t1.i <> i and i = ii);
-- joins in both query blocks
select test.s.i, test.t.i from test.s, test.t 
where test.t.i = (select ii from test.ttt, test.tt where test.s.i = test.t.i and test.t.i = test.tt.ii and iii = 22 and ii <> 1);
select t1.i, t2.i from test.s t1, test.t t2 
where t2.i = (select ii from test.ttt t3, test.tt t4 where t1.i = t2.i and t2.i = t4.ii and iii = 22 and ii <> 1);

----------------------------------
-- update
create table test.u (i int, s smallint, c char(30), vc char(30));
insert into test.u select * from test.s;
select * from test.u;

update test.u set i = 2
where vc <> (select vc from test.s where vc = '1');
select * from test.u;

delete from test.u;
insert into test.u select * from test.s;

-- delete
delete from test.u where c < (select c from test.t where c = '2');
select * from test.u;




-- reset autocommit
autocommit on;

-- bug 5146 - drop schema did not invalidate plan for create table.
-- now schemas are implictly created.
create schema B5146;
create table B5146.DT(i int);
insert into B5146.DT values 5146, 6415;


create schema A5146;
prepare PS5146_TABLE as 'create table A5146.I(i int)';
drop schema A5146 restrict;
execute PS5146_TABLE;
insert into A5146.I values 3;
select * from A5146.I;
drop table A5146.I;

prepare PS5146_VIEW as 'create view A5146.V AS SELECT * FROM B5146.DT';
drop schema A5146 restrict;
execute PS5146_VIEW;
select * from A5146.V;
drop view A5146.V;

prepare PS5146_TRIGGER as 'create trigger A5146.DT_TRIG AFTER INSERT ON B5146.DT FOR EACH STATEMENT MODE DB2SQL UPDATE  B5146.DT SET I = I + 1';
drop schema A5146 restrict;
execute PS5146_TRIGGER;
drop trigger A5146.DT_TRIG;

prepare PS5146_PROCEDURE as 'create procedure A5146.DUMMY() language java external name ''asdf.asdf'' parameter style java';
drop schema A5146 restrict;
execute PS5146_PROCEDURE;
drop procedure A5146.DUMMY;

-- check implicit schema creation for all CREATE statements that create schema objects.
-- TABLE,VIEW,PROCEDURE TRIGGER, STATEMENT
-- Cloudscape requires that the INDEX schema matches the (existing) table schema so
-- there is no implict schema creation for CREATE INDEX.

prepare ISC_TABLE as 'create table ISC.I(i int)';
execute ISC_TABLE;
insert into ISC.I values 3;
select * from ISC.I;
drop table ISC.I;
drop schema ISC restrict;

prepare ISC_VIEW as 'create view ISC.V AS SELECT * FROM B5146.DT';
execute ISC_VIEW;
select * from ISC.V;
drop view ISC.V;
drop schema ISC restrict;

prepare ISC_TRIGGER as 'create trigger ISC.DT_TRIG AFTER INSERT ON B5146.DT FOR EACH STATEMENT MODE DB2SQL UPDATE  B5146.DT SET I = I + 1';
execute ISC_TRIGGER;
insert into B5146.DT values 999;
drop trigger ISC.DT_TRIG;
drop schema ISC restrict;
select * from B5146.DT;

prepare ISC_PROCEDURE as 'create procedure ISC.DUMMY() language java external name ''asdf.asdf'' parameter style java';
execute ISC_PROCEDURE;
CALL ISC.DUMMY();
drop procedure ISC.DUMMY;
drop schema ISC restrict;

-- check declare of a temp table does not create a SESSION schema.
DECLARE GLOBAL TEMPORARY TABLE SESSION.ISCT(c21 int) on commit delete rows not logged;
select count(*) from SYS.SYSSCHEMAS WHERE SCHEMANAME = 'SESSION';
drop table SESSION.ISCT;

drop table B5146.DT;
drop schema B5146 restrict;

create schema SYSDJD;
drop schema SYSDJD restrict;

create schema "sys";
drop schema "sys" restrict;

set schema test;

-- drop the tables
drop table s;
drop table t;
drop table tt;
drop table ttt;
drop table u;
set schema app;
drop schema test restrict;

