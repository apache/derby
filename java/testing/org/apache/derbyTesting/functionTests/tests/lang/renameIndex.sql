-- rename index tests
--
autocommit off;
--

-- negative tests
--

-- rename a non-existing index
-- should fail because there is no index by name i1t1
rename index i1t1 to i1rt1;
--

-- rename as some existing index name
create table t1(c11 int, c12 int);
create index i1t1 on t1(c11);
create index i2t1 on t1(c12);
rename index i1t1 to i2t1;
drop table t1;
--

-- rename a system table's index
set schema sys;
-- will fail because it is a system table
rename index syscolumns_index1 to newName;
set schema app;
--

-- rename an index when a view is on a table
create table t1(c11 int, c12 int);
create index t1i1 on t1(c11);
create view v1 as select * from t1;
select * from v1;
-- this succeeds with no exceptions
rename index t1i1 to t1i1r;
-- this succeeds with no exceptions
select * from v1;
drop view v1;
drop table t1;
-- another test for views
create table t1(c11 int not null primary key, c12 int);
create index i1t1 on t1(c11);
create view v1 as select * from t1;
-- following rename shouldn't fail
rename index i1t1 to i1rt1;
drop view v1;
-- even though there is no index i1t1 it still doesn't fail
create view v1 as select * from t1;
-- this succeeds with no exceptions
select * from v1;
rename index i1rt1 to i1t1;
-- now succeeds
select * from v1;
drop view v1;
drop table t1;
--

-- cannot rename an index when there is an open cursor on it
create table t1(c11 int, c12 int);
create index i1 on t1(c11);
get cursor c1 as 'select * from t1';
-- following rename should fail because of the cursor c1
rename index i1 to i1r;
close c1;
-- following rename should pass because cursor c1 has been closed
rename index i1 to i1r;
drop table t1;
--

-- creating a prepared statement on a table
create table t1(c11 int not null primary key, c12 int);
-- bug 5685
create index i1 on t1(c11);
autocommit off;
prepare p1 as 'select * from t1 where c11 > ?';
execute p1 using 'values (1)';
-- doesn't fail
rename index i1 to i1r;
-- statement passes
execute p1 using 'values (1)';
remove p1;
autocommit on;
drop table t1;
--

-- positive tests
-- a column with an index on it can be renamed
create table t3(c31 int not null primary key, c32 int);
create index i1_t3 on t3(c32);
rename index i1_t3 to i1_3r;
-- make sure that i1_t3 did get renamed. Following rename should fail to prove that.
rename index i1_t3 to i1_3r;
drop table t3;
--

-- creating a prepared statement on a table
autocommit off;
create table t3(c31 int not null primary key, c32 int);
create index i1_t3 on t3(c32);
prepare p3 as 'select * from t3 where c31 > ?';
execute p3 using 'values (1)';
-- can rename with no errors
rename index i1_t3 to i1_t3r;
execute p3 using 'values (1)';
rename index i1_t3r to i1_t3;
-- this should pass know because we restored the original index name
execute p3 using 'values (1)';
remove p3;
autocommit on;
drop table t3;
