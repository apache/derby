-- tests for views

-- set autocommit off
autocommit off;

-- create some tables
create table t1(i int, s smallint, f float, dp double precision);
create table t2(i int, s smallint, f float, dp double precision);
create table insert_test (f float);

-- create some views
create view sv1 (s_was_i, dp_was_s, i_was_f, f_was_dp) as
select * from t1;
create view sv2 as select * from t1;
create view sv3 as select dp, f from t1 where i = s;
create view sv4(i) as values 1, 2, 3;
create view sv5 (c1) as select * from sv4;

create view cv1 (t1_i, t2_s, t1_f, t2_dp) as
select t1.i, t2.s, t1.f, t2.dp from t1, t2 where t1.i between t2.s and t2.i;
create view cv2 as select * from sv1, sv3 where dp = f_was_dp;
create view cv3(i,s,f,dp) as select i, s, f, dp from sv2 union
				   select dp_was_s, s_was_i, f_was_dp, i_was_f from sv1;
create view cv4 (distinct_i) as select distinct i from t1;
create view cv5(i,s) as select * from (select i, s from cv3 where i = s) xyz;
create view cv6 (c1, c2) as select a.c1 as x, b.c1 as y from sv5 a, sv5 b 
where a.c1 <> b.c1;
create view cv7 as select t.i, v.c1 from t1 t, cv6 v where t.i = v.c1;
create view cv8(col1, col2) as select 'Column 1',  'Value = ' || cast(c1 as char(5)) from cv7
		where 1 in (select i from sv5);

-- populate the tables
insert into t1 values (1, 1, 1.0, 1.0);
insert into t1 values (1, 2, 3.0, 4.0);
insert into t1 values (8, 7, 6.0, 5.0);

insert into t2 values (1, 1, 1.0, 1.0);
insert into t2 values (1, 2, 3.0, 4.0);
insert into t2 values (8, 7, 6.0, 5.0);

-- negative tests

-- view with a parameter
create view vneg as select * from t1 where i = ?;

-- drop view on table
drop view t1;

-- drop table on view
drop table sv1;

-- views and tables share same name space
create view sv1(i) as values 1;
create table sv1 (c1 int);
create view t1(i) as values 1;

-- drop non-existant view
drop view notexists;

-- duplicate column name in view's column list
create view shouldntwork (c1, c2, c1) as select i, s, f from t1;

-- # of columns in view's column list does not match that in view definition
create view shouldntwork (c1, c2, c3) as select i, s from t1;
create view shouldntwork (c1, c2, c3) as select i, s, f, dp from t1;

-- try to drop a table out from under a view
drop table t1;
drop table t2;

-- try to drop a view out from under another view
drop view sv1;
drop view sv3;

-- try to drop a view out from under a cursor
get cursor c1 as 'select * from cv8';
drop view cv8;
drop view sv5;
drop view sv4;
close c1;

-- view updateability
-- (No views are currently updateable)
insert into sv1 values 1;
delete from sv1;
update sv1 set s_was_i = 0;
get cursor c2 as 'select * from sv1 for update of s_was_i';

-- create index on a view
create index i1 on sv2(i);


-- positive tests
select * from sv1;
select * from sv2;
select * from sv3;
select * from sv4;
select * from sv5;

select * from cv1;
select * from cv2;
select * from cv3;
select * from cv4 order by 1;
select * from cv5;
select * from cv6;
select * from cv7;
select * from cv8;

select * from (select * from cv3) x order by 1,2;
select * from (select * from cv4) x order by 1;
select * from (select * from cv5) x;

-- verify that we can create and drop indexes on underlying tables
create index i on t1(i);
drop index i;

-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

-- test inserts from a view
insert into insert_test select * from sv5;
select * from insert_test;

-- drop the views
drop view cv8;
drop view cv7;
drop view cv6;
drop view cv5;
drop view cv4;
drop view cv3;
drop view cv2;
drop view cv1;
drop view sv5;
drop view sv4;
drop view sv3;
drop view sv2;
drop view sv1;

-- drop the tables
drop table t1;
drop table t2;
drop table insert_test;

-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

-- bug 2745
CREATE TABLE orgtable (
	name VARCHAR(255),
	supervisorname VARCHAR(255),
	jobtitle VARCHAR(255)
);

CREATE VIEW orgview AS
    SELECT name, supervisorname, jobtitle FROM orgtable;

SELECT name,jobtitle FROM orgview WHERE name IN (SELECT supervisorname FROM orgview WHERE name LIKE 'WYATT%');

drop view orgview;
drop table orgtable;

-- reset autocommit
autocommit on;
