--
-- tests for DISTINCT
--
-- these tests assume: no indexes, no order by, no grouping
--
-- test plan is represented by '.' items in comments. 
-- the flavors of select are shown in distinct.subsql, which is
-- run over a variety of data configurations.
-- this file expects to be run from a directory under $WS/systest.

-- speed up a fraction with autocommit off...
autocommit off;

create table t (i int, s smallint, r real, f float, d date, t time,
	ts timestamp, c char(10), v varchar(20));

-- data flavor:
-- . no data at all (filtered out or just plain empty)

run resource 'distinct.subsql';

-- . 1 row
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');

run resource 'distinct.subsql';

-- . all rows the same
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');

run resource 'distinct.subsql';

-- . variety of rows, some same and some different
insert into t values (2, 1, 4, 3, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');

run resource 'distinct.subsql';

-- . variety of rows, all different
delete from t;
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');
insert into t values (2, 1, 4, 3, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');

run resource 'distinct.subsql';

-- . variety of rows, some same in some columns but not others
delete from t;
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');
insert into t values (1, 1, 3, 4, '1992-01-02', '19:01:01',
'1992-01-01 19:01:01.000', 'goodbye', 'planet');

run resource 'distinct.subsql';

-- . just nulls

delete from t;
-- all the defaults are null, so just get a row in easily
insert into t (i) values (null);
insert into t (i) values (null);

run resource 'distinct.subsql';

-- . 1 null in the mix
delete from t;
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');
insert into t values (1, 1, 3, 4, '1992-01-02', '19:01:01',
'1992-01-01 19:01:01.000', 'goodbye', 'planet');
insert into t (i) values (null);

run resource 'distinct.subsql';
-- . several nulls in the mix
insert into t (i) values (null);
insert into t (i) values (null);

run resource 'distinct.subsql';

-- . nulls in some columns, not others
delete from t where i is null;
insert into t values (null, 1, null, 4, null, '19:01:01',
null, 'goodbye', null);
insert into t values (1, null, 3, null, '1992-01-02', null,
'1992-01-01 19:01:01.000', null, 'planet');

run resource 'distinct.subsql';

drop table t;
rollback;

-- sure would like to dump all those selects now...they are invalid, at least,
-- maybe that frees up some space (FLW)

-- here are other things to test:

-- . select distinct over a values table
-- three rows
select distinct * from (values (1,2),(1,3),(1,2),(2,3)) as t(a,b);
-- two rows
select distinct a from (values (1,2),(1,3),(1,2),(2,3)) as t(a,b);

-- . show that distinct is a keyword, not a column name
select distinct from t;

rollback;

-- . usertypes
-- To test usertypes in a way that works with weblogic, we
-- pick one we can construct with other functionality available to us
-- (UUID won't work)
-- At the time these tests were written, user type comparisons
-- were not supported.

create table userInt (u integer);
insert into userInt values (123);
insert into userInt values (123);
insert into userInt values (456);
insert into userInt values (null);
create table sqlInt (i int not null);
insert into sqlInt values(123);

-- expect two rows, 123 and 456
select distinct u from userInt where u is not null;
-- two rows, 123 and 456
select u from userInt where u is not null;

-- multiple rows in subquery get correct complaint
select distinct i 
from sqlInt 
where i = (select distinct u from userInt);

drop table userInt;
drop table sqlInt;

rollback;

-- . varchar blank padding is ignored, length will vary depending on row selected

create table v (v varchar(40));
insert into v values ('hello');
insert into v values ('hello   ');
insert into v values ('hello      ');

-- the |'s are just for visual demarcation
select distinct '|' as "|", v, '|' as "|" from v;
select {fn length(c)} from (select distinct v from v) as t(c);

drop table v;
rollback;

-- distinct bigint
create table li (l bigint, i int);
insert into li values(1, 1);
insert into li values(1, 1);
insert into li values(9223372036854775807, 
					  2147483647);

select distinct l from li;
(select distinct l from li) union all (select distinct i from li) order by 1;
select distinct l from li union select distinct i from li;
select distinct l 
from (select l from li union all select i from li) a(l);

drop table li;
rollback;

autocommit off; -- was off above, ensure it stayed off for this part of test
create table u (d date);
-- three rows
insert into u values ('1997-09-09'),('1997-09-09');
insert into u values (null);

-- . nexting past the last row of a distinct
get cursor past as 'select distinct d from u';
next past;
next past;
-- should report no current row:
next past;
next past;
close past;

-- . for update on a select distinct
-- both should get errors, not updatable.
select distinct d from u for update;
select distinct d from u for update of d;

-- . positioned update/delete on a select distinct
get cursor c1 as 'select distinct d from u';
next c1;
-- both should fail with cursor not updatable
update u set d='1992-01-01' where current of c1;
delete from u where current of c1;
close c1;

get cursor c1 as 'select distinct d from u';
-- both should fail with cursor not updatable (not no current row)
update u set d='1992-01-01' where current of c1;
delete from u where current of c1;
next c1;
next c1;
next c1;
next c1;
-- both should fail with cursor not updatable, or cursor closed/does not exist
update u set d='1992-01-01' where current of c1;
delete from u where current of c1;
close c1;

get cursor c1 as 'select distinct d from u';
close c1;
-- both should fail with cursor not updatable, or cursor closed/does not exist
update u set d='1992-01-01' where current of c1;
delete from u where current of c1;

drop table u;
rollback;

-- insert tests
create table t (i int, s smallint, r real, f float, d date, t time,
	ts timestamp, c char(10), v varchar(20));
create table insert_test (i int, s smallint, r real, f float, d date, t time,
	ts timestamp, c char(10), v varchar(20));

-- populate the tables
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');
insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');
insert into t values (2, 1, 4, 3, '1992-01-01', '19:01:01',
'1992-01-01 19:01:01.000', 'hello', 'planet');

insert into insert_test select distinct * from t;
select * from insert_test;
delete from insert_test;

insert into insert_test select distinct * from t union select * from t;
select * from insert_test;
delete from insert_test;


rollback;

-- for bug 4194, "insert into select distinct" into a table with a generated column
create table destWithAI(c11 int generated always as identity, c12 int);
alter table destWithAI alter c11 set increment by 1;
create table destWithNoAI(c21 int, c22 int);
create table source(c31 int, c32 int, c33 int);
insert into source values(1,1,1);
insert into source values(1,2,1);
insert into source values(2,1,1);
insert into source values(2,2,1);
select distinct(c31) from source;
insert into destWithAI(c12) select distinct(c31) from source;
-- we will see gaps in the autoincrement column for all the duplicate rows from source
select * from destWithAI;
insert into destWithNoAI(c22) select distinct(c31) from source;
select * from destWithNoAI;

-- test for beetle 4402
-- problem with check that a  result set is in order since it is retrieved using
-- an index

CREATE TABLE netbutton1 (
  lname                 varchar(128) not null,
  name                  varchar(128),
  summary               varchar(256),
  lsummary              varchar(256),
  description           varchar(2000),
  ldescription          varchar(2000),
  publisher_username    varchar(256),
  publisher_lusername   varchar(256),
  version               varchar(16),
  source                
 long varchar for bit data, 
  updated               
 timestamp, 
  created               
 timestamp DEFAULT current_timestamp,
	primary key (lname)) 
;

insert into netbutton1 values('lname1','name1','sum2','lsum1', 'des1','ldes1','pubu1', 'publu1', 'ver1', null, current_timestamp, default);
insert into netbutton1 values('lname2','name2','sum2','lsum2', 'des2','ldes2','pubu2', 'publu2', 'ver2', null, current_timestamp, default);

CREATE TABLE library_netbutton (
  netbuttonlibrary_id   
 int not null, 
  lname         varchar(128) not null,
	primary key (netbuttonlibrary_id, lname))
;

insert into library_netbutton values(1, 'lname1');
insert into library_netbutton values(2, 'lname2');

-- this is the index which causes the bug to be exposed
create unique index ln_library_id on library_netbutton(netbuttonlibrary_id);

ALTER TABLE library_netbutton
ADD CONSTRAINT ln_lname_fk
FOREIGN KEY (lname) REFERENCES netbutton1(lname)
;

CREATE TABLE netbuttonlibraryrole1 (
  lusername             varchar(512) not null,
  netbuttonlibrary_id   
 int not null, 
  username              varchar(512),
  role                  varchar(24),
  created               
 timestamp DEFAULT current_timestamp,
	primary key (lusername, netbuttonlibrary_id)) 
;
insert into netbuttonlibraryrole1 values('lusername1', 1,'user1', 'role1', default);
insert into netbuttonlibraryrole1 values('lusername2', 2,'user2', 'role2', default);


autocommit off;
prepare c1 as 'SELECT DISTINCT nb.name AS name, nb.summary AS summary
               FROM netbutton1 nb, netbuttonlibraryrole1 nlr,
                    library_netbutton ln
              WHERE nlr.netbuttonlibrary_id = ln.netbuttonlibrary_id
                AND nb.lname = ln.lname
                AND (   nlr.lusername = ?
                     OR nlr.lusername = ?)
                AND nb.lname = ?
           ORDER BY summary';
execute c1 using 'values(''lusername1'', ''lusername2'', ''lname1'')';
rollback;

-- reset autocomiit
autocommit on;
