-- this test relies heavily on old statistics functionality that is no longer supported and hence taking this test out of nightly suite.
maximumdisplaywidth 5000;


create table two (x int);
insert into two values (1),(2);

create table ten (x int);
insert into ten values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);

create table twenty (x int);
insert into twenty values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20);

create table hundred (x int generated always as identity, dc int);
insert into hundred (dc) select t1.x from ten t1, ten t2;

create table template (id int not null generated always as identity,
					   two int, twenty int, hundred int);

-- 4000 rows.
insert into template (two, twenty, hundred)
	   select two.x, twenty.x, hundred.x from two, twenty, hundred;

create index template_two on template(two);
create index template_twenty on template(twenty);

-- 20 distinct values.
create index template_22 on template(twenty,two);

create unique index template_id on template(id);

create index template_102 on template(hundred,two);

create table test (id int, two int, twenty int, hundred int);
create index test_id on test(id);
insert into test select * from template;

create view showstats as
select cast (conglomeratename as varchar(20)) indexname, 
	   cast (statistics->toString() as varchar(40)) stats,
	   creationtimestamp createtime, 
	   colcount ncols
from sys.sysstatistics, sys.sysconglomerates 
where conglomerateid = referenceid;

select * from showstats order by indexname, stats, createtime, ncols;

update statistics for table template;
update statistics for table test;

create table rts_table 
(
	id int generated always as identity,
    comments  varchar(128),
	stats boolean,
	rts double
);

--- SINGLE COLUMN TESTS------
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);

-- choose whatever plan you want but the row estimate should be.
-- (n * n) * 0.5
get cursor c as 
'select template.id
from properties joinOrder=fixed test, template
where test.two = template.two';

close c;

insert into rts_table 
	   values (default, 'join on two, template inner, all rows.', true, runtimestatistics()->getEstimatedRowCount());  
 
-- choose hash join. selectivity should be the same.
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=hash 
where test.two = template.two';

close c;

insert into rts_table 
	   values (default, 'join on two. template inner, hash join,  all rows.', true, runtimestatistics()->getEstimatedRowCount());  

-- choose NL join. selectivity should be the same.
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.two = template.two';

close c;

insert into rts_table 
	   values (default, 'join on two. template inner, NL, no index, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_two
where test.two = template.two';

close c;

insert into rts_table 
	   values (default, 'join on two. template inner, NL, index=two, all rows.', true, runtimestatistics()->getEstimatedRowCount());  


select id, comments, rts
from rts_table where comments like 'join on two%';

-- do joins on 20
-- first do NL
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_twenty
where test.twenty = template.twenty';

close c;

insert into rts_table 
	   values (default, 'join on twenty. template inner, NL, index=template_twenty, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

-- join on 20 but using index 20_2. cost as well as selectivity
-- should be divided using selectivity. cost should almost be the
-- same as using template_twenty but a shade more...
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_22
where test.twenty = template.twenty';

close c;

insert into rts_table 
	   values (default, 'join on twenty. template inner, NL, index=template_22, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

-- join on twenty but no index.
-- rc should be divided using selectivity. cost should be way different!
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.twenty = template.twenty';

close c;

insert into rts_table 
	   values (default, 'join on twenty. template inner, NL, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on twenty%';

-- still single column, try stuff on 100 but with extra qualification.
-- on outer table.
-- row count is 100 * 4000 * 0.01 = 4000
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_102
where test.hundred = template.hundred and test.id <= 100';

close c;

insert into rts_table 
	   values (default, 'join on hundred. template inner, NL, index=template_102, 100 rows from outer.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.hundred = template.hundred and test.id <= 100';

close c;

insert into rts_table 
	   values (default, 'join on hundred. template inner, NL, index=null, 100 rows from outer .', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=hash, index=null
where test.hundred = template.hundred and test.id <= 100';

close c;

insert into rts_table 
	   values (default, 'join on hundred. template inner, hash, index=null, 100 rows from outer.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on hundred%';


-- multi predicate tests.
-- first do a join involving twenty and two.
-- force use of a single column index to do the join.
-- the row count should involve statistics from both
-- 10 and 2 though....

-- row count should 4K * 4K * 1/40= 400,000
-- cost doesn't show up in runtest output but should depend on the
-- index being used. verify by hand before checking in.

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=hash, index=null
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. template inner, hash, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_two
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. template inner, NL, index=template_two, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_twenty
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. template inner, NL, index=template_twenty, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_22
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. template inner, NL, index=template_22, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on twenty/two%';

-- multi predicate tests continued.
-- drop index twenty,two -- use above predicates. should
-- be smart enough to figure out the selectivity
-- by combining twenty and two.

drop index template_22;

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=hash, index=null
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. index twenty_two dropped. template inner, hash, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_two
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. index twenty_two dropped. template inner, NL, index=template_two, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_twenty
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. index twenty_two dropped. template inner, NL, index=template_twenty, all rows.', true, runtimestatistics()->getEstimatedRowCount());  


select id, comments, rts
from rts_table where comments like 'join on twenty/two. index twenty_two dropped%';

drop index template_two;
-- we only have index template_twenty-- for the 
-- second predicate we should use 0.1 instead of 0.5
-- thus reducing earlier row count by a factor of 5.
-- 80,000 instead of 400000

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. index twenty_two and two dropped. template inner, NL, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_twenty
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. index twenty_two and two dropped. template inner, NL, index=template_twenty, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on twenty/two. index twenty_two and two dropped%';

-- now drop index template_twenty.
-- selectivity should become 0.1 * 0.1 = 0.01.
-- 16 * 10^6 * .01 = 160,000
drop index template_twenty;

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.twenty = template.twenty and test.two=template.two';

close c;

insert into rts_table 
	   values (default, 'join on twenty/two. all indexes dropped. template inner, NL, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on twenty/two. all indexes dropped%';



create index template_two on template(two);
create index template_twenty on template(twenty);
create index template_22 on template(twenty,two);

update statistics for table template;

-- throw in additional predicates
-- see that the optimizer does the right thing. 
-- index on template_102. join on hundred, 
-- constant predicate on two. should be able to use
-- statistics for hundred_two to come up with row estimate.

-- selectivity should be 0.01 * 0.5 == 0.005.
-- row count is 16*10^6 * 0.005 = 8*10^4.

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.hundred = template.hundred and 1=template.two';

close c;

insert into rts_table 
	   values (default, 'join on hundred. constant pred on two. NL, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

-- JUST retry above query with different access paths-- 
-- row count shouldn't change! 
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_102
where test.hundred = template.hundred and 1=template.two';

close c;

insert into rts_table 
	   values (default, 'join on hundred. constant pred on two. NL, index=template_102, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on hundred. constant pred on two%';

-- hundred and twenty-- we can use statistics for 100,2 to get
-- selectivity for 100 and twenty to get selectivity for 20.
-- selectivity should 0.01 * 0.05 = 0.0005 -> 80,000
get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=null
where test.hundred = template.hundred and 1=template.twenty';

close c;

insert into rts_table 
	   values (default, 'join on hundred. constant pred on twenty. NL, index=null, all rows.', true, runtimestatistics()->getEstimatedRowCount());  

get cursor c as 
'select template.id
from properties joinOrder=fixed 
test, template properties joinStrategy=nestedLoop, index=template_102
where test.hundred = template.hundred and 1=template.twenty';

close c;

insert into rts_table 
	   values (default, 'join on hundred. constant pred on twenty. NL, index=template_102 all rows.', true, runtimestatistics()->getEstimatedRowCount());  

select id, comments, rts
from rts_table where comments like 'join on hundred. constant pred on twenty%';


-- some 3 way joins.

--drop table template;
--drop table test;

-- 4000
create table t1 (id int generated always as identity, two int, twenty int, hundred varchar(3));
insert into t1 (hundred, twenty, two) select CAST(CHAR(hundred.x) AS VARCHAR(3)), twenty.x, two.x from hundred, twenty, two;

create table t2 (id int generated always as identity, two int, twenty int, hundred varchar(3));
insert into t2 (hundred, twenty, two) select CAST(CHAR(hundred.x) AS VARCHAR(3)) , twenty.x, two.x from hundred, twenty, two;

create table t3 (id int generated always as identity, two int, twenty int, hundred varchar(3));
insert into t3 (hundred, twenty, two) select CAST(CHAR(hundred.x) AS VARCHAR(3)), twenty.x, two.x from hundred, twenty, two;

create index t1_hundred on t1(hundred);
create index t1_two_twenty on t1(two,twenty);
create index t1_twenty_hundred on t1(twenty, hundred);

create index t2_hundred on t2(hundred);
create index t2_two_twenty on t2(two,twenty);
create index t2_twenty_hundred on t2(twenty, hundred);

create index t3_hundred on t3(hundred);
create index t3_two_twenty on t3(two,twenty);
create index t3_twenty_hundred on t3(twenty, hundred);

update statistics for table t1;
update statistics for table t2;
update statistics for table t3;

select * from showstats where indexname like 'T1%' order by indexname;
select * from showstats where indexname like 'T2%' order by indexname;
select * from showstats where indexname like 'T3%' order by indexname;


-- t1 x t2 yields 8000 rows.
-- x t3 yield 8*4 * 10^6 / 2= 16*10^6.
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2, t3 
where t1.hundred=t2.hundred and t1.twenty = t2.twenty and  t2.two = t3.two';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- t1 x t2 --> 16 * 10^4.
-- x t3    --> 32 * 10^7.
-- additional pred --> 32 * 10^5.
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2, t3 
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- variations on above query: try different join strategies.
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2, t3 properties joinStrategy=hash
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2, t3 properties joinStrategy=nestedLoop
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2 properties joinStrategy=nestedLoop, t3 
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2 properties joinStrategy=hash, t3 
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();


-- duplicate predicates; this time t1.hundred=?
-- will show up twice when t1 is optimized at the end.
-- selectivity should be same as above!
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t2, t3, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- variations on above query: try different join strategies.
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t3, t2, t1 properties joinStrategy=hash
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t3, t2, t1 properties joinStrategy=nestedLoop
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t2, t3 properties joinStrategy=nestedLoop, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t3, t2 properties joinStrategy=hash, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- some more variations on the above theme
-- some constant predicates thrown in.
-- remember hundred is a char column-- for some reason if you give the constant 
-- as a numeric argument it doesn't recognize that as a constant start/stop 
-- value for the index (is this a bug?)
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t2, t3, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred and t1.hundred = ''1''';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- we have t1.100=t2.100 and t1.100=t3.100, so t2.100=t3.100 is redundant
-- row count shouldn't factor in the redundant predicate.
-- row count should be 3200000.0
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t2, t3, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t1.hundred = t3.hundred and t2.hundred = t3.hundred';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- slightly different join predicates-- use composite stats.
-- t1 x t2			  --> 16 * 10.4.
--         x t3		  --> 16 * 10.4 * 4000 * 1/40 = 16*10.6
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t2, t3, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- same as above but muck around with join order.
get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t2, t3
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t2, t1, t3
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t1, t3, t2
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t3, t2, t1
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

get cursor c as 
'select t1.id 
from properties joinOrder=fixed t3, t1, t2
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- and just for fun, what would we have gotton without statistics.
get cursor c as 
'select t1.id 
from properties useStatistics=false, joinOrder=fixed t3, t1, t2
where t1.hundred=t2.hundred and t2.two=t3.two and t2.twenty=t3.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount() / 1000;

-- gosh what now? I'm tired of writing queries....
-- make sure we do a good job of stats on 1/3.

create table scratch_table (id int, two int, twenty int, hundred int);
insert into scratch_table select id, two, twenty, CAST(CHAR(hundred) AS INTEGER) from t1;

create index st_all on scratch_table (two, twenty, hundred);
update statistics for table scratch_table;

-- since the statistics (rowEstimates) are not precise, force a checkpoint
-- to force out all the row counts to the container header, and for good
-- measure do a count which will update the row counts exactly.

CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();

select count(*) from t1;
select count(*) from scratch_table;

-- preds are on columns 1 and 3.
-- should use default stats for 100 (0.1) and 0.5 for two.

-- 16*10.6 * 5*10.-2 = 80*10.4
get cursor c as
'select s.id
from properties joinOrder=fixed t1, scratch_table s
where t1.two=s.two  and s.hundred= CAST(CHAR(t1.hundred) AS INTEGER)';

close c;
values runtimestatistics()->getEstimatedRowCount();

-- preds are on column 2.
-- 0.1 --> 16*10.5
get cursor c as
'select s.id
from properties joinOrder=fixed t1, scratch_table s
where t1.twenty=s.twenty';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- pred are on column 2,3
-- 0.01 --> 16*10.4
get cursor c as
'select s.id
from properties joinOrder=fixed t1, scratch_table s
where t1.twenty=s.twenty  and s.hundred = CAST(CHAR(t1.hundred) AS INTEGER)';

close c;

values runtimestatistics()->getEstimatedRowCount();

-- test of statistics matcher algorithm; make sure that 
-- we choose the best statistics (the weight stuff in
-- predicatelist).

-- 2,20,100
get cursor c as 'select t1.id from t1, t2 where t1.two=t2.two and t1.twenty=t2.twenty and t1.hundred=t2.hundred';
close c;
values runtimestatistics()->getEstimatedRowCount();

-- now muck around with the order of the predicates.
--2,100,20
get cursor c as 'select t1.id from t1, t2 where t1.two=t2.two and t1.hundred=t2.hundred and t1.twenty=t2.twenty ';
close c;
values runtimestatistics()->getEstimatedRowCount();

--100,20,2
get cursor c as 'select t1.id from t1, t2 where t1.hundred=t2.hundred and t1.twenty=t2.twenty and t1.two=t2.two';
close c;
values runtimestatistics()->getEstimatedRowCount();

--100,2,20
get cursor c as 'select t1.id from t1, t2 where t1.hundred=t2.hundred and t1.two=t2.two and t1.twenty=t2.twenty ';
close c;
values runtimestatistics()->getEstimatedRowCount();

get cursor c as 'select t1.id from t1, t2 where t1.twenty=t2.twenty  and t1.hundred=t2.hundred and t1.two=t2.two ';
close c;
values runtimestatistics()->getEstimatedRowCount();

get cursor c as 'select t1.id from t1, t2 where t1.twenty=t2.twenty  and t1.two=t2.two and t1.hundred=t2.hundred';
close c;
values runtimestatistics()->getEstimatedRowCount();

--Beetle 4321
-- check what happens when we need to remove non-continguous predicates
create table complex (id int generated always as identity,
					   two int, twenty int, hundred int, a int, b int);
insert into complex (two, twenty, hundred, a, b)
	   select two.x, twenty.x, hundred.x, two.x, twenty.x from two, twenty, hundred;
create index complexind on complex(two, twenty, hundred, a, b);

-- since the statistics (rowEstimates) are not precise
-- we force them to be updated for some queries.
select count(*) from complex;
select count(*) from template;

get cursor c as 'select t1.two from complex t1, template t2 where t1.two = 1 and t1.hundred = 2 and t1.a = 2 and t1.b = 2';
close c;
values runtimestatistics()->getEstimatedRowCount();

drop table two;
drop table ten;
drop table twenty;
drop table hundred;
drop table template;
drop table test;
drop table rts_table;
drop table t1;
drop table t2;
drop table t3;
drop table scratch_table;
drop table complex;
drop view showstats;
-- basic tests for update statistics; make sure that 
-- statistics with correct values are created and dropped
-- and such.

create view showstats as
select cast (conglomeratename as varchar(20)) indexname, 
	   cast (statistics->toString() as varchar(40)) stats,
	   creationtimestamp createtime, 
	   colcount ncols
from sys.sysstatistics, sys.sysconglomerates 
where conglomerateid = referenceid;

-- first on int, multi-column
create table t1 (c1 int generated always as identity, c2 int, c3 int);

insert into t1 values (default, 1, 1);
insert into t1 values (default, 1, 1);
insert into t1 values (default, 1, 2);
insert into t1 values (default, 1, 2);

insert into t1 values (default, 2, 1);
insert into t1 values (default, 2, 1);
insert into t1 values (default, 2, 2);
insert into t1 values (default, 2, 2);

insert into t1 values (default, 3, 1);
insert into t1 values (default, 3, 1);
insert into t1 values (default, 3, 2);
insert into t1 values (default, 3, 2);

insert into t1 values (default, 4, 1);
insert into t1 values (default, 4, 1);
insert into t1 values (default, 4, 2);
insert into t1 values (default, 4, 2);

-- create index should automatically create stats.
create index t1_c1c2 on t1 (c1, c2);
select * from showstats order by indexname, stats, createtime, ncols;

-- index dropped stats should be dropped.
drop index t1_c1c2;
select * from showstats order by indexname, stats, createtime, ncols;


-- second part of the test.
-- check a few extra types.

create table t2
(
	i int not null,
	vc varchar(32) not null, 
	dt date, 
	ch char(20), 
	constraint pk primary key (i, vc)
);

create index t2_ch_dt on t2(ch, dt);
create index t2_dt_vc on t2(dt,vc);

-- do normal inserts. 
insert into t2 values (1, 'one', '2001-01-01', 'one');
insert into t2 values (2, 'two', '2001-01-02', 'two');
insert into t2 values (3, 'three', '2001-01-03', 'three');

insert into t2 values (1, 'two', '2001-01-02', 'one');
insert into t2 values (1, 'three', '2001-01-03', 'one');
insert into t2 values (2, 'one', '2001-01-01', 'two');

select * from showstats order by indexname, stats, createtime, ncols;

-- do another insert then just updstat for whole table.
insert into t2 values (2, 'three', '2001-01-03', 'two');

update statistics for table t2;

-- make sure that stats are correct.

select * from showstats where indexname = 'T2_CH_DT' order by indexname, stats, createtime, ncols;
select count(*) from (select distinct ch from t2) t;
select count(*) from (select distinct ch,dt from t2) t;

select * from showstats where indexname = 'T2_DT_VC' order by indexname, stats, createtime, ncols;
select count(*) from (select distinct dt from t2) t;
select count(*) from (select distinct dt,vc from t2) t;

select stats, ncols from showstats where indexname not like 'T2%' order by stats, ncols;

-- delete everything from t2, do bulkinsert see what happens.
delete from t2;
-- no material impact on stats.

-- bulk insert; all indexes should have stats.
insert into t2 properties insertMode=bulkInsert values (2, 'one', '2001-01-01', 'two');

select * from showstats where indexname like 'T2%' order by indexname, stats, createtime, ncols;

-- now try bulk insert replace.
insert into t2 properties insertMode=replace values (2, 'one', '2001-01-01', 'two'), (1, 'one', '2001-01-01', 'two');

select * from showstats where indexname like 'T2%' order by indexname, stats, createtime, ncols;

drop table t2;


-- various alter table operations to ensure correctness.
-- 1. add and drop constraint.
create table t3 (x int not null generated always as identity, y int not null, z int);
insert into t3 (y,z) values (1,1),(1,2),(1,3),(1,null),(2,1),(2,2),(2,3),(2,null);

select * from showstats order by indexname, stats, createtime, ncols;
-- first alter table to add primary key;
alter table t3 add constraint pk_t3 primary key (x,y);

select stats, ncols from showstats order by stats, ncols;

-- now drop the constraint
alter table t3 drop constraint pk_t3;

select * from showstats order by indexname, stats, createtime, ncols;

-- try compress with tons of rows. you can never tell
-- what a few extra pages can do :)
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;
insert into t3 (y,z) select y,z from t3;

select count(*) from t3;

create index t3_xy on t3(x,y);
select * from showstats order by indexname, stats, createtime, ncols;

delete from t3 where z is null;
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T3', 0);

select * from showstats order by indexname, stats, createtime, ncols; -- all should be hunky dory.

drop table t3;

create table t4 (x int, y int, z int);
insert into t4 values (1,1,1);
insert into t4 values (1,2,1);
insert into t4 values (1,1,2);

create index t4_x on t4(x);
create index t4_xy on t4(x,y);
create index t4_yz on t4(y,z);

select * from showstats order by indexname, stats, createtime, ncols;

-- if I drop column x, then stats for t4_x should get dropped
-- index t4_xy should get rebuilt to only be on y. so one of the
-- stats should be recreated. and t4_yz shouldn remain in its
-- entirety.

alter table t4 drop column x;
select * from showstats order by indexname, stats, createtime, ncols;
drop table t4;

-- SPS tests make sure drop/update statistics statements
-- get written to disk correctly.
create table t5 (x int, y int);
insert into t5 values (1,1), (1,2);
create index t5_y on t5(y);

insert into t5 values (2,1);
select * from showstats order by indexname, stats, createtime, ncols;

update statistics for table t5;
select * from showstats order by indexname, stats, createtime, ncols;

create index t5_x on t5(x);
select * from showstats order by indexname, stats, createtime, ncols;

drop statistics for index t5_x;

-- t5_y should be there.
select * from showstats order by indexname, stats, createtime, ncols;

drop table t5;

create table t6 (i int generated always as identity, j varchar(10));
create index t6_i on t6(i);
create index t6_j on t6(j);
create index t6_ji on t6(j,i);

insert into t6 values (default, 'a');
insert into t6 values (default, 'b');
insert into t6 values (default, 'c');
insert into t6 values (default, 'd');
insert into t6 values (default, 'e');
insert into t6 values (default, 'f');
insert into t6 values (default, 'g');
insert into t6 values (default, 'h');

insert into t6 values (default, 'a');
insert into t6 values (default, 'b');
insert into t6 values (default, 'c');
insert into t6 values (default, 'd');
insert into t6 values (default, 'e');
insert into t6 values (default, 'f');
insert into t6 values (default, 'g');
insert into t6 values (default, 'h');
insert into t6 values (default, 'i');

insert into t6 values (default, 'a');
insert into t6 values (default, 'b');
insert into t6 values (default, 'c');
insert into t6 values (default, 'd');
insert into t6 values (default, 'e');
insert into t6 values (default, 'f');
insert into t6 values (default, 'g');
insert into t6 values (default, 'h');

update statistics for index t6_j;
update statistics for table t6;

select * from showstats order by indexname, stats, createtime, ncols;
delete from t6;

insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');
insert into t6 values (default, 'a');

-- make the 17th row the same as the 16th;
-- make sure we switch to the next group
-- fetch we handle the case correctly.

insert into t6 values (default, 'a');

update statistics for table t6;
select * from showstats order by indexname, stats, createtime, ncols;

-- will be table with no rows.
create table et (x int, y int);
create index etx on et(x);
create index ety on et(y);

update statistics for index etx;
update statistics for table et;

select * from showstats where indexname like 'ET%' order by indexname, stats, createtime, ncols;
drop table et;

-- tests for nulls.
create table null_table (x int, y varchar(2));
create index nt_x on null_table(x desc);

insert into null_table values (1,'a');
insert into null_table values (2,'b');
insert into null_table values (3,'c');

insert into null_table values (null,'a');
insert into null_table values (null,'b');
insert into null_table values (null,'c');

insert into null_table values (null,'a');
insert into null_table values (null,'b');
insert into null_table values (null,'c');

update statistics for table null_table;

select * from showstats where indexname = 'NT_X' order by indexname, stats, createtime, ncols;

-- try composite null keys (1,null) is unique from (1,null)
-- as is (null,1) from (null,1)
drop index nt_x;

create index nt_yx on null_table(y,x);

-- the first key y has 3 unique values.
-- the second key y,x has 9 unique values because of nulls.

select * from showstats where indexname = 'NT_YX' order by indexname, stats, createtime, ncols;

-- keyword autoincrement is no more supported in both Cloudscape and DB2 mode. Instead need to use generated always as identity
create table autoinKeywordNotAllowed (x int default autoincrement, dc int);
-- Negative test for create statement in cloudscape mode. CREATE STATEMENT is no more supported in both cloudscape and DB2 mode.
create statement s1 as values 1,2;
