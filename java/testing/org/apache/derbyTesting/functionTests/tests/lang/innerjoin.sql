-- test inner joins
-- (NO NATURAL JOIN)


autocommit off;

-- create some tables

create table t1(c1 int);
create table t2(c1 int);
create table t3(c1 int);
create table insert_test(c1 int, c2 int, c3 int);

-- populate the tables
insert into t1 values 1, 2, 3, 4;
insert into t2 values 1, 3, 5, 6;
insert into t3 values 2, 3, 5, 7;

-- negative tests

-- no join clause
select * from t1 join t2;
select * from t1 inner join t2;

-- empty column list
select * from t1 join t2 using ();

-- non-boolean join clause
select * from t1 join t2 on 1;

-- duplicate exposed names, DB2 extension
-- DB2 UDB: PASS
-- DB2 CS:  FAIL
select * from t1 join t1 on 1=1;

-- duplicate exposed names
select * from t1 join t1 on c1 = 1;
select * from t1 join t1 on (c1);

-- join clause only allowed to contain column references from tables being
-- joined. DB2 doesn't allow references to correlated columns
select * from t1, t2 join t3 on t1.c1 = t2.c1;
-- should match db2's behavior by raising an error
select * from t2 b inner join t3 c on a.c1 = b.c1 and b.c1 = c.c1;
select * from t3 b where exists (select * from t1 a inner join t2 on b.c1 = t2.c1);
select * from t3 where exists (select * from t1 inner join t2 on t3.c1 = t2.c1);

-- positive tests

select a.c1 from t1 a join t2 b on a.c1 = b.c1;
select a.x from t1 a (x) join t2 b (x) on a.x = b.x;

-- ANSI "extension" - duplicate exposed names allowed when no column references
-- this may go away if we can figure out how to detect this error and
-- get bored enough to prioritize the fix
get cursor c as 'select 1 from t1 join t1 on 1=1';
next c;
close c;

-- parameters and join clause
prepare asdf as 'select * from t1 join t2 on ?=1 and t1.c1 = t2.c1';
execute asdf using 'values 1';
remove asdf;

prepare asdf as 'select * from t1 join t2 on t1.c1 = t2.c1 and t1.c1 = ?';
execute asdf using 'values 1';
remove asdf;

-- additional predicates outside of the join clause
select * from t1 join t2 on t1.c1 = t2.c1 where t1.c1 = 1;
select * from t1 join t2 on t1.c1 = 1 where t2.c1 = t1.c1;

-- subquery in join clause, not allowed in DB2 compatibility mode
select * from t1 a join t2 b 
on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1);
select * from t1 a join t2 b 
on a.c1 = b.c1 and a.c1 in (select c1 from t1 where a.c1 = t1.c1);

-- correlated columns
select * from t1 a
where exists (select * from t1 inner join t2 on a.c1 = t2.c1);

-- nested joins
select * from t1 join t2 on t1.c1 = t2.c1 inner join t3 on t1.c1 = t3.c1;

-- parens
select * from (t1 join t2 on t1.c1 = t2.c1) inner join t3 on t1.c1 = t3.c1;
select * from t1 join (t2 inner join t3 on t2.c1 = t3.c1) on t1.c1 = t2.c1;

-- [inner] joins
select * from t1 a left outer join t2 b on a.c1 = b.c1 inner join t3 c on b.c1 = c.c1;
select * from (t1 a left outer join t2 b on a.c1 = b.c1) inner join t3 c on b.c1 = c.c1;

select * from t1 a join t2 b on a.c1 = b.c1 inner join t3 c on c.c1 = a.c1 where c.c1 > 2 and a.c1 > 2;
select * from (t1 a join t2 b on a.c1 = b.c1) inner join t3 c on c.c1 = a.c1 where c.c1 > 2 and a.c1 > 2;
select * from t1 a join (t2 b inner join t3 c on c.c1 = b.c1) on a.c1 = b.c1 where c.c1 > 2 and b.c1 > 2;

-- test insert/update/delete
insert into insert_test
select * from t1 a join t2 b on a.c1 = b.c1 inner join t3 c on a.c1 <> c.c1;
select * from insert_test;

update insert_test
set c1 = (select 9 from t1 a join t1 b on a.c1 = b.c1 where a.c1 = 1)
where c1 = 1;
select * from insert_test;

delete from insert_test
where c1 = (select 9 from t1 a join t1 b on a.c1 = b.c1 where a.c1 = 1);
select * from insert_test;

-- multicolumn join
select * from insert_test a join insert_test b 
on a.c1 = b.c1 and a.c2 = b.c2 and a.c3 = b.c3;

-- continue with insert tests
delete from insert_test;

insert into insert_test
select * from (select * from t1 a join t2 b on a.c1 = b.c1 inner join t3 c on a.c1 <> c.c1) d (c1, c2, c3);
select * from insert_test;
delete from insert_test;

-- reset autocomiit
autocommit on;

-- drop the tables
drop table t1;
drop table t2;
drop table t3;
drop table insert_test;
