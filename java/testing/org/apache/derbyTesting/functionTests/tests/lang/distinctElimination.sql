-- test distinct elimination
set isolation to rr;

-- eliminate distincts based on a uniqueness condition

-- create tables
create table one(c1 int, c2 int, c3 int, c4 int, c5 int);
create unique index one_c1 on one(c1);
create table two(c1 int, c2 int, c3 int, c4 int, c5 int);
create unique index two_c1c3 on two(c1, c3);
create table three(c1 int, c2 int, c3 int, c4 int, c5 int);
create unique index three_c1 on three(c1);
create table four(c1 int, c2 int, c3 int, c4 int, c5 int);
create unique index four_c1c3 on four(c1, c3);

insert into one values (1, 1, 1, 1, 1);
insert into one values (2, 1, 1, 1, 1);
insert into one values (3, 1, 1, 1, 1);
insert into one values (4, 1, 1, 1, 1);
insert into one values (5, 1, 1, 1, 1);
insert into one values (6, 1, 1, 1, 1);
insert into one values (7, 1, 1, 1, 1);
insert into one values (8, 1, 1, 1, 1);

insert into two values (1, 1, 1, 1, 1);
insert into two values (1, 1, 2, 1, 1);
insert into two values (1, 1, 3, 1, 1);
insert into two values (2, 1, 1, 1, 1);
insert into two values (2, 1, 2, 1, 1);
insert into two values (2, 1, 3, 1, 1);
insert into two values (3, 1, 1, 1, 1);
insert into two values (3, 1, 2, 1, 1);
insert into two values (3, 1, 3, 1, 1);

insert into three values (1, 1, 1, 1, 1);
insert into three values (2, 1, 1, 1, 1);
insert into three values (3, 1, 1, 1, 1);
insert into three values (4, 1, 1, 1, 1);
insert into three values (5, 1, 1, 1, 1);
insert into three values (6, 1, 1, 1, 1);
insert into three values (7, 1, 1, 1, 1);
insert into three values (8, 1, 1, 1, 1);

insert into four values (1, 1, 1, 1, 1);
insert into four values (1, 1, 2, 1, 1);
insert into four values (1, 1, 3, 1, 1);
insert into four values (2, 1, 1, 1, 1);
insert into four values (2, 1, 2, 1, 1);
insert into four values (2, 1, 3, 1, 1);
insert into four values (3, 1, 1, 1, 1);
insert into four values (3, 1, 2, 1, 1);
insert into four values (3, 1, 3, 1, 1);

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

-- queries that cannot eliminate the distinct

-- no unique index
select distinct c2 from one;
-- Following runtime statistics output should have Distinct Scan in it
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- result ordering is not guaranteed, but order by clause will change how
-- distinct is executed.  So test by retrieving data into a temp table and
-- return results ordered after making sure the query was executed as expected.
create table temp_result (c2 int, c3 int); 

insert into temp_result
    select distinct c2, c3 from two;
-- Following runtime statistics output should have Distinct Scan in it
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select c2, c3 from temp_result order by c2, c3;
drop table temp_result;

-- Try same query, but with an order by at the end.  This will use the sort for
-- the "order by" to do the distinct and not do a "DISTINCT SCAN".
select distinct c2, c3 from two order by c2, c3;
-- Following runtime statistics output should not have Distinct Scan in it
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- more than one table in the select list
select distinct a.c1, b.c1 from one a, two b where a.c1 = b.c1 and b.c2 =1;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- cross product join
select distinct a.c1 from one a, two b;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- no single table will yield at most 1 row
select distinct a.c1, a.c3, a.c2 from two a, two b where a.c1 = b.c1;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct a.c1, a.c3, a.c2 from two a, two b where a.c1 = b.c1 and a.c2 = 1;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- both keys from unique index in where clause but joined to different tables
select distinct a.c1 from one a, two b, three c where a.c1 = b.c1 and c.c1 = b.c3
and a.c1 = 1;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- join between two tables using one columns of unique key
select distinct a.c1 from two a, four b where a.c1 = b.c1 and b.c3 = 1;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- join between two tables with no join predicate
select distinct a.c1, a.c3 from two a, one b;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- join between three tables with two tables joined uniquely 
select distinct a.c1 from one a, two b, three c where a.c1 = c.c1 and a.c1 = 1;
-- Following runtime statistics output should have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
--
-- queries that should eliminate the distinct

-- single table queries

-- unique columns in select list
select distinct c1 from one;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c1, c2 + c3 from one;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c1 from two;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- query returns single row
select distinct c2 from one where c1 = 3;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3 from one where c1 = 3;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- superset in select list
select distinct c2, c5, c1 from one;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c2, c3, c1 from two;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- multi table queries

-- 1 to 1 join, select list is superset
select distinct a.c1 from one a, one b where a.c1 = b.c1;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct a.c1, 3 from one a, one b where a.c1 = b.c1;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct a.c1, a.c3, a.c2 from two a, one b where a.c1 = b.c1;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct a.c1, a.c3, a.c2 from two a, two b where a.c1 = b.c1 and b.c3 = 1;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- join between two tables using both columns of unique key
select distinct a.c1 from two a, four b where a.c1 = b.c1 and a.c3 = b.c3 and b.c3 = 1;
-- Following runtime statistics output should not have Eliminate duplicates = true
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- clean up
drop table one;
drop table two;
drop table three;
drop table four;
