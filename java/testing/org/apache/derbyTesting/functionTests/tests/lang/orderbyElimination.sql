-- test elimination of sort for order by
set isolation to rr;

-- test combining of sorts for distinct and order by

-- create some tables
create table t1(c1 int, c2 int, c3 int, c4 int);

insert into t1 values (1, 2, 3, 4);
insert into t1 values (2, 3, 4, 5);
insert into t1 values (-1, -2, -3, -4);
insert into t1 values (-2, -3, -4, -5);
insert into t1 values (1, 2, 4, 3);
insert into t1 values (1, 3, 2, 4);
insert into t1 values (1, 3, 4, 2);
insert into t1 values (1, 4, 2, 3);
insert into t1 values (1, 4, 3, 2);
insert into t1 values (2, 1, 4, 3);

maximumdisplaywidth 7000;
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);

-- no index on t1
-- full match
select distinct c1, c2, c3, c4 from t1 order by 1, 2, 3, 4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c1, c2, c3, c4 from t1 order by c1, c2, c3, c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- in order prefix
select distinct c3, c4 from t1 order by 1, 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c4 from t1 order by c3, c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- no prefix
select distinct c3, c4 from t1 order by 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c4 from t1 order by c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- expression
select distinct c3, 1 from t1 order by 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, 1 from t1 order by 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- verify that a sort is still done when a unique index 
-- exists
create unique index i1 on t1(c1, c2, c3, c4);
select distinct c4, c3 from t1 where c1 = 1 and c2 = 2 order by c4, c3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select distinct c3, c4 from t1 where c1 = 1 and c2 = 2 order by c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- order by and union
select c1 from t1 union select c2 from t1 order by 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select c1 from t1 union select c2 as c1 from t1 order by c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- RESOLVE: next 2 will do 2 sorts (bug 58)
select c3, c4 from t1 union select c2, c1 from t1 order by 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select c3, c4 from t1 union select c2, c1 as c4 from t1 order by c4;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- test recognition of single row tables
-- even when scanning heap
create table u1(c1 int, c2 int);
create table u2(c2 int, c3 int);
create table u3(c3 int, c4 int);
insert into u1 values (1, 1), (2, 2);
insert into u2 values (1, 1), (2, 2);
insert into u3 values (1, 1), (2, 2);
create unique index u1_i1 on u1(c1);
create unique index u2_i1 on u2(c2);
create unique index u3_i1 on u3(c3);

select * from
u1,
u2,
u3
where u1.c1 = 1 and u1.c1 = u2.c2
order by u3.c3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- clean up
drop table t1;
drop table u1;
drop table u2;
drop table u3;
