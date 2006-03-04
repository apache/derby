-- Test predicate pushdown into expressions in a FROM list.  As of
-- DERBY-805 this test only looks at pushing predicates into UNION
-- operators, but this test will likely grow as additional predicate
-- pushdown functionality is added to Derby. Note that "noTimeout"
-- is set to true for this test because we print out a lot of query
-- plans and we don't want the plans to differ from one machine
-- to another (which can happen if some machines are faster than
-- others when noTimeout is false).

-- Create the tables/views for DERBY-805 testing.  For DERBY-805
-- we can tell if a predicate has been "pushed" by looking at
-- the query plan information for the tables in question: if the
-- table has an index on a column that is used as part of the
-- pushed predicate, then the optimizer will (for these tests)
-- do an Index scan instead of a Table scan.  If the table does
-- not have such an index then the predicate will show up as a
-- "qualifier" for a Table scan.  In all of these tests T3 and T4
-- have appropriate indexes, so if we push a predicate to either
-- of those tables we should see index scans.  Neither T1 nor T2
-- has indexes, so if we push a predicate to either of those tables
-- we should see a qualifier in the table scan information.

CREATE TABLE "APP"."T1" ("I" INTEGER, "J" INTEGER);
insert into t1 values (1, 2), (2, 4), (3, 6), (4, 8), (5, 10);

CREATE TABLE "APP"."T2" ("I" INTEGER, "J" INTEGER);
insert into t2 values (1, 2), (2, -4), (3, 6), (4, -8), (5, 10);

CREATE TABLE "APP"."T3" ("A" INTEGER, "B" INTEGER);
CREATE INDEX "APP"."T3_IX1" ON "APP"."T3" ("A");
CREATE INDEX "APP"."T3_IX2" ON "APP"."T3" ("B");
insert into T3 values (1,1), (2,2), (3,3), (4,4), (6, 24),
  (7, 28), (8, 32), (9, 36), (10, 40); 

insert into t3 (a) values 11, 12, 13, 14, 15, 16, 17, 18, 19, 20;
insert into t3 (a) values 21, 22, 23, 24, 25, 26, 27, 28, 29, 30;
insert into t3 (a) values 31, 32, 33, 34, 35, 36, 37, 38, 39, 40;
insert into t3 (a) values 41, 42, 43, 44, 45, 46, 47, 48, 49, 50;
insert into t3 (a) values 51, 52, 53, 54, 55, 56, 57, 58, 59, 60;
insert into t3 (a) values 61, 62, 63, 64, 65, 66, 67, 68, 69, 70;
insert into t3 (a) values 71, 72, 73, 74, 75, 76, 77, 78, 79, 80;
insert into t3 (a) values 81, 82, 83, 84, 85, 86, 87, 88, 89, 90;
insert into t3 (a) values 91, 92, 93, 94, 95, 96, 97, 98, 99, 100;
update t3 set b = 2 * a where a > 10;

CREATE TABLE "APP"."T4" ("A" INTEGER, "B" INTEGER);
CREATE INDEX "APP"."T4_IX1" ON "APP"."T4" ("A");
CREATE INDEX "APP"."T4_IX2" ON "APP"."T4" ("B");
insert into t4 values (3, 12), (4, 16);

insert into t4 (a) values 11, 12, 13, 14, 15, 16, 17, 18, 19, 20;
insert into t4 (a) values 21, 22, 23, 24, 25, 26, 27, 28, 29, 30;
insert into t4 (a) values 31, 32, 33, 34, 35, 36, 37, 38, 39, 40;
insert into t4 (a) values 41, 42, 43, 44, 45, 46, 47, 48, 49, 50;
insert into t4 (a) values 51, 52, 53, 54, 55, 56, 57, 58, 59, 60;
insert into t4 (a) values 61, 62, 63, 64, 65, 66, 67, 68, 69, 70;
insert into t4 (a) values 71, 72, 73, 74, 75, 76, 77, 78, 79, 80;
insert into t4 (a) values 81, 82, 83, 84, 85, 86, 87, 88, 89, 90;
insert into t4 (a) values 91, 92, 93, 94, 95, 96, 97, 98, 99, 100;
update t4 set b = 2 * a where a > 10;

CREATE TABLE "APP"."T5" ("I" INTEGER, "J" INTEGER);
insert into t5 values (5, 10);

CREATE TABLE "APP"."T6" ("P" INTEGER, "Q" INTEGER);
insert into t5 values (2, 4), (4, 8);

CREATE TABLE "APP"."XX1" ("II" INTEGER NOT NULL, "JJ" CHAR(10),
  "MM" INTEGER, "OO" DOUBLE, "KK" BIGINT);
CREATE TABLE "APP"."YY1" ("II" INTEGER NOT NULL, "JJ" CHAR(10),
  "AA" INTEGER, "OO" DOUBLE, "KK" BIGINT);

ALTER TABLE "APP"."YY1" ADD CONSTRAINT "PK_YY1" PRIMARY KEY ("II");
ALTER TABLE "APP"."XX1" ADD CONSTRAINT "PK_XX1" PRIMARY KEY ("II");

create view V1 as select i, j from T1 union select i,j from T2;
create view V2 as select a,b from T3 union select a,b from T4;

create view xxunion as select all ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1;

create view yyunion as select all ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1;

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 20000;

-- Predicate push-down should occur for next two queries.  Thus we
-- we should see Index scans for T3 and T4--and this should be the
-- case regardless of the order of the FROM list.
select * from V1, V2 where V1.j = V2.b;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from V2, V1 where V1.j = V2.b;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Changes for DERBY-805 don't affect non-join predicates (ex. "IN" or one-
-- sided predicates), but make sure things still behave--i.e. these queries
-- should still compile and execute without error, and there should be a
-- qualifier on T1 for the scalar predicate.
select count(*) from V1, V2 where V1.i in (2,4);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select count(*) from V1, V2 where V1.j > 0;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Combination of join predicate and non-join predicate: the join predicate
-- should be pushed to V2 (T3 and T4), the non-join predicate should operate
-- as usual.
select * from V1, V2 where V1.j = V2.b and V1.i in (2,4);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Make sure predicates are pushed even if the subquery is explicit (as
-- opposed to a view). Should see index scans on T3 and T4.
select * from
  (select * from t1 union select * from t2) x1,
  (select * from t3 union select * from t4) x2
where x1.i = x2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- In this case optimizer will consider pushing predicate to X1 but will
-- choose not to because it's cheaper to do a hash join between X1 and T3.
-- So should see regular table scans on T1 and T2 with hash scan on T3.
select * from
  (select * from t1 union select * from t2) x1,
  t3
where x1.i = t3.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- UNION ALL should behave just like normal UNION.  I.e. predicates should
-- still be pushed to T3 and T4.
select * from
  (select * from t1 union all select * from t2) x1,
  (select * from t3 union select * from t4) x2
where x1.i = x2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from
  (select * from t1 union all select * from t2) x1,
  (select * from t3 union all select * from t4) x2
where x1.i = x2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Predicate with both sides referencing same UNION isn't a join predicate, so
-- no pushing should happen.  So should see regular table scans on all tables.
select * from v1, v2 where V1.i = V1.j;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Pushing predicates should still work even if user specifies explicit
-- column names.  In 1st and 2nd queries we push to X2 (T3 and T4); in 3rd
-- query we push to X1 (T1 and T3).
select * from
  (select * from t1 union select * from t2) x1 (c, d),
  (select * from t3 union select * from t4) x2 (e, f)
where x1.c = x2.e;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from
  (select * from t1 union select * from t2) x1 (a, b),
  (select * from t3 union select * from t4) x2 (i, j)
where x1.a = x2.i;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select count(*) from
  (select * from t1 union select * from t3) x1 (c, d),
  (select * from t2 union select * from t4) x2 (e, f)
where x1.c = x2.e; 
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- If we have nested unions, the predicate should get pushed all the way down
-- to the base table(s) for every level of nesting.  Should see index scans for
-- T3 and for _both_ instances of T4.
select * from
  (select * from t1 union
    select * from t2 union
      select * from t1 union
        select * from t2
  ) x1,
  (select * from t3 union
    select * from t4 union
      select * from t4
  ) x2
where x1.i = x2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Nested unions with non-join predicates should work as usual (no change
-- with DERBY-805).  So should see scalar qualifiers on scans for all
-- instances of T1 and T2.
select * from
  (select * from t1
    union select * from t2
      union select * from t1
        union select * from t2
  ) x1
where x1.i > 0;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- In this case there are no qualifiers, but the restriction is enforced
-- at the ProjectRestrictNode level.  That hasn't changed with DERBY-805.
select count(*) from
  (select * from t1
    union select * from t2
      union select * from t3
        union select * from t4
  ) x1 (i, b)
where x1.i > 0;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Predicate pushdown should work with explicit use of "inner join" just like
-- it does for implicit join.  So should see index scans on T3 and T4.
select * from
  (select * from t1 union select * from t2) x1 inner join
  (select * from t3 union select * from t4) x2
on x1.j = x2.b;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Can't push predicates into VALUES clauses.  Predicate should end up
-- at V2 (T3 and T4).
select * from (
  select i,j from t2 union
    values (1,1),(2,2),(3,3),(4,4)
      union select i,j from t1
  ) x0 (i,j),
  v2
where x0.i = v2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Can't push predicates into VALUES clauses.  Optimizer might consider pushing
-- but shouldn't do it; in the end we'll do a hash join between X1 and T2.
select * from
  t2,
  (select * from t1 union values (3,3), (4,4), (5,5), (6,6)) X1 (a,b)
where X1.a = t2.i;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Can't push predicates into VALUES clause.  We'll try to push it to X0, but
-- it will only make it to T4; it won't make it to T3 because the "other side"
-- of the union with T3 is a VALUES clause.  So we'll see an index scan on T4
-- and table scan on T3--but the predicate should still be applied to T3 at a
-- higher level (through a ProjectRestrictNode), so we shouldn't get any extra
-- rows.
select * from
  (select i,j from t2 union
    values (1,1),(2,2),(3,3),(4,4) union
      select i,j from t1
  ) x0 (i,j),
  (select a, b from t3 union
    values (4, 5), (5, 6), (6, 7) union
      select a, b from t4
  ) x1 (a,b)
where x0.i = x1.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Make sure optimizer is still considering predicates for other, non-UNION
-- nodes.  Here we should use the predicate to do a hash join between X0 and
-- T5 (i.e. we will not push it down to X0 because a) there are VALUES clauses
-- to which we can't push, and b) it's cheaper to do the hash join).
select * from
  t5,
  (values (2,2), (4,4) union
    values (1,1),(2,2),(3,3),(4,4) union
      select i,j from t1
  ) x0 (i,j)
where x0.i = t5.i;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- When we have very deeply nested union queries, make sure predicate push-
-- down logic still works (esp. the scoping logic).  These queries won't return
-- any results, but the predicate should get pushed to EVERY instance of the
-- base table all the way down.  We're just checking to make sure these compile
-- and execute without error.  The query plan for these two queries alone would
-- be several thousand lines so we don't print them out.  We have other
-- (smaller) tests to check that predicates are correctly pushed through nested
-- unions.
select distinct xx0.kk, xx0.ii, xx0.jj from
  xxunion xx0,
  yyunion yy0
where xx0.mm = yy0.ii;
prepare sel1 as
  'select distinct xx0.kk, xx0.ii, xx0.jj from
    xxunion xx0,
    yyunion yy0
  where xx0.mm = yy0.ii and yy0.aa in (?) for fetch only';
execute sel1 using 'values (1)';

-- Predicate push-down should only affect the UNIONs referenced; other UNIONs
-- shouldn't interfere or be affected.  Should see table scans for T1 and T2;
-- then an index scan for the first instance of T3 and a table scan for second
-- instance of T3; likewise for two instances of T4.
select count(*) from
  (select * from t1 union select * from t2) x1,
  (select * from t3 union select * from t4) x2,
  (select * from t4 union select * from t3) x3
where x1.i = x3.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Here we should see index scans for both instances of T3 and for both
-- instances of T4.
select count(*) from
  (select * from t1 union select * from t2) x1,
  (select * from t3 union select * from t4) x2,
  (select * from t4 union select * from t3) x3
where x1.i = x3.a and x3.b = x2.b;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Predicates pushed from outer queries shouldn't interfere with inner
-- predicates for subqueries.  Mostly checking for correct results here.
select * from
  (select i, b j from t1, t4 where i = j union select * from t2) x1,
  t3
where x1.j = t3.a;

-- Inner predicate should be handled as normal, outer predicate should
-- still be pushed to V2 (T3 and T4).
select * from
  (select i, b j from t1, t4 where i = j union select * from t2) x1,
  v2
where x1.j = v2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Outer predicate should still be pushed to V2 (T3 and T4); inner predicate
-- should be used for hash join between T1 and T3.
select * from
  (select i, j from t1, t3 where i = a union select * from t2) x1,
  v2
where x1.i = v2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Inner predicates treated as restrictions, outer predicate should
-- get pushed to X2 (T2 and T1).  So scans for last instances of T1
-- and T2 should have qualifiers that came from the predicate.
select * from
  (select i, b j from t1, t4 where i = j union select * from t2) x1,
  (select i, b j from t2, t3 where i = j union select * from t1) x2
where x1.j = x2.i;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Following queries deal with nested subqueries, which deserve extra
-- testing because "best paths" for outer queries might not agree with
-- "best paths" for inner queries, so we need to make sure the correct
-- paths (based on predicates that are or are not pushed) are ultimately
-- generated.

-- Predicate should get pushed to V2 (T3 and T4).
select count(*) from
    (select i,a,j,b from
        V1,
        V2
     where V1.j = V2.b
    ) X3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Multiple subqueries but NO UNIONs.  All predicates are used for hash joins
-- at their current level (no pushing).  So should see hash scan on T1, T6,
-- and T2.
select t2.i,p from
  (select distinct i,p from
    (select distinct i,a from t1, t3 where t1.j = t3.b) X1,
    t6
  where X1.a = t6.p) X2,
  t2
where t2.i = X2.i;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Multiple, non-flattenable subqueries, but NO UNIONs.  Shouldn't push
-- anything.
select x1.j, x2.b from
  (select distinct i,j from t1) x1,
  (select distinct a,b from t3) x2
where x1.i = x2.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

select x1.j, x2.b from
  (select distinct i,j from t1) x1,
  (select distinct a,b from t3) x2,
  (select distinct i,j from t2) x3,
  (select distinct a,b from t4) x4
where x1.i = x2.a and x3.i = x4.a;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Multiple subqueries that are UNIONs.  Outer-most predicate
-- X0.b = X2.j can be pushed to union X0 but NOT to subquery X2.
-- Inner predicate T6.p = X1.i is eligible for being pushed into
-- union X1, but optimizer won't choose to do so (because it's
-- cheaper to do a hash between X1 and T6).  So should see
-- predicate pushed to T3 and T4, but not to T1 nor T2.
select X0.a, X2.i from
   (select a,b from t4 union select a,b from t3) X0,
   (select i,j from
      (select i,j from t1 union select i,j from t2) X1,
       T6
    where T6.p = X1.i) X2
where X0.b = X2.j
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Same as above but without the inner predicate (so no
-- hash on T6).
select X0.a, X2.i from
   (select a,b from t4 union select a,b from t3) X0,
   (select i,j from
      (select i,j from t1 union select i,j from t2) X1,
       T6
   ) X2
where X0.b = X2.j
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Same as above, but without the outer predicate.  Should see
-- table scan on T3 and T4 (because nothing is pushed).
select X0.a, X2.i from
   (select a,b from t4 union select a,b from t3) X0,
   (select i,j from
      (select i,j from t1 union select i,j from t2) X1,
       T6
    where T6.p = X1.i) X2
;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 
-- Additional tests with VALUES clauses.  Mostly just checking to make sure
-- these queries compile and execute, and to ensure that all predicates are
-- enforced even if they can't be pushed all the way down into a UNION.  So
-- we shouldn't get back any extra rows here.  NOTE: Row order is not important
-- in these queries, just so long as the correct rows are returned.

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);

select * from
  (select * from t1 union select * from t2) x1,
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a;

select * from
  (select * from t1 union (values (1, -1), (2, -2), (5, -5))) x1 (i, j),
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a;

select * from
  (select * from t1 union all (values (1, -1), (2, -2), (5, -5))) x1 (i, j),
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a;

select * from
  (select * from t1 union (values (1, -1), (2, -2), (5, -5))) x1 (i, j),
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a and x2.b = x1.j;

select * from
  (values (2, -4), (3, -6), (4, -8) union
    values (1, -1), (2, -2), (5, -5)
  ) x1 (i, j),
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a; 

select * from
  (values (2, -4), (3, -6), (4, -8) union
    values (1, -1), (2, -2), (5, -5)
  ) x1 (i, j),
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a and x2.b = x1.j;

select * from
  (values (1, -1), (2, -2), (5, -5) union select * from t1) x1 (i,j),
  (values (2, 4), (3, 6), (4, 8)) x2 (a, b)
where x1.i = x2.a;

-- Clean up DERBY-805 objects.

drop view v1;
drop view v2;
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;

drop view xxunion;
drop view yyunion;
drop table xx1;
drop table yy1;

