-- This test is an adaptation of the Wisconsin benchmark, as documented in
-- The Benchmark Handbook, Second Edition (edited by Jim Gray).  The structure
-- of the tables and the data in the tables are taken from there.
--
-- The original benchmark talks about clustered and non-clustered
-- indexes - as far as I can tell, this really means indexes where the
-- row ordering is or is not the same as in the base table.  It does
-- not mean special types of indexes.  I am putting in queries that
-- use both ordered and unordered indexes, despite the fact that
-- our optimizer does not currently distinguish these cases.
--
-- Another difference is that the original Wisconsin benchmark is a performance
-- test, while this test is only intended to ensure that the optimizer comes
-- up with the right query plan.  Therefore, this test doesn't include those
-- parts of the Wisconsin benchmark where the optimizer has no choice of
-- access path (e.g. single-table query with no indexes), nor does it include
-- the projection and update queries.
--
-- This test only does the first variation of each query, since that is
-- all that is documented in The Benchmark Handbook (it wouldn't be a true
-- academic reference text if everything were spelled out).
--
-- After the original Wisconsin queries are a bunch of queries that use the
-- Wisconsin schema but that were written at Cloudscape specifically for
-- testing our optimizer.

autocommit off;

set isolation serializable;
-- the method refers to a method in performance suite that takes a Connection.
--create function WISCInsert(rowcount int, tableName varchar(20)) returns int language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.lang.WiscVTI';
CREATE PROCEDURE WISCINSERT(rowcount int, tableName varchar(20)) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.WiscVTI.WISCInsertWOConnection';

create table TENKTUP1 (
		unique1 int not null,
		unique2 int not null,
		two int,
		four int,
		ten int,
		twenty int,
		onePercent int,
		tenPercent int,
		twentyPercent int,
		fiftyPercent int,
		unique3 int,
		evenOnePercent int,
		oddOnePercent int,
		stringu1 char(52) not null,
		stringu2 char(52) not null,
		string4 char(52)
	);

--insert 10000 rows into TENKTUP1 
call WISCINSERT( 10000, 'TENKTUP1'); 

create unique index TK1UNIQUE1 on TENKTUP1(unique1);
create unique index TK1UNIQUE2 on TENKTUP1(unique2);
create index TK1TWO on TENKTUP1(two);
create index TK1FOUR on TENKTUP1(four);
create index TK1TEN on TENKTUP1(ten);
create index TK1TWENTY on TENKTUP1(twenty);
create index TK1ONEPERCENT on TENKTUP1(onePercent);
create index TK1TWENTYPERCENT on TENKTUP1(twentyPercent);
create index TK1EVENONEPERCENT on TENKTUP1(evenOnePercent);
create index TK1ODDONEPERCENT on TENKTUP1(oddOnePercent);
create unique index TK1STRINGU1 on TENKTUP1(stringu1);
create unique index TK1STRINGU2 on TENKTUP1(stringu2);
create index TK1STRING4 on TENKTUP1(string4);

create table TENKTUP2 (
		unique1 int not null,
		unique2 int not null,
		two int,
		four int,
		ten int,
		twenty int,
		onePercent int,
		tenPercent int,
		twentyPercent int,
		fiftyPercent int,
		unique3 int,
		evenOnePercent int,
		oddOnePercent int,
		stringu1 char(52),
		stringu2 char(52),
		string4 char(52)
	);

-- insert 10000 rows into TENKTUP2
call WISCInsert( 10000, 'TENKTUP2'); 

create unique index TK2UNIQUE1 on TENKTUP2(unique1);
create unique index TK2UNIQUE2 on TENKTUP2(unique2);

create table ONEKTUP (
		unique1 int not null,
		unique2 int not null,
		two int,
		four int,
		ten int,
		twenty int,
		onePercent int,
		tenPercent int,
		twentyPercent int,
		fiftyPercent int,
		unique3 int,
		evenOnePercent int,
		oddOnePercent int,
		stringu1 char(52),
		stringu2 char(52),
		string4 char(52)
	);

-- insert 1000 rows into ONEKTUP
call WISCInsert( 1000, 'ONEKTUP'); 

create unique index ONEKUNIQUE1 on ONEKTUP(unique1);
create unique index ONEKUNIQUE2 on ONEKTUP(unique2);

create table BPRIME (
		unique1 int,
		unique2 int,
		two int,
		four int,
		ten int,
		twenty int,
		onePercent int,
		tenPercent int,
		twentyPercent int,
		fiftyPercent int,
		unique3 int,
		evenOnePercent int,
		oddOnePercent int,
		stringu1 char(52),
		stringu2 char(52),
		string4 char(52)
	);

insert into BPRIME
select * from TENKTUP2
where TENKTUP2.unique2 < 1000;

commit;

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 8000;

-- Wisconsin Query 3

get cursor c as
	'select * from TENKTUP1
	where unique2 between 0 and 99';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 4

get cursor c as
	'select * from TENKTUP1
	where unique2 between 792 and 1791';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 5
get cursor c as
	'select * from TENKTUP1
	where unique1 between 0 and 99';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 6
get cursor c as
	'select * from TENKTUP1
	where unique1 between 792 and 1791';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 7
get cursor c as
	'select *
	from TENKTUP1
	where unique2 = 2001';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 12
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where (TENKTUP1.unique2 = TENKTUP2.unique2)
	and (TENKTUP2.unique2 < 1000)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 13
get cursor c as
	'select * from TENKTUP1, BPRIME
	where (TENKTUP1.unique2 = BPRIME.UNIQUE2)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin query 14
-- NOTE: This could benefit from transitive closure, which our optimizer
-- doesn't do (yet).
-- Note that after fix for optimizer bug 5868, in runtime statistics info, we will see 2 qualifiers for table TENKTUP2. This is because as fix for
-- bug 5868, while getting rid of a redundant predicate which is a start and/or stop AND a qualifier, we mark the predicate we are going to keep 
-- as start and/or stop AND as a qualifier. Prior to fix of bug 5868, we were disregarding the qualifier flag on the redundant predicate if it 
-- was a start and/or stop predicate too.
get cursor c as
	'select * from ONEKTUP, TENKTUP1, TENKTUP2
	where (ONEKTUP.unique2 = TENKTUP1.unique2)
	and (TENKTUP1.unique2 = TENKTUP2.unique2)
	and (TENKTUP1.unique2 < 1000)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 15
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where (TENKTUP1.unique1 = TENKTUP2.unique1)
	and (TENKTUP1.unique1 < 1000)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 16
get cursor c as
	'select * from TENKTUP1, BPRIME
	where (TENKTUP1.unique1 = BPRIME.unique1)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Wisconsin Query 17

-- NOTE: This could benefit from transitive closure, which our optimizer
-- doesn't do (yet).
-- Note that after fix for optimizer bug 5868, in runtime statistics info, we will see 2 qualifiers for table TENKTUP2. This is because as fix for
-- bug 5868, while getting rid of a redundant predicate which is a start and/or stop AND a qualifier, we mark the predicate we are going to keep 
-- as start and/or stop AND as a qualifier. Prior to fix of bug 5868, we were disregarding the qualifier flag on the redundant predicate if it 
-- was a start and/or stop predicate too.

get cursor c as
	'select * from ONEKTUP, TENKTUP1, TENKTUP2
	where (ONEKTUP.unique1 = TENKTUP1.unique1)
	and (TENKTUP1.unique1 = TENKTUP2.unique1)
	and (TENKTUP1.unique1 < 1000)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- This is the end of the Wisconsin queries.  Now do some queries that are
-- not part of the original Wisconsin benchmark, using the Wisconsin schema.

-- Single-table queries using index on column 'two'

-- 50% selectivity index that doesn't cover query - should do index scan
get cursor c as
	'select * from TENKTUP1
	where two = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 50% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where two = 3';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 100% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where two >= 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where two > 1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 50% selectivity index that covers query - should do index scan
get cursor c as
	'select two from TENKTUP1
	where two = 1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'four'

-- 25% selectivity index that doesn't cover query - should do index scan
get cursor c as
	'select * from TENKTUP1
	where four = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where four = 4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 75% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where four >= 1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where four > 3';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% selectivity index that covers query - should do index scan
get cursor c as
	'select four from TENKTUP1
	where four = 2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'twentyPercent'

-- 20% selectivity index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where twentyPercent = 2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 20% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where twentyPercent = 5';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 60% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where twentyPercent > 1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where twentyPercent > 4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 20% selectivity index that covers query - should do index scan
get cursor c as
	'select twentyPercent from TENKTUP1
	where twentyPercent = 3';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'ten'

-- 10% selectivity index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where ten = 5';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where ten = 10';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where ten <= 4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 60% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where ten <= 5';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where ten > 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10% selectivity index that covers query - should do index scan
get cursor c as
	'select ten from TENKTUP1
	where ten = 7';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'twenty'

-- 5% selectivity index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where twenty = 17';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where twenty = 20';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where twenty <= 9';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 55% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where twenty <= 10';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where twenty < 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5% selectivity index that covers query - should do index scan
get cursor c as
	'select twenty from TENKTUP1
	where twenty = 19';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'onePercent'

-- 1% selectivity index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where onePercent = 63';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where onePercent = 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where onePercent > 49';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 60% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where onePercent > 40';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where onePercent > 101';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% selectivity index that covers query - should do index scan
get cursor c as
	'select onePercent from TENKTUP1
	where onePercent = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'evenOnePercent'

-- 1% selectivity index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where evenOnePercent = 64';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where evenOnePercent = 200';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where evenOnePercent > 99';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 60% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where evenOnePercent > 80';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where evenOnePercent > 198';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% selectivity index that covers query - should do index scan
get cursor c as
	'select evenOnePercent from TENKTUP1
	where evenOnePercent = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'oddOnePercent'

-- 1% selectivity index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where oddOnePercent = 63';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where oddOnePercent = 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 40% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where oddOnePercent > 120';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 60% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where oddOnePercent > 80';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where oddOnePercent > 199';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% selectivity index that covers query - should do index scan
get cursor c as
	'select oddOnePercent from TENKTUP1
	where oddOnePercent = 1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'stringu1'

-- unique index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where stringu1 = ''AAAAJKLxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- unique index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where stringu1 = ''AAAAZZZxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where stringu1 > ''AAAAHKHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 51% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where stringu1 > ''AAAAHOCxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where stringu1 > ''AAAAOUPxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- unique index that covers query - should do index scan
get cursor c as
	'select stringu1 from TENKTUP1
	where stringu1 = ''AAAAAABxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'stringu2'

-- unique index that doesn't cover query - should use index
get cursor c as
	'select * from TENKTUP1
	where stringu2 = ''AAAAJKLxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- unique index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where stringu2 = ''AAAAZZZxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where stringu2 > ''AAAAHKHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 51% of rows - should do table scan
get cursor c as
	'select * from TENKTUP1
	where stringu2 > ''AAAAHOCxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where stringu2 > ''AAAAOUPxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- unique index that covers query - should do index scan
get cursor c as
	'select stringu2 from TENKTUP1
	where stringu2 = ''AAAAAABxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Single-table queries using index on column 'string4'

-- 25% selectivity index that doesn't cover query - should do index scan
get cursor c as
	'select * from TENKTUP1
	where string4 = ''AAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% selectivity index with 0 matching rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where string4 = ''EEEExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 50% of rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where string4 > ''HHHHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- matches 0 rows - should do index scan
get cursor c as
	'select * from TENKTUP1
	where string4 > ''VVVVxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% selectivity index that covers query - should do index scan
get cursor c as
	'select string4 from TENKTUP1
	where string4 = ''OOOOxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx''';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Now test equijoins with different selectivities and different numbers
-- of outer rows.  The approach taken is that different join columns are
-- used, and that TENKTUP1 has indexes on all the joining columns, while
-- TENKTUP2 does not.  We use the unique1 column of TENKTUP2 to select
-- different numbers of rows.  The two tables will always appear in the
-- FROM clause with TENKTUP1 first, and TENKTUP2 second - it is up to
-- the optimizer to figure out which should come first in the join order.

-- Joins on unique1

-- Join on unique1, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on two

-- Join on two, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on four

-- Join on four, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on ten

-- Join on ten, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on twenty

-- Join on twenty, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on onePercent

-- Join on onePercent, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on twentyPercent

-- Join on twentyPercent, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on stringu1

-- Join on stringu1, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on string4

-- Join on string4, all rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 60% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 25% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 10% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 5% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 1% of rows in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 1 row in TENKTUP2
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Now do the same equijoin tests, but select only one column from TENKTUP1.
-- This way, it can choose hash join where appropriate (it avoids it where
-- it thinks the hash table will take too much memory).

-- Joins on unique1

-- Join on unique1, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on unique1, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.unique1 = TENKTUP2.unique1
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on two

-- Join on two, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on two, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on four

-- Join on four, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on four, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.four = TENKTUP2.four
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on ten

-- Join on ten, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on ten, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.ten = TENKTUP2.ten
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on twenty

-- Join on twenty, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twenty, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twenty = TENKTUP2.twenty
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on onePercent

-- Join on onePercent, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on onePercent, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on twentyPercent

-- Join on twentyPercent, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on twentyPercent, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.twentyPercent = TENKTUP2.twentyPercent
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on stringu1

-- Join on stringu1, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on stringu1, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.stringu1 = TENKTUP2.stringu1
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joins on string4

-- Join on string4, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 6000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 2500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 1000';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 500';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 < 100';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Join on string4, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.string4 = TENKTUP2.string4
	and TENKTUP2.unique1 = 0';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Test the effect of ORDER BY on access path.  The optimizer takes
-- the cost of sorting into account, and may choose an access path
-- in the same order as the ORDER BY, especially if the sort is
-- expensive.
--
-- First try single-table queries.

-- No where clause, try ordering on different indexed columns

get cursor c as
	'select * from TENKTUP1 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by four';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by ten';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by twenty';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by onePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by twentyPercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by evenOnePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by oddOnePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by stringu1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by stringu2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1 order by string4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Now try the same thing with covering indexes

get cursor c as
	'select unique1 from TENKTUP1 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select unique2 from TENKTUP1 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select two from TENKTUP1 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select four from TENKTUP1 order by four';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select ten from TENKTUP1 order by ten';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select twenty from TENKTUP1 order by twenty';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select onePercent from TENKTUP1 order by onePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select twentyPercent from TENKTUP1 order by twentyPercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select evenOnePercent from TENKTUP1 order by evenOnePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select oddOnePercent from TENKTUP1 order by oddOnePercent';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select stringu1 from TENKTUP1 order by stringu1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select stringu2 from TENKTUP1 order by stringu2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select string4 from TENKTUP1 order by string4';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Where clause on same column as order by, with different selectivities.

-- 60%
get cursor c as
	'select * from TENKTUP1 where unique1 < 6000 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25%
get cursor c as
	'select * from TENKTUP1 where unique1 < 2500 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10%
get cursor c as
	'select * from TENKTUP1 where unique1 < 1000 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5%
get cursor c as
	'select * from TENKTUP1 where unique1 < 500 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1%
get cursor c as
	'select * from TENKTUP1 where unique1 < 100 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- one row
get cursor c as
	'select * from TENKTUP1 where unique1 = 0 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Where clause and order by on different columns - non-covering

-- 60%
get cursor c as
	'select * from TENKTUP1 where unique1 < 6000 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25%
get cursor c as
	'select * from TENKTUP1 where unique1 < 2500 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10%
get cursor c as
	'select * from TENKTUP1 where unique1 < 1000 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5%
get cursor c as
	'select * from TENKTUP1 where unique1 < 500 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1%
get cursor c as
	'select * from TENKTUP1 where unique1 < 100 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- one row
get cursor c as
	'select * from TENKTUP1 where unique1 = 0 order by unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Where clause and order by on different columns - covering

create index TK1UNIQUE1TWO on TENKTUP1(unique1, two);
create index TK1TWOUNIQUE1 on TENKTUP1(two, unique1);

-- 60%
get cursor c as
	'select two from TENKTUP1 where unique1 < 6000 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25%
get cursor c as
	'select two from TENKTUP1 where unique1 < 2500 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10%
get cursor c as
	'select two from TENKTUP1 where unique1 < 1000 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5%
get cursor c as
	'select two from TENKTUP1 where unique1 < 500 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1%
get cursor c as
	'select two from TENKTUP1 where unique1 < 100 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- one row
-- RESOLVE: For some reason, this avoids the sort by choosing the
-- index on column two, rather than by treating it as a one-row table.
-- It does not do this if you run the query by itself, outside of this
-- test.
get cursor c as
	'select two from TENKTUP1 where unique1 = 0 order by two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

drop index TK1UNIQUE1TWO;

-- Constant search condition on first column of index, order on second
-- column.
get cursor c as
	'select two, unique1 from TENKTUP1 where two = 0 order by unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Constant search condition on first column of index, order on first and second
-- columns.
get cursor c as
	'select two, unique1 from TENKTUP1 where two = 0 order by two, unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

drop index TK1TWOUNIQUE1;

commit;

-- Now test sort avoidance with joins.
--
-- First try two-way joins where the order by column is in only one table

-- Order by column same as joining column
--
-- 100% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 60% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 < 6000
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 < 2500
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 < 2500
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 < 1000
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 < 500
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 < 100
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- one row from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP2.unique1 = 0
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Order by column different from joining column
--
-- 100% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 60% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 < 6000
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 < 2500
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 25% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 < 2500
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 10% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 < 1000
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 5% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 < 500
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- 1% of rows from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 < 100
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- one row from joining table
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and TENKTUP2.unique1 = 0
	 order by TENKTUP1.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Sort avoidance with joins and order by on columns in different tables
--
-- order on joining columns
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- order on joining columns with qualifications on non-joining columns
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 6000
	 and TENKTUP2.unique2 < 6000
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 2500
	 and TENKTUP2.unique2 < 2500
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 1000
	 and TENKTUP2.unique2 < 1000
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 500
	 and TENKTUP2.unique2 < 500
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 100
	 and TENKTUP2.unique2 < 100
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 = 0
	 and TENKTUP2.unique2 = 0
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 6000
	 and TENKTUP2.unique2 = 0
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 2500
	 and TENKTUP2.unique2 < 100
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 1000
	 and TENKTUP2.unique2 < 500
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- order on non-joining columns
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- order on non-joining columns with qualifications on non-joining columns
get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 6000
	 and TENKTUP2.unique2 < 6000
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 2500
	 and TENKTUP2.unique2 < 2500
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 1000
	 and TENKTUP2.unique2 < 1000
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 500
	 and TENKTUP2.unique2 < 500
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 100
	 and TENKTUP2.unique2 < 100
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 = 0
	 and TENKTUP2.unique2 = 0
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 6000
	 and TENKTUP2.unique2 = 0
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 2500
	 and TENKTUP2.unique2 < 100
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and TENKTUP1.unique2 < 1000
	 and TENKTUP2.unique2 < 500
	 order by TENKTUP1.unique2, TENKTUP2.unique2';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Three-way join, order on columns from only two tables
get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and ONEKTUP.unique1 = TENKTUP1.unique1
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and ONEKTUP.unique1 = TENKTUP1.unique1
	 and TENKTUP1.unique1 < 6000
	 and TENKTUP2.unique1 < 6000
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and ONEKTUP.unique1 = TENKTUP1.unique1
	 and TENKTUP1.unique1 = 0
	 and TENKTUP2.unique1 = 0
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and ONEKTUP.unique1 = TENKTUP1.unique1
	 and TENKTUP1.unique2 < 6000
	 and TENKTUP2.unique2 < 6000
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique1 = TENKTUP2.unique1
	 and ONEKTUP.unique1 = TENKTUP1.unique1
	 and TENKTUP1.unique2 = 0
	 and TENKTUP2.unique2 = 0
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Joining columns different from ordering columns
get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and ONEKTUP.unique2 = TENKTUP1.unique2
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and ONEKTUP.unique2 = TENKTUP1.unique2
	 and TENKTUP1.unique2 < 6000
	 and TENKTUP2.unique2 < 6000
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, TENKTUP2, ONEKTUP
	 where TENKTUP1.unique2 = TENKTUP2.unique2
	 and ONEKTUP.unique2 = TENKTUP1.unique2
	 and TENKTUP1.unique2 = 0
	 and TENKTUP2.unique2 = 0
	 order by TENKTUP1.unique1, TENKTUP2.unique1';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Values clause is a single-row result set, so should not cause optimizer
-- to require sort.

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.unique1 = t.x
	 order by TENKTUP1.unique1, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Try with a join on unique column and order on non-unique column
get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.unique1 = t.x
	 order by TENKTUP1.two, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.two = t.x
	 order by TENKTUP1.two, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.four = t.x
	 order by TENKTUP1.four, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.ten = t.x
	 order by TENKTUP1.ten, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.twenty = t.x
	 order by TENKTUP1.twenty, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.onePercent = t.x
	 order by TENKTUP1.onePercent, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.tenPercent = t.x
	 order by TENKTUP1.tenPercent, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.twentyPercent = t.x
	 order by TENKTUP1.twentyPercent, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

get cursor c as
	'select * from TENKTUP1, (values 1) as t(x)
	 where TENKTUP1.fiftyPercent = t.x
	 order by TENKTUP1.fiftyPercent, t.x';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Test for bug 2307:
-- Join between primary & foreign keys, w/= clause on foreign tab &
-- ORDER on indexed col of prim. tab returns rows in wrong order

get cursor c as
	'select * from TENKTUP1, TENKTUP2
	 where TENKTUP1.unique1 = TENKTUP2.ten
	 and TENKTUP2.onePercent = 63
	 order by TENKTUP1.two';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- Test multi-level outer join

-- Extra-wide output because many tables.
maximumdisplaywidth 8000;
get cursor c as
	'select * from TENKTUP1
		left outer join TENKTUP2 on
		(
			TENKTUP1.unique1 = TENKTUP2.unique1
		)
		left outer join ONEKTUP on
		(
			TENKTUP2.unique2 = ONEKTUP.unique2
		)
		left outer join BPRIME on
		(
			ONEKTUP.onePercent = BPRIME.onePercent
		)';
close c;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

commit;

-- tests to show selectivity - rowcount estimates
-- the numbers skip a value for reference to original Cloudscape test cases
-- with identical queries using properties useStatistics=false.
-- do simple joins on columns and look at row count/cost.

-- Join on two, all rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two';
close c;
-- 1, join on two--all rows
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Join on two, 60% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 6000';
close c;
-- 3, join on two--60%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Join on two, 25% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 2500';
close c;
-- 5, join on two--25%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Join on two, 10% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 1000';
close c;
-- 7, join on two--10%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Join on two, 5% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 500';
close c;
-- 9, join on two--5%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Join on two, 1% of rows in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 < 100';
close c;
-- 11, join on two--1%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Join on two, 1 row in TENKTUP2
get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.two = TENKTUP2.two
	and TENKTUP2.unique1 = 0';
close c;
-- 13, join on two--1 row
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

--  now do joins on a very low cardinality table

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from 
	TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent';
close c;

-- 15, join on onePercent--all rows
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 6000';
close c;
-- 17, join on onePercent--60%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 2500';
close c;
-- 19, join on onePercent--25%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 1000';
close c;
-- 21, join on onePercent--10%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 500';
close c;
-- 23, join on onePercent--5%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 < 100';
close c;
-- 25, join on onePercent--1%
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

get cursor c as
	'select TENKTUP1.unique2, TENKTUP2.* from TENKTUP1, TENKTUP2
	where TENKTUP1.onePercent = TENKTUP2.onePercent
	and TENKTUP2.unique1 = 0';
close c;
-- 27, join on onePercent--1 row
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
