-- minimal testing to verify no scans left open
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ConsistencyChecker.runConsistencyChecker'
LANGUAGE JAVA PARAMETER STYLE JAVA;
autocommit off;


autocommit off;
create table t1(c1 int, c2 int);

-- do consistency check on scans, etc.
values ConsistencyChecker();
insert into t1 values (1, 1);

-- do consistency check on scans, etc.
values ConsistencyChecker();
create index i1 on t1(c1);

-- do consistency check on scans, etc.
values ConsistencyChecker();
create index i2 on t1(c2);
insert into t1 values (2, 2);

-- do consistency check on scans, etc.
values ConsistencyChecker();
-- scan heap
select * from t1;
-- scan covering index
select c1 from t1;
-- index to base row
select * from t1;
select * from t1 where c1 = 1;

-- do consistency check on scans, etc.
values ConsistencyChecker();

commit;

-- test cursor which doesn't get drained
get cursor c1 as 'select c1 + c2 from t1 order by 1';
next c1;
close c1;

-- do consistency check on scans, etc.
values ConsistencyChecker();

commit;
