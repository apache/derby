--
-- this test shows the consistency checker in action;
--

-- create a table with some indexes
create table t1(i int, s smallint, c10 char(10), vc10 varchar(10), dc decimal(5,2));
create index t1_i on t1(i);
create index t1_s on t1(s);
create index t1_c10 on t1(c10);
create index t1_vc10 on t1(vc10);
create index t1_dc on t1(dc);


-- populate the tables
insert into t1 values (1, 11, '1 1', '1 1 1 ', 111.11);
insert into t1 values (2, 22, '2 2', '2 2 2 ', 222.22);
insert into t1 values (3, 33, '3 3', '3 3 3 ', 333.33);
insert into t1 values (4, 44, '4 4', '4 4 4 ', 444.44);

-- verify that everything is alright
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');

CREATE PROCEDURE RFHR(P1 VARCHAR(128), P2 VARCHAR(128))
LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.reinsertFirstHeapRow'
PARAMETER STYLE JAVA;
CREATE PROCEDURE DFHR(P1 VARCHAR(128), P2 VARCHAR(128))
LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.deleteFirstHeapRow'
PARAMETER STYLE JAVA;
CREATE PROCEDURE NFHR(P1 VARCHAR(128), P2 VARCHAR(128))
LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.nullFirstHeapRow'
PARAMETER STYLE JAVA;


autocommit off;

-- differing row counts
call RFHR('APP', 'T1');
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
-- drop and recreate each index to see differing count move to next index
drop index t1_i;
create index t1_i on t1(i);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_s;
create index t1_s on t1(s);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_c10;
create index t1_c10 on t1(c10);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_vc10;
create index t1_vc10 on t1(vc10);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_dc;
create index t1_dc on t1(dc);
-- everything should be back to normal
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');

-- delete 1st row from heap
call DFHR('APP', 'T1');
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
-- drop and recreate each index to see differing count move to next index
drop index t1_i;
create index t1_i on t1(i);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_s;
create index t1_s on t1(s);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_c10;
create index t1_c10 on t1(c10);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_vc10;
create index t1_vc10 on t1(vc10);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_dc;
create index t1_dc on t1(dc);
-- everything should be back to normal
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');

-- set 1st row from heap to all nulls
select * from t1;
call NFHR('APP', 'T1');
select * from t1;
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
-- drop and recreate each index to see differing count move to next index
drop index t1_i;
create index t1_i on t1(i);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_s;
create index t1_s on t1(s);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_c10;
create index t1_c10 on t1(c10);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_vc10;
create index t1_vc10 on t1(vc10);
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');
drop index t1_dc;
create index t1_dc on t1(dc);
-- everything should be back to normal
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');

-- RESOLVE - Next test commented out due to inconsistency in store error
-- message (sane vs. insane).  Check every index once store returns
-- consistent error.
-- insert a row with a bad row location into index
-- call org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker::insertBadRowLocation('APP', 'T1', 'T1_I');
-- values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');

-- cleanup
drop table t1;
commit;

