-- tests for primary/unique key
-- most testing currently deferred since we have extensive index tests
-- and no foreign keys yet.


-- negative tests

-- duplicate primary keys
create table neg (c1 int not null primary key, c2 int, c3 int not null constraint asdf primary key);
create table neg (c1 int not null primary key, c2 int, c3 int not null constraint asdf primary key);

create table neg (c1 int not null primary key, c2 int not null, primary key(c1, c2));

-- duplicate constraint names
create table neg (c1 int not null constraint asdf primary key, c2 int, c3 int constraint asdf unique);

-- duplicate column names in same constraint column list
create table neg (c1 int not null, c2 int not null, primary key(c1, c2, c1));

-- non-existant columns in constraint column list
create table neg (c1 int not null, c2 int not null, primary key(c1, c2, cx));

-- invalid constraint schema name
create table neg (c1 int not null, c2 int not null, constraint bar.pkneg primary key(c1, c2));
create table neg (c1 int not null, c2 int not null, constraint sys.pkneg primary key(c1, c2));
create table neg (c1 int not null constraint bar.pkneg primary key, c2 int);
create table neg (c1 int not null constraint sys.pkneg primary key, c2 int);


-- constraint names must be unique within a schema
create table neg1(c1 int not null constraint asdf primary key);
create table neg2(c1 int not null constraint asdf primary key);
drop table neg1;
create table neg2(c1 int not null constraint asdf primary key);
drop table neg2;

-- again with explict schema names, should fail
create table neg1(c1 int not null constraint app.asdf primary key);
create table neg2(c1 int not null constraint app.asdf primary key);
create table neg2(c1 int not null constraint app.asdf primary key);

-- again with mixing schema names
create table neg1(c1 int not null constraint asdf primary key);
create table neg2(c1 int not null constraint app.asdf primary key);
drop table neg1;
create table neg2(c1 int not null constraint app.asdf primary key);

-- primary key cannot be explicitly nullable
create table neg2(c1 int null constraint asdf primary key);
create table neg2(c1 int null, c2 int, constraint asdf primary key(c1, c2));

-- verify that you can not create a primary key column with default null
-- a primary key column can only be create if the column is explicitly not null
create table neg1 (c1 int default null primary key);
create table neg1 (c1 int default null, c2 int not null, primary key(c2, c1));

-- test that a unique key can be not be explicitly nullable
create table neg1(c1 int null unique);
create table neg1(c1 int null, c2 int, constraint asdf unique(c1));



-- positive tests

-- verify that a unique key can not contain nulls
create table pos1 (c1 int not null unique, c2 int);
insert into pos1 (c1) values(null);
insert into pos1 (c1) values(null);
select * from pos1;
drop table pos1;

-- verify that you can combine not null and unique/primary key constraints
create table pos1 (c1 int not null unique, c2 int not null primary key);
insert into pos1 (c1) values (null);
insert into pos1 (c2) values (null);
drop table pos1;

-- verify that you can combine multiple column constraints
select count(*) from sys.sysconstraints;
select count(*) from sys.syskeys;
-- we will be adding 6 rows to both sysconstraints and syskeys
create table pos1 (c1 int not null unique, c2 int not null primary key);
insert into pos1 (c1) values (null);
insert into pos1 (c2) values (null);
insert into pos1 values (1, 1), (1, 2);
insert into pos1 values (1, 1), (2, 1);
select count(*) from sys.sysconstraints;
select count(*) from sys.syskeys;
drop table pos1;


-- verify that you can delete from a primary key
create table pos1 (c1 int not null, c2 int not null, primary key(c2, c1));
insert into pos1 values (1, 2);
select * from pos1;
delete from pos1;
select * from pos1;

-- create a table with lots key columns
create table pos2 (i int not null, s smallint not null, r real not null, dp double precision not null,
				   c30 char(30) not null, vc10 varchar(10) not null, d date not null, t time not null,
				   ts timestamp not null,
				   primary key(ts, t, d, vc10, c30, dp, r, s, i));
insert into pos2 values(111111, 1, 1.11, 11111.1111, 'char(30)',
					    'vc(10)', '1999-9-9',
					    '8:08:08', '1999-9-9 8:08:08');
insert into pos2 values(111111, 1, 1.11, 11111.1111, 'char(30)',
					    'vc(10)', '1999-9-9',
					    '8:08:08', '1999-9-9 8:08:08');


-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

-- drop tables
drop table pos1;
drop table pos2;

-- verify the consistency of the indexes on the system catalogs
select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)
from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1';

-- test that a unique key can be explicitly not nullable
create table pos1(c1 int not null unique);
drop table pos1;
create table pos1(c1 int not null, c2 int, constraint asdf unique(c1));

create table t1 (c1 int, c2 int, c3 int not null);
create unique index i11 on t1 (c3);
create unique index i12 on t1 (c1, c3 desc);
insert into t1 values (1,2,3);
insert into t1 values (null, 4,5);
create table t2 (c1 int, c2 int, c3 int);
insert into t2 values (1,2,3), (null, 4,5), (7,8,9);
create unique index i21 on t2 (c3);
create unique index i22 on t2 (c1, c3 desc);
drop table t1;
drop table t2;

-- bug 5520 - constraint names in new schemas.
create table B5420_1.t1 (c1 int not null primary key);
create table B5420_2.t2 (c2 int not null constraint c2pk primary key);
-- two part constraint names are not allowed
create table B5420_3.t3 (c3 int not null constraint B5420_3.c3pk primary key);

create table B5420_4.t4 (c4 int not null, primary key (c4));
create table B5420_5.t5 (c5 int not null, constraint c5pk primary key (c5));
-- two part constraint names are not allowed
create table B5420_6.t6 (c6 int not null, constraint B5420_6.c6pk primary key (c6));

SELECT CAST (S.SCHEMANAME AS VARCHAR(12)), CAST (C.CONSTRAINTNAME AS VARCHAR(36)), CAST (T.TABLENAME AS VARCHAR(12)) FROM SYS.SYSCONSTRAINTS C , SYS.SYSTABLES T, SYS.SYSSCHEMAS S
WHERE C.SCHEMAID = S.SCHEMAID AND C.TABLEID = T.TABLEID AND T.SCHEMAID = S.SCHEMAID
AND S.SCHEMANAME LIKE 'B5420_%' ORDER BY 1,2,3;

-- clean up
drop table B5420_1.t1;
drop table B5420_2.t2;
drop table B5420_4.t4;
drop table B5420_5.t5;
