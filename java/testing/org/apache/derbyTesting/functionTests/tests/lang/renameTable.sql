-- rename table tests


-- create some database objects
create table t1(c11 int not null primary key);
create table t2(c21 int not null primary key);
create table t3(c31 int not null primary key);
create table t4(c41 int not null primary key);
-- create table with foreign key constraint
create table t5 (c51 int, constraint fk foreign key(c51) references t4);
create view v1 as select * from t1;
-- bug 5685
create index i1_t3 on t3(c31);

-- do some population
insert into t1 values 11;
insert into t2 values 21;
insert into t2 values 22;
insert into t3 values 31;
insert into t3 values 32;
insert into t3 values 33;

autocommit off;

-- negative tests

-- rename a non-existing table
rename table notexists to notexists1;

-- the new table name for rename already exists
rename table t1 to t2;

-- rename a system table
rename table sys.systables to fake;

-- rename a view
rename table v1 to fake;

-- cannot rename a table when there is an open cursor on it
get cursor c1 as 'select * from t2';
rename table t2 to fake;
close c1;

-- cannot rename a table when there is a view on it
rename table t1 to fake;

-- cannot rename because t5's foreign key depends on t4
rename table t4 to fake;
-- only dropping the fk constraint can allow the table to be renamed
alter table t5 drop constraint fk;
-- this statement should not fail
rename table t4 to realTab;

-- positive tests

select * from t3;

-- can rename a table when there is an index defined on it
rename table t3 to t3r;

select * from t3r;

-- creating a prepared statement on a table
autocommit off;
prepare p3 as 'select * from t3r where c31 > ?';
execute p3 using 'values (30)';
-- can rename with no errors
rename table t3r to t3;
-- but the execute statement will fail
execute p3 using 'values (30)';
remove p3;
autocommit on;

-- creating a table with triggers defined on it
create table t6 (c61 int default 1);
create table t7(c71 int);
-- bug 5684
create trigger t7insert after insert on t7 referencing new as NEWROW for each row mode db2sql insert into t6 values(NEWROW.c71);
insert into t7 values(1);
-- bug 5683. Should fail
rename table t7 to t7r;
select * from t7r;
select * from t7;

rename table t6 to t6r;
insert into t7 values(3);
select * from t6r;
select * from t7r;

-- Rename should fail if there is a check constraint
create table tcheck (i int check(i>5));
rename table tcheck to tcheck1;
drop table tcheck;

-- Rename should pass after dropping the check constriant
create table tcheck (i int, j int, constraint tcon check (i+j>2));
rename table tcheck to tcheck1;
alter table tcheck drop constraint tcon;
rename table tcheck to tcheck1;
select * from tcheck1;
drop table tcheck1;

-- clean up
drop view v1;
drop table t1;
drop table t2;
drop table t3;
drop table realTab;
drop table t5;
drop table t6r;
drop table t7r;
