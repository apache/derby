-- tests for check constraints

autocommit off;

-- negative

-- The following are not allowed in check constraints:
--	?, subquery, datetime functions
create table neg1(c1 int check(?));
create table neg1(c1 int check(c1 in (select c1 from neg1)));
create table neg1(c1 int check(CURRENT_DATE = CURRENT_DATE));
create table neg1(c1 int check(CURRENT_TIME = CURRENT_TIME));
create table neg1(c1 int check(CURRENT_TIMESTAMP = CURRENT_TIMESTAMP));

-- The check constraint definition must evaluate to a boolean
create table neg1(c1 int check(c1));
create table neg1(c1 int check(1));
create table neg1(c1 int check(c1+c1));

-- All column references are to target table
create table neg1(c1 int check((c2 = 1)));

-- verify that a check constraint can't be used as an optimizer override
create table t1(c1 int constraint asdf check(c1 = 1));
select * from t1 properties constraint = asdf;
-- alter table t1 drop constraint asdf;
rollback;
-- alter table t1 drop constraint asdf;

-- forward references should fail
create table neg1(c1 int check(c2 = 1), c2 int);
create table neg2(c1 int constraint asdf check(c2 = 1), c2 int);
rollback;

-- positive

-- multiple check constraints on same table
create table pos1(c1 int check(c1 > 0), constraint asdf check(c1 < 10));
-- verify both constraints are enforced
insert into pos1 values 0;
insert into pos1 values 1;
insert into pos1 values 9;
insert into pos1 values 10;
select * from pos1;

-- verify constraint violation rolls back entire statement
update pos1 set c1 = c1 + 1;
select * from pos1;
update pos1 set c1 = c1 - 1;
select * from pos1;
rollback;

-- conflicting constraints, should fail
create table negcks(c1 int constraint ck1st check(c1 > 4), c2 int constraint ck2nd check(c2 > 2), c3 int, constraint ckLast check(c2 > c1));
-- constraint ck1st fails
insert into negcks values (1, 3, 3);
-- constraint ckLast fails (ck2nd fails too)
insert into negcks values (5, 1, 3);
-- constraint ck1st fails (ckLast fails too)
insert into negcks values (2, 3, 3);
rollback;

-- same source and target tables
create table pos1(c1 int, c2 int, constraint ck1 check (c1 < c2));
insert into pos1 values (1, 2), (2, 3), (3, 4);
commit;
-- these should work
insert into pos1 select * from pos1;
select count(*) from pos1;
update pos1 set c2 = (select max(c1) from pos1),
				c1 = (select min(c2) from pos1);
select * from pos1;
rollback;
-- these should fail
insert into pos1 select c2, c1 from pos1;
select count(*) from pos1;
update pos1 set c2 = (select min(c1) from pos1),
				c1 = (select max(c2) from pos1);
select * from pos1;

drop table pos1; 
commit;

-- union under insert
create table t1(c1 int, c2 int, constraint ck1 check(c1 = c2));
insert into t1 values (1, 1), (2, 1);
select * from t1;

-- normalize result set under insert/update
insert into t1 values (1.0, 1);
insert into t1 values (2.0, 1);
select * from t1;
update t1 set c2 = 1.0;
update t1 set c2 = 2.0;
select * from t1;
update t1 set c1 = 3.0, c2 = 3.0;
select * from t1;
rollback;

-- positioned update
create table t1(c1 int, c2 int, constraint ck1 check(c1 = c2), constraint ck2 check(c2=c1));
insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4);
create index i1 on t1(c1);
get cursor c1 as 'select * from t1 where c2 = 2 for update of c1';
next c1;
-- this update should succeed
update t1 set c1 = c1 where current of c1;
-- this update should fail
update t1 set c1 = c1 + 1 where current of c1;
close c1;

get cursor c2 as 'select * from t1 where c1 = 2 for update of c2';
next c2;
-- this update should succeed
update t1 set c2 = c2 where current of c2;
-- this update should fail
update t1 set c2 = c2 + 1 where current of c2;
close c2;

get cursor c3 as 'select * from t1 where c1 = 2 for update of c1, c2';
next c3;
-- this update should succeed
update t1 set c2 = c1, c1 = c2 where current of c3;
-- this update should fail
update t1 set c2 = c2 + 1, c1 = c1 + 3 where current of c3;
-- this update should succeed
update t1 set c2 = c1 + 3, c1 = c2 + 3 where current of c3;
select * from t1;
close c3;
rollback;

-- complex expressions
create table t1(c1 int check((c1 + c1) = (c1 * c1) or 
							 (c1 + c1)/2 = (c1 * c1)), c2 int);
-- this insert should succeed
insert into t1 values (1, 9), (2, 10);
-- these updates should succeed
update t1 set c2 = c2 * c2;
update t1 set c1 = 2 where c1 = 1;
update t1 set c1 = 1 where c1 = 2;
-- this update should fail
update t1 set c1 = c2;
select * from t1;

rollback;

-- built-in functions in a check constraint
create table charTab (c1 char(4) check(CHAR(c1) = c1));
insert into charTab values 'asdf';
insert into charTab values 'fdsa';
-- beetle 5805 - support built-in function INT
-- should fail until beetle 5805 is implemented
create table intTab (c1 int check(INT(1) = c1));
insert into intTab values 1;
-- this insert should fail, does not satisfy check constraint
insert into intTab values 2;
create table maxIntTab (c1 int check(INT(2147483647) > c1));
insert into maxIntTab values 1;
-- this insert should fail, does not satisfy check constraint
insert into maxIntTab values 2147483647;
rollback;

-- verify that inserts, updates and statements with forced constraints are
-- indeed dependent on the constraints
create table t1(c1 int not null constraint asdf primary key);
insert into t1 values 1, 2, 3, 4, 5;
commit;
prepare p1 as 'insert into t1 values 1';
prepare p2 as 'update t1 set c1 = 3 where c1 = 4';
prepare p3 as 'select * from t1';
-- the insert and update should fail, select should succeed
execute p1;
execute p2;
execute p3;
alter table t1 drop constraint asdf;

-- rollback and verify that constraints are enforced and select succeeds
rollback;
execute p1;
execute p2;
execute p3;
remove p1;
remove p2;
remove p3;

drop table t1;

-- check constraints with parameters
create table t1(c1 int constraint asdf check(c1 = 1));

prepare p1 as 'insert into t1 values (?)';
execute p1 using 'values (1)';

-- clean up
drop table t1;

create table t1(active_flag char(2) check(active_flag IN ('Y', 'N')), araccount_active_flag char(2) check(araccount_active_flag IN ('Y', 'N')), automatic_refill_flag char(2) check(automatic_refill_flag IN ('Y', 'N')), call_when_ready_flag char(2) check(call_when_ready_flag IN ('Y', 'N')), compliance_flag char(2) check(compliance_flag IN ('Y', 'N')), delivery_flag char(2) check(delivery_flag IN ('Y', 'N')), double_count_flag char(2) check(double_count_flag IN ('Y', 'N')), gender_ind char(2) check(gender_ind IN ('M', 'F', 'U')), geriatric_flag char(2) check(geriatric_flag IN ('Y', 'N')), refuse_inquiry_flag char(2) check(refuse_inquiry_flag IN ('Y', 'N')), animal_flag char(2) check(animal_flag IN ('Y', 'N')), terminal_flag char(2) check(terminal_flag IN ('Y', 'N')), unit_flag char(2) check(unit_flag IN ('Y', 'N')), VIP_flag char(2) check(VIP_flag IN ('Y', 'N')), snap_cap_flag char(2) check(snap_cap_flag IN ('Y', 'N')), consent_on_file_flag char(2) check(consent_on_file_flag IN ('Y', 'N')), enlarged_SIG_flag char(2) check(enlarged_SIG_flag IN ('Y', 'N')),aquired_patient_flag char(2) check(aquired_patient_flag IN ('Y', 'N')));

-- bug 5622 - internal generated constraint names are re-worked to match db2's naming convention.

drop table t1;
create table t1 (c1 int not null primary key, c2 int not null unique, c3 int check (c3>=0));
alter table t1 add column c4 int not null default 1;
alter table t1 add constraint c4_unique UNIQUE(c4);
alter table t1 add column c5 int check(c5 >= 0);
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t where c.tableid = t.tableid and tablename='T1';

drop table t2;
create table t2 (c21 int references t1);
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t where c.tableid = t.tableid and tablename='T2';

drop table t3;
create table t3 (c1 int check (c1 >= 0), c2 int check (c2 >= 0), c3 int check (c3 >= 0), c4 int check (c4 >= 0), c5 int check (c5 >= 0), 
c6 int check (c6 >= 0), c7 int check (c7 >= 0), c8 int check (c8 >= 0), c9 int check (c9 >= 0), c10 int check (c10 >= 0), 
c11 int check (c11 >= 0), c12 int check (c12 >= 0), c13 int check (c13 >= 0));
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t where c.tableid = t.tableid and tablename='T3';

drop table t4;
create table t4(c11 int not null, c12 int not null, primary key (c11, c12));
select c.constraintname, c.type from sys.sysconstraints c, sys.systables t where c.tableid = t.tableid and tablename='T4';
