-- ** insert fkBulkInsert.sql
--
-- test foreign key checking.  first
-- check that we do proper checking.
-- then make sure that dependencies interact
-- correctly with foreign keys

CREATE PROCEDURE WAIT_FOR_POST_COMMIT() DYNAMIC RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_Access.waitForPostCommitToFinish' PARAMETER STYLE JAVA;

create table p (c1 char(1), y int not null, c2 char(1), x int not null, constraint pk primary key (x,y));
create table f (x int not null, s smallint, y int not null, constraint fk foreign key (x,y) references p);

insert into p values ('1',1,'1',1);

-- should pass, foreign key constraint satisfied
insert into f 
values 
	(1,1,1),
	(1,1,1),
	(1,1,1),	
	(1,1,1),
	(1, 0, 1),
	(1,1,1),
	(1,0,1),
	(1, 0, 1);

-- should FAIL, foreign key constraint violated
delete from f;
insert into f 
values 
	(1,1,1),
	(1,1,1),
	(1,1,1),	
	(1,1,1),
	(1, 1, 1),
	(2,1,666),
	(1,1,0),
	(0, 1, 0);

drop table f;
CALL WAIT_FOR_POST_COMMIT();
 
-- make sure boundary conditions are ok, null insert set
create table f (c1 char(1), y int, c2 char(1), x int, constraint fk foreign key (x,y) references p);
insert into f 
	select * from p where 1 = 2;

drop table f;
drop table p;
CALL WAIT_FOR_POST_COMMIT();
 

-- self referencing
create table s (x int not null primary key, y int references s, z int references s);

-- ok
insert into s 
values 
	(1,1,1),
	(2,1,1),
	(10,2,1),	
	(11,1,2),
	(12,4,4),
	(4,1,1),
	(13,null,null),
	(14,1,2),
	(15,null, 1);

delete from s;

-- bad
insert into s 
values 
	(1,1,1),
	(2,1,1),
	(10,2,1),	
	(11,1,2),
	(12,4,4),
	(4,1,1),
	(13,null,null),
	(14,1,2),
	(15,666, 1);

-- now a test for depenencies.
-- the insert will create new index conglomerate numbers,
-- so we want to test that a statement with a constraint
-- check that is dependent on the conglomerate number that
-- is being changed is invalidated

create table x (x int not null, y int, constraint pk primary key (x));
create table y (x int , y int, constraint fk foreign key (x) references x);
prepare ix as 
	'insert into x 
	values
		(0,0),
		(1,1),
		(2,2)';

prepare ix2 as 
	'insert into x 
	values
		(3,3),
		(4,4)';

prepare iy as 
	'insert into y 
	values
		(0,0),
		(1,1),
		(2,2)';

prepare dy as 'delete from y where x = 1';
prepare dx as 'delete from x where x = 1';

execute ix;

autocommit off;
commit;

-- ok
execute dy;

-- ok
execute dx;

-- will fail, no key 1 in x
execute iy;

rollback;
commit;

execute iy;
execute dy;
execute dx;

remove ix;
remove ix2;
remove iy;
remove dy;
remove dx;

drop table y;
drop table x;
drop table s;
autocommit on;
CALL WAIT_FOR_POST_COMMIT();
 
-- ** insert fkddl.sql
-- simple syntax checks
-- column constraint
create table p1 (x int not null, constraint pk1 primary key(x));

create table u1 (x int not null unique);

-- table constraint
create table p2 (x int not null, y dec(5,2) not null, constraint pk2 primary key (x,y));

create table u2 (x int not null, y dec(5,2) not null, constraint uk2 unique (x,y));

create table p3 (x char(10) not null, constraint pk3 primary key (x));

-- for future use
create schema otherschema;
create table otherschema.p1 (x int not null primary key);


-- 
-- Negative test cases for foreign key TABLE
-- constraints
--

-- negative: fk table, no table
create table f (x int, constraint fk foreign key (x) references notthere);

-- negative: fk table, bad column
create table f (x int, constraint fk foreign key (x) references p1(notthere));

-- negative: fk table, no constraint
create table f (x int, constraint fk foreign key (x) references p2(y));

-- negative: fk table, wrong type
create table f (x smallint, constraint fk foreign key (x) references p1(x));

-- negative: cannot reference a system table
create table f (x char(36), constraint fk foreign key (x) references sys.sysforeignkeys(constraintid));

-- negative: bad schema
create table f (x char(36), constraint fk foreign key (x) references badschema.x);

-- negative: bad column list
create table f (x dec(5,2), y int, constraint fk foreign key (x,z) references p2(x,y));

-- negative: wrong number of columns
create table f (x dec(5,2), y int, constraint fk foreign key (x) references p2(x,y));
create table f (x dec(5,2), y int, constraint fk foreign key (x,y) references p2(x));


-- 
-- Negative test cases for foreign key COLUMN
-- constraints
--

-- negative: fk column, no table
create table f (x int references notthere);

-- negative: fk column, bad column
create table f (x int references p1(notthere));

-- negative: fk column, no constraint
create table f (x int references p2(y));

-- negative: fk column, wrong type
create table f (x smallint references p1(x));

-- negative: cannot reference a system table
create table f (x char(36) references sys.sysforeignkeys(constraintid));

-- negative: bad schema
create table f (x char(36) references badschema.x);


--
-- Some type checks.  Types must match exactly
--

-- ok
create table f (d dec(5,2), i int, constraint fk foreign key (i,d) references p2(x,y));
drop table f;
CALL WAIT_FOR_POST_COMMIT();
create table f (i int, d dec(5,2), constraint fk foreign key (i,d) references p2(x,y));
drop table f;
CALL WAIT_FOR_POST_COMMIT(); 

create table f (d dec(5,2), i int, constraint fk foreign key (i,d) references u2(x,y));
drop table f;
CALL WAIT_FOR_POST_COMMIT();
create table f (i int, d dec(5,2), constraint fk foreign key (i,d) references u2(x,y));
drop table f;
CALL WAIT_FOR_POST_COMMIT();
create table f (c char(10) references p3(x));
drop table f;
CALL WAIT_FOR_POST_COMMIT();

-- type mismatch
create table f (i int, d dec(5,1), constraint fk foreign key (i,d) references p2(x,y));
create table f (i int, d dec(4,2), constraint fk foreign key (i,d) references p2(x,y));
create table f (i int, d dec(4,2), constraint fk foreign key (i,d) references p2(x,y));
create table f (i int, d numeric(5,2), constraint fk foreign key (i,d) references p2(x,y));
create table f (c char(11) references p3(x));
create table f (c varchar(10) references p3(x));

-- wrong order
create table f (d dec(5,2), i int, constraint fk foreign key (d,i) references p2(x,y));


-- check system tables 
create table f (x int, constraint fk foreign key (x) references p1);
select constraintname, referencecount 
	from sys.sysconstraints c, sys.sysforeignkeys fk
	where fk.keyconstraintid = c.constraintid order by constraintname;

create table f2 (x int, constraint fk2 foreign key (x) references p1(x));
create table f3 (x int, constraint fk3 foreign key (x) references p1(x));
create table f4 (x int, constraint fk4 foreign key (x) references p1(x));

select distinct constraintname, referencecount 
	from sys.sysconstraints c, sys.sysforeignkeys fk
	where fk.keyconstraintid = c.constraintid order by constraintname;

select constraintname 
	from sys.sysconstraints c, sys.sysforeignkeys fk
	where fk.constraintid = c.constraintid
	order by 1;

-- we should not be able to drop the primary key 
alter table p1 drop constraint pk1;
drop table p1;
CALL WAIT_FOR_POST_COMMIT();
-- now lets drop the foreign keys and try again
drop table f2;
drop table f3;
drop table f4;
CALL WAIT_FOR_POST_COMMIT();

select constraintname, referencecount 
	from sys.sysconstraints c, sys.sysforeignkeys fk
	where fk.keyconstraintid = c.constraintid order by constraintname;

alter table f drop constraint fk;
CALL WAIT_FOR_POST_COMMIT();

-- ok
alter table p1 drop constraint pk1;
CALL WAIT_FOR_POST_COMMIT();

-- we shouldn't be able to add an fk on p1 now
alter table f add constraint fk foreign key (x) references p1;

-- add the constraint and try again
alter table p1 add constraint pk1 primary key (x);

create table f2 (x int, constraint fk2 foreign key (x) references p1(x));
create table f3 (x int, constraint fk3 foreign key (x) references p1(x));
create table f4 (x int, constraint fk4 foreign key (x) references p1(x));

-- drop constraint
alter table f4 drop constraint fk4;
alter table f3 drop constraint fk3;
alter table f2 drop constraint fk2;
alter table p1 drop constraint pk1;
CALL WAIT_FOR_POST_COMMIT();

-- all fks are gone, right?
select constraintname 
	from sys.sysconstraints c, sys.sysforeignkeys fk
	where fk.constraintid = c.constraintid order by constraintname;

-- cleanup what we have done so far
drop table p1;
drop table p2;
drop table u1;
drop table u2;
drop table otherschema.p1;
drop schema otherschema restrict;
CALL WAIT_FOR_POST_COMMIT();

-- will return dependencies for SPS metadata queries now created by default 
-- database is created.
create table default_sysdepends_count(a int);

insert into default_sysdepends_count select count(*) from sys.sysdepends;
select * from default_sysdepends_count;

-- 
-- now we are going to do some self referencing
-- tests.
-- 
create table selfref (p char(10) not null primary key, 
		f char(10) references selfref);
drop table selfref;
CALL WAIT_FOR_POST_COMMIT();

-- ok
create table selfref (p char(10) not null, 
		f char(10) references selfref, 
		constraint pk primary key (p));
drop table selfref;
CALL WAIT_FOR_POST_COMMIT();

-- ok
create table selfref (p char(10) not null, f char(10), 
		constraint f foreign key (f) references selfref(p), 
		constraint pk primary key (p));

-- should fail
alter table selfref drop constraint pk;
CALL WAIT_FOR_POST_COMMIT();

-- ok
alter table selfref drop constraint f;
alter table selfref drop constraint pk;
drop table selfref;
CALL WAIT_FOR_POST_COMMIT();


-- what if a pk references another pk?  should just
-- drop the direct references (nothing special, really)
create table pr1(x int not null, 
		constraint pkr1 primary key (x));
create table pr2(x int not null, 
		constraint pkr2 primary key(x), 
		constraint fpkr2 foreign key (x) references pr1);
create table pr3(x int not null, 
		constraint pkr3 primary key(x), 
		constraint fpkr3 foreign key (x) references pr2);
select constraintname, referencecount from sys.sysconstraints order by constraintname;

-- now drop constraint pkr1
alter table pr2 drop constraint fpkr2;
alter table pr1 drop constraint pkr1;
CALL WAIT_FOR_POST_COMMIT();

-- pkr1 and pfkr2 are gone
select constraintname, referencecount from sys.sysconstraints order by constraintname;

-- cleanup
drop table pr3;
drop table pr2;
drop table pr1;
CALL WAIT_FOR_POST_COMMIT();

-- should return 0, confirm no unexpected dependencies
-- verify that all rows in sys.sysdepends got dropped 
-- apart from sps dependencies
create table default_sysdepends_count2(a int);
insert into default_sysdepends_count2 select count(*) from sys.sysdepends;
select default_sysdepends_count2.a - default_sysdepends_count.a
    from default_sysdepends_count2, default_sysdepends_count;

-- dependencies and spses
create table x (x int not null primary key, y int, constraint xfk foreign key (y) references x);
create table y (x int, constraint yfk foreign key (x) references x);
prepare ss as 'select * from x';
prepare si as 'insert into x values (1,1)';
prepare su as 'update x set x = x+1, y=y+1';

alter table x drop constraint xfk;
CALL WAIT_FOR_POST_COMMIT();

autocommit off;

-- drop the referenced fk, should force su to be recompiled
-- since it no longer has to check the foreign key table
alter table y drop constraint yfk;
commit;
CALL WAIT_FOR_POST_COMMIT();

drop table y;
commit;
CALL WAIT_FOR_POST_COMMIT();

-- ok
drop table x;

remove ss;
remove si;
remove su;

drop table f3;
drop table f2;
drop table f;
commit;
CALL WAIT_FOR_POST_COMMIT();

-- verify that all rows in sys.sysdepends got dropped 
-- apart from sps dependencies
-- Since, with beetle 5352; we create metadata SPS for network server at database bootup time
-- so the dependencies for SPS are there.
 
create table default_sysdepends_count3(a int);
insert into default_sysdepends_count3 select count(*) from sys.sysdepends;
select default_sysdepends_count3.a - default_sysdepends_count.a
    from default_sysdepends_count3, default_sysdepends_count;

-- ** insert fkdml.sql
autocommit on;
--
-- DML and foreign keys
--
drop table s;
drop table f3;
drop table f2;
drop table f;
drop table p;
CALL WAIT_FOR_POST_COMMIT();


create table p (x int not null, y int not null, constraint pk primary key (x,y));
create table f (x int, y int, constraint fk foreign key (x,y) references p);

insert into p values (1,1);

-- ok
insert into f values (1,1);

-- fail
insert into f values (2,1);
insert into f values (1,2);

-- nulls are ok
insert into f values (1,null);
insert into f values (null,null);
insert into f values (1,null);

-- update on pk, fail
update p set x = 2;
update p set y = 2;
update p set x = 1, y = 2;
update p set x = 2, y = 1;
update p set x = 2, y = 2;


-- ok
update p set x = 1, y = 1;

-- delete pk, fail
delete from p;

-- delete fk, ok
delete from f;

insert into f values (1,1);

-- update fk, fail
update f set x = 2;
update f set y = 2;
update f set x = 1, y = 2;
update f set x = 2, y = 1;

-- update fk, ok
update f set x = 1, y = 1;

-- nulls ok
update f set x = null, y = 1;
update f set x = 1, y = null;
update f set x = null, y = null;

delete from f;

insert into f values (1,1);
insert into p values (2,2);

-- ok
update f set x = x+1, y = y+1;

select * from f;
select * from p;

-- ok
update p set x = x+1, y = y+1;

-- fail
update p set x = x+1, y = y+1;


-- 
-- BOUNDARY CONDITIONS
--
delete from f;
delete from p;

insert into f select * from f;
delete from p where x = 9999;
update p set x = x+1, y=y+1 where x = 999;

insert into p values (1,1);
insert into f values (1,1);
update p set x = x+1, y=y+1 where x = 999;
delete from p where x = 9999;
insert into f select * from f;

--
-- test a CURSOR
--
delete from f;
delete from p;
insert into p values (1,1);
insert into f values (1,1);

autocommit off;
get cursor  c as 'select * from p for update of x';
next c;

-- fail
update p set x = 666 where current of c;
close c;

get cursor  c as 'select * from f for update of x';
next c;

-- fail
update f set x = 666 where current of c;
close c;

commit;
autocommit on;

delete from f;
delete from p;
insert into p values (0,0), (1,1), (2,2), (3,3), (4,4);
insert into f values (1,1);

-- lets add some additional foreign keys to the mix
create table f2 (x int, y int, constraint fk2 foreign key (x,y) references p);
insert into f2 values (2,2);
create table f3 (x int, y int, constraint fk3 foreign key (x,y) references p);
insert into f3 values (3,3);

-- ok
update p set x = x+1, y = y+1;

-- error, fk1
update p set x = x+1;
update p set y = y+1;
update p set x = x+1, y = y+1;

-- fail of fk3
update p set y = 666 where y = 3;

-- fail of fk2
update p set x = 666 where x = 2;

-- cleanup
drop table f;
drop table f2;
drop table f3;
drop table p;
CALL WAIT_FOR_POST_COMMIT();

--
-- SELF REFERENCING
--
create table s (x int not null primary key, y int references s, z int references s);

-- ok
insert into s values (1,null,null);

-- ok
update s set y = 1;

-- fail
update s set z = 2;

-- ok
update s set z = 1;

-- ok
insert into s values (2, 1, 1);

-- ok
update s set x = 666 where x = 2;

-- ok
update s set x = x+1, y = y+1, z = z+1;

delete from s;

-- ok
insert into s values (1,null,null);
-- ok
insert into s values (2,null,null);

-- ok
update s set y = 2 where x = 1;
-- ok
update s set z = 1 where x = 2;

select * from s;

-- fail 
update s set x = 0 where x = 1; 

--
-- Now we are going to do a short but sweet
-- check to make sure we are actually hitting
-- the correct columns
--
create table p (c1 char(1), y int not null, c2 char(1), x int not null, constraint pk primary key (x,y));
create table f (x int, s smallint, y int, constraint fk foreign key (x,y) references p);

insert into p values ('1',1,'1',1);
-- ok
insert into f values (1,1,1);

insert into p values ('0',0,'0',0);
-- ok
update p set x = x+1, y=y+1;

-- fail
delete from p where y = 1;

-- fail
insert into f values (1,1,4);

delete from f;
delete from p;

--
-- Lets make sure we don't interact poorly with
-- 'normal' deferred dml

insert into p values ('1',1,'1',1);
insert into f values (1,1,1);

insert into p values ('0',0,'0',0);
-- ok
update p set x = x+1, y=y+1 where x < (select max(x)+10000 from p);

-- fail
delete from p where y = 1 and y in (select y from p);

-- inserts
create table f2 (x int, t smallint, y int);
insert into f2 values (1,1,4);

-- fail
insert into f select * from f2;


-- ok
insert into f2 values (1,1,1);
insert into f select * from f2 where y = 1;

drop table f2;
drop table f;
drop table p;
CALL WAIT_FOR_POST_COMMIT();

--
-- PREPARED STATEMENTS
--
drop table f;
drop table p;
--the reason for this wait call is to wait unitil system tables row deletes
--are completed other wise we will get different order fk checks
--that will lead different error messages depending on when post commit thread runs
CALL WAIT_FOR_POST_COMMIT();
 
prepare s as 
	'create table p (w int not null primary key, x int references p, y int not null, z int not null, constraint uyz unique (y,z))';
execute s;
remove s;

prepare s as 
	'create table f (w int references p, x int, y int, z int, constraint fk foreign key (y,z) references p (y,z))';
execute s;
remove s;

prepare s as 
	'alter table f drop constraint fk';
execute s;
remove s;
--the reason for this wait call is to wait unitil system tables row deletes
--are completed other wise we will get different order fk checks
CALL WAIT_FOR_POST_COMMIT(); 
prepare s as 
	'alter table f add constraint fk foreign key (y,z) references p (y,z)';
execute s;
remove s;

prepare sf as 
	'insert into f values (1,1,1,1)';

prepare sp as 
	'insert into p values (1,1,1,1)';

-- fail
execute sf;

-- ok
execute sp;
execute sf;

insert into p values (2,2,2,2);


remove sf;
prepare sf as 
	'update f set w=w+1, x = x+1, y=y+1, z=z+1';
-- ok
execute sf;

remove sp;
prepare sp as 
	'update p set w=w+1, x = x+1, y=y+1, z=z+1';
-- ok
execute sp;

remove sp;
prepare sp as 
	'delete from p where x =1';
-- ok
execute sp;

remove sp;
remove sf;
drop table f;
drop table p;
CALL WAIT_FOR_POST_COMMIT();
drop procedure WAIT_FOR_POST_COMMIT;
