--Unsupported cases for referential actions , some of these are not supported currently in db2 udb also.
--SQL0632N
--FOREIGN KEY "<name>" is not valid because the table cannot be defined as a dependent of 
--table "<table-name>" because of del--ete rule restrictions (reason code = "<reason-code>").  
--Explanation: A referential constraint cannot be defined because the object table of the CREATE TABLE or 
--ALTER TABLE statement cannot be defined as a dependent of table "<table-name>" for one of the following reason codes: 
--(01) The relationship is self-referencing and a self-referencing relationship already exists
-- with the SET NULL delete rule. 
--(02) The relationship forms a cycle of two or more tables that cause the table to be delete-connected 
--to itself (all other delete rules in the cycle would be CASCADE). 
--(03) The relationship causes the table to be delete-connected to the indicated table through 
--multiple relationships and the delete rule of the existing relationship is SET NULL. 
--The delete rules of the existing relationships cause an error, not the delete rule specified in 
--the FOREIGN KEY clause of the CREATE TABLE or ALTER TABLE statement. 
--sqlcode: -632 
-- sqlstate: 42915 

-- case sql0632-01
create table t1(a int not null primary key , b int references t1(a) ON DELETE SET NULL, 
                c int references t1(a) ON DELETE CASCADE);

create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE SET NULL, 
                  z int references tself(b) ON DELETE SET NULL);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE SET NULL, 
                  z int references tself(b) ON DELETE CASCADE);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE SET NULL, 
                  z int references tself(b) ON DELETE RESTRICT);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE SET NULL, 
                  z int references tself(b) ON DELETE NO ACTION);

-- case sql0632 -02 (c2 fails)
create table t1(a int not null primary key, b int not null unique);
create table t2(x int not null primary key, y int);

alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete set null;
drop table t1;
drop table t2;

-- constraint c4 fails
create table t1( a int not null primary key, b int);
create table t2(x int, y int not null unique);
create table t3(l int, m int not null unique , k int );

alter table t2 add constraint c1 foreign key (x)
                             references t1(a) on delete cascade;
alter table t1 add constraint c2 foreign key (b)
                              references t3(m) on delete cascade;
alter table t3 add constraint c3 foreign key (m)
                              references t2(y) on delete cascade;
alter table t3 add constraint c4 foreign key (k)
                              references t2(y) on delete set null;
alter table t2 drop constraint c1;
alter table t1 drop constraint c2;
alter table t3 drop constraint c3;
drop table t1;
drop table t2;
drop table t3;

create table t1( a int not null primary key, b int);
create table t2(x int, y int not null unique, z int);
create table t3(l int, m int not null unique , k int );

create table t4(c1 int not null unique , c2 int);
create table t5(c1 int not null unique , c2 int);
create table t6(c1 int not null unique , c2 int);


--delete connected cycle
--different path from t2
alter table t2 add constraint c3 foreign key (z)
                              references t4(c1) on delete cascade;
alter table t4 add constraint c4 foreign key (c2)
                              references t5(c1) on delete cascade;
alter table t5 add constraint c5 foreign key (c2)
                              references t6(c1) on delete cascade;

--cycle forming alter -- c6 should fail
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete cascade;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete cascade;
alter table t3 add constraint c6 foreign key (k)
                              references t2(y) on delete SET NULL;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t2 drop constraint c3;
alter table t4 drop constraint c4;
alter table t5 drop constraint c5;

drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;

-- case sql0632 - 3 (c2 fails) 
create table t1( a int not null primary key);
create table t2(x int, y int not null unique);
create table t3(l int, m int );

alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete cascade;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete set null;
alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
drop table t1;
drop table t2;
drop table t3;



--SQL0633N The delete rule of FOREIGN KEY "<name>" must be "<delete-rule>" (reason code = "<reason-code>").  
--Explanation: The delete rule specified in a FOREIGN KEY clause of the CREATE TABLE or ALTER TABLE 
--statement is not valid. The indicated delete rule is required for one of the following reason codes: 
--(01) The referential constraint is self-referencing and an existing self-referencing constraint has the
-- indicated delete rule (NO ACTION, RESTRICT or CASCADE). 
--(02) The referential constraint is self-referencing and the table is dependent in a relationship with
-- a delete rule of CASCADE. 
--(03) The relationship would cause the table to be delete-connected to the same table through multiple 
--relationships and such relationships must have the same delete rule (NO ACTION, RESTRICT or CASCADE). 

-- case sql0633-01 (t1 creation should fail)
create table t1(a int not null primary key , b int references t1(a) ON DELETE CASCADE, 
                c int references t1(a) ON DELETE SET NULL);

create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE RESTRICT, 
                  z int references tself(b) ON DELETE CASCADE);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE RESTRICT, 
                  z int references tself(b) ON DELETE NO ACTION);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE RESTRICT, 
                  z int references tself(b) ON DELETE SET NULL);

create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE NO ACTION, 
                  z int references tself(b) ON DELETE CASCADE);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE NO ACTION, 
                  z int references tself(b) ON DELETE RESTRICT);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE NO ACTION, 
                  z int references tself(b) ON DELETE SET NULL);



create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE CASCADE, 
                  z int references tself(b) ON DELETE SET NULL);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE CASCADE, 
                  z int references tself(b) ON DELETE NO ACTION);
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE CASCADE, 
                  z int references tself(b) ON DELETE RESTRICT);

--FOLLOWING CASES SHOULD PASS
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE NO ACTION, 
                  z int references tself(b) ON DELETE NO ACTION);
drop table tself;
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE CASCADE, 
                  z int references tself(b) ON DELETE CASCADE);
drop table tself;
create table tself( a int not null primary key, b int not null  unique,
                  x int references tself(a) ON DELETE RESTRICT, 
                  z int references tself(b) ON DELETE RESTRICT);
drop table tself;
-- END PASS CASES




-- case sql0633-02 (t2 fails)
create table t1(a int not null primary key) ;
create table t2(x int references t1(a) ON DELETE CASCADE, 
                y int not null unique, z int references t2(y) ON DELETE SET NULL);
create table t2(x int references t1(a) ON DELETE CASCADE, 
                y int not null unique, z int references t2(y) ON DELETE NO ACTION);
create table t2(x int references t1(a) ON DELETE CASCADE, 
                y int not null unique, z int references t2(y) ON DELETE RESTRICT);

--START  PASS CASES
--These cases is to make sure we don;t throw errors for the valid cases.
create table t2(x int references t1(a) ON DELETE CASCADE, 
                y int not null unique, z int references t2(y) ON DELETE CASCADE);

drop table t2 ;

create table t2(x int references t1(a) ON DELETE RESTRICT, 
                y int not null unique, z int references t2(y) ON DELETE SET NULL);
drop table t2;
create table t2(x int references t1(a) ON DELETE RESTRICT, 
                y int not null unique, z int references t2(y) ON DELETE RESTRICT);
drop table t2;
create table t2(x int references t1(a) ON DELETE RESTRICT, 
                y int not null unique, z int references t2(y) ON DELETE CASCADE);
drop table t2;
create table t2(x int references t1(a) ON DELETE RESTRICT, 
                y int not null unique, z int references t2(y) ON DELETE NO ACTION);
drop table t2;

create table t2(x int references t1(a) ON DELETE NO ACTION, 
                y int not null unique, z int references t2(y) ON DELETE CASCADE);
drop table t2;
create table t2(x int references t1(a) ON DELETE NO ACTION, 
                y int not null unique, z int references t2(y) ON DELETE NO ACTION);
drop table t2;
create table t2(x int references t1(a) ON DELETE NO ACTION, 
                y int not null unique, z int references t2(y) ON DELETE SET NULL);
drop table t2;
create table t2(x int references t1(a) ON DELETE NO ACTION, 
                y int not null unique, z int references t2(y) ON DELETE RESTRICT);
drop table t2;

create table t2(x int references t1(a) ON DELETE SET NULL, 
                y int not null unique, z int references t2(y) ON DELETE SET NULL);
drop table t2;
create table t2(x int references t1(a) ON DELETE SET NULL, 
                y int not null unique, z int references t2(y) ON DELETE RESTRICT);
drop table t2;
create table t2(x int references t1(a) ON DELETE SET NULL, 
                y int not null unique, z int references t2(y) ON DELETE NO ACTION);
drop table t2;
create table t2(x int references t1(a) ON DELETE SET NULL, 
                y int not null unique, z int references t2(y) ON DELETE CASCADE);
drop table t2;
drop table t1;
--END PASS CASES

-- case sql0633-03 (c3 fails)
create table t1( a int not null primary key);
create table t2(x int, y int not null unique);
create table t3(l int, m int );
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete cascade;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete set null;
alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
drop table t1;
drop table t2;
drop table t3;

-- table t3 creation should fail.
create table t1( a int not null primary key);
create table t2(x int references t1(a) ON DELETE CASCADE, 
                                       y int not null constraint c1 unique);
create table t3(l int references t1(a) ON DELETE CASCADE , 
                         m int references t2(y) ON DELETE SET NULL);
alter table t2 drop constraint c1;
drop table t1;
drop table t2;

 
-- SQL0634N The delete rule of FOREIGN KEY "<name>" must not be CASCADE (reason-code = "<reason-code>").  
-- Explanation: The CASCADE delete rule specified in the FOREIGN KEY clause of the CREATE TABLE 
-- or ALTER TABLE statement is not valid for one of the following reason codes: 
-- (01) A self-referencing constraint exists with a delete rule of SET NULL, NO ACTION or RESTRICT. 
-- (02) The relationship would form a cycle that would cause a table to be delete-connected to itself. 
-- One of the existing delete rules in the cycle is not CASCADE, so this relationship may be definable 
-- if the delete rule is not CASCADE. 
-- (03) The relationship would cause another table to be delete-connected to the same table through
--  multiple paths with different delete rules or with delete rule equal to SET NULL. 

-- case sql0634 - 01
create table t1( a int not null primary key, b int , c int );
create table t2(x int, y int not null unique);
alter table t1 add constraint c1 foreign key (b) 
                             references t1(a) on delete set null;
alter table t1 add constraint c2 foreign key (c) 
                              references t2(y) on delete cascade;
drop table t1;
drop table t2;

-- t2 should fail
create table t1(a int not null primary key) ;
create table t2(x int not null unique, y int references t2(x) ON DELETE SET NULL, 
       		                       z int references t1(a) ON DELETE CASCADE);
create table t2(x int not null unique, y int references t2(x) ON DELETE NO ACTION, 
       		                       z int references t1(a) ON DELETE CASCADE);
create table t2(x int not null unique, y int references t2(x) ON DELETE RESTRICT, 
       		                       z int references t1(a) ON DELETE CASCADE);

--START  SHOULD PASS CASES
create table t2(x int not null unique, y int references t2(x) ON DELETE CASCADE, 
       		                       z int references t1(a) ON DELETE SET NULL);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE CASCADE, 
       		                       z int references t1(a) ON DELETE NO ACTION);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE CASCADE, 
       		                       z int references t1(a) ON DELETE RESTRICT);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE CASCADE, 
       		                       z int references t1(a) ON DELETE CASCADE);

drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE RESTRICT, 
       		                       z int references t1(a) ON DELETE SET NULL);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE RESTRICT, 
       		                       z int references t1(a) ON DELETE NO ACTION);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE RESTRICT, 
       		                       z int references t1(a) ON DELETE RESTRICT);
drop table t2;

create table t2(x int not null unique, y int references t2(x) ON DELETE NO ACTION, 
       		                       z int references t1(a) ON DELETE SET NULL);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE NO ACTION, 
       		                       z int references t1(a) ON DELETE RESTRICT);
drop table t2;
create table t2(x int not null unique, y int references t2(x) ON DELETE NO ACTION, 
       		                       z int references t1(a) ON DELETE NO ACTION);
drop table t2;
--END PASS CASES
drop table t1;


-- case sql0634 - 02 (c1 fails)
create table t1(a int not null primary key, b int not null unique);
create table t2(x int not null primary key, y int);
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete set null;
alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete cascade;
alter table t2 drop constraint c2;
drop table t1;
drop table t2;

-- case sql0634 - 03 
create table t1( a int not null primary key, b int);
create table t2(x int, y int not null unique, z int);
create table t3(l int, m int not null unique , k int );
create table t4(c1 int not null unique , c2 int);


-- error scenario 1: adding constraint c4 will make t2 get two paths from t1 with SET NULLS
alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete set null;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete set null;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete cascade;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;


alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

-- error scenario 2: adding constraint c4 will make t2 get two paths from t1 with a SET NULL and
--- a CASCADE.
alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete set null;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete cascade;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

-- error scenario 3: adding constraint c4 will make t2 get two paths from t1 with a NO ACTION 
--- and a CASCADE.
alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete NO ACTION;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete set null;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete cascade;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

-- error scenario 4: adding constraint c4 will make t2 get two paths from t1 with a CASCADE
--- and a RESTRICT.
alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete RESTRICT;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete cascade;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

--FOLLOWING SHOULD PASS

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete set null;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete set null;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete set null;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete set null;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete set null;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete CASCADE;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete set null;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete CASCADE;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete CASCADE;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete cascade;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete SET NULL;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete SET NULL;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete SET NULL;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete RESTRICT;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete SET NULL;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete SET NULL;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete CASCADE;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete RESTRICT;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;

alter table t2 add constraint c1 foreign key (x)
                              references t1(a) on delete SET NULL;
alter table t2 add constraint c2 foreign key (z)
                              references t4(c1) on delete CASCADE;
alter table t3 add constraint c3 foreign key (l)
                              references t1(a) on delete CASCADE;
alter table t4 add constraint c4 foreign key (c1)
                              references t3(m) on delete RESTRICT;

alter table t2 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;
alter table t4 drop constraint c4;


drop table t1;
drop table t2;
drop table t3;
drop table t4;








--- END OF ACTUAL ERROR CASES

--- MISC CASES 
--Following should give error because of delete-rule restrictions
create table t1( a int not null primary key);
create table t2(x int references t1(a) ON DELETE CASCADE, 
                                       y int not null unique);
create table t3(l int references t1(a) ON DELETE CASCADE , 
                         m int references t2(y) ON DELETE SET NULL);

drop table t3 ;
drop table t2 ;
drop table t1;

--DB21034E  The command was processed as an SQL statement because it was not a
--valid Command Line Processor command.  During SQL processing it returned:
--SQL0633N  The delete rule of FOREIGN KEY "M..." must be "CASCADE" (reason code
--= "3").  SQLSTATE=42915


create table t1( a int not null primary key);
create table t2(x int references t1(a) ON DELETE SET NULL, 
                                       y int not null unique);
create table t3(l int references t1(a) ON DELETE CASCADE , 
                         m int references t2(y) ON DELETE SET NULL);

drop table t3 ;
drop table t2 ;
drop table t1 ;

--Following should pass.
create table t1( a int not null primary key);
create table t4(s int not null unique);
create table t2(x int references t4(s) ON DELETE CASCADE, y int not null unique);
create table t3(l int references t1(a) ON DELETE CASCADE , 
                         m int references t2(y) ON DELETE SET NULL);


drop table t3;
drop table t2;
drop table t4;
drop table t1;


--Following should give error because of delete-rule restrictions
create table t1( a int not null primary key);
create table t2(x int, y int not null unique);
create table t3(l int, m int );

-- all should pass
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete cascade;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete cascade;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;


-- c3 fails: sql0633N - 3
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete cascade;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete set null;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;

-- c3 fails; sql0632N - 3 
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete CASCADE;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete cascade;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;


-- passes
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete set null;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete cascade;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;

-- succeds
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete set null;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete set null;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;

-- succeds
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete set null;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete cascade;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;

-- passes 
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete set null;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete set null;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;


-- c3 fails - sql0632 - 3
alter table t2 add constraint c1 foreign key (x) 
                             references t1(a) on delete cascade;
alter table t3 add constraint c2 foreign key (l) 
                              references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (m) 
                              references t2(y) on delete set null;

alter table t2 drop constraint c1;
alter table t3 drop constraint c2;
alter table t3 drop constraint c3;

drop table t1;
drop table t2;
drop table t3;

--cyclic case with two tables.
create table t1(a int not null primary key, b int not null unique);
create table t2(x int not null primary key, y int);

--passes
alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete cascade;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;


alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete NO ACTION;
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete cascade;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;

--c2 fails - sql0632N - reason code 2
alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete set null;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;

--c1 fails - sql0634N - reason code 2
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete set null;
alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete cascade;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;

-- c1 fails : column b can not contain null values
alter table t1 add constraint c1 foreign key (b)
                             references t2(x) on delete NO ACTION;
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete set null;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;


drop table t2;
drop table t1;

-- should pass
create table t1(a int not null unique, b int not null unique);
create table t3(l int unique not null  , y int);
create table t2(x int references t1(a) ON DELETE CASCADE ,
                y int references t3(l) ON DELETE RESTRICT);

drop table t2;
drop table t3;
drop table t1;


--creating t2 should fail
create table t1(a int not null unique, b int not null unique);
create table t3(l int unique not null  ,
               y int references t1(b) ON DELETE CASCADE);
create table t2(x int references t1(a) ON DELETE CASCADE ,
                y int references t3(l) ON DELETE RESTRICT);

drop table t2;
drop table t3;
drop table t1;



-- cyclic references
-- t1 refs  t3 refs t2 refs t1
create table t1( a int not null primary key, b int);
create table t2(x int, y int not null unique);
create table t3(l int, m int not null unique , k int );

insert into t1  values (1  , 1) ;
insert into t2 values ( 1 , 1) ;
insert into t3 values (1 , 1, 1) ;

--delete connected cycle 
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete cascade;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (m)
                              references t2(y) on delete cascade;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

--c3 should fail SQL0632N - 2
--delete connected cycle all refactions inside the cycle should be same
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete cascade;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (k)
                              references t2(y) on delete set null;


alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

--c3 should fail SQL0634N - 2 -- PROBLEMATIC CASE
-- DELETE CONNECTED CYCLE
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete cascade;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (k)
                              references t2(y) on delete cascade;


alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

--c3 should fail - SQL0634N - 2
--DELETE CONNECTED CYCLE
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete set null;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (k)
                              references t2(y) on delete cascade;


alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;


-- passes
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete set null;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (k)
                              references t2(y) on delete cascade;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

--passes
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete cascade;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete set null;
alter table t3 add constraint c3 foreign key (k)
                              references t2(y) on delete set null;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

--passes
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete set null;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete cascade;
alter table t3 add constraint c3 foreign key (k)
                              references t2(y) on delete set null;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t3 drop constraint c3;

drop table t1 ;
drop table t2 ;
drop table t3 ;


-- self referencing errors
create table tself(a int not null primary key , 
               b int references tself(a) ON DELETE SET NULL,  
               c int references tself(a) ON DELETE SET NULL);

create table tself(a int not null primary key , 
               b int references tself(a) ON DELETE CASCADE,  
               c int references tself(a) ON DELETE SET NULL);

create table tself(a int not null primary key , 
               b int references tself(a) ON DELETE SET NULL,  
               c int references tself(a) ON DELETE CASCADE);

create table tself(a int not null primary key , 
               b int references tself(a) ,  
               c int references tself(a) ON DELETE CASCADE);

create table tparent( a int not null  primary key);
--THIS ONE SHOULD PASS , but currently we are throwing ERRROR
create table tself(a int not null primary key , 
               b int references tparent(a) ON DELETE SET NULL ,  
               c int references tself(a) ON DELETE CASCADE);
drop table tself;
--should pass
create table tself(a int not null primary key , 
               b int references tparent(a) ON DELETE CASCADE ,  
               c int references tself(a) ON DELETE CASCADE);
drop table tself;
--should throw error
create table tself(a int not null primary key , 
               b int references tparent(a) ON DELETE CASCADE ,  
               c int references tself(a) ON DELETE SET NULL);
drop table tself;
--should pass
create table tself(a int not null primary key , 
               b int references tparent(a) ON DELETE SET NULL,  
               c int references tself(a) ON DELETE SET NULL);

drop table tself;
drop table tparent;


--two consectuvie set null  CYCLE

create table t1( a int not null primary key, b int);
create table t2(x int, y int not null unique);
create table t3(l int, m int not null unique , k int );
create table t4(s int, t int not null unique , y int );

--all should pass
--two consectuvie set null  CYCLE , but not a delete connected cylcle
alter table t1 add constraint c1 foreign key (b)
                              references t3(m) on delete CASCADE;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete SET NULL;
alter table t4 add constraint c3 foreign key (s)
                              references t2(y) on delete SET NULL;
alter table t3 add constraint c4 foreign key (k)
                              references t4(t) on delete cascade;

alter table t1 drop constraint c1;
alter table t2 drop constraint c2;
alter table t4 drop constraint c3;
alter table t3 drop constraint c4;

--two continuos set nulls , but not a cycle
alter table t3 add constraint c1 foreign key (l)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete SET NULL;
alter table t4 add constraint c3 foreign key (s)
                              references t2(y) on delete SET NULL;
alter table t4 add constraint c4 foreign key (y)
                              references t3(m) on delete cascade;

alter table t3 drop constraint c1;
alter table t2 drop constraint c2;
alter table t4 drop constraint c3;
alter table t4 drop constraint c4;

--c4 fails error case NULL followed by a cascade in the path
alter table t3 add constraint c1 foreign key (l)
                              references t1(a) on delete CASCADE;
alter table t2 add constraint c2 foreign key (x)
                             references t1(a) on delete CASCADE;
alter table t4 add constraint c3 foreign key (s)
                              references t2(y) on delete SET NULL;
alter table t4 add constraint c4 foreign key (y)
                              references t3(m) on delete cascade;

alter table t3 drop constraint c1;
alter table t2 drop constraint c2;
alter table t4 drop constraint c3;

drop table t4 ;
drop table t3 ;
drop table t2 ;
drop table t1 ;

-- t2 should fail for these 4 cases below
create table t1( a int not null primary key, b int not null  unique);
create table t2(x int references t1(a) ON DELETE RESTRICT, 
                y int not null unique, z int references t1(b) ON DELETE CASCADE);

drop table t1;
create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE SET NULL ,
y int references t1(b) ON DELETE CASCADE);

drop table t1;
create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE SET NULL ,
y int references t1(b) ON DELETE SET NULL);

drop table t1;
create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE SET NULL ,
y int references t1(b));

drop table t1;
