
--no cascade delete , just default check
create table t1(a int not null primary key);
create table t2(b int references t1(a));
insert into t1 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
delete from t1;
drop table t2;

--simple cascade delete
create table t2(b int references t1(a) ON DELETE CASCADE);
insert into t2 values (1) , (2) , (3) , (4);
delete from t1 where a =2 ;
select * from t2;
delete from t1 ;
select * from t2;

--multiple rows in the dependent table for a single row in the parent
insert into t1 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
delete from t1 where a = 3 ;
select * from t1;
delete from t1;
select * from t2;

drop table t2;
drop table t1;


--chain of cascade delete 
--every table has one depedent table referencing it
create table t1 (a int not null primary key ) ;
create table t2 (b int not null primary key  references t1(a) ON DELETE CASCADE);
create table t3 (c int not null primary key  references t2(b) ON DELETE CASCADE) ;
create table t4 (d int not null primary key  references t3(c) ON DELETE CASCADE) ;
create table t5 (e int not null primary key  references t4(d) ON DELETE CASCADE) ;

insert into t1 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t2  values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t3 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t4 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t5 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
delete from t1 where a = 5;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
delete from t1 ;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;


--check the prepared statement cascade delete
insert into t1 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t2  values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t3 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t4 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t5 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
autocommit off;
prepare sdelete as 'delete from t1 where a = ?';
execute sdelete using 'values (2)';
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
prepare sdelete1 as 'delete from t2 where b = ?';
execute sdelete1 using 'values (3)';

--Make sure the ps recompile on a DDL action
drop table t5 ;
execute sdelete using 'values (5)';
execute sdelete1 using 'values (6)';
select * from t1;
select * from t2;
select * from t3;
select * from t4;

drop table t4;
drop table t3 ;
execute sdelete using 'values (7)';
execute sdelete1 using 'values (8)';
select * from t1;
select * from t2;
remove sdelete;
remove sdelete1;
autocommit on;
delete from t1 ;
select * from t1;
select * from t2;
drop table t2 ;
drop table t1;


--two foreign keys and less number of columns on the dependent table.
create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t1(a) ON DELETE CASCADE ) ;

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) ;
insert into t2 values (3) , (6), (9) ;
insert into t3 values (1) , (4) , (7) ;
delete from t1 ;
select * from t1;
select * from t2;
select * from t3;
drop table t3;
drop table t2;
drop table t1;

--triggers on the  dependen tables 

create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t1(a) ON DELETE CASCADE) ;
create table t4(z int , op char(2));

--create triggers such a way that the all deleted row
--in t2 are inserted into t4

create trigger trig_delete after DELETE on t2
referencing old as deletedrow
for each row mode db2sql
insert into t4 values(deletedrow.x , 'bd');

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) ;
insert into t2 values (3) , (6), (9) ;
insert into t3 values (1) , (4) , (7) ;
delete from t1 ;
select * from t4;
select * from t1;
select * from t2;
select * from t3;

drop table t4;
drop table t3;
drop table t2;
drop table t1;


--test for multiple fkeys on the same table referrring to
--different columns on the parent table.

create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE CASCADE ,
y int references t1(b) ON DELETE CASCADE);
insert into t1 values(1 , 4) , (2,3) , (3, 2) , (4, 1);
insert into t2 values(1 , 4) , (2,3) , (3, 2) , (4, 1);
delete from t1;
select * from t1;
select * from t2;

drop table t2;
drop table t1;


--check for the unique nulls case
--check for sefl referencing





---ON DELETE SET NULL TEST CASES


--simple cascade delete set to null
create table t1(a int not null primary key);
create table t2(b int references t1(a) ON DELETE SET NULL);
insert into t1 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
delete from t1 where a =2 ;
select * from t2;
delete from t1 ;
select * from t2;

--multiple rows in the dependent table for a single row in the parent
insert into t1 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
delete from t1 where a = 3 ;
select * from t1;
delete from t1;
select * from t2;

drop table t2;
drop table t1;


--chain of cascade delete 
--every table has one depedent table referencing it
create table t1 (a int not null primary key ) ;
create table t2 (b int not null primary key  references t1(a) ON DELETE CASCADE);
create table t3 (c int not null primary key  references t2(b) ON DELETE CASCADE) ;
create table t4 (d int not null primary key  references t3(c) ON DELETE CASCADE) ;
create table t5 (e int references t4(d) ON DELETE SET NULL) ;

insert into t1 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t2  values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t3 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t4 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t5 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
delete from t1 where a = 5;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
delete from t1 ;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;


--check the prepared statement cascade delete
insert into t1 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t2  values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t3 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t4 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t5 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
autocommit off;
prepare sdelete as 'delete from t1 where a = ?';
execute sdelete using 'values (2)';
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
prepare sdelete1 as 'delete from t2 where b = ?';
execute sdelete1 using 'values (3)';

--Make sure the ps recompile on a DDL action
drop table t5 ;
execute sdelete using 'values (5)';
execute sdelete1 using 'values (6)';
select * from t1;
select * from t2;
select * from t3;
select * from t4;

drop table t4;
drop table t3 ;
execute sdelete using 'values (7)';
execute sdelete1 using 'values (8)';
select * from t1;
select * from t2;
remove sdelete;
remove sdelete1;
autocommit on;
delete from t1 ;
select * from t1;
select * from t2;
drop table t2 ;
drop table t1;


--two foreign keys and less number of columns on the dependent table.
create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t1(a) ON DELETE SET NULL ) ;

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) ;
insert into t2 values (3) , (6), (9) ;
insert into t3 values (1) , (4) , (7) ;
delete from t1 ;
select * from t1;
select * from t2;
select * from t3;
drop table t3;
drop table t2;
drop table t1;

--triggers on the  dependen tables 

create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int references t1(c) ON DELETE SET NULL ) ;
create table t3( y int references t1(a) ON DELETE SET NULL) ;
create table t4(z int , op char(2));

--create triggers such a way that the all deleted row
--in t2 are inserted into t4

create trigger trig_update after UPDATE on t2
referencing old as updatedrow
for each row mode db2sql
insert into t4 values(updatedrow.x , 'bu');

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) ;
insert into t2 values (3) , (6), (9) ;
insert into t3 values (1) , (4) , (7) ;
delete from t1 ;
select * from t4;
select * from t1;
select * from t2;
select * from t3;

drop table t4;
drop table t3;
drop table t2;
drop table t1;


--test for multiple fkeys on the same table referrring to
--different columns on the parent table.

create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE SET NULL ,
y int);
insert into t1 values(1 , 4) , (2,3) , (3, 2) , (4, 1);
insert into t2 values(1 , 4) , (2,3) , (3, 2) , (4, 1);
delete from t1;
select * from t1;
select * from t2;

drop table t2;
drop table t1;

create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE SET NULL ,
y int);
insert into t1 values(1 , 4) , (2,3) , (3, 2) , (4, 1);
insert into t2 values(1 , 3) , (2,3) , (3, 4) , (4, 1);
delete from t1 where a =1 ;

drop table t2;
drop table t1;
--following is ACTAULL CASCADE DELETE CASE
create table  t1(a int not null unique , b int not null unique);
create table  t2(x int references t1(a) ON DELETE CASCADE ,
y int references t1(b) ON DELETE CASCADE);
insert into t1 values(1 , 4) , (2,3) , (3, 2) , (4, 1);
insert into t2 values(1 , 3) , (2,3) , (3, 4) , (4, 1);
delete from t1 where a =1 ;
--Above delete should delete two rows.
drop table t2;
drop table t1;

create table t1 (a int not null primary key ) ;
create table t2 (b int not null primary key  references t1(a) ON DELETE NO ACTION);
insert into t1 values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
insert into t2  values (1) , (2) , (3) , (4) , (5) , (6) , (7) , (8) ;
delete from t1;
select * from t2;
drop table t2;
drop table t1;

--test for DELETE RESTRICT
--first check with an after trigger and NO ACTION
create table t1(a int not null unique, b int not null unique);
create table t2(x int references t1(a) ON DELETE NO ACTION , y int);
create trigger trig_delete after DELETE on t1
referencing old as deletedrow
for each row mode db2sql
delete from t2 where x = deletedrow.a;

insert into t1 values(1 , 2);
insert into t1 values(2 , 3);
insert into t2 values(1, 2);
insert into t2 values(2, 3);

-- should fail
-- parent row can not be deleted because of a dependent relationship from another table
delete from t1 where a =1;

drop table t2;

--do the same case as above with RESTRICT
--we should get error, because RESTRICT rules are checked before firing triggers
create table t2(x int references t1(a) ON DELETE RESTRICT , y int);
insert into t2 values(1, 2);
insert into t2 values(2, 3);

--following delete should throw constraint violations error
delete from t1 where a =1;
drop table t2;
drop table t1;

--test for ON UPDATE RESTRICT
--first check with a trigger and NO ACTION
autocommit off ;
create table t1(a int not null unique, b int not null unique);
create table t2(x int references t1(a) ON UPDATE NO ACTION , y int);
create trigger trig_update after UPDATE on t1
referencing old as old for each  row mode db2sql
update t2 set x = 2 where x = old.a;

insert into t1 values(1 , 2);
insert into t1 values(2 , 3);
insert into t2 values(1, 2);
insert into t2 values(2, 3);
commit;
-- this update should fail
-- parent row can not be deleted because of a dependent relationship from another table
update t1 set a = 7 where a =1;
-- should pass because no foreign key constraints are violated
update t1 set b = 7 where a =1;
select * from t1 ;
select * from t2 ;
rollback;
drop table t2;
commit;
--do the same case as above with RESTRICT
--we should get error, because RESTRICT is check before firing triggers
create table t2(x int references t1(a) ON UPDATE RESTRICT , y int);
insert into t2 values(1, 2);
insert into t2 values(2, 3);
commit;
--following update should throw an error
update t1 set a = 7 where a =1;
select * from t1 ;
select * from t2;
autocommit on;
drop table t2;
drop table t1;


--After ROW triggers on the  dependen tables
create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t1(a) ON DELETE CASCADE) ;
create table t4(z int , op char(2));

--create triggers such a way that the all deleted row
--in t2 are inserted into t4

create trigger trig_delete after DELETE on t2
referencing old as deletedrow
for each row mode db2sql
insert into t4 values(deletedrow.x , 'ad');

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) ;
insert into t2 values (3) , (6), (9) ;
insert into t3 values (1) , (4) , (7) ;
delete from t1 ;
select * from t4;
select * from t1;
select * from t2;
select * from t3;

drop table t4;
drop table t3;
drop table t2;
drop table t1;


--After Statement triggers on the  dependen tables
create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t1(a) ON DELETE CASCADE) ;
create table t4(z int , op char(2));

--create triggers such a way that the all deleted row
--in t2 are inserted into t4

create trigger trig_delete after DELETE on t2
REFERENCING OLD_Table AS deletedrows
for each statement mode db2sql 
insert into t4 select x, 'ad' from deletedrows;

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) ;
insert into t2 values (3) , (6), (9) ;
insert into t3 values (1) , (4) , (7) ;
delete from t1 ;
select * from t4;
select * from t1;
select * from t2;
select * from t3;

drop table t4;
drop table t3;
drop table t2;
drop table t1;


--After triggers on a self referencing table

create table emp(empno char(2) not null, mgr char(2), constraint emp primary key(empno),
  constraint manages foreign key(mgr) references emp(empno) on delete cascade);

create table tempemp(empno char(2) , mgr char(2)  , op char(2));

insert into emp values('e1', null);
insert into emp values('e2', 'e1');
insert into emp values('e3', 'e1');
insert into emp values('e4', 'e2');
insert into emp values('e5', 'e4');
insert into emp values('e6', 'e5');
insert into emp values('e7', 'e6');
insert into emp values('e8', 'e7');
insert into emp values('e9', 'e8');


create trigger trig_emp_delete after DELETE on emp
REFERENCING OLD_Table AS deletedrows
for each statement mode db2sql 
insert into tempemp select empno, mgr,  'ad' from deletedrows;

delete from emp where empno = 'e1';

select * from emp;
select * from tempemp;

drop table emp;
drop table tempemp;

-- triggers on a self referencing table

create table emp(empno char(2) not null, mgr char(2), constraint emp primary key(empno),
  constraint manages foreign key(mgr) references emp(empno) on delete cascade);

create table tempemp(empno char(2) , mgr char(2)  , op char(2));

insert into emp values('e1', null);
insert into emp values('e2', 'e1');
insert into emp values('e3', 'e1');
insert into emp values('e4', 'e2');
insert into emp values('e5', 'e4');
insert into emp values('e6', 'e5');
insert into emp values('e7', 'e6');
insert into emp values('e8', 'e7');
insert into emp values('e9', 'e8');


create trigger trig_emp_delete AFTER DELETE on emp
REFERENCING OLD_Table AS deletedrows
for each statement mode db2sql 
insert into tempemp select empno, mgr,  'bd' from deletedrows;

delete from emp where empno = 'e1';

select * from emp;
select * from tempemp;

drop table emp;
drop table tempemp;


--After triggers on a cyclic referential actions
create table t1(a int not null primary key, b int not null unique);
create table t2(x int not null primary key, y int);

insert into t1 values (1, 2);
insert into t1 values (2, 1);
insert into t2 values (1, 2);
insert into t2 values (2, 1);

insert into t1 values (3, 4);
insert into t1 values (4, 3);
insert into t2 values (3, 4);
insert into t2 values (4, 3);

insert into t1 values (6, 7);
insert into t1 values (7, 6);
insert into t2 values (6, 7);
insert into t2 values (7, 6);


alter table t1 add constraint c1 foreign key (b) 
                             references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y) 
                              references t1(b) on delete cascade;

create table t1temp(l int , m int, op char(2));

create trigger trig_cyclic_del after DELETE on t1
REFERENCING OLD_Table AS deletedrows
for each statement mode db2sql 
insert into t1temp  select a, b,  'ad' from deletedrows;

select * from t1; 
select * from t2;
---following delete should delete all the rows
delete from t1 where a = 3;
select * from t1; 
select * from t2;
select * from t1temp;
delete from t1;

select * from t1; 
select * from t2;
select * from t1temp;


drop table t1temp;
alter table t1 drop constraint c1;
drop table t2;
drop table t1;

-- triggers on a cyclic referential actions
create table t1(a int not null primary key, b int not null unique);
create table t2(x int not null primary key, y int);

insert into t1 values (1, 2);
insert into t1 values (2, 1);
insert into t2 values (1, 2);
insert into t2 values (2, 1);

insert into t1 values (3, 4);
insert into t1 values (4, 3);
insert into t2 values (3, 4);
insert into t2 values (4, 3);

insert into t1 values (6, 7);
insert into t1 values (7, 6);
insert into t2 values (6, 7);
insert into t2 values (7, 6);


alter table t1 add constraint c1 foreign key (b) 
                             references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y) 
                              references t1(b) on delete cascade;

create table t1temp(l int , m int, op char(2));

create trigger trig_cyclic_del AFTER DELETE on t1
REFERENCING OLD_Table AS deletedrows
for each statement mode db2sql 
insert into t1temp  select a, b,  'bd' from deletedrows;

select * from t1; 
select * from t2;
---following delete should delete all the rows
delete from t1 where a = 3;
select * from t1; 
select * from t2;
select * from t1temp;

delete from t1;

select * from t1; 
select * from t2;
select * from t1temp;


drop table t1temp;
alter table t1 drop constraint c1;
drop table t2;
drop table t1;


--ROW triggers on a cyclic referential actions
create table t1(a int not null primary key, b int not null unique);
create table t2(x int not null primary key, y int);

insert into t1 values (1, 2);
insert into t1 values (2, 1);
insert into t2 values (1, 2);
insert into t2 values (2, 1);

insert into t1 values (3, 4);
insert into t1 values (4, 3);
insert into t2 values (3, 4);
insert into t2 values (4, 3);

insert into t1 values (6, 7);
insert into t1 values (7, 6);
insert into t2 values (6, 7);
insert into t2 values (7, 6);


alter table t1 add constraint c1 foreign key (b) 
                             references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y) 
                              references t1(b) on delete cascade;

create table t1temp(l int , m int, op char(2));

create trigger trig_cyclic_del1 after DELETE on t1
referencing old as deletedrow
for each row mode db2sql
insert into t1temp values(deletedrow.a , deletedrow.b, 'ad');

create trigger trig_cyclic_del2 AFTER DELETE on t1
referencing old as deletedrow
for each row mode db2sql
insert into t1temp values(deletedrow.a , deletedrow.b, 'bd');

select * from t1; 
select * from t2;
---following delete should delete all the rows
delete from t1 where a = 1;
select * from t1; 
select * from t2;
select * from t1temp;
delete from t1;

select * from t1; 
select * from t2;
select * from t1temp;

drop table t1temp;
alter table t1 drop constraint c1;
drop table t2;
drop table t1;

--SET NULL UPDATE  STETEMENT triggers on a self referencing table

create table emp(empno char(2) not null, mgr char(2), constraint emp primary key(empno),
  constraint manages foreign key(mgr) references emp(empno) on delete set null);

create table tempemp(empno char(2) , mgr char(2)  , op char(2));

insert into emp values('e1', null);
insert into emp values('e2', 'e1');
insert into emp values('e3', 'e1');
insert into emp values('e4', 'e2');
insert into emp values('e5', 'e4');
insert into emp values('e6', 'e5');
insert into emp values('e7', 'e6');
insert into emp values('e8', 'e7');
insert into emp values('e9', 'e8');


create trigger trig_emp_delete AFTER UPDATE on emp
REFERENCING OLD_Table AS updatedrows
for each statement mode db2sql 
insert into tempemp select empno, mgr,  'bu' from updatedrows;

create trigger trig_emp_delete1 AFTER UPDATE on emp
REFERENCING NEW_Table AS updatedrows
for each statement mode db2sql 
insert into tempemp select empno, mgr,  'au' from updatedrows;

delete from emp where empno = 'e1';

select * from emp;
select * from tempemp;

drop table emp;
drop table tempemp;

--SET NULL UPDATE  ROW triggers on a self referencing table

create table emp(empno char(2) not null, mgr char(2), constraint emp primary key(empno),
  constraint manages foreign key(mgr) references emp(empno) on delete set null);

create table tempemp(empno char(2) , mgr char(2)  , op char(2));

insert into emp values('e1', null);
insert into emp values('e2', 'e1');
insert into emp values('e3', 'e1');
insert into emp values('e4', 'e2');
insert into emp values('e5', 'e4');
insert into emp values('e6', 'e5');
insert into emp values('e7', 'e6');
insert into emp values('e8', 'e7');
insert into emp values('e9', 'e8');


create trigger trig_emp_delete after UPDATE on emp
REFERENCING OLD AS updatedrow
for each row mode db2sql
insert into tempemp values(updatedrow.empno, updatedrow.mgr, 'bu');

create trigger trig_emp_delete1 AFTER UPDATE on emp
REFERENCING NEW AS updatedrow
for each  row mode db2sql
insert into tempemp values(updatedrow.empno, updatedrow.mgr, 'au');

delete from emp where empno = 'e1';

select * from emp;
select * from tempemp;

delete from emp;
select * from emp;
select * from tempemp;

drop table emp;
drop table tempemp;

-- prepared statements check like in cview
create table t1(a int not null primary key);
create table t2(b int references t1(a) ON DELETE SET NULL);
insert into t1 values (1) , (2) , (3) , (4) ;
insert into t2 values (1) , (2) , (3) , (4) ;
autocommit off;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (2)';
commit;
select * from t2;
execute sdelete using 'values (3)';
execute sdelete using 'values (4)';
commit;
remove sdelete;
drop table t2;
create table t2(b int references t1(a) ON DELETE CASCADE);
insert into t1 values (1) , (2) , (3) , (4) ;
insert into t2 values (1) , (2) , (3) , (4) ;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (2)';
commit;
select * from t2;
execute sdelete using 'values (3)';
execute sdelete using 'values (4)';
commit;
remove sdelete;
drop table t2;
drop table t1;
autocommit on;
--make sure prepared statements are recompiled after a DDL changes works
create table t1(a int not null primary key);
create table t2(b int references t1(a) ON DELETE CASCADE, c int);
insert into t1 values (1) , (2) , (3) , (4) ;
insert into t2 values (1, 1) , (2, 2) , (3, 3) , (4, 4) ;
autocommit off;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (2)';
commit;
select * from t2;
create index idx1 on t2(c) ;
execute sdelete using 'values (3)';
execute sdelete using 'values (4)';
commit;
drop table t2;
commit;
insert into t1 values(5);
execute sdelete using 'values (5)';
select * from t1;
remove sdelete;
autocommit on;
drop table t1;
commit;

--do some rollbacks that involved prepared statement executtions
create table t1(a int not null primary key);
create table t2(b int references t1(a) ON DELETE CASCADE, c int);
insert into t1 values (1) , (2) , (3) , (4) ;
insert into t2 values (1, 1) , (2, 2) , (3, 3) , (4, 4) ;
commit;
autocommit off;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (2)';
rollback;
select * from t2;
execute sdelete using 'values (3)';
create index idx1 on t2(c) ;
execute sdelete using 'values (4)';
commit;
select * from t1;
select * from t2;
drop table t2;
rollback;
insert into t1 values(5);
execute sdelete using 'values (5)';
select * from t1;
select * from t2;
remove sdelete;
autocommit on;
drop table t2;
drop table t1;

---UNIQUE COLUMN NOT NULL VALUE CHECKS
--delete cascade on non-nullable unique column
create table t1 ( a int not null unique) ;
insert into t1 values(0) ;
insert into t1 values(1) ;
insert into t1 values(2) ;
create table t2(b int references t1(a) ON DELETE CASCADE) ;
insert into t2 values(null) ;
insert into t2 values(null) ;
insert into t2 values(null) ;
insert into t2 values(null) ;
insert into t2 values(null) ;
insert into t2 values(null) ;
insert into t2 values(null) ;
select * from t1 ;
select * from t2 ;

delete from t1 where a = 0 ;

select * from t1 ;
-- null values from t1 are not deleted
select * from t2 ;


drop table t2;
drop table t1;

--self ref foreign key without null values

create table t1( a int not null unique , b int references t1(a) 
ON DELETE SET NULL);

insert into t1 values ( 1 , null) ;
delete from t1 where b is null ;

select * from t1 ;

drop table t1 ;

create table t1( a int not null unique , b int references t1(a) 
ON DELETE CASCADE);
insert into t1 values ( 1 , null) ;
insert into t1 values ( 0 , 1) ;
delete from t1 where b is null ;
select * from t1 ;
drop table t1 ;

--mutiple tables
create table parent( a int not null unique) ;
create table child1(b int not null unique references parent(a)
ON DELETE CASCADE);
create table child2(c int not null unique references child1(b)
ON DELETE CASCADE);

insert into parent values(0) ;
insert into parent values(1) ;
insert into parent values(2) ;
insert into child1 values(0) ;
insert into child1 values(1) ;
insert into child1 values(2) ;
insert into child2 values(0) ;
insert into child2 values(1) ;
insert into child2 values(2) ;

select * from parent ;
select * from child1;
select * from child2 ;

delete from parent where a = 1 ;
select * from parent ;
select * from child1;
select * from child2 ;

delete from parent where a = 0 ;
select * from parent ;
select * from child1;

--delete all the rows
delete from parent;

drop table child2;
create table child2(c int references child1(b)
ON DELETE SET NULL);

insert into parent values(0) ;
insert into parent values(1) ;
insert into parent values(2) ;
insert into child1 values(0) ;
insert into child1 values(1) ;
insert into child1 values(2) ;
insert into child2 values(null) ;
insert into child2 values(1) ;
insert into child2 values(2) ;

select * from parent ;
select * from child1;
select * from child2 ;

delete from parent where a = 1 ;
select * from parent ;
select * from child1;
select * from child2;

delete from parent where a = 0;
select * from parent ;
select * from child1;
select * from child2;

delete from child2 where c is null;
delete from child2 where c is not null;
delete from parent where a = 2 ;


select * from parent ;
select * from child1;
select * from child2;

delete from parent;
delete from child1;
delete from child2;

drop table child2;
drop table child1;
drop table parent;



--foreign key on two non-nullable unique keys
create table t1(a int not null unique , b int not null unique) ;
alter table t1 add constraint c2 unique(a , b ) ;

create table t2( x1 int , x2 int , constraint c1 foreign key (x1, x2)
references t1(a , b ) ON DELETE CASCADE ) ;
insert into t1 values (0 , 1) ;
insert into t1 values (1 , 2) ;
insert into t2 values (0 , 1) ;
insert into t2 values (1 , 2) ;

delete from t1 where a = 0;
select * from t1 ;
select * from t2 ;

insert into t1 values (0 , 0) ;
insert into t2 values (0 , 0) ;

delete from t1 where a = 0;
select * from t1 ;
select * from t2 ;

delete from t1;
drop table t2 ;

create table t2( x1 int , x2 int , constraint c1 foreign key (x1, x2)
references t1(a , b ) ON DELETE SET NULL ) ;
insert into t1 values (0 , 1) ;
insert into t1 values (1 , 2) ;
insert into t2 values (0 , 1) ;
insert into t2 values (1 , 2) ;

select * from t1 ;
select * from t2 ;

delete from t1 where a = 0;
select * from t1 ;
select * from t2 ;

drop table t2 ;
drop table t1;

--cyclic non-nulls case
create table t1(a int not null unique, b int not null unique);
create table t2(x int not null unique, y int not null unique);

insert into t1 values (0, 2);
insert into t1 values (2, 0);
insert into t2 values (0, 2);
insert into t2 values (2, 0);

insert into t1 values (3, 4);
insert into t1 values (4, 3);
insert into t2 values (3, 4);
insert into t2 values (4, 3);

insert into t1 values (6, 7);
insert into t1 values (7, 6);
insert into t2 values (6, 7);
insert into t2 values (7, 6);

insert into t1 values (9, 10);
insert into t1 values (10, 9);
insert into t2 values (9, 10);
insert into t2 values (10, 9);

alter table t1 add constraint c1 foreign key (b)
			      references t2(x) on delete cascade;
alter table t2 add constraint c2 foreign key (y)
                              references t1(b) on delete cascade;

select * from t1;
select * from t2;
delete from t1 where a = 0 ;
select * from t1;
select * from t2;

delete from t2 where x=3 ;
select * from t1;
select * from t2;

delete from t1 where b = 9;
select * from t1;
select * from t2;
delete from t2;
select * from t1;
select * from t2;

alter table t1 drop constraint c1;
drop table t2;
drop table t1;

--END OF NULL CHECK

--BEGIN NON NULL ERROR CHECK FOR ON DELETE SET NULL
--do not allow ON DELETE SET NULL on non nullable foreign key columns
create table n1 ( a int not null primary key);
create table n2 ( b int not null primary key references n1(a) ON DELETE SET NULL);
drop table n1;

create table n1 ( a int not null unique);
create table n2 ( b int not null references n1(a) ON DELETE SET NULL);
drop table n1;
--multi column foreign key reference
create table n1(a int not null , b int not null);
create table n2(x int not null, y int not null) ;
alter table n1 add constraint c1 unique(a, b) ;
alter table n2 add constraint c2 foreign key(x, y) 
references n1(a,b) ON  DELETE SET NULL ;
drop table n1;
drop table n2;
--just make sure we are allowing SET NULL on nullable columns
create table n1(a int not null , b int not null);
create table n2(x int, y int) ;
alter table n1 add constraint c1 unique(a, b) ;
alter table n2 add constraint c2 foreign key(x, y) 
references n1(a,b) ON  DELETE SET NULL ;
drop table n2;
drop table n1;
--make sure  ON DELETE CASCADE works fine
create table n1(a int not null , b int not null);
create table n2(x int not null, y int not null) ;
alter table n1 add constraint c1 unique(a, b) ;
alter table n2 add constraint c2 foreign key(x, y) 
references n1(a,b) ON  DELETE CASCADE;
drop table n2;
drop table n1;
--only some coulmns of foreign key are nullable
create table n1(a int not null , b int not null, c int not null , 
               d int not null , e int not null);
create table n2(c1 int not null, c2 int not null, c3 int , c4 int,
                c5 int not null, c6 int ) ;
alter table n1 add constraint c1 unique(b, c, d, e) ;
alter table n2 add constraint c2 foreign key(c2, c3, c4, c5)
references n1(b, c, d, e) ON  DELETE SET NULL ;

insert into n1 values(1 , 2, 3, 4, 5);
insert into n1 values(21, 22, 23, 24, 25);
insert into n1 values(6, 7 , 8, 9, 10);
insert into n1 values(100 , 101, 102, 103, 104);
insert into n2 values(111, 2, 3, 4, 5, 0);
insert into n2 values(212, 22, 23, 24, 25, 0);
insert into n2 values(6, 7 , 8, 9, 10, 0);
select * from n1;
select * from n2;
delete from n1 where e =10;
select * from n1 ;
select * from n2;
delete from n1 where a =1;
select * from n1;
select * from n2;
delete from n1;
select * from n1;
select * from n2;

drop table n2;
drop table n1;

--END NON NULL ERROR CHECK


create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int not null unique references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t2(x) ON DELETE CASCADE) ;

create trigger trig_delete after DELETE on t1
referencing old as deletedrow
for each  row mode db2sql
delete from t2; 

create trigger trig_delete1 after DELETE on t2
referencing old as deletedrow
for each row mode db2sql
delete from t3;

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) , (10,11,12), 
       (13,14,15), (16,17,18), (19, 20, 21), (22, 23, 24), (25,26,27);
insert into t2 values (3) , (6), (9), (12), (15), (18), (21), (24), (27);
insert into t3 values (3) , (6), (9), (12), (15), (18), (21), (24), (27);

autocommit off;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (4)';
execute sdelete using 'values (7)';
execute sdelete using 'values (10)';
execute sdelete using 'values (13)';
execute sdelete using 'values (16)';
execute sdelete using 'values (19)';
execute sdelete using 'values (22)';
execute sdelete using 'values (25)';
commit;
autocommit on;
select * from t1 ;
select * from t2 ;
select * from t3;

drop table t3;
drop table t2;
drop table t1;

--checks for bug fix for 4743

create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int not null unique references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t2(x) ON DELETE NO ACTION) ;

create trigger trig_delete after DELETE on t1
referencing old as deletedrow
for each row mode db2sql
delete from t2; 

create trigger trig_delete1 after DELETE on t2
referencing old as deletedrow
for each row mode db2sql
delete from t3;

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) , (10,11,12), 
       (13,14,15), (16,17,18), (19, 20, 21), (22, 23, 24), (25,26,27);
insert into t2 values (3) , (6), (9), (12), (15), (18), (21), (24), (27);
insert into t3 values (3) , (6), (9), (12), (15), (18), (21), (24), (27);

-- should fail
-- parent row can not be deleted because of a dependent relationship from another table
autocommit off;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (4)';
execute sdelete using 'values (7)';
execute sdelete using 'values (10)';
execute sdelete using 'values (13)';
execute sdelete using 'values (16)';
execute sdelete using 'values (19)';
execute sdelete using 'values (22)';
execute sdelete using 'values (25)';
commit;
autocommit on;
select * from t1 ;
select * from t2 ;
select * from t3;

drop table t3;
drop table t2;
drop table t1;

create table t1( a int not null primary key , b int , c int not null unique) ;
create table t2( x int not null unique references t1(c) ON DELETE CASCADE ) ;
create table t3( y int references t2(x) ON DELETE NO ACTION) ;

insert into t1 values (1, 2, 3), (4,5,6) , (7,8,9) , (10,11,12), 
       (13,14,15), (16,17,18), (19, 20, 21), (22, 23, 24), (25,26,27);
insert into t2 values (3) , (6), (9), (12), (15), (18), (21), (24), (27);
insert into t3 values (3) , (6), (9), (12), (15), (18), (21), (24), (27);

autocommit off;
prepare sdelete as 'delete from t1 where a = ?' ;
execute sdelete using 'values (1)';
execute sdelete using 'values (4)';
execute sdelete using 'values (7)';
execute sdelete using 'values (10)';
execute sdelete using 'values (13)';
execute sdelete using 'values (16)';
execute sdelete using 'values (19)';
execute sdelete using 'values (22)';
execute sdelete using 'values (25)';
commit;
autocommit on;
select * from t1 ;
select * from t2 ;
select * from t3;

drop table t3;
drop table t2;
drop table t1;
--bug5186; mutiple cascade paths , execute a delete where
--one path does not qualify any rows.
create table t1 (c1 int not null primary key ) ;
create table t2 (c1 int not null primary key  references t1(c1) ON DELETE CASCADE);
create table t3 (c1 int references t2(c1) ON DELETE CASCADE,
                 c2 int references t1(c1) ON DELETE CASCADE);

insert into t1 values(1);
insert into t1 values(2);
insert into t2 values(2);
insert into t3 values(2, 1) ;
delete from t1 where c1 = 1 ;

--now make sure that we havw rows in both the paths and get meged properly
insert into t1 values(1);
insert into t1 values(3);
insert into t2 values(1);

insert into t3 values(2, 1) ;
insert into t3 values(1, 2) ;
insert into t3 values(2, 3) ;

delete from t1 where c1 = 1 ;
select * from t3 ;
delete from t1 ;
---now create a statement trigger and see what happens on a empty delete.
create table t4(c1 char (20));

create trigger trig_delete after DELETE on t3
for each statement mode db2sql
insert into t4 values('ad');

delete from t1 ;
select * from t4 ;
drop trigger trig_delete;
delete from t4 ;
create trigger trig_delete after DELETE on t3
for each statement mode db2sql
insert into t4 values('bd');
delete from t1 ;
delete from t1 ;
select * from t4 ;
drop trigger trig_delete;
delete from t4 ;
--row level trigger case
drop table t4;
create table t4(z int not null primary key , op char(2));
create trigger trig_delete after DELETE on t3
referencing old as deletedrow
for each row mode db2sql
insert into t4 values(deletedrow.c1 , 'bd');
delete from t1 ;
delete from t1 ;
select * from t4 ;
insert into t1 values(1);
insert into t1 values(2);
insert into t2 values(2);
insert into t3 values(2, 1) ;
delete from t1 where c1 = 1 ;
select * from t4 ;
delete from t4;
insert into t1 values(1);
insert into t1 values(3);
insert into t2 values(1);

insert into t3 values(2, 1) ;
insert into t3 values(1, 2) ;
insert into t3 values(2, 3) ;

delete from t1 where c1 = 1 ;
select * from t4 ;
drop table t4;
drop table t3;
drop table t2;
drop table t1;

---multiple foreign keys pointing to the same table and has  dependens
-- first foreign key path has zero rows qualified(bug 5197 from webshphere)
CREATE SCHEMA DB2ADMIN;
SET SCHEMA DB2ADMIN;
CREATE TABLE DB2ADMIN.PAGE_INST
   (
      OID BIGINT NOT NULL ,
      IS_ACTIVE CHAR(1) DEFAULT 'Y' NOT NULL ,
      IS_SYSTEM CHAR(1) DEFAULT 'N' NOT NULL ,
      IS_SHARED CHAR(1) DEFAULT 'N' NOT NULL ,
      ALL_PORT_ALLOWED CHAR(1) DEFAULT 'Y' NOT NULL ,
      PARENT_OID BIGINT,
      CONT_PARENT_OID BIGINT,
      SKIN_DESC_OID BIGINT,
      THEME_DESC_OID BIGINT,
      CREATE_TYPE CHAR(1) DEFAULT 'E' NOT NULL ,
      TYPE INT NOT NULL ,
      CREATED BIGINT NOT NULL ,
      MODIFIED BIGINT NOT NULL
   );

CREATE TABLE DB2ADMIN.PORT_WIRE
   (
      OID BIGINT NOT NULL ,
      CREATED BIGINT NOT NULL ,
      MODIFIED BIGINT NOT NULL ,
      USER_DESC_OID BIGINT NOT NULL ,
      ORDINAL INT NOT NULL ,
      SRC_COMPOS_OID BIGINT NOT NULL ,
      SRC_PORT_INST_OID BIGINT NOT NULL ,
      SRC_PORT_PARM_OID BIGINT,
      SRC_PORT_PROP_OID BIGINT,
      TGT_COMPOS_OID BIGINT NOT NULL ,
      TGT_PORT_INST_OID BIGINT NOT NULL ,
      TGT_PORT_PARM_OID BIGINT,
      TGT_PORT_PROP_OID BIGINT,
      VERSION VARCHAR(255),
      EXTRA_DATA VARCHAR(1024)
   );


CREATE TABLE DB2ADMIN.PORT_WIRE_LOD
   (
      PORT_WIRE_OID BIGINT NOT NULL ,
      LOCALE VARCHAR(64) NOT NULL ,
      TITLE VARCHAR(255),
      DESCRIPTION VARCHAR(1024)
   );

ALTER TABLE DB2ADMIN.PAGE_INST
   ADD CONSTRAINT PK280 Primary Key (
      OID);

ALTER TABLE DB2ADMIN.PORT_WIRE
   ADD CONSTRAINT PK930 Primary Key (
      OID);

ALTER TABLE DB2ADMIN.PORT_WIRE
   ADD CONSTRAINT FK930B Foreign Key (
      SRC_COMPOS_OID)
   REFERENCES PAGE_INST (
      OID)
      ON DELETE CASCADE
      ON UPDATE NO ACTION;

ALTER TABLE DB2ADMIN.PORT_WIRE
   ADD CONSTRAINT FK930F Foreign Key (
      TGT_COMPOS_OID)
   REFERENCES PAGE_INST (
      OID)
      ON DELETE CASCADE
      ON UPDATE NO ACTION;

ALTER TABLE DB2ADMIN.PORT_WIRE_LOD
   ADD CONSTRAINT FK940 Foreign Key (
      PORT_WIRE_OID)
   REFERENCES PORT_WIRE (
      OID)
      ON DELETE CASCADE
      ON UPDATE NO ACTION;

INSERT INTO DB2ADMIN.PAGE_INST (OID, CREATED, MODIFIED, TYPE)
    VALUES (1301, 0, 0, 5555);
INSERT INTO DB2ADMIN.PAGE_INST (OID, CREATED, MODIFIED, TYPE)
    VALUES (1302, 0, 0, 5555);

INSERT INTO DB2ADMIN.PORT_WIRE (OID, CREATED, MODIFIED, 
    USER_DESC_OID, ORDINAL, SRC_COMPOS_OID, SRC_PORT_INST_OID, 
    TGT_COMPOS_OID, TGT_PORT_INST_OID)
    VALUES (2001, 0, 0, 1401, 1, 1301, 1202, 1302, 1203);

INSERT INTO DB2ADMIN.PORT_WIRE_LOD (PORT_WIRE_OID, 
                        LOCALE, TITLE, DESCRIPTION)
    VALUES (2001, 'en', 'TestPortletWire', 'blahblah');

DELETE FROM DB2ADMIN.PAGE_INST WHERE OID = 1302;

select * from DB2ADMIN.PAGE_INST;
select * from DB2ADMIN.PORT_WIRE;
select * from DB2ADMIN.PORT_WIRE_LOD;

INSERT INTO DB2ADMIN.PAGE_INST (OID, CREATED, MODIFIED, TYPE)
    VALUES (1302, 0, 0, 5555);
INSERT INTO DB2ADMIN.PORT_WIRE (OID, CREATED, MODIFIED, 
    USER_DESC_OID, ORDINAL, SRC_COMPOS_OID, SRC_PORT_INST_OID, 
    TGT_COMPOS_OID, TGT_PORT_INST_OID)
    VALUES (2001, 0, 0, 1401, 1, 1301, 1202, 1302, 1203);
INSERT INTO DB2ADMIN.PORT_WIRE_LOD (PORT_WIRE_OID, 
                        LOCALE, TITLE, DESCRIPTION)
    VALUES (2001, 'en', 'TestPortletWire', 'blahblah');

DELETE FROM DB2ADMIN.PAGE_INST WHERE OID = 1301;
select * from DB2ADMIN.PAGE_INST;
select * from DB2ADMIN.PORT_WIRE;
select * from DB2ADMIN.PORT_WIRE_LOD;

drop table DB2ADMIN.PORT_WIRE_LOD;
drop table DB2ADMIN.PORT_WIRE;
drop table DB2ADMIN.PAGE_INST;
drop schema DB2ADMIN restrict;
