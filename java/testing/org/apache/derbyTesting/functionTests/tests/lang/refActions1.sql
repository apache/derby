-- #########################################################################
--- some Test cases from IBM DB2. Reusing them to test Cloudscape
-- # MODIFIED BY      :     WHO       WHEN             WHY
-- #                  : ----------- ------- -------------------------------
-- #                  : P. Selinger 950128  new testcase
-- #                  : M. Snowbell 970716  beef up for better MPP coverage  
-- #                  :                     - add first column for partition
-- #                  :                     - add rows
-- #                  : Suresh T    020616  - Modified for Cloudscape
-- #                  : Mark C      040309  - Modified for DB2 Cloudscape
-- #########################################################################
-- # TEST CASE        : cself301.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases where of delete statements
-- #                  : where the deleted table also appears in a
-- #                  : subquery that qualifies which rows are changed
-- #                  : and the deleted table has a self-ref'g RI const.
-- *************************************************************************

create schema db2test;
set schema db2test;

-- "START OF TESTCASE: cself301.sql";
autocommit off;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10));
insert into db2test.dept values (1, 'K55', 'DB');
insert into db2test.dept values (2, 'K52', 'OFC');
insert into db2test.dept values (3, 'K51', 'CS');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp on delete
  cascade, dno char(3) references db2test.dept on delete
  set null);
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.origemp on delete
  cascade, dno char(3) references db2test.origdept
  on delete set null );
insert into db2test.origemp select * from db2test.emp;

-- "END OF TESTUNIT: 01";

-- *************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on same table on RI cascade col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.emp where name in (select name from db2test.emp
   where mgrname = 'JOHN') order by 2, 3, 4;
delete from db2test.emp where name in (select name from db2test.emp
   where mgrname = 'JOHN');
select * from db2test.emp order by name, mgrname, dno;

-- "END OF TESTUNIT: 02";


-- *************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : delete with 2 levels of SQ and self-ref in 2nd
--                  : correlated to 1st SQ on foreign key
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
select * from db2test.emp where dno in (select dno from db2test.dept D
   where D.dno in (select dno from db2test.emp E where E.dno = D.dno
   and e.mgrname = 'JOHN')) order by 2, 3, 4;
delete from db2test.emp where dno in (select dno from db2test.dept D
   where D.dno in (select dno from db2test.emp E where E.dno = D.dno
   and e.mgrname = 'JOHN'));
select * from db2test.emp order by name, mgrname, dno;

-- "END OF TESTUNIT: 03";

-- *************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : delete with SQ GB Having SQ on
--                  : modified table
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
select * from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from db2test.emp
  where mgrname = 'ASHOK')) order by 2, 3, 4;
delete from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from db2test.emp
  where mgrname = 'ASHOK'));
select * from db2test.emp order by name, mgrname, dno;
-- "END OF TESTUNIT: 04";

-- *************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : delete with SQ GB Having SQ on
--                  : modified table -- 3 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
select * from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from
   db2test.dept D where dno in (select dno from db2test.emp E2
   where D. dno = E2.dno))) order by 2, 3, 4;
delete from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from
   db2test.dept D where dno in (select dno from db2test.emp E2
   where D. dno = E2.dno)));
select * from db2test.emp order by name, mgrname, dno;
-- "END OF TESTUNIT: 05";

-- *************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : delete on view with SQ GB Having SQ on
--                  : modified table -- 3 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
create view db2test.vemp (vc0, vname, vmgrname, vdno) as
  select * from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from
   db2test.dept D where dno in (select dno from db2test.emp E2
   where D. dno = E2.dno)));
select * from db2test.vemp order by 2, 3, 4;
delete from db2test.vemp;
select * from db2test.emp order by name, mgrname, dno;
-- "END OF TESTUNIT: 06";

-- "cleanup";
drop view db2test.vemp;
drop table db2test.emp;
drop table db2test.origemp;
drop table db2test.dept;
drop table db2test.origdept;
--
rollback;

-- #########################################################################
-- # TESTCASE NAME    : cself302.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases of delete statements
-- #                  : where the deleted table is connected to other
-- #                  : tables in the query by cascade on delete.
-- #                  : shape of the RI tree is a 1 level star fanout.
-- #########################################################################
-- "START OF TESTCASE: cself302.sql";

autocommit off;
-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10));
insert into db2test.dept values (1, 'K55', 'DB');
insert into db2test.dept values (2, 'K52', 'OFC');
insert into db2test.dept values (3, 'K51', 'CS');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3));
insert into db2test.origemp select * from db2test.emp;

create table db2test.secondemp (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp on delete cascade,
  dno char(3)  references db2test.origdept
  on delete cascade );
insert into db2test.secondemp select * from db2test.emp;
commit;
-- "END OF TESTUNIT: 01";

-- *************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on same table on RI cascade col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.dept where dno in (select dno from db2test.emp
   where mgrname = 'JOHN') order by 2,3;
delete from db2test.dept where dno in (select dno from db2test.emp
   where mgrname = 'JOHN');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;

-- "END OF TESTUNIT: 02";


-- *************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : delete with 2 levels of SQ and self-ref in 2nd
--                  : correlated to 1st SQ on foreign key
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
select * from db2test.dept where dno in (select dno from
   db2test.secondemp E
   where E.dno in (select dno from db2test.emp D where E.dno = D.dno
   and D.dno = 'K55')) order by 2, 3;
delete from db2test.dept where dno in (select dno
   from db2test.secondemp E
   where E.dno in (select dno from db2test.emp D where E.dno = D.dno
   and D.dno = 'K55'));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;

-- "END OF TESTUNIT: 03";

-- *************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : delete with SQ GB Having SQ on
--                  : child table
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
select * from db2test.dept where exists ( select max(mgrname) from
  db2test.secondemp group by dno having dno in (select dno from
  db2test.emp where mgrname = 'ASHOK')) order by 2, 3;
delete from db2test.dept where exists ( select max(mgrname) from
  db2test.secondemp group by dno having dno in (select dno from
  db2test.emp where mgrname = 'ASHOK'));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
-- "END OF TESTUNIT: 04";

-- *************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : above -- 7 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
commit;
select * from db2test.dept dtop where exists
  (select * from db2test.secondemp  where exists
     (select dno from
      db2test.dept D1 where dno = dtop.dno and dno in
         (select dno from db2test.emp E2 where D1.dno = E2.dno and dno in
         (select dno from db2test.emp E3 where E2.dno = E3.dno and dno in
         (select dno from db2test.emp E4 where E3.dno = E4.dno and dno in
         (select dno from db2test.emp E5 where E4.dno = E5.dno and dno in
         (select dno from db2test.emp E6 where E5.dno = E6.dno)
      )))))) order by 2, 3;
-- begin RJC950405 (commented out and added ; to the following line as I think we're fixed?)
-- -- testcase commented out pending defect fix;
-- end RJC950405
 delete from db2test.dept where exists
  (select * from db2test.secondemp  where exists
     (select dno from
      db2test.dept D1 where dno = db2test.dept.dno and dno in
         (select dno from db2test.emp E2 where D1.dno = E2.dno and dno in
         (select dno from db2test.emp E3 where E2.dno = E3.dno and dno in
         (select dno from db2test.emp E4 where E3.dno = E4.dno and dno in
         (select dno from db2test.emp E5 where E4.dno = E5.dno and dno in
         (select dno from db2test.emp E6 where E5.dno = E6.dno)
      ))))));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
-- "END OF TESTUNIT: 05";

-- *************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : on second child table, delete cascade on each
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
-- create a second child table like emp
create table db2test.emp2 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp2 select * from db2test.emp;
commit;
select * from db2test.dept dtop where exists
    (select dno from db2test.emp2 E2 where Dtop.dno = E2.dno and dno in
    (select dno from db2test.emp E3 where E2.dno = E3.dno))
    order by 2, 3;
delete from db2test.dept where exists
    (select dno from db2test.emp2 E2 where db2test.dept.dno = E2.dno and dno in
    (select dno from db2test.emp E3 where E2.dno = E3.dno));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 06";


-- *************************************************************************
-- TESTUNIT         : 07
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : on second child table, delete cascade on each
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 07";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
commit;
select * from db2test.dept dtop where exists
    (select dno from db2test.emp2 E2 where dtop.dno = E2.dno)
     and exists
    (select dno from db2test.emp E3 where dtop.dno = E3.dno)
    order by 2,3;
delete from db2test.dept  where exists
    (select dno from db2test.emp2 E2 where db2test.dept.dno = E2.dno)
     and exists
    (select dno from db2test.emp E3 where db2test.dept.dno = E3.dno);
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 07";

-- *************************************************************************
-- TESTUNIT         : 08
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : on second child table, delete cascade on each
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 08";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.secondemp;
delete from db2test.dept;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
select * from db2test.dept dtop where exists
    (select dno from db2test.emp2 E2 where dtop.dno = E2.dno)
     or exists
    (select dno from db2test.emp E3 where dtop.dno = E3.dno)
    order by 2, 3;
delete from db2test.dept where exists
    (select dno from db2test.emp2 E2 where db2test.dept.dno = E2.dno)
     or exists
    (select dno from db2test.emp E3 where db2test.dept.dno = E3.dno);
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 08";

-- *************************************************************************
-- TESTUNIT         : 09
-- DESCRIPTION      : delete on view with SQ GB Having SQ on
--                  : modified table -- 3 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 09";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
create view db2test.vempjoin (vname1, vname2, vmgrname, vdno) as
  select e.name, e2.name, e.mgrname, e.dno
  from db2test.emp e, db2test.emp2 e2
  where e.dno = e2.dno;
commit;
select * from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52') order by 2, 3;
delete from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 09";

-- *************************************************************************
-- TESTUNIT         : 10
-- DESCRIPTION      : delete on iudt where SQ on
--                  : view with join on 15 child tables
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 10";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
-- create a third child table like emp
create table db2test.emp3 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp3 select * from db2test.emp;
-- create a 4th child table like emp
create table db2test.emp4 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp4 select * from db2test.emp;
-- create a 5th child table like emp
create table db2test.emp5 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp5 select * from db2test.emp;
-- create a 6th child table like emp
create table db2test.emp6 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp6 select * from db2test.emp;
-- create a 7th child table like emp
create table db2test.emp7 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp7 select * from db2test.emp;
-- create a 8th child table like emp
create table db2test.emp8 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp8 select * from db2test.emp;
-- create a 9th child table like emp
create table db2test.emp9 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp9 select * from db2test.emp;
-- create a 10th child table like emp
create table db2test.emp10 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp10 select * from db2test.emp;
-- create a 11th child table like emp
create table db2test.emp11 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp11 select * from db2test.emp;
-- create a 12th child table like emp
create table db2test.emp12 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp12 select * from db2test.emp;
-- create a 13th child table like emp
create table db2test.emp13 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp13 select * from db2test.emp;
-- create a 14th child table like emp
create table db2test.emp14 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp14 select * from db2test.emp;
-- create a 15th child table like emp
create table db2test.emp15 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete cascade,
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp15 select * from db2test.emp;
create view db2test.vempjoin12 (vname1, vname2, vname3, vname4, vname5,
   vname6, vname7, vname8, vname9, vname10, vname11, vname12,
   vmgrname, vdno) as
  select e.name, e2.name, e3.name, e4.name, e5.name, e6.name, e7.name,
   e8.name, e9.name, e10.name, e11.name, e12.name,
   e.mgrname, e.dno
  from db2test.emp e, db2test.emp2 e2, db2test.emp3 e3, db2test.emp4 e4,
  db2test.emp5 e5, db2test.emp6 e6, db2test.emp7 e7, db2test.emp8 e8,
  db2test.emp9 e9, db2test.emp10 e10, db2test.emp11 e11,
  db2test.emp12 e12
  where e.dno = e2.dno
  and e.dno = e2.dno
  and e.dno = e2.dno
  and e.dno = e3.dno
  and e.dno = e4.dno
  and e.dno = e5.dno
  and e.dno = e6.dno
  and e.dno = e7.dno
  and e.dno = e8.dno
  and e.dno = e9.dno
  and e.dno = e10.dno
  and e.dno = e11.dno
  and e.dno = e12.dno;
commit;

-- FOLLOWING TWO QUERIES HANG IN CLOUDSCAPE NOW ..
-- UNCOMMENT once they pass.
-- select * from db2test.dept where dno in (select vdno from
--  db2test.vempjoin12)
--  and dno in ('K55', 'K52') order by 2, 3;

--delete from db2test.dept where dno in (select vdno from
--  db2test.vempjoin12)
--  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;
select * from db2test.emp6 order by dno, name, mgrname;
select * from db2test.emp7 order by dno, name, mgrname;
select * from db2test.emp8 order by dno, name, mgrname;
select * from db2test.emp9 order by dno, name, mgrname;
select * from db2test.emp10 order by dno, name, mgrname;
select * from db2test.emp11 order by dno, name, mgrname;
select * from db2test.emp12 order by dno, name, mgrname;
select * from db2test.emp13 order by dno, name, mgrname;
select * from db2test.emp14 order by dno, name, mgrname;
select * from db2test.emp15 order by dno, name, mgrname;
-- "END OF TESTUNIT: 10";


-- *************************************************************************
-- TESTUNIT         : 11
-- DESCRIPTION      : delete with many SQ levels correl'd on child tables
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 11";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.secondemp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
delete from db2test.emp6;
delete from db2test.emp7;
delete from db2test.emp8;
delete from db2test.emp9;
delete from db2test.emp10;
delete from db2test.emp11;
delete from db2test.emp12;
delete from db2test.emp13;
delete from db2test.emp14;
delete from db2test.emp15;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.emp;
insert into db2test.emp4 select * from db2test.emp;
insert into db2test.emp5 select * from db2test.emp;
insert into db2test.emp6 select * from db2test.emp;
insert into db2test.emp7 select * from db2test.emp;
insert into db2test.emp8 select * from db2test.emp;
insert into db2test.emp9 select * from db2test.emp;
insert into db2test.emp10 select * from db2test.emp;
insert into db2test.emp11 select * from db2test.emp;
insert into db2test.emp12 select * from db2test.emp;
insert into db2test.emp13 select * from db2test.emp;
insert into db2test.emp14 select * from db2test.emp;
insert into db2test.emp15 select * from db2test.emp;
commit;
select * from db2test.dept d where
  dno in (select dno from db2test.emp e where
 e.dno = d.dno and e.dno in (select dno from db2test.emp2 e2 where
 e2.dno = e.dno and e2.dno in (select dno from db2test.emp3 e3 where
 e3.dno = e2.dno and e3.dno in (select dno from db2test.emp4 e4 where
 e4.dno = e3.dno and e4.dno in (select dno from db2test.emp5 e5 where
 e5.dno = e4.dno and e5.dno in (select dno from db2test.emp6 e6 where
 e6.dno = e5.dno and e6.dno in ('K55', 'K52')))))))
 order by 2, 3;
delete from db2test.dept  where
  dno in (select dno from db2test.emp e where
 e.dno = db2test.dept.dno and e.dno in (select dno from db2test.emp2 e2 where
 e2.dno = e.dno and e2.dno in (select dno from db2test.emp3 e3 where
 e3.dno = e2.dno and e3.dno in (select dno from db2test.emp4 e4 where
 e4.dno = e3.dno and e4.dno in (select dno from db2test.emp5 e5 where
 e5.dno = e4.dno and e5.dno in (select dno from db2test.emp6 e6 where
 e6.dno = e5.dno and e6.dno in ('K55', 'K52')))))));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;
select * from db2test.emp6 order by dno, name, mgrname;
select * from db2test.emp7 order by dno, name, mgrname;
select * from db2test.emp8 order by dno, name, mgrname;
select * from db2test.emp9 order by dno, name, mgrname;
select * from db2test.emp10 order by dno, name, mgrname;
select * from db2test.emp11 order by dno, name, mgrname;
select * from db2test.emp12 order by dno, name, mgrname;
select * from db2test.emp13 order by dno, name, mgrname;
select * from db2test.emp14 order by dno, name, mgrname;
select * from db2test.emp15 order by dno, name, mgrname;
-- "END OF TESTUNIT: 11";


-- *************************************************************************
-- TESTUNIT         : 12
-- DESCRIPTION      : delete on view with union of 15 child tables
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 12";
-- reset to original rows
delete from db2test.dept;
delete from db2test.secondemp;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
delete from db2test.emp6;
delete from db2test.emp7;
delete from db2test.emp8;
delete from db2test.emp9;
delete from db2test.emp10;
delete from db2test.emp11;
delete from db2test.emp12;
delete from db2test.emp13;
delete from db2test.emp14;
delete from db2test.emp15;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.emp;
insert into db2test.emp4 select * from db2test.emp;
insert into db2test.emp5 select * from db2test.emp;
insert into db2test.emp6 select * from db2test.emp;
insert into db2test.emp7 select * from db2test.emp;
insert into db2test.emp8 select * from db2test.emp;
insert into db2test.emp9 select * from db2test.emp;
insert into db2test.emp10 select * from db2test.emp;
insert into db2test.emp11 select * from db2test.emp;
insert into db2test.emp12 select * from db2test.emp;
insert into db2test.emp13 select * from db2test.emp;
insert into db2test.emp14 select * from db2test.emp;
insert into db2test.emp15 select * from db2test.emp;
create view db2test.vempunion15 (vname,
   vmgrname, vdno) as
  (select e.name, e.mgrname, e.dno
  from db2test.emp e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp2 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp3 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp4 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp5 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp6 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp7 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp8 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp9 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp10 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp11 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp12 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp13 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp14 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp15 e);
commit;
select * from db2test.dept where dno in
(select vdno from db2test.vempunion15)
  and dno in ('K55', 'K52') order by 1, 2;
delete from db2test.dept where dno in
(select vdno from db2test.vempunion15)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;
select * from db2test.emp6 order by dno, name, mgrname;
select * from db2test.emp7 order by dno, name, mgrname;
select * from db2test.emp8 order by dno, name, mgrname;
select * from db2test.emp9 order by dno, name, mgrname;
select * from db2test.emp10 order by dno, name, mgrname;
select * from db2test.emp11 order by dno, name, mgrname;
select * from db2test.emp12 order by dno, name, mgrname;
select * from db2test.emp13 order by dno, name, mgrname;
select * from db2test.emp14 order by dno, name, mgrname;
select * from db2test.emp15 order by dno, name, mgrname;
-- "END OF TESTUNIT: 12";
-- "cleanup";

drop view VEMPUNION15;
drop view VEMPJOIN12;
drop view VEMPJOIN;
drop table db2test.emp15;
drop table db2test.emp14;
drop table db2test.emp13;
drop table db2test.emp12;
drop table db2test.emp11;
drop table db2test.emp10;
drop table db2test.emp9;
drop table db2test.emp8;
drop table db2test.emp7;
drop table db2test.emp6;
drop table db2test.emp5;
drop table db2test.emp4;
drop table db2test.emp3;
drop table db2test.emp2;
drop table db2test.secondemp;
drop table db2test.emp;
drop table db2test.origemp;
drop table db2test.origdept;
drop table db2test.dept;
commit;

-- "cself302.sql ENDED";

-- #########################################################################
-- # TESTCASE NAME    : cself303.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases where of delete statements
-- #                  : where the deleted table is connected to other
-- #                  : tables in the query by cascade on delete.
-- #                  : shape of the RI tree is a 6 level chain.
-- #########################################################################

-- "START OF TESTCASE: cself303.sql";

autocommit off;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10));
insert into db2test.dept values (1, 'K55', 'DB');
insert into db2test.dept values (2, 'K52', 'OFC');
insert into db2test.dept values (3, 'K51', 'CS');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3));
insert into db2test.origemp select * from db2test.emp;
-- create a second child table like emp
create table db2test.emp2 (c0 int, name char(10) not null primary key
  references db2test.emp  on delete cascade,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp2 select * from db2test.emp;
-- create a third child table like emp
create table db2test.emp3 (c0 int, name char(10) not null primary key
  references db2test.emp  on delete cascade,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp3 select * from db2test.emp;
-- create a 4th child table like emp
create table db2test.emp4 (c0 int, name char(10) not null primary key
  references db2test.emp  on delete cascade,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp4 select * from db2test.emp;
-- create a 5th child table like emp
create table db2test.emp5 (c0 int, name char(10) not null primary key
  references db2test.emp  on delete cascade,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp5 select * from db2test.emp;
-- create a 6th child table like emp
create table db2test.emp6 (c0 int, name char(10) not null primary key
  references db2test.emp  on delete cascade,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp6 select * from db2test.emp;

-- "END OF TESTUNIT: 01";

-- *************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on child table on RI cascade col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.emp e where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN' and e3.mgrname = e2.mgrname)) order by 2, 3, 4;
delete from db2test.emp  where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN' and e3.mgrname = e2.mgrname));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 02";

-- *************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : delete where SQ on child table on RI cascade col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN')) order by 2,3,4;
delete from db2test.emp where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN'));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 03";

-- *************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e5.name = e4.mgrname and e4.dno in
     (select dno from db2test.emp3 e3 where e4.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e3.name = e2.mgrname and
                  e2.dno in (select dno from db2test.emp  e1
                  where e1.name = e.mgrname and e1.mgrname = 'JOHN')))))
    order by 2, 3, 4;
delete from db2test.emp 
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e5.name = e4.mgrname and e4.dno in
     (select dno from db2test.emp3 e3 where e4.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e3.name = e2.mgrname and
                  e2.dno in (select dno from db2test.emp  e1
                  where e1.name = db2test.emp.mgrname and 
                  e1.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 04";

-- *************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
--FOLLOWING TWO QUERIES are giving syntax errors currently
--select * from db2test.emp e
--  where dno in (select dno from db2test.emp5 e5
--   where e5.dno in (select dno from db2test.emp4 e4 where
--   e5.name = e4.mgrname group by dno having dno in
--     (select dno from db2test.emp3 e3 where e4.dno = e3.dno and
--       e3.dno in (select dno from db2test.emp2 e2
--                  where e3.name = e2.mgrname group by dno having
--                  e2.dno in (select dno from db2test.emp e1
--                  where e1.mgrname = e.mgrname and
--                  e1.mgrname = 'JOHN'))))) order by 2, 3, 4;
-- delete from db2test.emp 
--  where dno in (select dno from db2test.emp5 e5
--   where e5.dno in (select dno from db2test.emp4 e4 where
--   e5.name = e4.mgrname group by dno having dno in
--     (select dno from db2test.emp3 e3 where e4.dno = e3.dno and
--       e3.dno in (select dno from db2test.emp2 e2
--                  where e3.name = e2.mgrname group by dno having
--                  e2.dno in (select dno from db2test.emp  e1
--                  where e1.mgrname = e.mgrname and
--                  e1.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 05";


-- *************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having, correl to iudt
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   db2test.emp.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e.mgrname = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where db2test.emp.mgrname = 'JOHN'))))) order by 2,3,4;
delete from db2test.emp 
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   db2test.emp.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where db2test.emp.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e.mgrname = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 06";

-- *************************************************************************
-- TESTUNIT         : 07
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having, correl to grandparent
-- EXPECTED RESULTS : SQL commands should get error -119
-- *************************************************************************
-- "START OF TESTUNIT: 07";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.dno = e2.dno group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN'))))) order by 2, 3, 4;
delete from db2test.emp 
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   db2test.emp.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.dno = e2.dno group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where db2test.emp.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 07";

-- *************************************************************************
-- TESTUNIT         : 08
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having, correl to grandparent
-- EXPECTED RESULTS : select should get -119, delete should get ???
-- *************************************************************************
-- "START OF TESTUNIT: 08";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.name = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN'))))) order by 2, 3, 4;
-- select should get -119;
delete from db2test.emp 
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   db2test.emp.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.name = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                  where db2test.emp.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 08";

-- *************************************************************************
-- TESTUNIT         : 09
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having, correl to grandparent
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 09";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.dno = e2.dno group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN'))))) order by 2, 3, 4;
delete from db2test.emp 
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   db2test.emp.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.dno = e2.dno group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 09";
-- "cleanup";
drop table db2test.origemp;
drop table db2test.emp6;
drop table db2test.emp5;
drop table db2test.emp4;
drop table db2test.emp3;
drop table db2test.emp2;
drop table db2test.emp;
drop table db2test.dept;
drop table db2test.origdept;

rollback;
-- "cself303.clp ENDED";

-- #########################################################################
-- # TESTCASE NAME    : cself304.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # COMPONENT(S)     : SQN and SQR
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases where of delete statements
-- #                  : where the deleted table is connected to other
-- #                  : tables in the query by cascade on delete.
-- #                  : shape of the RI tree is a 3-way cycle to parent
-- #########################################################################
-- "START OF TESTCASE: cself304.sql";
autocommit off ;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10), dmgrname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10), dmgrname char(10));
insert into db2test.dept values (1, 'K55', 'DB', 'JOHN');
insert into db2test.dept values (2, 'K52', 'OFC', 'ROBIN');
insert into db2test.dept values (3, 'K51', 'CS', 'ASHOK');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete cascade );
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3));
insert into db2test.origemp select * from db2test.emp;
-- create a second child table like emp
create table db2test.emp2 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp on delete cascade,
  dno char(3));
insert into db2test.emp2 select * from db2test.emp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
-- "END OF TESTUNIT: 01";

-- ************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on child table on RI cascade col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";

select * from db2test.emp e where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where dmgrname = 'JOHN'));

delete from db2test.emp where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where dmgrname = 'JOHN'));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;
-- "END OF TESTUNIT: 02";

-- ************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : same as 02, but with correlation to top table
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
select * from db2test.emp e where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where d.dmgrname = 'john' and e2.name = d.dmgrname));
delete from db2test.emp where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where mgrname = 'john' and e2.name = d.dmgrname));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 03";

-- ************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : same as 02 but SQ's reversed
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
select * from db2test.emp e where dno in
   (select dno from db2test.dept d
   where  d.dno in
     (select dno from db2test.emp2 e2
       where e2.mgrname = 'JOHN')) order by dno, name, mgrname;
delete from db2test.emp  where dno in
   (select dno from db2test.dept d
   where  d.dno in
     (select dno from db2test.emp2 e2
       where e2.mgrname = 'JOHN'));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 04";

-- ************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : same as 03 but SQ's reversed
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
select * from db2test.emp e where dno in
   (select dno from db2test.dept d
   where  d.dno = e.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'JOHN')) order by dno, name, mgrname;
delete from db2test.emp  where dno in
   (select dno from db2test.dept d
   where  d.dno = db2test.emp.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'JOHN'));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 05";

-- ************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : same as 05 but extra table
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
select * from db2test.emp e where dno in
  (select dno from db2test.origdept where dno in
   (select dno from db2test.dept d
   where  d.dno = e.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'JOHN'))) order by dno, name, mgrname;
delete from db2test.emp where dno in
  (select dno from db2test.origdept where dno in
   (select dno from db2test.dept d
   where  d.dno = db2test.emp.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'JOHN')));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 06";

-- *************************************************************************
-- TESTUNIT         : 07
-- DESCRIPTION      : delete on parent with SQ on join view of children
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 07";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
create view db2test.vempjoin (vname1, vname2, vmgrname, vdno) as
  select e.name, e2.name, e.mgrname, e.dno
  from db2test.emp e, db2test.emp2 e2
  where e.dno = e2.dno;
select * from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52') order by dno, dname, dmgrname;
delete from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;
-- "END OF TESTUNIT: 07";

-- *************************************************************************
-- TESTUNIT         : 08
-- DESCRIPTION      : delete on parent with SQ on union view of children
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 08";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete cascade;

commit;
create view db2test.vempunion (vname, vmgrname, vdno) as
  (select e.name,  e.mgrname, e.dno
  from db2test.emp e)
union all
  (select e2.name, e2.mgrname, e2.dno from db2test.emp2 e2);

select * from db2test.dept where dno in (select vdno from
  db2test.vempunion)
  and dno in ('K55', 'K52') order by dno, dname, dmgrname;
delete from db2test.dept where dno in (select vdno from
  db2test.vempunion)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;
-- "END OF TESTUNIT: 08";
-- "cleanup";
drop view VEMPJOIN;
drop view VEMPUNION;
drop table db2test.emp2;
drop table db2test.emp;
drop table db2test.origemp;
drop table db2test.dept;
drop table db2test.origdept;

--drop view db2test.vempjoin;
--drop view db2test.vempunion;
commit;
-- "cself304.sql ENDED";

-- #########################################################################
-- # TESTCASE NAME    : cself311.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases where of delete statements
-- #                  : where the deleted table also appears in a
-- #                  : subquery that qualifies which rows are changed
-- #                  : and the deleted table has a self-ref'g RI const.
-- # EXPECTED RESULTS : File should run successfully with no errors.
-- #########################################################################

-- "START OF TESTCASE: cself311.sql";

autocommit off;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10));
insert into db2test.dept values (1, 'K55', 'DB');
insert into db2test.dept values (2, 'K52', 'OFC');
insert into db2test.dept values (3, 'K51', 'CS');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp on delete
  set null, dno char(3) references db2test.dept on delete
  set null);
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.origemp on delete
  set null, dno char(3) references db2test.origdept
  on delete set null );
insert into db2test.origemp select * from db2test.emp;

-- "END OF TESTUNIT: 01";

-- *************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on same table on RI set null col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.emp where name in (select name from db2test.emp
   where mgrname = 'JOHN') order by 2,3,4;
delete from db2test.emp where name in (select name from db2test.emp
   where mgrname = 'JOHN');
select * from db2test.emp order by name, mgrname, dno;

-- "END OF TESTUNIT: 02";


-- *************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : delete with 2 levels of SQ and self-ref in 2nd
--                  : correlated to 1st SQ on foreign key
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
select * from db2test.emp where dno in (select dno from db2test.dept D
   where D.dno in (select dno from db2test.emp E where E.dno = D.dno
   and e.mgrname = 'JOHN')) order by 2, 3, 4;
delete from db2test.emp where dno in (select dno from db2test.dept D
   where D.dno in (select dno from db2test.emp E where E.dno = D.dno
   and e.mgrname = 'JOHN'));
select * from db2test.emp order by name, mgrname, dno;

-- "END OF TESTUNIT: 03";

-- *************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : delete with SQ GB Having SQ on
--                  : modified table
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
select * from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from db2test.emp
  where mgrname = 'ASHOK')) order by 2, 3, 4;
delete from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from db2test.emp
  where mgrname = 'ASHOK'));
select * from db2test.emp order by name, mgrname, dno;
-- "END OF TESTUNIT: 04";

-- *************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : delete with SQ GB Having SQ on
--                  : modified table -- 3 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
select * from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from
   db2test.dept D where dno in (select dno from db2test.emp E2
   where D. dno = E2.dno))) order by 2, 3, 4;
delete from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from
   db2test.dept D where dno in (select dno from db2test.emp E2
   where D. dno = E2.dno)));
select * from db2test.emp order by name, mgrname, dno;
-- "END OF TESTUNIT: 05";

-- *************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : delete on view with SQ GB Having SQ on
--                  : modified table -- 3 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.emp;
insert into db2test.emp select * from db2test.origemp;
create view db2test.vemp (vc0, vname, vmgrname, vdno) as
  select * from db2test.emp where exists ( select max(mgrname) from
  db2test.origemp group by dno having dno in (select dno from
   db2test.dept D where dno in (select dno from db2test.emp E2
   where D. dno = E2.dno)));
select * from db2test.vemp order by 2, 3, 4;
delete from db2test.vemp;
select * from db2test.emp order by name, mgrname, dno;
-- "END OF TESTUNIT: 06";

-- "cleanup";
drop view db2test.vemp;
drop table db2test.emp;
drop table db2test.origemp;
drop table db2test.dept;
drop table db2test.origdept;
rollback;

-- "cself311.clp ENDED";

-- #########################################################################
-- # TESTCASE NAME    : cself312.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases of delete statements
-- #                  : where the deleted table is connected to other
-- #                  : tables in the query by set null on delete.
-- #                  : shape of the RI tree is a 1 level star fanout.
-- #########################################################################

-- "START OF TESTCASE: cself312.sql";

autocommit off;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10));
insert into db2test.dept values (1, 'K55', 'DB');
insert into db2test.dept values (2, 'K52', 'OFC');
insert into db2test.dept values (3, 'K51', 'CS');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3));
insert into db2test.origemp select * from db2test.emp;

create table db2test.secondemp (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp on delete set null,
  dno char(3)  references db2test.origdept
  on delete set null );
insert into db2test.secondemp select * from db2test.emp;
commit;
-- "END OF TESTUNIT: 01";

-- *************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on same table on RI set null col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.dept where dno in (select dno from db2test.emp
   where mgrname = 'JOHN') order by 2,3;
delete from db2test.dept where dno in (select dno from db2test.emp
   where mgrname = 'JOHN');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;

-- "END OF TESTUNIT: 02";


-- *************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : delete with 2 levels of SQ and self-ref in 2nd
--                  : correlated to 1st SQ on foreign key
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
select * from db2test.dept where dno in (select dno from
   db2test.secondemp E
   where E.dno in (select dno from db2test.emp D where E.dno = D.dno
   and D.dno = 'K55')) order by 2, 3;
delete from db2test.dept where dno in (select dno
   from db2test.secondemp E
   where E.dno in (select dno from db2test.emp D where E.dno = D.dno
   and D.dno = 'K55'));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;

-- "END OF TESTUNIT: 03";

-- *************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : delete with SQ GB Having SQ on
--                  : child table
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
select * from db2test.dept where exists ( select max(mgrname) from
  db2test.secondemp group by dno having dno in (select dno from
  db2test.emp where mgrname = 'ASHOK')) order by 2, 3;
delete from db2test.dept where exists ( select max(mgrname) from
  db2test.secondemp group by dno having dno in (select dno from
  db2test.emp where mgrname = 'ASHOK'));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
-- "END OF TESTUNIT: 04";

-- *************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : above -- 7 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
select * from db2test.dept dtop where exists
  (select * from db2test.secondemp  where exists
     (select dno from
      db2test.dept D1 where dno = dtop.dno and dno in
        (select dno from db2test.emp E2 where D1.dno = E2.dno and dno in
         (select dno from db2test.emp E3 where E2.dno = E3.dno and dno in
         (select dno from db2test.emp E4 where E3.dno = E4.dno and dno in
         (select dno from db2test.emp E5 where E4.dno = E5.dno and dno in
         (select dno from db2test.emp E6 where E5.dno = E6.dno)
      )))))) order by 2, 3;
delete from db2test.dept where exists
  (select * from db2test.origemp  where exists
     (select dno from
      db2test.dept D1 where dno = db2test.dept.dno and dno in
       (select dno from db2test.emp E2 where D1.dno = E2.dno and dno in
       (select dno from db2test.emp E3 where E2.dno = E3.dno and dno in
       (select dno from db2test.emp E4 where E3.dno = E4.dno and dno in
       (select dno from db2test.emp E5 where E4.dno = E5.dno and dno in
       (select dno from db2test.emp E6 where E5.dno = E6.dno)
    ))))));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
-- "END OF TESTUNIT: 05";

-- *************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : on second child table, delete set null on each
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.emp;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
-- create a second child table like emp
create table db2test.emp2 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp2 select * from db2test.emp;
commit;
select * from db2test.dept dtop where exists
    (select dno from db2test.emp2 E2 where Dtop.dno = E2.dno and dno in
    (select dno from db2test.emp E3 where E2.dno = E3.dno))
    order by 2, 3;
delete from db2test.dept where exists
    (select dno from db2test.emp2 E2 where db2test.dept.dno = E2.dno and dno in
    (select dno from db2test.emp E3 where E2.dno = E3.dno));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 06";


-- *************************************************************************
-- TESTUNIT         : 07
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : on second child table, delete set null on each
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 07";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
select * from db2test.dept dtop where exists
    (select dno from db2test.emp2 E2 where dtop.dno = E2.dno)
     and exists
    (select dno from db2test.emp E3 where dtop.dno = E3.dno)
    order by 2,3;
delete from db2test.dept where exists
    (select dno from db2test.emp2 E2 where db2test.dept.dno = E2.dno)
     and exists
    (select dno from db2test.emp E3 where db2test.dept.dno = E3.dno);
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 07";

-- *************************************************************************
-- TESTUNIT         : 08
-- DESCRIPTION      : delete with SQ on child table correlated to SQ
--                  : on second child table, delete set null on each
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 08";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.secondemp;
delete from db2test.dept;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
select * from db2test.dept dtop where exists
    (select dno from db2test.emp2 E2 where dtop.dno = E2.dno)
     or exists
    (select dno from db2test.emp E3 where dtop.dno = E3.dno)
    order by 2, 3;
delete from db2test.dept where exists
    (select dno from db2test.emp2 E2 where db2test.dept.dno = E2.dno)
     or exists
    (select dno from db2test.emp E3 where db2test.dept.dno = E3.dno);
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 08";

-- *************************************************************************
-- TESTUNIT         : 09
-- DESCRIPTION      : delete on view with SQ GB Having SQ on
--                  : modified table -- 3 levels
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 09";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
create view db2test.vempjoin (vname1, vname2, vmgrname, vdno) as
  select e.name, e2.name, e.mgrname, e.dno
  from db2test.emp e, db2test.emp2 e2
  where e.dno = e2.dno;
select * from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52') order by 2, 3;
delete from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
-- "END OF TESTUNIT: 09";

-- *************************************************************************
-- TESTUNIT         : 10
-- DESCRIPTION      : delete on iudt where SQ on
--                  : view with join on 15 child tables
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 10";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
delete from db2test.secondemp;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
-- create a third child table like emp
create table db2test.emp3 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp3 select * from db2test.emp;
-- create a 4th child table like emp
create table db2test.emp4 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp4 select * from db2test.emp;
-- create a 5th child table like emp
create table db2test.emp5 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp5 select * from db2test.emp;
-- create a 6th child table like emp
create table db2test.emp6 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp6 select * from db2test.emp;
-- create a 7th child table like emp
create table db2test.emp7 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp7 select * from db2test.emp;
-- create a 8th child table like emp
create table db2test.emp8 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp8 select * from db2test.emp;
-- create a 9th child table like emp
create table db2test.emp9 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp9 select * from db2test.emp;
-- create a 10th child table like emp
create table db2test.emp10 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp10 select * from db2test.emp;
-- create a 11th child table like emp
create table db2test.emp11 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp11 select * from db2test.emp;
-- create a 12th child table like emp
create table db2test.emp12 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp12 select * from db2test.emp;
-- create a 13th child table like emp
create table db2test.emp13 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp13 select * from db2test.emp;
-- create a 14th child table like emp
create table db2test.emp14 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp14 select * from db2test.emp;
-- create a 15th child table like emp
create table db2test.emp15 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp2 on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp15 select * from db2test.emp;
create view db2test.vempjoin12 (vname1, vname2, vname3, vname4, vname5,
   vname6, vname7, vname8, vname9, vname10, vname11, vname12,
   vmgrname, vdno) as
  select e.name, e2.name, e3.name, e4.name, e5.name, e6.name, e7.name,
   e8.name, e9.name, e10.name, e11.name, e12.name,
   e.mgrname, e.dno
  from db2test.emp e, db2test.emp2 e2, db2test.emp3 e3, db2test.emp4 e4,
  db2test.emp5 e5, db2test.emp6 e6, db2test.emp7 e7, db2test.emp8 e8,
  db2test.emp9 e9, db2test.emp10 e10, db2test.emp11 e11,
  db2test.emp12 e12
  where e.dno = e2.dno
  and e.dno = e2.dno
  and e.dno = e2.dno
  and e.dno = e3.dno
  and e.dno = e4.dno
  and e.dno = e5.dno
  and e.dno = e6.dno
  and e.dno = e7.dno
  and e.dno = e8.dno
  and e.dno = e9.dno
  and e.dno = e10.dno
  and e.dno = e11.dno
  and e.dno = e12.dno;
commit;
--CURRENTLY These queries hang in Cloudscape 
--have to uncomment once they start passing
--select * from db2test.dept where dno in (select vdno from
--  db2test.vempjoin12)
--  and dno in ('K55', 'K52') order by 2, 3;
--delete from db2test.dept where dno in (select vdno from
--  db2test.vempjoin12)
--  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;
select * from db2test.emp6 order by dno, name, mgrname;
select * from db2test.emp7 order by dno, name, mgrname;
select * from db2test.emp8 order by dno, name, mgrname;
select * from db2test.emp9 order by dno, name, mgrname;
select * from db2test.emp10 order by dno, name, mgrname;
select * from db2test.emp11 order by dno, name, mgrname;
select * from db2test.emp12 order by dno, name, mgrname;
select * from db2test.emp13 order by dno, name, mgrname;
select * from db2test.emp14 order by dno, name, mgrname;
select * from db2test.emp15 order by dno, name, mgrname;
-- "END OF TESTUNIT: 10";


-- *************************************************************************
-- TESTUNIT         : 11
-- DESCRIPTION      : delete with many SQ levels correl'd on child tables
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 11";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.secondemp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
delete from db2test.emp6;
delete from db2test.emp7;
delete from db2test.emp8;
delete from db2test.emp9;
delete from db2test.emp10;
delete from db2test.emp11;
delete from db2test.emp12;
delete from db2test.emp13;
delete from db2test.emp14;
delete from db2test.emp15;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.emp;
insert into db2test.emp4 select * from db2test.emp;
insert into db2test.emp5 select * from db2test.emp;
insert into db2test.emp6 select * from db2test.emp;
insert into db2test.emp7 select * from db2test.emp;
insert into db2test.emp8 select * from db2test.emp;
insert into db2test.emp9 select * from db2test.emp;
insert into db2test.emp10 select * from db2test.emp;
insert into db2test.emp11 select * from db2test.emp;
insert into db2test.emp12 select * from db2test.emp;
insert into db2test.emp13 select * from db2test.emp;
insert into db2test.emp14 select * from db2test.emp;
insert into db2test.emp15 select * from db2test.emp;
commit;
select * from db2test.dept d where
  dno in (select dno from db2test.emp e where
 e.dno = d.dno and e.dno in (select dno from db2test.emp2 e2 where
 e2.dno = e.dno and e2.dno in (select dno from db2test.emp3 e3 where
 e3.dno = e2.dno and e3.dno in (select dno from db2test.emp4 e4 where
 e4.dno = e3.dno and e4.dno in (select dno from db2test.emp5 e5 where
 e5.dno = e4.dno and e5.dno in (select dno from db2test.emp6 e6 where
 e6.dno = e5.dno and e6.dno in ('K55', 'K52')))))))
 order by 2, 3;
delete from db2test.dept d where
  dno in (select dno from db2test.emp e where
 e.dno = d.dno and e.dno in (select dno from db2test.emp2 e2 where
 e2.dno = e.dno and e2.dno in (select dno from db2test.emp3 e3 where
 e3.dno = e2.dno and e3.dno in (select dno from db2test.emp4 e4 where
 e4.dno = e3.dno and e4.dno in (select dno from db2test.emp5 e5 where
 e5.dno = e4.dno and e5.dno in (select dno from db2test.emp6 e6 where
 e6.dno = e5.dno and e6.dno in ('K55', 'K52')))))));
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;
select * from db2test.emp6 order by dno, name, mgrname;
select * from db2test.emp7 order by dno, name, mgrname;
select * from db2test.emp8 order by dno, name, mgrname;
select * from db2test.emp9 order by dno, name, mgrname;
select * from db2test.emp10 order by dno, name, mgrname;
select * from db2test.emp11 order by dno, name, mgrname;
select * from db2test.emp12 order by dno, name, mgrname;
select * from db2test.emp13 order by dno, name, mgrname;
select * from db2test.emp14 order by dno, name, mgrname;
select * from db2test.emp15 order by dno, name, mgrname;
-- "END OF TESTUNIT: 11";


-- *************************************************************************
-- TESTUNIT         : 12
-- DESCRIPTION      : delete on view with union of 15 child tables
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 12";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.secondemp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
delete from db2test.emp6;
delete from db2test.emp7;
delete from db2test.emp8;
delete from db2test.emp9;
delete from db2test.emp10;
delete from db2test.emp11;
delete from db2test.emp12;
delete from db2test.emp13;
delete from db2test.emp14;
delete from db2test.emp15;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.secondemp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.emp;
insert into db2test.emp4 select * from db2test.emp;
insert into db2test.emp5 select * from db2test.emp;
insert into db2test.emp6 select * from db2test.emp;
insert into db2test.emp7 select * from db2test.emp;
insert into db2test.emp8 select * from db2test.emp;
insert into db2test.emp9 select * from db2test.emp;
insert into db2test.emp10 select * from db2test.emp;
insert into db2test.emp11 select * from db2test.emp;
insert into db2test.emp12 select * from db2test.emp;
insert into db2test.emp13 select * from db2test.emp;
insert into db2test.emp14 select * from db2test.emp;
insert into db2test.emp15 select * from db2test.emp;
commit;
create view db2test.vempunion15 (vname,
   vmgrname, vdno) as
  (select e.name, e.mgrname, e.dno
  from db2test.emp e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp2 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp3 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp4 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp5 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp6 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp7 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp8 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp9 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp10 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp11 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp12 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp13 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp14 e)
union all
  (select e.name, e.mgrname, e.dno
  from db2test.emp15 e);
select * from db2test.dept where dno in
(select vdno from db2test.vempunion15)
  and dno in ('K55', 'K52') order by 2, 3;
delete from db2test.dept where dno in
(select vdno from db2test.vempunion15)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.secondemp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;
select * from db2test.emp6 order by dno, name, mgrname;
select * from db2test.emp7 order by dno, name, mgrname;
select * from db2test.emp8 order by dno, name, mgrname;
select * from db2test.emp9 order by dno, name, mgrname;
select * from db2test.emp10 order by dno, name, mgrname;
select * from db2test.emp11 order by dno, name, mgrname;
select * from db2test.emp12 order by dno, name, mgrname;
select * from db2test.emp13 order by dno, name, mgrname;
select * from db2test.emp14 order by dno, name, mgrname;
select * from db2test.emp15 order by dno, name, mgrname;
-- "END OF TESTUNIT: 12";
-- "cleanup";


drop view db2test.vempunion15;
drop view db2test.vempjoin12;
drop view db2test.vempjoin;
drop table db2test.emp15;
drop table db2test.emp14;
drop table db2test.emp13;
drop table db2test.emp12;
drop table db2test.emp11;
drop table db2test.emp10;
drop table db2test.emp9;
drop table db2test.emp8;
drop table db2test.emp7;
drop table db2test.emp6;
drop table db2test.emp5;
drop table db2test.emp4;
drop table db2test.emp3;
drop table db2test.emp2;
drop table db2test.secondemp;
drop table db2test.origemp;
drop table db2test.emp;
drop table db2test.dept;
drop table db2test.origdept;

--drop view db2test.vempjoin;
--drop view db2test.vempjoin12;
--drop view db2test.vempunion15;
-- *************************************************************************
--     NO MORE TESTUNITS. SUMMARIZE TESTCASE RESULTS AND HOUSE KEEPING
-- *************************************************************************
commit;

-- #########################################################################
-- # TESTCASE NAME    : cself313.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases where of delete statements
-- #                  : where the deleted table is connected to other
-- #                  : tables in the query by set null on delete.
-- #                  : shape of the RI tree is a 6way star.
-- #########################################################################

-- "START OF TESTCASE: cself313.sql";

autocommit off;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10));
insert into db2test.dept values (1, 'K55', 'DB');
insert into db2test.dept values (2, 'K52', 'OFC');
insert into db2test.dept values (3, 'K51', 'CS');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3));
insert into db2test.origemp select * from db2test.emp;
-- create a second child table like emp
create table db2test.emp2 (c0 int, name char(10) not null primary key,
  mgrname char(10)
  references db2test.emp  on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp2 select * from db2test.emp;
-- create a third child table like emp
create table db2test.emp3 (c0 int, name char(10) not null primary key,
  mgrname char(10)
  references db2test.emp  on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp3 select * from db2test.emp;
-- create a 4th child table like emp
create table db2test.emp4 (c0 int, name char(10) not null primary key,
  mgrname char(10)
  references db2test.emp  on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp4 select * from db2test.emp;
-- create a 5th child table like emp
create table db2test.emp5 (c0 int, name char(10) not null primary key,
  mgrname char(10)
  references db2test.emp  on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp5 select * from db2test.emp;
-- create a 6th child table like emp
create table db2test.emp6 (c0 int, name char(10) not null primary key,
  mgrname char(10)
  references db2test.emp  on delete set null,
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp6 select * from db2test.emp;

-- "END OF TESTUNIT: 01";

-- *************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on child table on RI set null col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.emp e where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN' and e3.name = e2.mgrname));
delete from db2test.emp where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN' and e3.name = e2.mgrname));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 02";

-- *************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : delete where SQ on child table on RI set null col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN')) order by 2,3,4;
delete from db2test.emp  where dno in (select dno from db2test.emp3 e3
   where e3.dno in (select dno from db2test.emp2 e2
   where mgrname = 'JOHN'));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 03";

-- *************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e5.name = e4.mgrname and e4.dno in
     (select dno from db2test.emp3 e3 where e4.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e3.name = e2.mgrname and
                  e2.dno in (select dno from db2test.emp  e1
                  where e1.name = e.mgrname and e1.mgrname = 'JOHN')))))
   order by  1,2,3;
delete from db2test.emp 
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e5.name = e4.mgrname and e4.dno in
     (select dno from db2test.emp3 e3 where e4.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e3.name = e2.mgrname and
                  e2.dno in (select dno from db2test.emp  e1
                  where e1.name = db2test.emp.mgrname and
                  e1.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 04";

-- *************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e5.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e4.dno = e3.dno and
       e3.dno in (select dno from db2test.emp2 e2
                  where e3.name = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp e1
                  where e1.name = e.mgrname and e1.mgrname = 'JOHN')))))
   order by 2,3,4;
delete from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e5.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e4.dno = e3.dno and
       e3.dno in (select dno from db2test.emp2 e2
                  where e3.name = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                  where e1.name = e.mgrname and e1.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 05";


-- *************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having, correl to iudt
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e.name = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN'))))) order by 2,3,4;
delete from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e.name = e2.mgrname group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 06";

-- *************************************************************************
-- TESTUNIT         : 07
-- DESCRIPTION      : delete where SQ chain reversing RI child chain
--                  : combining where and having, correl to grandparent
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 07";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.emp3;
delete from db2test.emp4;
delete from db2test.emp5;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
insert into db2test.emp3 select * from db2test.origemp;
insert into db2test.emp4 select * from db2test.origemp;
insert into db2test.emp5 select * from db2test.origemp;
select * from db2test.emp e
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   e.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.dno = e2.dno group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where e.mgrname = 'JOHN'))))) order by 2,3,4;
delete from db2test.emp
  where dno in (select dno from db2test.emp5 e5
   where e5.dno in (select dno from db2test.emp4 e4 where
   db2test.emp.name = e4.mgrname group by dno having dno in
     (select dno from db2test.emp3 e3 where e5.name = e3.mgrname and
       e3.dno in (select dno from db2test.emp2 e2
                  where e4.dno = e2.dno group by dno having
                  e2.dno in (select dno from db2test.emp  e1
                   where db2test.emp.mgrname = 'JOHN')))));
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
select * from db2test.emp3 order by dno, name, mgrname;
select * from db2test.emp4 order by dno, name, mgrname;
select * from db2test.emp5 order by dno, name, mgrname;

-- "END OF TESTUNIT: 07";
-- "cleanup";

drop table db2test.emp6;
drop table db2test.emp5;
drop table db2test.emp4;
drop table db2test.emp3;
drop table db2test.emp2;
drop table db2test.emp;
drop table db2test.origemp;
drop table db2test.dept;
drop table db2test.origdept;
rollback;

-- "cself313.clp ENDED";

-- #########################################################################
-- # TESTCASE NAME    : cself314.sql
-- # LINE ITEM        : Self-referencing subqueries
-- # COMPONENT(S)     : SQN and SQR
-- # DESCRIPTION      : Allow use of subqueries on the same table being
-- #                  : inserted, deleted, or updated.  Cursors updated
-- #                  : or delete where current of are now similarly
-- #                  : unrestricted. Also allowed are subqueries
-- #                  : on tables related to the modified table by
-- #                  : referential relationships, either directly or
-- #                  : indirectly.
-- #                  : This file covers cases where of delete statements
-- #                  : where the deleted table is connected to other
-- #                  : tables in the query by set null on delete.
-- #                  : shape of the RI tree is a 3-way cycle to parent

-- "START OF TESTCASE: cself314.sql";

autocommit off;

-- *************************************************************************
-- TESTUNIT         : 01
-- DESCRIPTION      : Create tables db2test.emp
--                  : insert some data into it
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 01";
create table db2test.dept (c0 int, dno char(3) not null primary key,
   dname char(10), dmgrname char(10));
create table db2test.origdept (c0 int, dno char(3) not null primary key,
   dname char(10), dmgrname char(10));
insert into db2test.dept values (1, 'K55', 'DB', 'JOHN');
insert into db2test.dept values (2, 'K52', 'OFC', 'ROBIN');
insert into db2test.dept values (3, 'K51', 'CS', 'ASHOK');
insert into db2test.origdept select * from db2test.dept;
create table db2test.emp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3)  references db2test.dept
  on delete set null );
insert into db2test.emp (c0, name, dno) values (1, 'ASHOK', 'K51');
insert into db2test.emp values (2, 'JOHN', 'ASHOK', 'K51');
insert into db2test.emp values (3, 'ROBIN', 'ASHOK', 'K51');
insert into db2test.emp values (4, 'JOE1', 'ASHOK', 'K51');
insert into db2test.emp values (5, 'JOE2', 'ASHOK', 'K51');
insert into db2test.emp values (6, 'HAMID', 'JOHN', 'K55');
insert into db2test.emp values (7, 'TRUONG', 'HAMID', 'K55');
insert into db2test.emp values (8, 'LARRY1', 'HAMID', 'K55');
insert into db2test.emp values (9, 'LARRY2', 'HAMID', 'K55');
insert into db2test.emp values (10, 'BOBBIE', 'HAMID', 'K55');
insert into db2test.emp values (11, 'ROGER', 'ROBIN', 'K52');
insert into db2test.emp values (12, 'JIM', 'ROGER', 'K52');
insert into db2test.emp values (13, 'DAN', 'ROGER', 'K52');
insert into db2test.emp values (14, 'SAM1', 'ROGER', 'K52');
insert into db2test.emp values (15, 'SAM2', 'ROGER', 'K52');
insert into db2test.emp values (16, 'GUY', 'JOHN', 'K55');
insert into db2test.emp values (17, 'DON', 'GUY', 'K55');
insert into db2test.emp values (18, 'MONICA', 'GUY', 'K55');
insert into db2test.emp values (19, 'LILY1', 'GUY', 'K55');
insert into db2test.emp values (20, 'LILY2', 'GUY', 'K55');
create table db2test.origemp (c0 int, name char(10) not null primary key,
  mgrname char(10),
  dno char(3));
insert into db2test.origemp select * from db2test.emp;
-- create a second child table like emp
create table db2test.emp2 (c0 int, name char(10) not null primary key,
  mgrname char(10) references db2test.emp on delete set null,
  dno char(3));
insert into db2test.emp2 select * from db2test.emp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
-- "END OF TESTUNIT: 01";

-- ************************************************************************
-- TESTUNIT         : 02
-- DESCRIPTION      : delete where SQ on child table on RI set null col
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 02";
select * from db2test.emp e where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where dmgrname = 'JOHN'))
   order by name;
delete from db2test.emp  where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where dmgrname = 'JOHN'));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;
-- "END OF TESTUNIT: 02";

-- ************************************************************************
-- TESTUNIT         : 03
-- DESCRIPTION      : same as 02, but with correlation to top table
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 03";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
select * from db2test.emp e where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where mgrname = 'john' and e2.name = d.dmgrname));
delete from db2test.emp  where dno in (select dno from db2test.emp2 e2
   where e2.dno in (select dno from db2test.dept d
   where mgrname = 'john' and e2.name = d.dmgrname));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 03";

-- ************************************************************************
-- TESTUNIT         : 04
-- DESCRIPTION      : same as 02 but SQ's reversed
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 04";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
select * from db2test.emp e where dno in
   (select dno from db2test.dept d
   where  d.dno in
     (select dno from db2test.emp2 e2
       where e2.mgrname = 'john'));
delete from db2test.emp  where dno in
   (select dno from db2test.dept d
   where  d.dno in
     (select dno from db2test.emp2 e2
       where e2.mgrname = 'john'));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 04";

-- ************************************************************************
-- TESTUNIT         : 05
-- DESCRIPTION      : same as 03 but SQ's reversed
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 05";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
select * from db2test.emp e where dno in
   (select dno from db2test.dept d
   where  d.dno = e.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'john'));
delete from db2test.emp where dno in
   (select dno from db2test.dept d
   where  d.dno = db2test.emp.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'john'));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 05";

-- ************************************************************************
-- TESTUNIT         : 06
-- DESCRIPTION      : same as 05 but extra table
-- EXPECTED RESULTS : SQL commands should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 06";
-- reset to original rows
delete from db2test.dept;
delete from db2test.emp;
delete from db2test.emp2;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
select * from db2test.emp e where dno in
  (select dno from db2test.origdept where dno in
   (select dno from db2test.dept d
   where  d.dno = e.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'john')));
delete from db2test.emp  where dno in
  (select dno from db2test.origdept where dno in
   (select dno from db2test.dept d
   where  d.dno = db2test.emp.dno and d.dno in
     (select dno from db2test.emp2 e2 where e2.dno = d.dno and
        e2.mgrname = 'john')));
select * from db2test.dept order by dno, dname, dmgrname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;

-- "END OF TESTUNIT: 06";

-- *************************************************************************
-- TESTUNIT         : 07
-- DESCRIPTION      : delete on parent with SQ on join view of children
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 07";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
create view db2test.vempjoin (vname1, vname2, vmgrname, vdno) as
  select e.name, e2.name, e.mgrname, e.dno
  from db2test.emp e, db2test.emp2 e2
  where e.dno = e2.dno;
select * from db2test.dept where dno in (select vdno from
  db2test.vempjoin) 
  and dno in ('K55', 'K52')
  order by dno;
delete from db2test.dept where dno in (select vdno from
  db2test.vempjoin)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;
-- "END OF TESTUNIT: 07";

-- *************************************************************************
-- TESTUNIT         : 08
-- DESCRIPTION      : delete on parent with SQ on union view of children
-- EXPECTED RESULTS : SQL command should complete successfully
-- *************************************************************************
-- "START OF TESTUNIT: 08";
-- reset to original rows
delete from db2test.emp;
delete from db2test.emp2;
delete from db2test.dept;
insert into db2test.dept select * from db2test.origdept;
insert into db2test.emp select * from db2test.origemp;
insert into db2test.emp2 select * from db2test.origemp;
alter table db2test.dept add constraint dmgr foreign key (dmgrname)
  references db2test.emp2 on delete set null;

commit;
create view db2test.vempunion (vname, vmgrname, vdno) as
  (select e.name,  e.mgrname, e.dno
  from db2test.emp e)
union all
  (select e2.name, e2.mgrname, e2.dno from db2test.emp2 e2);
-- #BEGIN;
select * from db2test.dept where dno in (select vdno from
  db2test.vempunion)
  and dno in ('K55', 'K52');
-- #END;

delete from db2test.dept where dno in (select vdno from
  db2test.vempunion)
  and dno in ('K55', 'K52');
select * from db2test.dept order by dno, dname;
select * from db2test.emp order by dno, name, mgrname;
select * from db2test.emp2 order by dno, name, mgrname;
alter table db2test.dept drop constraint dmgr;
commit;
-- "END OF TESTUNIT: 08";
-- "cleanup";
drop view db2test.vempjoin ;
drop view db2test.vempunion;
drop table db2test.emp2;
drop table db2test.emp;
drop table db2test.origemp;
drop table db2test.dept;
drop table db2test.origdept;


-- "cself314.clp ENDED";
drop schema db2test restrict;
-- END OF REFACTIONS1.sql --

