-- alter table tests for ALTER TABLE DROP COLUMN.
-- These tests are in a separate file from altertable.sql because of
-- bug DERBY-1909 involving DROP COLUMN being broken under sqlAuthorization.
-- When DROP COLUMN works correctly with sqlAuthorization = true, these tests
-- should be merged back into altertable.sql, and this file should be deleted

-- Some tests of ALTER TABLE DROP COLUMN
-- The overall syntax is:
--    ALTER TABLE tablename DROP [ COLUMN ] columnname [ CASCADE | RESTRICT ]
-- 
create table atdc_0 (a integer);
create table atdc_1 (a integer, b integer);
insert into atdc_1 values (1, 1);
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
alter table atdc_1 drop column b;
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
alter table atdc_1 add column b varchar (20);
insert into atdc_1 values (1, 'new val');
insert into atdc_1 (a, b) values (2, 'two val');
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
alter table atdc_1 add column c integer;
insert into atdc_1 values (3, null, 3);
select * from atdc_1;
alter table atdc_1 drop b;
select * from atdc_1;
select columnname,columnnumber,columndatatype
       from sys.syscolumns where referenceid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
-- Demonstrate that we can drop a column which is the primary key. Also
-- demonstrate that when we drop a column which is the primary key, that
-- cascade processing will drop the corresponding foreign key constraint
create table atdc_1_01 (a int, b int, c int not null primary key);
alter table atdc_1_01 drop column c cascade;
create table atdc_1_02 (a int not null primary key, b int);
create table atdc_1_03 (a03 int, 
   constraint a03_fk foreign key (a03) references atdc_1_02(a));
alter table atdc_1_02 drop column a cascade;
-- drop column restrict should fail because column is used in a constraint:
alter table atdc_1 add constraint atdc_constraint_1 check (a > 0);
select * from sys.sysconstraints where tableid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
select sc.* from sys.syschecks sc,sys.sysconstraints con, sys.systables st
		where sc.constraintid = con.constraintid and con.tableid = st.tableid
              and st.tablename = 'ATDC_1';
alter table atdc_1 drop column a restrict;
-- drop column cascade should also drop the check constraint:
alter table atdc_1 drop column a cascade;
select * from sys.sysconstraints where tableid in
            (select tableid from sys.systables where tablename = 'ATDC_1');
-- Verify the behavior of the various constraint types:
-- check, primary key, foreign key, unique, not null
create table atdc_1_constraints (a int not null primary key,
   b int not null,
   c int constraint atdc_1_c_chk check (c is not null),
   d int not null unique,
   e int,
   f int,
   constraint atdc_1_e_fk foreign key (e) references atdc_1_constraints(a));
-- In restrict mode, none of the columns a, c, d, or e should be droppable,
-- but in cascade mode each of them should be droppable, and at the end
-- we should have only column f
-- column b is droppable because an unnamed NOT NULL constraint doesn't
-- prevent DROP COLUMN, only an explicit CHECK constraint does.
describe atdc_1_constraints;
alter table atdc_1_constraints drop column a restrict;
alter table atdc_1_constraints drop column b restrict;
alter table atdc_1_constraints drop column c restrict;
alter table atdc_1_constraints drop column d restrict;
alter table atdc_1_constraints drop column e restrict;
describe atdc_1_constraints;
alter table atdc_1_constraints drop column a cascade;
alter table atdc_1_constraints drop column c cascade;
alter table atdc_1_constraints drop column d cascade;
alter table atdc_1_constraints drop column e cascade;
describe atdc_1_constraints;
-- Some negative testing of ALTER TABLE DROP COLUMN
-- Table does not exist:
alter table atdc_nosuch drop column a;
-- Table exists, but column does not exist:
create table atdc_2 (a integer);
alter table atdc_2 drop column b;
alter table atdc_2 drop b;
-- Column name is spelled incorrectly (wrong case)
alter table atdc_2 drop column 'a';
-- Some special reserved words to cause parser errors
alter table atdc_2 drop column column;
alter table atdc_2 drop column;
alter table atdc_2 drop column constraint;
alter table atdc_2 drop column primary;
alter table atdc_2 drop column foreign;
alter table atdc_2 drop column check;
create table atdc_3 (a integer);
create index atdc_3_idx_1 on atdc_3 (a);
-- This fails because a is the only column in the table.
alter table atdc_3 drop column a restrict;
drop index atdc_3_idx_1;
-- cascade/restrict processing doesn't currently consider indexes.
-- The column being dropped is automatically dropped from all indexes
-- as well. If that was the only (last) column in the index, then the
-- index is dropped, too.
create table atdc_4 (a int, b int, c int, d int, e int);
insert into atdc_4 values (1,2,3,4,5);
create index atdc_4_idx_1 on atdc_4 (a);
create index atdc_4_idx_2 on atdc_4 (b, c, d);
create index atdc_4_idx_3 on atdc_4 (c, a);
select conglomeratename,isindex from sys.sysconglomerates where tableid in
    (select tableid from sys.systables where tablename = 'ATDC_4');
show indexes from atdc_4;
-- This succeeds, because cascade/restrict doesn't matter for indexes. The
-- effect of dropping column a is that:
--    index atdc_4_idx_1 is entirely dropped
--    index atdc_4_idx_2 is left alone but the column positions are fixed up
--    index atdc_4_idx_3 is modified to refer only to column c
alter table atdc_4 drop column a restrict;
select conglomeratename,isindex from sys.sysconglomerates where tableid in
    (select tableid from sys.systables where tablename = 'ATDC_4');
show indexes from atdc_4;
describe atdc_4;
-- The effect of dropping column c is that:
--    index atdc_4_idx_2 is modified to refer to columns b and d
--    index atdc_4_idx_3 is entirely dropped
alter table atdc_4 drop column c restrict;
show indexes from atdc_4;
select * from atdc_4 where c = 3;
select count(*) from sys.sysconglomerates where conglomeratename='ATDC_4_IDX_2';
select conglomeratename, isindex from sys.sysconglomerates
     where conglomeratename like 'ATDC_4%';
drop index atdc_4_idx_2;
-- drop column restrict should fail becuase column is used in a view:
create table atdc_5 (a int, b int);
create view atdc_vw_1 (vw_b) as select b from atdc_5;
alter table atdc_5 drop column b restrict;
select * from atdc_vw_1;
-- drop column cascade drops the column, and also drops the dependent view:
alter table atdc_5 drop column b cascade;
select * from atdc_vw_1;
-- cascade processing should transitively drop a view dependent on a view
-- dependent in turn on the column being dropped:
create table atdc_5a (a int, b int, c int);
create view atdc_vw_5a_1 (vw_5a_b, vw_5a_c) as select b,c from atdc_5a;
create view atdc_vw_5a_2 (vw_5a_c_2) as select vw_5a_c from atdc_vw_5a_1;
alter table atdc_5a drop column b cascade;
select * from atdc_vw_5a_1;
select * from atdc_vw_5a_2;
-- drop column restrict should fail because column is used in a trigger:
create table atdc_6 (a integer, b integer);
create trigger atdc_6_trigger_1 after update of b on atdc_6
	for each row mode db2sql values current_date;
alter table atdc_6 drop column b restrict;
select triggername from sys.systriggers where triggername='ATDC_6_TRIGGER_1';
alter table atdc_6 drop column b cascade;
select triggername from sys.systriggers where triggername='ATDC_6_TRIGGER_1';
create table atdc_7 (a int, b int, c int, primary key (a));
alter table atdc_7 drop column a restrict;
alter table atdc_7 drop column a cascade;
create table atdc_8 (a int, b int, c int, primary key (b, c));
alter table atdc_8 drop column c restrict;
alter table atdc_8 drop column c cascade;
create table atdc_9 (a int not null, b int);
alter table atdc_9 drop column a restrict;
-- Verify that a GRANTED privilege fails a drop column in RESTRICT mode,
-- and verify that the privilege is dropped in CASCADE mode:
create table atdc_10 (a int, b int, c int);
grant select(a, b, c) on atdc_10 to bryan;
select * from sys.syscolperms;
alter table atdc_10 drop column b restrict;
select * from sys.syscolperms;
alter table atdc_10 drop column b cascade;
select * from sys.syscolperms;
