-- tests for synonym support

set schema APP;
-- negative tests
-- Create a synonym to itself. Error.
create synonym syn for syn;
create synonym syn for APP.syn;
create synonym APP.syn for syn;
create synonym APP.syn for APP.syn;

-- Create a simple synonym loop. Error.
create synonym synonym1 for synonym;
create synonym synonym for synonym1;
drop synonym synonym1;

-- Create a larger synonym loop.
create synonym ts1 for ts;
create synonym ts2 for ts1;
create synonym ts3 for ts2;
create synonym ts4 for ts3;
create synonym ts5 for ts4;
create synonym ts6 for ts5;
create synonym ts for ts6;
drop synonym App.ts1;
drop synonym "APP".ts2;
drop synonym TS3;
drop synonym ts4;
drop synonym ts5;
drop synonym app.ts6;

-- Synonyms and table/view share same namespace. Negative tests for this.
create table table1 (i int, j int);
insert into table1 values (1,1), (2,2);
create view view1 as select i, j from table1;

create synonym table1 for t1;
create synonym APP.Table1 for t1;
create synonym app.TABLE1 for "APP"."T";

create synonym APP.VIEW1 for v1;
create synonym "APP"."VIEW1" for app.v;

-- Synonyms can't be created on temporary tables
declare global temporary table session.t1 (c1 int) not logged;
create synonym synForTemp for session.t1;
create synonym synForTemp for session."T1";

-- Synonyms can't be created in session schemas
create synonym session.table1 for APP.table1;

-- Creating a table or a view when a synonym of that name is present. Error.
create synonym myTable for table1;

create table myTable(i int, j int);

create view myTable as select * from table1;


-- Positive test cases

-- Using synonym in DML
select * from myTable;
select * from table1;
insert into myTable values (3,3), (4,4);

select * from mytable;

update myTable set i=3 where j=4;

select * from mytable;
select * from table1;

delete from myTable where i> 2;

select * from "APP"."MYTABLE";
select * from APP.table1;

-- Try some cursors
get cursor c1 as 'select * from myTable';

next c1;
next c1;

close c1;

-- Try updatable cursors

autocommit off;
get cursor c2 as 'select * from myTable for update';

next c2;
update myTable set i=5 where current of c2;
close c2;

autocommit on;

select * from table1;

-- Try updatable cursors, with synonym at the top, base table inside.
autocommit off;
get cursor c2 as 'select * from app.table1 for update';

next c2;
update myTable set i=6 where current of c2;
close c2;

autocommit on;

select * from table1;

-- trigger tests
create table table2 (i int, j int);

-- Should fail
create trigger tins after insert on myTable referencing new_table as new for each statement mode db2sql insert into table2 select i,j from table1;

-- Should pass
create trigger tins after insert on table1 referencing new_table as new for each statement mode db2sql insert into table2 select i,j from table1;

drop trigger tins;

create trigger triggerins after insert on table2 referencing new_table as new for each statement mode db2sql insert into myTable select i,j from new;

select * from myTable;
insert into table2 values (5, 5);
select * from myTable;

drop table table2;

-- Try referential constraints. Synonyms should not be allowed there.

create table primaryTab (i int not null primary key, j int, c char(10));

create synonym synPrimary for primaryTab;

-- Should fail
create table foreignTab(i int, j int references synPrimary(i));

create table foreignTab(i int, j int references primaryTab(i));

drop table foreignTab;
drop table primaryTab;
drop synonym synPrimary;

-- Tests with non existant schemas
-- Implicitly creates junkSchema
create synonym junkSchema.syn1 for table2;
select * from junkSchema.syn1;
set schema junkSchema;
create table table2(c char(10));
select * from syn1;
set schema APP;

-- Should resolve to junkSchema.table2
select * from junkSchema.syn1;
drop table junkSchema.table2;

-- Should fail. Need to drop synonym first
drop schema junkSchema restrict;
drop synonym junkSchema.syn1;
drop schema junkSchema restrict;

-- Test with target schema missing
create synonym mySynonym for notPresent.t1;
select * from mySynonym;
create table notPresent.t1(j int, c char(10));
insert into notPresent.t1 values (100, 'satheesh');
-- Should resolve now
select * from mySynonym;

drop table notPresent.t1;
drop synonym mySynonym;

-- Positive test case with three levels of synonym chaining

create schema synonymSchema;

create synonym synonymSchema.mySynonym1 for APP.table1;
create synonym APP.mySynonym2 for "SYNONYMSCHEMA"."MYSYNONYM1";
create synonym mySynonym for mySynonym2;


select * from table1;
select * from mySynonym;

insert into mySynonym values (6,6);
insert into mySynonym select * from mySynonym where i<2;

select * from mySynonym;

update mySynonym set j=5;

update mySynonym set j=4 where i=5;

delete from mySynonym where j=6;

select * from mySynonym;
select * from table1;

-- cursor on mySynonym
get cursor c1 as 'select * from mySynonym';

next c1;
next c1;
next c1;

close c1;

-- More negative tests to check dependencies
select * from mySynonym;
drop synonym mySynonym;

-- Previously compiled cached statement should get invalidated
select * from mySynonym;

create synonym mySyn for table1;
create view v1 as select * from mySyn;
create view v2 as select * from v1;

-- Drop synonym should fail since it is used in two views.
drop synonym mySyn;

drop view v2;

-- fail still
drop synonym mySyn;

drop view v1;

-- should pass
drop synonym mySyn;

-- drop and recreate schema test
create schema testSchema;

create synonym multiSchema for testSchema.testtab;

select * from multiSchema;

create table testSchema.testtab(i int, c char(10));
insert into testSchema.testtab values (1, 'synonym');

select * from multiSchema;

drop table testSchema.testtab;
drop schema testSchema restrict;

create schema testSchema;

create table testSchema.testtab(j int, c1 char(10), c2 char(20));
insert into testSchema.testtab values (1, 'synonym', 'test');

select * from multiSchema;

drop synonym multiSchema;
drop table testSchema.testtab;

drop view view1;
drop table table1;

