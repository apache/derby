--------------------------------------------------------------------------------
-- Test multi user lock interaction of ddl. 
--------------------------------------------------------------------------------
autocommit off;

connect 'wombat' as create1;
connect 'wombat' as create2;

--------------------------------------------------------------------------------
-- Test 0: create1:serializable, create2:serializable
--------------------------------------------------------------------------------

-- set up
set connection create1;
autocommit off;
drop table create1;
drop table data;
create table data (keycol int, data char(250));
create unique index d1 on data (keycol);
insert into data values (0, '0');
insert into data values (10, '100');
insert into data values (20, '200');
insert into data values (30, '300');
insert into data values (40, '400');
insert into data values (50, '100');
insert into data values (60, '200');
insert into data values (70, '300');
insert into data values (80, '400');

set isolation RR;
commit;
set connection create2;
autocommit off;
drop table create2;
set current isolation = serializable;
commit;

run resource 'cisco.subsql';

--------------------------------------------------------------------------------
-- Test 1: create1:serializable, create2:repeatable read
--------------------------------------------------------------------------------

-- set up
set connection create1;
autocommit off;
drop table create1;
drop table data;
create table data (keycol int, data char(250));
create unique index d1 on data (keycol);
insert into data values (0, '0');
insert into data values (10, '100');
insert into data values (20, '200');
insert into data values (30, '300');
insert into data values (40, '400');
insert into data values (50, '100');
insert into data values (60, '200');
insert into data values (70, '300');
insert into data values (80, '400');

set isolation to repeatable READ;
commit;
set connection create2;
autocommit off;
drop table create2;
set isolation RS;
commit;

run resource 'cisco.subsql';

--------------------------------------------------------------------------------
-- Test 1: create1:repeatable read, create2:serializable
--------------------------------------------------------------------------------

-- set up
set connection create1;
autocommit off;
drop table create1;
drop table data;
create table data (keycol int, data char(250));
create unique index d1 on data (keycol);
insert into data values (0, '0');
insert into data values (10, '100');
insert into data values (20, '200');
insert into data values (30, '300');
insert into data values (40, '400');
insert into data values (50, '100');
insert into data values (60, '200');
insert into data values (70, '300');
insert into data values (80, '400');

set isolation to rs;
commit;
set connection create2;
autocommit off;
drop table create2;
set isolation serializable;
commit;

run resource 'cisco.subsql';


--------------------------------------------------------------------------------
-- cleanup
--------------------------------------------------------------------------------
set connection create1;
disconnect;
set connection create2;
disconnect;


exit;
