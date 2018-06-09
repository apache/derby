--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- reusing container id case
run resource 'createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    'extinout/mybackup', 0);
create table t1(a int not null primary key) ;
insert into t1 values(1) ;
insert into t1 values(2) ;
insert into t1 values(3 ) ;
drop table t1;
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat';
--checkpoint to make sure that 
--the stub is dropped and we use the 
--the same container id which we dropped earlier
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();

create table t1(a int not null primary key) ;
insert into t1 values(4) ;
insert into t1 values(5);
insert into t1 values(6);
select * from t1;
connect 'wombat;shutdown=true';
disconnect;
--performa rollforward recovery
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t1 ;
--drop the above tables and create
--again tables with foreign key references and
--make surte they are properly recovered
drop table t1;
create table t1(a int not null);
insert into t1 values(1) ;
insert into t1 values(2) ;
insert into t1 values(3 ) ;
insert into t1 values(4 ) ;
insert into t1 values(5 ) ;
alter table t1 add constraint uk1 unique(a);
create table t2(b int);
insert into t2 values(1);
insert into t2 values(2);
insert into t2 values(3);
alter table t2 add constraint c1 foreign key (b)
                             references t1(a);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
insert into t2 values(4);
insert into t2 values(5);
select * from t1;
select * from t2;
--add a duplicate value to make sure index is intact
insert into t1 values(1);
--add a value that does not exits in the parent table
--to make reference indexes are fine.
insert into t2 values(999);

---REGULAR UNLOGGED CASES , BUT LOGGED WHEN ARCHIVE MODE IS ENABLED.
--compress table 
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T1', 0);
select * from t1;
create table t3(c1 int not null);
create table t4(c1 int not null);
--insert
insert into t3 (c1) 
values(1) ,(2) , (3), (4), (5), (6), (7) , (8), (9) , (10) , (11), (12) , (13) , (14) , (15),
(16), (17), (18) , (19) , (20) , (21) , (22) , (23) , (24) , (25) , (26) , (27) , (28) , (29) , (30);

insert into t4 
values(101) ,(102) , (103), (104), (105), (106), (107) , (108), (109) , (110) , (111), (112) , (113), (114),
(115), (116), (117), (118) , (119) , (120) , (121) , (122) , (123) , (124) , (125) , (126) , (127) , (128), (129), (130);
insert into t4 values(1001);

alter table t3 add column c2 char(20);


--add constraint

--alter table t3 add column c2 int not null primary key;
--alter table t4 add column c2 int not null;
--alter table t3 add column c3 int not null unique;

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t1;
select * from t2;
select * from t3;
select * from t4;
insert into t3 (c1) 
values(101) ,(102) , (103), (104), (105), (106), (107) , (108), (109) , (110) , (111), (112) , (113), (114),
(115), (116), (117), (118) , (119) , (120) , (121) , (122) , (123) , (124) , (125) , (126) , (127) , (128), (129), (130);
insert into t3 (c1) values(1001), (1000);
--unlogged primary key add constraint
alter table t3 add constraint pk1 primary key(c1);
--unlogged foreign key add
alter table t4 add constraint fk1 foreign key (c1) references t3(c1);
--unlogged add unique constraint
alter table t4 add constraint uk2 unique(c1);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
--following insert should throw duplicate error.
insert into t4 values(101);
insert into t3 (c1) values(101);
--folowing should throw foreign key violations error.
insert into t4 values(9999);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t1;
select * from t2;
select c1 from t3;
select * from t4;
autocommit off;
insert into t3 (c1) values(100), 99, 999;
insert into t3 (c1) values(0), (-1);
--let's do some updates .
update t4 set c1 = c1 -1;
update t3 set c1 = c1 + 1;
update t3 set c2 = 'rollforward';
commit;
delete from t2;
delete from t2;
delete from t4;
delete from t3;
rollback;
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t1;
select * from t2;
select * from t3;
select * from t4;
create table t5(c1 int );
--unlogged add column because of primary key
alter table t5 add column c2 int not null primary key default 0;
--logged add column
alter table t5 add column c3 int not null default 0;
--unlogged add column
alter table t5 add column c4 char(100) not null default '0';
alter table t5 add constraint uconst UNIQUE(c4);
insert into t5 values ( 1 , 2, 3 , 'one'), 
(11 , 22, 33, 'eleven'), (111, 222, 333, 'one hundred eleven');
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t5 ;
--check if constraits are intact.
--following insert  should throw error because they violate constraints;
insert into t5 values ( 1 , 2, 3 , 'one');
insert into t5 values ( 1111 , 2222, null , 'one again');
insert into t5 values ( 1111 , 2222, 3333 , 'one');
insert into t5 values ( 1111 , 2222, 3333 , 'four ones ..');

select * from t5;
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from t5;
--- Have to check long varchar/binary  recovery stuff.
-- create a table with 5 rows, with 4K pageSize,
-- this should expand over 3 pages
create table testing 
	(a varchar(2024), b varchar(1024), c varchar(1024), d varchar(2048), e varchar(300)) ;

-- insert 9 rows into the table
insert into testing values (PADSTRING('1',2024),	PADSTRING('2',1024),        PADSTRING('3',1024),         PADSTRING('4',2048),         PADSTRING('5',300));
insert into testing values (PADSTRING('10',2024),	PADSTRING('20',1024),	    PADSTRING('30',1024),        PADSTRING('40',2048),        PADSTRING('50',300));
insert into testing values (PADSTRING('100',2024),	PADSTRING('200',1024),	    PADSTRING('300',1024),       PADSTRING('400',2048),       PADSTRING('500',300));
insert into testing values (PADSTRING('1000',2024),	PADSTRING('2000',1024),     PADSTRING('3000',1024),      PADSTRING('4000',2048),      PADSTRING('5000',300));
insert into testing values (PADSTRING('10000',2024),	PADSTRING('20000',1024),    PADSTRING('30000',1024),     PADSTRING('40000',2048),     PADSTRING('50000',300));
insert into testing values (PADSTRING('100000',2024),	PADSTRING('200000',1024),   PADSTRING('300000',1024),    PADSTRING('400000',2048),    PADSTRING('500000',300));
insert into testing values (PADSTRING('1000000',2024),  PADSTRING('2000000',1024),  PADSTRING('3000000',1024),   PADSTRING('4000000',2048),   PADSTRING('5000000',300));
insert into testing values (PADSTRING('10000000',2024), PADSTRING('20000000',1024), PADSTRING('30000000',1024),  PADSTRING('40000000',2048),  PADSTRING('50000000',300));
insert into testing values (PADSTRING('100000000',2024),PADSTRING('200000000',1024), PADSTRING('300000000',1024), PADSTRING('400000000',2048), PADSTRING('500000000',300));

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';

-- select the whole row, or individual columns.
-- 9 rows should be returned from each of the following selects
select * from testing;
select a from testing;
select b from testing;
select c from testing;
select d from testing;
select e from testing;

-- insert some partial rows. 
insert into testing (a) values (PADSTRING('a',2024));
insert into testing (a, b) values (PADSTRING('a',2024), PADSTRING('b',1024));
insert into testing (a, b, c) values (PADSTRING('a',2024), PADSTRING('b',1024), PADSTRING('c',1024));
insert into testing (a, b, c, d) values (PADSTRING('a',2024), PADSTRING('b',1024), PADSTRING('c',1024), PADSTRING('d',2048));
insert into testing (a, b, c, d, e) values (PADSTRING('a',2024), PADSTRING('b',1024), PADSTRING('c',1024), PADSTRING('d',2048), PADSTRING('e',300));
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- select some partial rows.
-- should select 14 rows
select * from testing;

-- should select 5 rows
select * from testing where a = PADSTRING('a',2024);

-- should select 4 rows
select a,c,d from testing where b = PADSTRING('b',1024);

-- should select 1 row
select b, e from testing where e = PADSTRING('e',300);

-- should select 14 rows
select a,c,e from testing order by a;

-- update 5 rows on the main data page
update testing set a = PADSTRING('aa',2024) where a = PADSTRING('a',2024);
-- following select should return 0 rows
select * from testing where a = PADSTRING('a',2024);
-- following select should return 5 rows
select * from testing where a = PADSTRING('aa',2024);

-- update 3 rows on the overflow page
update testing set c = PADSTRING('cc',1024) where c = PADSTRING('c',1024);
-- following should return 0 rows
select * from testing where c = PADSTRING('c',1024);
-- followign should return 3 rows
select a, b, c, d, e from testing where c = PADSTRING('cc',1024);

-- update 1 row on second overflow page
update testing set e = PADSTRING('ee',300) where e = PADSTRING('e',300);
-- following select should return 0 rows
select e from testing where e = PADSTRING('e',300);
-- following should return 1 row
select e from testing where e = PADSTRING('ee',300);

-- update all columns for 2 rows
update testing set a = PADSTRING('aaa',2024), b = PADSTRING('bbb',1024), c = PADSTRING('ccc',1024), d = PADSTRING('ddd',2048), e = PADSTRING('eee',300)
	where d = PADSTRING('d',2048);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';

-- following select should return 0 rows
select * from testing where d = PADSTRING('d',2048);
-- following select should return 2 rows
select * from testing where d = PADSTRING('ddd',2048);

-- create a table with 6 rows
drop table testing;
create table testing (a varchar(500), b varchar (500), c varchar(500), d varchar(500),
	e varchar(500), f varchar(500), g varchar(500), z varchar(3900)) ;
insert into testing values (PADSTRING('1',500), PADSTRING('2',500), PADSTRING('3',500), PADSTRING('4',500), PADSTRING('5',500), PADSTRING('6',500), PADSTRING('7',500), PADSTRING('1000',3900));
insert into testing values (PADSTRING('1',500), PADSTRING('2',500), PADSTRING('3',500), PADSTRING('4',500), PADSTRING('5',500), PADSTRING('6',500), PADSTRING('7',500), PADSTRING('2000',3900));
select * from testing;
select e from testing;
select g from testing;
select z from testing;

-- create long rows which expand over 3 or more pages. Test that various
-- qualifier work on the long row columns.
drop table testing;
create table testing (
    key1    int, 
    filler1 varchar(2500), 
    filler2 varchar(2500), 
    key2    int,
    filler3 varchar(2500), 
    filler4 varchar(2500), 
    key3    int,
    filler5 varchar(2400),
    filler6 varchar(2400),
    key4    int) ;

insert into testing values (3, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 30, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 300, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 3000);
insert into testing values (4, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 40, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 400, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 4000);
insert into testing values (1, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 10, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 100, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 1000);
insert into testing values (2, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 20, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 200, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 2000);

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';

select * from testing;
select key2 from testing;
select key3 from testing;
select key4 from testing;

select * from testing where key1 = 1;
select * from testing where key2 = 20;
select * from testing where key3 = 300;
select * from testing where key4 = 4000;

select * from testing where key1 = 1 and key2 = 10;
select * from testing where key2 = 20 and key3 = 200;
select * from testing where key3 = 300 and key4 = 3000;
select * from testing where key4 = 4000 and key1 = 4;

select * from testing where key1 = 1 or key2 = 20;
select * from testing where key2 = 20 or key3 = 300;
select * from testing where key3 = 300 or key4 = 4000;
select * from testing where key4 = 4000 or key1 = 1;
drop table testing;
--END OF LONG ROW TEST

-- test sinle long column table
-- create table with one long column
-- test 1: one long column
create table testing (a varchar(8096)) ;
insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0',8096));
insert into testing values (PADSTRING('a b c d e f g h i j',8096));
insert into testing values (PADSTRING('11 22 33 44 55 66 77',8096));
insert into testing values (PADSTRING('aa bb cc dd ee ff gg',8096));
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- should return 4 rows
select a from testing;
-- drop the table
drop table testing;

-- test 2: testing two column (1 short, 1 long) table
create table testing (a int, b varchar(32384)) ;
insert into testing values (1, PADSTRING('1 2 3 4 5 6 7 8 9 0',32384));
insert into testing values (2, PADSTRING('a b c d e f g h i j',32384));
insert into testing values (3, PADSTRING('11 22 33 44 55 66 77',32384));
insert into testing values (4, PADSTRING('aa bb cc dd ee ff gg',32384));

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';

-- should return 4 rows
select * from testing;
select a from testing;
select b from testing;
-- should return 1 row
select b from testing where a = 1;
-- drop the table
drop table testing;

-- test 3: testing two column (1 long, 1 shor) table
create table testing (a varchar(32384), b int) ;
insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0',32384), 1);
insert into testing values (PADSTRING('a b c d e f g h i j',32384), 2);
insert into testing values (PADSTRING('11 22 33 44 55 66 77',32384), 3);
insert into testing values (PADSTRING('aa bb cc dd ee ff gg',32384), 4);

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- should return 4 rows
select * from testing;
select a from testing;
select b from testing;
-- should return 1 row
select a from testing where b = 4;
-- drop the table
drop table testing;

-- test 4: testing three column (1 short, 1 long, 1 short) table
create table testing (z int, a varchar(32384), b int) ;
insert into testing values (0, PADSTRING('1 2 3 4 5 6 7 8 9 0',32384), 1);
insert into testing values (1, PADSTRING('a b c d e f g h i j',32384), 2);
insert into testing values (2, PADSTRING('11 22 33 44 55 66 77',32384), 3);
insert into testing values (4, PADSTRING('aa bb cc dd ee ff gg',32384), 4);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- should return 4 rows
select * from testing;
select a from testing;
select b from testing;
select z from testing;
-- should return 1 row
select b from testing where z = b;
-- try creating index on long column, should fail
create index zz on testing (a) ;
-- update the long column 5 times
update testing set a = PADSTRING('update once',32384);
update testing set a = PADSTRING('update twice',32384);
update testing set a = PADSTRING('update three times',32384);
update testing set a = PADSTRING('update four times',32384);
update testing set a = PADSTRING('update five times',32384);

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- select should return 4 rows
select a from testing;
-- drop the table
drop table testing;

-- test 5: testing three columns (1 long, 1 short, 1 long) table
create table testing (a varchar(32384), b int, c varchar(32084)) ;
insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0',32384), 1, PADSTRING('1 2 3 4 5 6 7 8 9 0',32084));
insert into testing values (PADSTRING('a b c d e f g h i j',32384), 2, PADSTRING('a b c d e f g h i j',32084));
insert into testing values (PADSTRING('11 22 33 44 55 66 77',32384), 3, PADSTRING('11 22 33 44 55 66 77',32084));
insert into testing values (PADSTRING('aa bb cc dd ee ff gg',32384), 4, PADSTRING('aa bb cc dd ee ff gg',32084));
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- should return 4 rows
select * from testing;
select a from testing;
select b from testing;
select c from testing;
-- should return one row
select * from testing where b = 4;
-- try creating index, should fail on long columns
create index zz on testing (a) ;
create index zz on testing (c) ;
create index zz on testing (b);
-- update the last long column 10 times
update testing set c = PADSTRING('update 0',32084);
update testing set c = PADSTRING('update 1',32084);
update testing set c = PADSTRING('update 2',32084);
update testing set c = PADSTRING('update 3',32084);
update testing set c = PADSTRING('update 4',32084);
update testing set c = PADSTRING('update 5',32084);
update testing set c = PADSTRING('update 6',32084);
update testing set c = PADSTRING('update 7',32084);
update testing set c = PADSTRING('update 8',32084);
update testing set c = PADSTRING('update 9',32084);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- select should return 4 rows
select * from testing;
-- drop the table
drop table testing;

-- test 6: table with 5 columns (1 short, 1 long, 1 short, 1 long, 1 short) table
create table testing (a int, b clob(64768), c int, d varchar(32384), e int) ;
insert into testing values (0, PADSTRING('1 2 3 4 5 6 7 8 9 0',64768),  1, PADSTRING('1 2 3 4 5 6 7 8 9 0',32384),  2);
insert into testing values (1, PADSTRING('a b c d e f g h i j',64768),  2, PADSTRING('a b c d e f g h i j',32384),  3);
insert into testing values (2, PADSTRING('11 22 33 44 55 66 77',64768), 3, PADSTRING('11 22 33 44 55 66 77',32384), 4);
insert into testing values (3, PADSTRING('aa bb cc dd ee ff gg',64768), 4, PADSTRING('aa bb cc dd ee ff gg',32384), 5);
insert into testing values (4, PADSTRING('1 2 3 4 5 6 7 8 9 0',64768),  5, PADSTRING('aa bb cc dd ee ff gg',32384), 6);
insert into testing values (5, PADSTRING('a b c d e f g h i j',64768),  6, PADSTRING('aa bb cc dd ee ff gg',32384), 7);
insert into testing values (6, PADSTRING('11 22 33 44 55 66 77',64768), 7, PADSTRING('aa bb cc dd ee ff gg',32384), 8);
insert into testing values (7, PADSTRING('aa bb cc dd ee ff gg',64768), 8, PADSTRING('aa bb cc dd ee ff gg',32384), 9);
connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- select shoudl return 8 rows
select * from testing;
select a from testing;
select b, d from testing;
select a, c, d from testing;
-- update column b 10 times
update testing set b = PADSTRING('update 0',64768);
update testing set b = PADSTRING('update 1',64768);
update testing set b = PADSTRING('update 2',64768);
update testing set b = PADSTRING('update 3',64768);
update testing set b = PADSTRING('update 4',64768);
update testing set b = PADSTRING('update 5',64768);
update testing set b = PADSTRING('update 6',64768);
update testing set b = PADSTRING('update 7',64768);
update testing set b = PADSTRING('update 8',64768);
update testing set b = PADSTRING('update 9',64768);

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
-- select should return 8 rows
select b from testing;
select a, b, e from testing;
-- drop the table
drop table testing;

-- test 7: table with 5 columns, all long columns
create table testing (a clob(64768), b varchar(32384), c clob(64768), d varchar(32384), e clob(64768)) ;
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d',32384), PADSTRING('e e e e e e e e',64768));

connect 'wombat;shutdown=true';
disconnect;
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';

-- select should return 10 rows
select * from testing;
select a from testing;
select b from testing;
select c from testing;
select d from testing;
select e from testing;
select a, c, e from testing;
select b, e from testing;
-- update the first and last column
update testing set a = PADSTRING('1 1 1 1 1 1 1 1 1 1',64768);
update testing set e = PADSTRING('9 9 9 9 9 9 9 9 9 9',64768);
-- select should return 10 rows
select a, e from testing;
select a, c, b, e from testing;
select e from testing;
-- drop the table
drop table testing;
--END OF LONG COL TEST WITH ROLLFORWARD RECOVERY.












