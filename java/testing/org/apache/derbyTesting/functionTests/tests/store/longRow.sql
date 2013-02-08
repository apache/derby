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
-- create a table with 5 rows, with 4K pageSize,
-- this should expand over 3 pages
run resource '/org/apache/derbyTesting/functionTests/tests/store/createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create table testing 
	(a varchar(2024), b varchar(1024), c varchar(1024), d varchar(2048), e varchar(300)) ;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

-- insert 9 rows into the table
insert into testing values (PADSTRING('1',2024),  PADSTRING('2',1024), 
       PADSTRING('3',1024), PADSTRING('4',2048),  PADSTRING('5',300));

insert into testing values (PADSTRING('10',2024),  
       PADSTRING('20',1024), PADSTRING('30',1024), 
       PADSTRING('40',2048), PADSTRING('50',300));

insert into testing values (PADSTRING('100',2024),  
       PADSTRING('200',1024), PADSTRING('300',1024), 
       PADSTRING('400',2048), PADSTRING('500',300));

insert into testing values (PADSTRING('1000',2024),  
       PADSTRING('2000',1024), PADSTRING('3000',1024), 
       PADSTRING('4000',2048), PADSTRING('5000',300));

insert into testing values (PADSTRING('10000',2024),  
       PADSTRING('20000',1024),	PADSTRING('30000',1024), 
       PADSTRING('40000',2048), PADSTRING('50000',300));

insert into testing values (PADSTRING('100000',2024), 
       PADSTRING('200000',1024), PADSTRING('300000',1024), 
       PADSTRING('400000',2048), PADSTRING('500000',300));

insert into testing values (PADSTRING('1000000',2024), 
       PADSTRING('2000000',1024), PADSTRING('3000000',1024), 
       PADSTRING('4000000',2048), PADSTRING('5000000',300));

insert into testing values (PADSTRING('10000000',2024), 
       PADSTRING('20000000',1024), PADSTRING('30000000',1024), 
       PADSTRING('40000000',2048), PADSTRING('50000000',300));

insert into testing values (PADSTRING('100000000',2024), 
       PADSTRING('200000000',1024), PADSTRING('300000000',1024), 
       PADSTRING('400000000',2048), PADSTRING('500000000',300));

-- select the whole row, or individual columns.
-- 9 rows should be returned from each of the following selects
select * from testing;
select a from testing;
select b from testing;
select c from testing;
select d from testing;
select e from testing;

-- insert some partial rows. 
insert into testing(a)  values (PADSTRING('a',2024));
insert into testing(a,b) values (PADSTRING('a',2024), PADSTRING('b',1024));
insert into testing(a,b,c) values (PADSTRING('a',2024), PADSTRING('b',1024)
			   , PADSTRING('c',1024));
insert into testing(a,b,c,d) values (PADSTRING('a',2024), PADSTRING('b',1024),
        PADSTRING('c',1024), PADSTRING('d',2048));
insert into testing(a,b,c,d,e) values (PADSTRING('a',2024), 
       PADSTRING('b',1024), PADSTRING('c',1024),
       PADSTRING('d',2048), PADSTRING('e',300));

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
update testing set a = PADSTRING('aaa',2024), b = PADSTRING('bbb',1024), 
       c = PADSTRING('ccc',1024), d = PADSTRING('ddd',2048), 
       e = PADSTRING('eee',300) 
	where d = PADSTRING('d',2048);
-- following select should return 0 rows
select * from testing where d = PADSTRING('d',2048);
-- following select should return 2 rows
select * from testing where d = PADSTRING('ddd',2048);

-- create a table with 6 rows
drop table testing;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create table testing (a varchar(500), b varchar(500), c varchar(500), d varchar(500),
	e varchar(500), f varchar(500), g varchar(500), z varchar(3900)) ;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

insert into testing values (PADSTRING('1',500), PADSTRING('2',500), 
       PADSTRING('3',500), PADSTRING('4',500),
       PADSTRING('5',500), PADSTRING('6',500), 
       PADSTRING('7',500), PADSTRING('1000',3900));

insert into testing values (PADSTRING('1',500), PADSTRING('2',500), 
       PADSTRING('3',500), PADSTRING('4',500),
       PADSTRING('5',500), PADSTRING('6',500), 
       PADSTRING('7',500), PADSTRING('1000',2000));

select * from testing;
select e from testing;
select g from testing;
select z from testing;

-- create long rows which expand over 3 or more pages. Test that various
-- qualifier work on the long row columns.
drop table testing;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
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
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);

insert into testing values (3, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 30, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 300, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 3000);
insert into testing values (4, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 40, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 400, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 4000);
insert into testing values (1, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 10, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 100, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 1000);
insert into testing values (2, PADSTRING('fill1',2500), PADSTRING('fill2',2500), 20, PADSTRING('fill3',2500), PADSTRING('fill4',2500), 200, PADSTRING('fill5',2400), PADSTRING('fill6',2400), 2000);

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




call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', 'NULL');
