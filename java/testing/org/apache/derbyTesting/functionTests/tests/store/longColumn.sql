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
-- test sinle long column table
-- create table with one long column
-- test 1: one long column
run resource 'createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create table testing (a varchar(8096)) ;
insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0', 8096));
insert into testing values (PADSTRING('a b c d e f g h i j', 8096));
insert into testing values (PADSTRING('11 22 33 44 55 66 77', 8096));
insert into testing values (PADSTRING('aa bb cc dd ee ff gg', 8096));
-- should return 4 rows
select a from testing;
-- drop the table
drop table testing;

-- test 2: testing two column (1 short, 1 long) table
create table testing (a int, b varchar(32384)) ;
insert into testing values (1, PADSTRING('1 2 3 4 5 6 7 8 9 0', 32384));
insert into testing values (2, PADSTRING('a b c d e f g h i j', 32384));
insert into testing values (3, PADSTRING('11 22 33 44 55 66 77', 32384));
insert into testing values (4, PADSTRING('aa bb cc dd ee ff gg', 32384));
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
-- should return 4 rows
select * from testing;
select a from testing;
select b from testing;
select z from testing;
-- should return 1 row
select b from testing where z = b;
-- try creating btree index on long column, should fail
create index zz on testing (a) ;
-- update the long column 5 times
update testing set a = PADSTRING('update once', 32384);
update testing set a = PADSTRING('update twice', 32384);
update testing set a = PADSTRING('update three times', 32384);
update testing set a = PADSTRING('update four times', 32384);
update testing set a = PADSTRING('update five times', 32384);
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
-- should return 4 rows
select * from testing;
select a from testing;
select b from testing;
select c from testing;
-- should return one row
select * from testing where b = 4;
-- try creating btree index, should fail on long columns
create index zz on testing (a) ;
create index zz on testing (c) ;
create index zz on testing (b);
-- update the last long column 10 times
update testing set c = PADSTRING('update 0', 32084);
update testing set c = PADSTRING('update 1', 32084);
update testing set c = PADSTRING('update 2', 32084);
update testing set c = PADSTRING('update 3', 32084);
update testing set c = PADSTRING('update 4', 32084);
update testing set c = PADSTRING('update 5', 32084);
update testing set c = PADSTRING('update 6', 32084);
update testing set c = PADSTRING('update 7', 32084);
update testing set c = PADSTRING('update 8', 32084);
update testing set c = PADSTRING('update 9', 32084);
-- select should return 4 rows
select * from testing;
-- drop the table
drop table testing;

-- test 6: table with 5 columns (1 short, 1 long, 1 short, 1 long, 1 short) table
create table testing (a int, b clob(64768), c int, d varchar(32384), e int) ;
insert into testing values (0, PADSTRING('1 2 3 4 5 6 7 8 9 0', 64768),  1, PADSTRING('1 2 3 4 5 6 7 8 9 0', 32384),  2);
insert into testing values (1, PADSTRING('a b c d e f g h i j', 64768),  2, PADSTRING('a b c d e f g h i j', 32384),  3);
insert into testing values (2, PADSTRING('11 22 33 44 55 66 77', 64768), 3, PADSTRING('11 22 33 44 55 66 77', 32384), 4);
insert into testing values (3, PADSTRING('aa bb cc dd ee ff gg', 64768), 4, PADSTRING('aa bb cc dd ee ff gg',32384), 5);
insert into testing values (4, PADSTRING('1 2 3 4 5 6 7 8 9 0', 64768),  5, PADSTRING('aa bb cc dd ee ff gg',32384), 6);
insert into testing values (5, PADSTRING('a b c d e f g h i j', 64768),  6, PADSTRING('aa bb cc dd ee ff gg',32384), 7);
insert into testing values (6, PADSTRING('11 22 33 44 55 66 77', 64768), 7, PADSTRING('aa bb cc dd ee ff gg',32384), 8);
insert into testing values (7, PADSTRING('aa bb cc dd ee ff gg', 64768), 8, PADSTRING('aa bb cc dd ee ff gg',32384), 9);
-- select shoudl return 8 rows
select * from testing;
select a from testing;
select b, d from testing;
select a, c, d from testing;
-- update column b 10 times
update testing set b = PADSTRING('update 0', 64768);
update testing set b = PADSTRING('update 1', 64768);
update testing set b = PADSTRING('update 2', 64768);
update testing set b = PADSTRING('update 3', 64768);
update testing set b = PADSTRING('update 4', 64768);
update testing set b = PADSTRING('update 5', 64768);
update testing set b = PADSTRING('update 6', 64768);
update testing set b = PADSTRING('update 7', 64768);
update testing set b = PADSTRING('update 8', 64768);
update testing set b = PADSTRING('update 9', 64768);
-- select should return 8 rows
select b from testing;
select a, b, e from testing;
-- drop the table
drop table testing;

-- test 7: table with 5 columns, all long columns
create table testing (a clob(64768), b varchar(32384), c clob(64768), d varchar(32384), e clob(64768)) ;
insert into testing values (PADSTRING('a a a a a a a a a a',64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
insert into testing values (PADSTRING('a a a a a a a a a a', 64768), PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c', 64768), PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768));
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
update testing set a = PADSTRING('1 1 1 1 1 1 1 1 1 1', 64768);
update testing set e = PADSTRING('9 9 9 9 9 9 9 9 9 9',64768);
-- select should return 10 rows
select a, e from testing;
select a, c, b, e from testing;
select e from testing;
-- drop the table
drop table testing;

exit;
