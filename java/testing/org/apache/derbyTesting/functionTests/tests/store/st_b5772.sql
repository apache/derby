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
autocommit off;

create table foo (a int, b char(250), c char(250), d int);
insert into foo values (1, '1', '1', 1);
insert into foo values (11, '11', '1', 1);
insert into foo values (12, '12', '1', 1);
insert into foo values (13, '13', '1', 1);
insert into foo values (14, '14', '1', 1);
insert into foo values (15, '15', '1', 1);
insert into foo values (16, '16', '1', 1);
insert into foo values (17, '17', '1', 1);
insert into foo values (18, '18', '1', 1);
insert into foo values (11, '111', '1', 1);
insert into foo values (12, '121', '1', 1);
insert into foo values (13, '131', '1', 1);
insert into foo values (14, '141', '1', 1);
insert into foo values (15, '151', '1', 1);
insert into foo values (16, '161', '1', 1);
insert into foo values (17, '171', '1', 1);
insert into foo values (18, '181', '1', 1);
insert into foo values (2, '2', '1', 1);
insert into foo values (3, '3', '1', 1);
insert into foo values (4, '4', '1', 1);
insert into foo values (5, '5', '1', 1);
insert into foo values (6, '6', '1', 1);
insert into foo values (7, '7', '1', 1);
insert into foo values (8, '8', '1', 1);
insert into foo values (9, '9', '1', 1);
create index foox on foo (b);


-- normal max optimization, last row in index is not deleted.
select max(b) from foo;

-- new max optimization, last row in index is deleted but others on page aren't.
delete from foo where a = 9;
select max(b) from foo;

-- new max optimization, last row in index is deleted but others on page aren't.
delete from foo where a = 8;
select max(b) from foo;

-- new max optimization, last row in index is null, real max on last page.
insert into foo values (9, null, '1', 1);
select max(b) from foo;

-- new max optimization, last is null and deleted, real max on last page.
delete from foo where a > 2;
select max(b) from foo;

-- max optimization does not work - fail over to scan, all rows on last page are
-- deleted, except for non-deleted null row on last page. max row on 1st page.
delete from foo where a > 1;
insert into foo values (9, null, '1', 1);
select max(b) from foo;

-- max optimization does not work - fail over to scan, all rows on last page are
-- deleted.  non-deleted null row on last page. max row is on 1st page.
delete from foo where a > 1;
select max(b) from foo;

create table b5772 (a int, b int);
create index b1 on b5772(b);

-- 0 row case
select max(b) from b5772;
select min(b) from b5772;

-- 1 row case
insert into b5772 values (1, 1);
select max(b) from b5772;
select min(b) from b5772;

-- 1 null row case
drop table b5772;
create table b5772 (a int, b int);
create index b1 on b5772(b);
insert into b5772 values (2, null);

select max(b) from b5772;
select min(b) from b5772;

-- 1 row plus, one null row.
insert into b5772 values (1, 1);

-- cleanup
drop table b5772;
drop table foo;
commit;
