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
--
-- this test shows the basic functionality
-- that does work in the language system for mad hare
--

-- this was the simple mad hare challenge

create table t(i int);
insert into t (i) values (1956);

select i from t;

-- we can also have multiple columns
create table s (i int, n int, t int, e int, g int, r int);
-- and reorder the columns on the insert
insert into s (i,r,t,n,g,e) values (1,6,3,2,5,4);
-- or not list the columns at all
-- (not to mention inserting more than one row into the table)
insert into s values (10,11,12,13,14,15);

-- and we can select some of the columns
select i from s;

-- and in funny orders
select n,e,r,i,t,g from s;

-- and with constants instead
select 20,n,22,e,24,r from s;

-- we do have prepare and execute support
prepare stmt as 'select i,n,t,e,g,r from s';
execute stmt;
-- execute can be done multiple times
execute stmt;

-- and, we also have smallint!
create table r (s smallint, i int);
insert into r values (23,2);
select s,i from r;

-- cleanup
drop table r;
drop table s;
drop table t;
