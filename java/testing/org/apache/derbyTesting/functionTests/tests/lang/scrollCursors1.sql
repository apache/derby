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
-- create some tables
create table t1(c50 char(50), i int);
create table t2(c50 char(50), i int);

-- populate tables
insert into t1 values ('b', 2), ('c', 3), ('d', 4), ('e', 5),
		      ('f', 6), ('g', 7), ('h', 8), ('i', 9),
		      ('j', 10), ('k', 11), ('l', 12), ('m', 13);

autocommit off;

-- negative

-- position on forward only cursor
get cursor c1 as 'select i from t1';
getcurrentrownumber c1;
first c1;
last c1;
previous c1;
next c1;
before first c1;
after last c1;
absolute 1 c1;
relative 1 c1;
close c1;

get scroll insensitive cursor c1 as 'select * from t1';
absolute 0 c1;
close c1;
get scroll insensitive cursor c1 as 'select * from t1';
relative 0 c1;
close c1;
get scroll insensitive cursor c1 as 'select * from t1';
relative 2 c1;
close c1;

-- positive

-- test positioning
get scroll insensitive cursor c1 as 'select * from t1';
-- 2
first c1;
getcurrentrownumber c1;
-- 3
next c1;
getcurrentrownumber c1;
-- 2
first c1;
getcurrentrownumber c1;
-- 3
next c1;
getcurrentrownumber c1;
-- 4
next c1;
getcurrentrownumber c1;
-- 2
first c1;
getcurrentrownumber c1;
-- 3
next c1;
getcurrentrownumber c1;
-- null
after last c1;
getcurrentrownumber c1;
-- beetle 5509
-- null
next c1;
-- beetle 5509
getcurrentrownumber c1;
-- beetle 5509
-- 13
previous c1;
-- beetle 5509
getcurrentrownumber c1;
-- beetle 5509
-- 12
previous c1;
-- beetle 5509
getcurrentrownumber c1;
-- 13
last c1;
getcurrentrownumber c1;
-- null
before first c1;
getcurrentrownumber c1;
-- 2
next c1;
getcurrentrownumber c1;
-- 13
absolute 12 c1;
getcurrentrownumber c1;
-- 3
absolute -11 c1;
getcurrentrownumber c1;
-- null
absolute 13 c1;
getcurrentrownumber c1;
-- null
absolute -13 c1;
getcurrentrownumber c1;
-- absolute -1 should be last row
absolute -1 c1;
getcurrentrownumber c1;
close c1;

-- do last first
get scroll insensitive cursor c1 as 'select * from t1';
-- 13
last c1;
getcurrentrownumber c1;
-- null
next c1;
getcurrentrownumber c1;
-- 13
last c1;
getcurrentrownumber c1;
-- 12
previous c1;
getcurrentrownumber c1;
-- 2
first c1;
getcurrentrownumber c1;
-- null
previous c1;
getcurrentrownumber c1;
-- 2
next c1;
getcurrentrownumber c1;
close c1;

-- do after last first
get scroll insensitive cursor c1 as 'select * from t1';
-- null
after last c1;
-- 13
previous c1;
-- 12
previous c1;
close c1;

-- go to next to last row, then do next
get scroll insensitive cursor c1 as 'select * from t1 where i >= 11';
-- 11
next c1;
getcurrentrownumber c1;
-- 12
next c1;
getcurrentrownumber c1;
-- 13
last c1;
getcurrentrownumber c1;
-- 12
previous c1;
getcurrentrownumber c1;
-- null
after last c1;
getcurrentrownumber c1;
-- 13
previous c1;
close c1;

-- start at after last
get scroll insensitive cursor c1 as 'select * from t1 where i >= 11';
-- null
after last c1;
getcurrentrownumber c1;
-- 13
previous c1;
getcurrentrownumber c1;
close c1;

-- use absolute to get rows before
-- scan would get to them
get scroll insensitive cursor c1 as 'select i from t1';
-- 6
absolute 5 c1;
getcurrentrownumber c1;
-- 9
absolute -5 c1;
getcurrentrownumber c1;
-- 6
absolute 5 c1;
getcurrentrownumber c1;
close c1;

get scroll insensitive cursor c1 as 'select i from t1';
-- null
absolute 13 c1;
getcurrentrownumber c1;
-- 13
previous c1;
getcurrentrownumber c1;
close c1;

get scroll insensitive cursor c1 as 'select i from t1';
-- null
absolute -13 c1;
getcurrentrownumber c1;
-- 2
next c1;
getcurrentrownumber c1;
close c1;

-- test relative implementation
get scroll insensitive cursor c1 as 'select i from t1';
-- 2
first c1;
getcurrentrownumber c1;
-- 13
relative 11 c1;
getcurrentrownumber c1;
-- null
relative 1 c1;
getcurrentrownumber c1;
-- 13
last c1;
getcurrentrownumber c1;
-- 2
relative -11 c1;
getcurrentrownumber c1;
close c1;


-- scroll sensitive cursor becomes scroll insensitive
commit;
get scroll sensitive cursor c1 as 'select i from t1';
first c1;
next c1;
update t1 set i = 666 where i = 2;
first c1;
rollback;
close c1;

-- verify that statement cache works
-- correctly with scroll and forward only
-- cursors on same query text
get scroll insensitive cursor c1 as 'select i from t1';
get cursor c2 as 'select i from t1';
first c1;
next c2;
first c2;
close c1;
close c2;

-- first, last, etc. on empty result set
get scroll insensitive cursor c1 as 'select i from t1 where 1=0';
first c1;
getcurrentrownumber c1;
previous c1;
getcurrentrownumber c1;
next c1;
getcurrentrownumber c1;
last c1;
getcurrentrownumber c1;
next c1;
getcurrentrownumber c1;
previous c1;
getcurrentrownumber c1;
absolute 1 c1;
getcurrentrownumber c1;
absolute -1 c1;
getcurrentrownumber c1;
close c1;

get scroll insensitive cursor c1 as 'select i from t1 where 1=0';
after last c1;
getcurrentrownumber c1;
previous c1;
getcurrentrownumber c1;
before first c1;
getcurrentrownumber c1;
next c1;
getcurrentrownumber c1;
close c1;

get scroll insensitive cursor c1 as 'select i from t1 where 1=0';
absolute 1 c1;
absolute -1 c1;
close c1;

get scroll insensitive cursor c1 as 'select i from t1 where 1=0';
absolute -1 c1;
absolute 1 c1;
close c1;

autocommit on;
get scroll insensitive with hold cursor c1 as 'select i from t1 where 1=0';
first c1;
first c1;
last c1;
last c1;
absolute 1 c1;
absolute -1 c1;
before first c1;
after last c1;
previous c1;
next c1;
-- beetle 5510
next c1;
close c1;

-- cursor on a sort
get scroll insensitive cursor c1 as 'select * from t1 order by i desc';
-- 2
last c1;
-- 13
first c1;
-- 2
relative 11 c1;
-- 3
previous c1;
close c1;

-- RTS
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;
get scroll insensitive cursor c1 as 'select * from t1';
last c1;
first c1;
next c1;
close c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
get scroll insensitive cursor c1 as 'select * from t1';
close c1;

-- for following set of tests, setting the holdability over commit to false for this connection since that is what we want to test below
-- Using this rather than passing with nohold to cursor statement because this test also runs in jdk13 and lower and there is no way to
-- set the holdability using jdbc api in those jdks (unless trying that through a jdbc program where one can use reflection to set holdability
-- in jdk131)
NoholdForConnection;
-- beetle 4551 - insensitive cursor uses estimated row count which might be
-- pessimistic and will get out of memory error
create table big(a int generated always as identity (start with 1, increment by 1));
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
insert into big values(default);
get scroll insensitive cursor s1 as
'select * from big b1 left outer join  big b2 on b1.a = b2.a left outer join  big b3 on b2.a = b3.a left outer join big b4 on b3.a = b4.a left outer join (big b5 left outer join (big b6 left outer join (big b7 left outer join big b8 on b7.a = b8.a) on b6.a=b7.a) on b5.a = b6.a) on b4.a = b5.a';

-- clean up
drop table t1;
drop table t2;
drop table big;
