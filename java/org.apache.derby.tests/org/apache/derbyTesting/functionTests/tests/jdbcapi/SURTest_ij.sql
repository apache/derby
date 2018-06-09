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
create table t1 (c1 int primary key, c2 int);
insert into t1 values 
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), 
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
get scroll insensitive cursor sc1 as 'SELECT * FROM t1 FOR UPDATE';
next sc1;
next sc1;
-- update row nr. 2 after positioning with next
update t1 set c2 = c1 + 20 where current of sc1;
absolute 5 sc1;
-- update row nr. 5 after positioning with absolute
update t1 set c2 = c1 + 20 where current of sc1;
relative 2 sc1;
-- update row nr. 7 after positioning with relative
update t1 set c2 = c1 + 20 where current of sc1;
previous sc1;
-- update row nr. 6 after positioning with previous
update t1 set c2 = c1 + 20 where current of sc1;
relative -1 sc1;
last sc1;
-- update row nr. 10 after positioning with last
update t1 set c2 = c1 + 20 where current of sc1;
after last sc1;
-- update when positioned after last should cause an error
update t1 set c2 = c1 + 20 where current of sc1;
first sc1;
-- update row nr. 1 after positioning with first
update t1 set c2 = c1 + 20 where current of sc1;
before first sc1;
-- update when positioned before first should cause an error
update t1 set c2 = c1 + 20 where current of sc1;
close sc1;
commit;
-- check that row where correctly updated
select * from t1;
get scroll insensitive cursor sc1 as 'SELECT * FROM t1 FOR UPDATE';
next sc1;
next sc1;
-- delete row nr. 2 after positioning with next
delete from t1 where current of sc1;
absolute 5 sc1;
-- delete row nr. 5 after positioning with absolute
delete from t1 where current of sc1;
relative 2 sc1;
-- delete row nr. 7 after positioning with relative
delete from t1 where current of sc1;
previous sc1;
-- delete row nr. 6 after positioning with previous
delete from t1 where current of sc1;
relative -1 sc1;
last sc1;
-- delete row nr. 10 after positioning with last
delete from t1 where current of sc1;
after last sc1;
-- delete when positioned after last should cause an error
delete from t1 where current of sc1;
first sc1;
-- delete row nr. 1 after positioning with first
delete from t1 where current of sc1;
before first sc1;
-- delete when positioned before first should cause an error
delete from t1 where current of sc1;
close sc1;
commit;
-- check that row where correctly updated
select * from t1;
