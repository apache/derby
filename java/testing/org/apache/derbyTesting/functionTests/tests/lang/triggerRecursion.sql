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
-- Trigger recursion test
--

-- test the maximum recursion level to be less than 16
create table t1 (x int);
create table t2 (x int);
create table t3 (x int);
create table t4 (x int);
create table t5 (x int);
create table t6 (x int);
create table t7 (x int);
create table t8 (x int);
create table t9 (x int);
create table t10 (x int);
create table t11 (x int);
create table t12 (x int);
create table t13 (x int);
create table t14 (x int);
create table t15 (x int);
create table t16 (x int);
create table t17 (x int);
create trigger tr1 after insert on t1 for each row insert into t2 values 666;
create trigger tr2 after insert on t2 for each row insert into t3 values 666;
create trigger tr3 after insert on t3 for each row insert into t4 values 666;
create trigger tr4 after insert on t4 for each row insert into t5 values 666;
create trigger tr5 after insert on t5 for each row insert into t6 values 666;
create trigger tr6 after insert on t6 for each row insert into t7 values 666;
create trigger tr7 after insert on t7 for each row insert into t8 values 666;
create trigger tr8 after insert on t8 for each row insert into t9 values 666;
create trigger tr9 after insert on t9 for each row insert into t10 values 666;
create trigger tr10 after insert on t10 for each row insert into t11 values 666;
create trigger tr11 after insert on t11 for each row insert into t12 values 666;
create trigger tr12 after insert on t12 for each row insert into t13 values 666;
create trigger tr13 after insert on t13 for each row insert into t14 values 666;
create trigger tr14 after insert on t14 for each row insert into t15 values 666;
create trigger tr15 after insert on t15 for each row insert into t16 values 666;
create trigger tr16 after insert on t16 for each row insert into t17 values 666;
create trigger tr17 after insert on t17 for each row values 1;

-- here we go
;
insert into t1 values 1;

-- prove it
select * from t1;

-- The following should work, but because of defect 5602, it raises NullPointerException.
-- After the fix for 5602, we could enable the following part of the test.
-- Reduce the recursion level to 16. It should pass now.
drop trigger tr17;

insert  into t1 values 2;

-- prove it
select * from t1;

-- clean up
drop table t17;
drop table t16;
drop table t15;
drop table t14;
drop table t13;
drop table t12;
drop table t11;
drop table t10;
drop table t9;
drop table t8;
drop table t7;
drop table t6;
drop table t5;
drop table t4;
drop table t3;
drop table t2;
drop table t1;

-- DERBY-2195
-- Nested triggers not working properly after maximum trigger count exception is thrown
create table t1 (i int);
insert into t1 values 1,2,3;
create trigger tr1 after update on t1 for each row update t1 set i=i+1;
update t1 set i=i+1;
drop trigger tr1;
create trigger tr1 after update on t1 referencing old as oldt for each row update t1 set i=i+1 where oldt.i=2;
-- ok
update t1 set i=i+1;
select * from t1;
drop trigger tr1;
drop table t1;

