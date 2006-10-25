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
create table t1( id integer not null primary key, i1 integer, i2 integer, c10 char(10), c30 char(30), tm time);
create table t2( id integer not null primary key, i1 integer, i2 integer, vc20 varchar(20), d double, dt date);
insert into t1(id,i1,i2,c10,c30) values
  (1,1,1,'a','123456789012345678901234567890'),
  (2,1,2,'a','bb'),
  (3,1,3,'b','bb'),
  (4,1,3,'zz','5'),
  (5,null,null,null,'1.0'),
  (6,null,null,null,'a');
insert into t2(id,i1,i2,vc20,d) values
  (1,1,1,'a',1.0),
  (2,1,2,'a',1.1),
  (5,null,null,'12345678901234567890',3),
  (100,1,3,'zz',3),
  (101,1,2,'bb',null),
  (102,5,5,'',null),
  (103,1,3,' a',null),
  (104,1,3,'null',7.4);

-- no duplicates
select id,i1,i2 from t1 intersect select id,i1,i2 from t2 order by id DESC,i1,i2;
select id,i1,i2 from t1 intersect distinct select id,i1,i2 from t2 order by id DESC,i1,i2;
select id,i1,i2 from t1 intersect all select id,i1,i2 from t2 order by 1,2,3;

-- Only specify order by on some columns
select id,i1,i2 from t1 intersect select id,i1,i2 from t2 order by i2, id DESC;
select id,i1,i2 from t1 intersect all select id,i1,i2 from t2 order by 3 DESC, 1;

-- duplicates
select i1,i2 from t1 intersect select i1,i2 from t2 order by 1,2;
select i1,i2 from t1 intersect distinct select i1,i2 from t2 order by 1,2;
select i1,i2 from t1 intersect all select i1,i2 from t2 order by 1,2;

-- right side is empty
select i1,i2 from t1 intersect select i1,i2 from t2 where id = -1;
select i1,i2 from t1 intersect all select i1,i2 from t2 where id = -1;

-- left side is empty
select i1,i2 from t1 where id = -1 intersect all select i1,i2 from t2;

-- check precedence
select i1,i2 from t1 intersect all select i1,i2 from t2 intersect values(5,5),(1,3) order by 1,2;
(select i1,i2 from t1 intersect all select i1,i2 from t2) intersect values(5,5),(1,3) order by 1,2;

values(-1,-1,-1) union select id,i1,i2 from t1 intersect select id,i1,i2 from t2 order by 1,2,3;
select id,i1,i2 from t1 intersect select id,i1,i2 from t2 union values(-1,-1,-1) order by 1,2,3;

-- check conversions
select c10 from t1 intersect select vc20 from t2 order by 1;
select c30 from t1 intersect select vc20 from t2;
select c30 from t1 intersect all select vc20 from t2;

-- check insert intersect into table and intersect without order by
create table r( i1 integer, i2 integer);
insert into r select i1,i2 from t1 intersect select i1,i2 from t2;
select i1,i2 from r order by 1,2;
delete from r;

insert into r select i1,i2 from t1 intersect all select i1,i2 from t2;
select i1,i2 from r order by 1,2;
delete from r;

-- test LOB
create table t3( i1 integer, cl clob(64), bl blob(1M));
insert into t3 values
  (1, cast( 'aa' as clob(64)), cast(X'01' as blob(1M)));
create table t4( i1 integer, cl clob(64), bl blob(1M));
insert into t4 values
  (1, cast( 'aa' as clob(64)), cast(X'01' as blob(1M)));

select cl from t3 intersect select cl from t4 order by 1;

select bl from t3 intersect select bl from t4 order by 1;

-- invalid conversion
select tm from t1 intersect select dt from t2;
select c30 from t1 intersect select d from t2;

-- different number of columns
select i1 from t1 intersect select i1,i2 from t2;

-- ? in select list of intersect
select ? from t1 intersect select i1 from t2;
select i1 from t1 intersect select ? from t2;

-- except tests
select id,i1,i2 from t1 except select id,i1,i2 from t2 order by id,i1,i2;
select id,i1,i2 from t1 except distinct select id,i1,i2 from t2 order by id,i1,i2;
select id,i1,i2 from t1 except all select id,i1,i2 from t2 order by 1 DESC,2,3;
select id,i1,i2 from t2 except select id,i1,i2 from t1 order by 1,2,3;
select id,i1,i2 from t2 except all select id,i1,i2 from t1 order by 1,2,3;

select i1,i2 from t1 except select i1,i2 from t2 order by 1,2;
select i1,i2 from t1 except distinct select i1,i2 from t2 order by 1,2;
select i1,i2 from t1 except all select i1,i2 from t2 order by 1,2;
select i1,i2 from t2 except select i1,i2 from t1 order by 1,2;
select i1,i2 from t2 except all select i1,i2 from t1 order by 1,2;

-- right side is empty
select i1,i2 from t1 except select i1,i2 from t2 where id = -1 order by 1,2;
select i1,i2 from t1 except all select i1,i2 from t2 where id = -1  order by 1,2;

-- left side is empty
select i1,i2 from t1 where id = -1 except select i1,i2 from t2 order by 1,2;
select i1,i2 from t1 where id = -1 except all select i1,i2 from t2 order by 1,2;

-- Check precedence. Union and except have the same precedence. Intersect has higher precedence.
select i1,i2 from t1 except select i1,i2 from t2 intersect values(-1,-1) order by 1,2;
select i1,i2 from t1 except (select i1,i2 from t2 intersect values(-1,-1)) order by 1,2;
select i1,i2 from t2 except select i1,i2 from t1 union values(5,5) order by 1,2;
(select i1,i2 from t2 except select i1,i2 from t1) union values(5,5) order by 1,2;
select i1,i2 from t2 except all select i1,i2 from t1 except select i1,i2 from t1 where id = 3 order by 1,2;
(select i1,i2 from t2 except all select i1,i2 from t1) except select i1,i2 from t1 where id = 3 order by 1,2;

-- check conversions
select c10 from t1 except select vc20 from t2 order by 1;
select c30 from t1 except select vc20 from t2 order by 1;
select c30 from t1 except all select vc20 from t2;

-- check insert except into table and except without order by
insert into r select i1,i2 from t2 except select i1,i2 from t1;
select i1,i2 from r order by 1,2;
delete from r;

insert into r select i1,i2 from t2 except all select i1,i2 from t1;
select i1,i2 from r order by 1,2;
delete from r;

-- test LOB
select cl from t3 except select cl from t4 order by 1;
select bl from t3 except select bl from t4 order by 1;

-- invalid conversion
select tm from t1 except select dt from t2;
select c30 from t1 except select d from t2;

-- different number of columns
select i1 from t1 except select i1,i2 from t2;

-- ? in select list of except
select ? from t1 except select i1 from t2;

-- Invalid order by
select id,i1,i2 from t1 intersect select id,i1,i2 from t2 order by t1.i1;
select id,i1,i2 from t1 except select id,i1,i2 from t2 order by t1.i1;

-- views using intersect and except
create view view_intr_uniq as select id,i1,i2 from t1 intersect select id,i1,i2 from t2;
select * from view_intr_uniq order by 1 DESC,2,3;

create view view_intr_all as select id,i1,i2 from t1 intersect all select id,i1,i2 from t2;
select * from  view_intr_all order by 1,2,3;

create view view_ex_uniq as select id,i1,i2 from t1 except select id,i1,i2 from t2;
select * from view_ex_uniq order by 1,2,3;

create view view_ex_all as select id,i1,i2 from t1 except all select id,i1,i2 from t2;
select * from view_ex_all order by 1 DESC,2,3;

-- intersect joins
select t1.id,t1.i1,t2.i1 from t1 join t2 on t1.id = t2.id
intersect select t1.id,t1.i2,t2.i2 from t1 join t2 on t1.id = t2.id;
