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
-- negative tests for ungrouped aggregates

-- create a table
create table t1 (c1 int);
create table t2 (c1 int);
insert into t2 values 1,2,3;

-- mix aggregate and non-aggregate expressions in the select list
select c1, max(c1) from t1;
select c1 * max(c1) from t1;

-- aggregate in where clause
select c1 from t1 where max(c1) = 1;

-- aggregate in ON clause of inner join
select * from t1 join t1 as t2 on avg(t2.c1) > 10;

-- correlated subquery in select list
select max(c1), (select t2.c1 from t2 where t1.c1 = t2.c1) from t1;

-- noncorrelated subquery that returns more than 1 row
select max(c1), (select t2.c1 from t2) from t1;

-- drop the table
drop table t1
