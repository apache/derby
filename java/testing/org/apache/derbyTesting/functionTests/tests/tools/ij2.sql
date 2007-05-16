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

-- this test shows some ij abilities against an active database

create table t (i int);
insert into t values (3), (4);

prepare s as 'select * from t';
execute s;

remove s;
-- now it won't find s
execute s;

prepare s as 'select * from t where i=?';
-- fails, needs parameter
execute s;

-- works, finds value
execute s using 'values 3';

prepare t as 'values 3';
-- same as last execute
execute s using t;

-- same as last execute
execute 'select * from t where i=?' using 'values 3';

-- same as last execute
execute 'select * from t where i=?' using t;

-- param that is not needed gets out of range message
execute 'select * from t where i=?' using 'values (3,4)';

-- ignores rows that are not needed
execute 'select * from t where i=?' using 'values 3,4';

-- with autocommit off, extra rows are processed and no warning results
autocommit off;
execute 'select * from t where i=?' using 'values 3,4';

execute 'select * from t where i=?' using 'values 3';

autocommit on;

-- will say params not set when no rows in using values
execute 'select * from t where i=?' using 'select * from t where i=9';

-- will say params not set when using values is not a query
execute 'select * from t where i=?' using 'create table s (i int)';

-- note that the using part was, however, executed...
drop table s;

-- DERBY-2558: Verify that we get a reasonable message when the 'dimension'
-- of the 'using-set' does not match the 'dimension' of the prepared statement:
create table t2558 (i int);
insert into t2558 values (3), (4);
-- First two statements below should fail. Third one should work.
execute 'select * from t2558 where i = ?' using 'values (3,4)';
execute 'select * from t2558 where i in (?,?,?)' using 'values (3,4)';
execute 'select * from t2558 where i = ? or i = ?' using 'values (3,4)';

-- bug 5926 - make sure the using clause result set got closed
drop table t;
create table t(c1 int);
insert into t values(1);
execute 'select * from t where c1=?' using 'select * from t where c1=1';
drop table t;
create table t(c1 int);
insert into t values(1);
insert into t values(2);
execute 'select * from t where c1=?' using 'select * from t where c1>=1';
drop table t;

-- Bug 4694 Test automatic rollback with close of connection
-- in ij
connect 'wombat';
autocommit off;
create table a (a int);
select count(*) from a;

disconnect;
set connection connection0;
select count(*) from a;


create table t ( c char(50));
insert into t values('hello');
select cast(c as varchar(20)) from t;
drop table t;

-- show multiconnect ability; db name is wombat, reuse it...
-- assumes ij.protocol is appropriately set...

connect 'wombat' as wombat;

show connections;

set connection connection0;

show connections;

set connection wombat;

disconnect;

show connections;

set connection connection0;

show connections;


