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
-- minimal testing to verify no scans left open
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker'
LANGUAGE JAVA PARAMETER STYLE JAVA;
autocommit off;


autocommit off;
create table t1(c1 int, c2 int);

-- do consistency check on scans, etc.
values ConsistencyChecker();
insert into t1 values (1, 1);

-- do consistency check on scans, etc.
values ConsistencyChecker();
create index i1 on t1(c1);

-- do consistency check on scans, etc.
values ConsistencyChecker();
create index i2 on t1(c2);
insert into t1 values (2, 2);

-- do consistency check on scans, etc.
values ConsistencyChecker();
-- scan heap
select * from t1;
-- scan covering index
select c1 from t1;
-- index to base row
select * from t1;
select * from t1 where c1 = 1;

-- do consistency check on scans, etc.
values ConsistencyChecker();

commit;

-- test cursor which doesn't get drained
get cursor c1 as 'select c1 + c2 from t1 order by 1';
next c1;
close c1;

-- do consistency check on scans, etc.
values ConsistencyChecker();

commit;
