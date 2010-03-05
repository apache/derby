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
-- This test will cover the qualified identifiers introduced by DERBY-4550
connect 'jdbc:derby:wombat;user=fred' as DERBY4550_1;
create table t1(a int, b int);
insert into t1(a,b) values (1,100), (2,200), (3,300);
prepare fred_select as 'select a from t1';

-- setup destination db
connect 'jdbc:derby:wombat;user=alice' as DERBY4550_2;
create table t2(a int);

-- execute prepared statements
autocommit off;
execute fred_select@DERBY4550_1;
execute 'insert into t2(a) values(?)' using fred_select@DERBY4550_1;
commit;
remove fred_select@DERBY4550_1;

-- check result
select a from t2;

-- prepare in a different connection/switch/execute
prepare fred_select2@DERBY4550_1 as 'select b from t1';
set connection DERBY4550_1;
execute fred_select2;
remove fred_select2;

set connection DERBY4550_2;

-- cursors
get scroll insensitive cursor fred_cursor@DERBY4550_1 as 'select b from t1';
next fred_cursor@DERBY4550_1;
-- getcurrentrownumber fred_cursor@DERBY4550_1;
last fred_cursor@DERBY4550_1;
previous fred_cursor@DERBY4550_1;
first fred_cursor@DERBY4550_1;
after last fred_cursor@DERBY4550_1;
before first fred_cursor@DERBY4550_1;
relative 2 fred_cursor@DERBY4550_1;
absolute 1 fred_cursor@DERBY4550_1;
close fred_cursor@DERBY4550_1;

-- non-existant connection
prepare fred_select@XXXX as 'values(1)';

-- async statements
async a@DERBY4550_1 'select a from t1';
wait for a@DERBY4550_1;

-- non existant statement
wait for xxxx@DERBY4550_1;

disconnect DERBY4550_2;
disconnect DERBY4550_1;
