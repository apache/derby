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
disconnect;

connect 'wombat;user=U1' AS C1;
connect 'wombat;user=U2' AS C2;

set connection C1;

create table t1(c1 int, c2 int);
insert into t1 values (1, 2), (3, 4), (5, 6), (7, 8), (9, 10);
get scroll insensitive cursor c1 as 'select * from t1';

set connection C2;
-- see what happens to other user when we close our cursor
-- before they are done.
get scroll insensitive cursor c1 as 'select * from U1.t1';

set connection C1;
next c1;

set connection C2;
next c1;

set connection C1;
last c1;

set connection C2;
last c1;

set connection C1;
previous c1;

set connection C2;
close c1;

set connection C1;
first c1;
close c1;

drop table t1;
disconnect;
set connection C2;
disconnect;
