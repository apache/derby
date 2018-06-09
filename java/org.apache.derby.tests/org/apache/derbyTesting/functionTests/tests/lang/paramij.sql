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
-- test that we do not allow incorrect where <boolean> syntax

create table t1 (c11 int);
insert into t1 values (1);

autocommit off;

-- positive tests
-- In following test cases, where clause results in a boolean value
prepare p1 as 'select * from t1 where ?=1'; 
execute p1 using 'values(1)';

prepare p1 as 'select * from t1 where ? like ''2'' ';
execute p1 using 'values(''a'')';

prepare p1 as 'select * from t1 where not ? > 1';
execute p1 using 'values(1)';

prepare p1 as 'select * from t1 where lower(?) = ''a''';
execute p1 using 'values(''a'')';

prepare p1 as 'select * from t1 where {fn length(?)} > 1';
execute p1 using 'values(''a'')';

prepare p1 as 'select * from t1 where {fn locate(?,''a'',1)} = 1';
execute p1 using 'values(''a'')';

prepare p1 as 'select * from t1 where ? between 1 and 3';
execute p1 using 'values(2)';

prepare p1 as 'select * from t1 where ? in (1, ?)';
execute p1 using 'values(2,1)';

prepare p1 as 'select * from t1 where ? is null';
execute p1 using 'values(1)';

prepare p1 as 'select * from t1 where ? is not null';
execute p1 using 'values(1)';

prepare p1 as 'select * from t1 where ? <> ALL (values(1))';
execute p1 using 'values(3)';

prepare p1 as 'select * from t1 where exists (select c11 from t1 where 1=?)';
execute p1 using 'values(3)';


prepare p1 as 'select * from t1 where cast(? as int) = 1';
execute p1 using 'values(1)';

-- negative tests
-- In following test cases, there is no way to ensure where with ? will result in a boolean value

prepare p1 as 'select * from t1 where c11';

prepare p1 as 'select * from t1 where c11+1';

prepare p1 as 'select * from t1 where 1';

prepare p1 as 'select * from t1 where ?';

prepare p1 as 'select * from t1 where ? for update';

prepare p1 as 'select * from t1 where (?)';

prepare p1 as 'select * from t1 where ? and 1=1';

prepare p1 as 'select * from t1 where ? and 1=? or 2=2'; 

prepare p1 as 'select * from t1 where not ?';

prepare p1 as 'select * from t1 where lower(?)';

prepare p1 as 'select * from t1 where lower(?) and 1=1';

prepare p1 as 'select * from t1 where {fn length(?)}';

prepare p1 as 'select * from t1 where {fn locate(?,''a'',1)}';

prepare p1 as 'select * from t1 where cast(? as int)';

prepare p1 as 'select * from t1 where (?||''1'')';
