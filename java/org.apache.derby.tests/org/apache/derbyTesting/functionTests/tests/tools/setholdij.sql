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
-- create a table
create table sethold(c1 int, c2 int);

-- insert data into tables
insert into sethold values(1,1);
insert into sethold values(2,2);
-- set autocommit off
autocommit off;

-- first test - make sure that cursors created with default holdability
-- have open resultsets after commit
get cursor jdk1 as 'SELECT * FROM sethold';
get scroll insensitive cursor jdk2 as 'SELECT * FROM sethold';

-- do fetches from these cursors
next jdk1;
next jdk2;

--commit and see if the cursors are still open
commit;

next jdk1;
next jdk2;

-- second test - make sure that cursors created with holdability false
-- do not have open resultsets after commit

-- set NoHold, then declare 2 different kind of cursors and fetch from them
NoHoldForConnection;
get cursor jdk3 as 'SELECT * FROM sethold';
get scroll insensitive cursor jdk4 as 'SELECT * FROM sethold';

-- do fetches from these cursors
next jdk3;
next jdk4;

--commit and see if the cursors are still open
commit;

next jdk3;
next jdk4;

-- third test - make sure that cursors created with holdability true
-- have open resultsets after commit
-- set Hold, then declare 2 different kind of cursors and fetch from them
HoldForConnection;
get cursor jdk5 as 'SELECT * FROM sethold';
get scroll insensitive cursor jdk6 as 'SELECT * FROM sethold';

-- do fetches from these cursors
next jdk5;
next jdk6;

--commit
commit;

next jdk5;
next jdk6;

-- fourth test - make sure that we get the same behavior as before after
-- setting the holdability to No Hold again.

-- set NoHold, then declare 2 different kind of cursors and fetch from them
NoHoldForConnection;
get cursor jdk7 as 'SELECT * FROM sethold';
get scroll insensitive cursor jdk8 as 'SELECT * FROM sethold';

-- do fetches from these cursors
next jdk7;
next jdk8;

--commit and see if the cursors are still open
commit;

next jdk7;
next jdk8;


-- clean up.
close jdk1;
close jdk2;
close jdk3;
close jdk4;
close jdk5;
close jdk6;
close jdk7;
close jdk8;
drop table sethold;
commit;
