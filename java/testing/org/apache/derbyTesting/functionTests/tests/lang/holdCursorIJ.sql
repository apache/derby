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
create table t1(c11 int, c12 int);

-- insert data into tables
insert into t1 values(1,1);
insert into t1 values(2,2);

-- set autocommit off
autocommit off;

-- first test - make sure that only cursors created with holdability true
-- have open resultsets after commit

-- declare 3 different kind of cursors one for each jdbc release so far
get with nohold cursor jdk1 as 'SELECT * FROM t1';
get scroll insensitive with nohold cursor jdk2 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';

-- do fetches from these cursors
next jdk1;
next jdk2;
next jdk4;

--commit
commit;

-- now try the fetch on cursors again after commit
-- cursors jdk1 and jdk2 will give errors
next jdk1;
next jdk2;
next jdk4;
-- end of resultset for jdk4, but try next again
next jdk4;
close jdk4;
next jdk4;

-- clean up.
close jdk1;
close jdk2;

-- second test - make sure that all the cursors (including holdability true)
-- have their resultsets closed after rollback.

-- declare the cursors again, this time, try with rollback
get with nohold cursor jdk1 as 'SELECT * FROM t1';
get scroll insensitive with nohold cursor jdk2 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';

-- do fetches from these cursors
next jdk1;
next jdk2;
next jdk4;

--rollback
rollback;

-- now try the fetch on cursors again after rollback
-- all the cursors will give errors
next jdk1;
next jdk2;
next jdk4;

-- clean up.
close jdk1;
close jdk2;
close jdk4;

drop table t1;

-- recreate and populate the table for next test
create table t1(c11 int, c12 int);
insert into t1 values(1,1);
insert into t1 values(2,2);

-- fourth test - try to change the isolation level while there are
-- held cursors
get with nohold cursor jdk1 as 'SELECT * FROM t1';
get with hold cursor jdk4 as 'SELECT * FROM t1';
next jdk1;
next jdk4;

-- changing isolation while cursor is open would fail; 
-- but for client/server, with small data set, the server would already be
-- closed. See discussion re DERBY-3801.
-- close jdk4 and then should be able to change isolation
close jdk4;
set isolation to serializable;

-- clean up.
close jdk1;

-- fifth test - try isolation level change alongwith changing the isolation
-- level of just one statement
get with hold cursor jdk4 as 'SELECT * FROM t1';
get with nohold cursor jdk1 as 'SELECT * FROM t1 WITH CS';
next jdk4;
next jdk1;
close jdk4;
-- should be able to change the isolation now
set isolation READ UNCOMMITTED;
set isolation RS;

-- clean up.
close jdk1;

-- sixth test - try positioned update with hold cursor
get with hold cursor jdk4 as 'SELECT * FROM t1 FOR UPDATE';
-- following should give error because cursor is not positioned on any row
update t1 set c12=12 where current of jdk4;
select * from t1;
next jdk4;
update t1 set c12=12 where current of jdk4;
select * from t1;
commit;
-- after commit, the next transaction should do a fetch again before doing
-- any positioned update
update t1 set c12=123 where current of jdk4;
select * from t1;
next jdk4;
update t1 set c12=23 where current of jdk4;
select * from t1;
close jdk4;
update t1 set c12=234 where current of jdk4;
select * from t1;

-- seventh test - try positioned delete with hold cursor
get with hold cursor jdk4 as 'SELECT * FROM t1 FOR UPDATE';
-- following should give error because cursor is not positioned on any row
delete from t1 where current of jdk4;
select * from t1;
next jdk4;
delete from  t1 where current of jdk4;
select * from t1;
commit;
-- after commit, the next transaction should do a fetch again before doing
-- any positioned delete
delete from t1 where current of jdk4;
select * from t1;
next jdk4;
delete from t1 where current of jdk4;
select * from t1;
close jdk4;
delete from t1 where current of jdk4;
select * from t1;

-- populate the table for next test
insert into t1 values(1,1);
insert into t1 values(2,2);

-- eighth test - scrollable cursors
get scroll insensitive with hold cursor jdk4 as 'SELECT * FROM t1';
commit;
previous jdk4;
after last jdk4;
before first jdk4;
first jdk4;
last jdk4;
next jdk4;
previous jdk4;
next jdk4;
close jdk4;
first jdk4;

-- ninth test - close the updateable holdable cursor after commit
-- we get npe
get with hold cursor jdk4 as 'SELECT * FROM T1 FOR UPDATE';
next jdk4;
commit;
close jdk4;

-- tenth test - bug 4515 - have a more useful message
-- update where current of fails in autocommit=true, held open cursor

autocommit on;
get with hold cursor scrollCursor as 'select * from t1 for update of c12';
next scrollCursor;
-- commented out for DERBY-4778
-- update t1 set c12=c12+1 where current of scrollCursor;

-- clean up.
close scrollCursor;


drop table t1;
