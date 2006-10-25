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
-- create table with row greater than 32K
-- try with just one row of data - JCC may handle those differently?
create table big(c1 varchar(10000), c2 varchar(10000), c3 varchar(10000), c4 varchar(10000));
create procedure INSERTDATA1(IN a int) language java parameter style java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.bigTestData';
prepare p1 as 'call INSERTDATA1(?)';
execute p1 using 'values 1';
select * from big;

-- multiple rows of data
execute p1 using 'values 2';
select * from big;

-- the overhead for DSS on QRYDTA is 15 bytes
-- let's try a row which is exactly 32767 (default JCC queryblock size)
drop table big;
create table big(c1 varchar(30000), c2 varchar(2752));
execute p1 using 'values 5';
select * from big;

-- Various tests for JIRA-614: handling of rows which span QRYDTA blocks.
-- what happens when the SplitQRYDTA has to span 3+ blocks
drop table big;
create table big(c1 varchar(32672), c2 varchar(32672), c3 varchar(32672), c4 varchar(32672));
execute p1 using 'values 9';
select * from big;
execute p1 using 'values 9';
execute p1 using 'values 9';
select * from big;
get scroll insensitive cursor c1 as 'select * from big';
first c1;
next c1;
previous c1;
last c1;
close c1;
drop table big;
-- Mix clob and varchar in the table.
create table big(c1 clob(32672), c2 varchar(32672), c3 varchar(32672), c4 clob(32672));
execute p1 using 'values 9';
select * from big;
execute p1 using 'values 9';
execute p1 using 'values 9';
select * from big;
-- This seems to reveal some sort of different problem than JIRA 614. I get a
-- DRDAProtocolException, but it isn't one which involves splitQRYDTA.
-- get scroll insensitive cursor c1 as 'select * from big';
-- first c1;
-- next c1;
-- previous c1;
-- last c1;
-- close c1;
-- End of the JIRA-614 tests.

-- what happens when the row + the ending SQLCARD is too big
drop table big;
create table big(c1 varchar(30000), c2 varchar(2750));
execute p1 using 'values 6';
select * from big;
-- let's try scrolling
drop table big;
create table big(c1 varchar(10000), c2 varchar(10000), c3 varchar(10000), c4 varchar(10000));
execute p1 using 'values 1';
execute p1 using 'values 2';
execute p1 using 'values 3';
execute p1 using 'values 4';
get scroll insensitive cursor c1 as 'select * from big';
first c1;
next c1;
previous c1;
last c1;
close c1;
-- try going directly to the last row
get scroll insensitive cursor c1 as 'select * from big';
last c1;
close c1;
drop table big;
-- try a column which is > 32767
create table big (c1 clob(40000));
execute p1 using 'values 7';
select * from big;
drop table big;
-- try several columns > 32767
create table big (c1 clob(40000), c2 clob(40000), c3 clob(40000));
execute p1 using 'values 8';
select * from big;
drop table big;

-- The tests below won't run with db2 compat mode.
-- try java objects of different sizes

-- create table big(s java.lang.String);

-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',1000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',2000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',3000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',32000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',33000));
-- select * from big;

-- drop table big;

-- big long varchar
-- create table big(lvc long varchar );
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',1000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',2000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',3000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',32000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',33000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',64000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',64499));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',65500));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',64501));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',66000));
-- select * from big;
-- drop table big;




-- create table big(vc varchar(32767));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',1000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',2000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',3000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',32000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',33000));
-- select * from big;
-- drop table big;

-- create table big(lvc long bit varying );
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',1000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',2000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',3000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',32000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',33000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',64000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',64499));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',65500));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',64501));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',66000));
-- select * from big;
-- drop table big;


-- create table big(vb bit varying(131072));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',1000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',2000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',3000));
-- These cannot be run until 4662 is fixed because we send 
-- a big arg to the localization method..
--  insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',32000));
-- insert into big values(org.apache.derbyTesting.functionTests.util.Formatters::repeatChar('a',33000));
-- select * from big;
-- drop table big;








