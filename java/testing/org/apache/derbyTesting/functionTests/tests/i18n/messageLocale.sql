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
-- This test uses dummy messages from org/apache/derby/loc/message_qq_PP_testOnly.properties
--
-- Message locale handling
--
--;

-- load a fake driver that is really a piece of
-- code that sets the default locale;
driver 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale';

--
-- create a databse in this default locale, should
-- get english messages, as there are no messages
-- for rr_TT;
connect 'jdbc:derby:wombat;create=true';

-- make sure the database is clean
drop procedure checkDefaultLoc;
create procedure checkDefaultLoc() parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDefaultLocale';
drop procedure checkDatabaseLoc;
create procedure checkDatabaseLoc(in locale char(10)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
drop procedure setDefaultDELoc;
create procedure setDefaultLoc(in locale char(10), in code char(10)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.setDefaultLocale';
drop table t1;
drop table t2;

call checkDefaultLoc();
call checkDatabaseLoc('rr_TT');

-- expect an error
create table t1 oops (i int primary key);
-- setup for
create table t2 (i int);
create index i2_a on t2(i);
-- expect a warning
create index i2_b on t2(i);
-- another error
drop table t3;

-- set the default locale to German;
--call java.util.Locale::setDefault(new java.util.Locale('de', 'DE'));
call setDefaultLoc('de','DE');

disconnect;

-- create a database with a locale that has a small
-- number of messages. Missing ones will default to
-- the locale of the default locale i.e. German;
connect 'jdbc:derby:testdb;create=true;territory=qq_PP_testOnly';

-- error (in qq_PP messages);
-- create table t1 (i longe);
create table t1 oops (i int primary key);

-- warning (in qq_PP messages);
-- create table t2 (i java.lang.Object);
create table t2 (i int);
create index i2_a on t2(i);
create index i2_b on t2(i);

-- from default locale (German);
drop table t3;

-- should be in German;
disconnect;
connect 'jdbc:derby:;shutdown=true';

-- Now, all Enlish messages;
connect 'jdbc:derby:enTest;create=true;territory=en_US';

-- create table t1 (i longe);
create table t1 oops (i int primary key);
-- create table t2 (i java.lang.Object);
create table t2 (i int);
create index i2_a on t2(i);
create index i2_b on t2(i);
drop table t3;
disconnect;

