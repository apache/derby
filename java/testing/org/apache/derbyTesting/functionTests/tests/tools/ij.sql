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

-- this test shows the ij commands in use,
-- and what happens when invalid stuff is entered.

-- no driver loaded yet, detected off of the url
-- this one is a bad url:
connect 'cloudscape:wombat';
-- this one will work.
connect 'jdbc:derby:wombat';

-- no connection yet, this will fail
create table t (i int);

-- no table yet, this will fail
select i from t;

-- invalid syntax ... incomplete statements
driver;
connect;
prepare;
execute;
run;
remove;

-- should fail because procedure is an illegal statement name
prepare procedure as 'select * from bar';

-- should fail because text is passed on to derby, which
-- barfs on the unknown statement name. execute procedure is
-- a foundation 2000 concept
execute procedure sqlj.install_jar( 'file:c:/p4c/systest/out/DigIt.jar', 'SourceWUs', 1 );

-- moved from errorcode.sql

-- specify an invalid driver
driver 'java.lang.Integer';

-- now a valid driver
driver 'org.apache.derby.jdbc.EmbeddedDriver';

-- specify an invalid database
connect 'asdfasdf';

-- now a valid database, but no create
connect 'jdbc:derby:wombat';
-- and, the help output:
help;

