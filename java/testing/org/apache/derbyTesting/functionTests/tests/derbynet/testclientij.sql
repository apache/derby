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
driver 'org.apache.derby.jdbc.ClientDriver';
--Bug 4632  Make the db italian to make sure string selects  are working
connect 'jdbc:derby://localhost:1527/testij;create=true;territory=it' USER 'dbadmin' PASSWORD 'dbadmin';

connect 'jdbc:derby://localhost:1527/testij' USER 'dbadmin' PASSWORD 'dbadbmin';
-- this is a comment, a comment in front of a select should not cause an error
select * from sys.systables where 1=0;
-- this is a comment, a comment in front of a values clauses should not cause an error
values(1);

-- Try some URL attributes
disconnect all;
connect 'jdbc:derby://localhost:1527/testij2;create=true' USER 'dbadmin' PASSWORD 'dbadbmin';
select * from APP.notthere;


-- examples from the docs

connect 'jdbc:derby://localhost:1527/testij2;create=true;user=judy;password=judy';

connect 'jdbc:derby://localhost:1527/./testij2;user=judy;password=judy';

connect 'jdbc:derby://localhost:1527/toursDB';

connect 'jdbc:derby://localhost:1527/toursDB' USER 'dbadmin' PASSWORD 'dbadbmin';

connect 'jdbc:derby://localhost:1527/wombat' USER 'APP' PASSWORD 'APP';

connect  'jdbc:derby://localhost:1527/testij2;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/testij2;upgrade=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/testij2;shutdown=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/./testij2;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/./testij2;create=true;user=usr;password=pwd';

connect  'jdbc:derby://localhost:1527/testij2;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/testij2;upgrade=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/testij2;shutdown=true;user=usr;password=pwd';

-- retrieveMessageText Testing
connect 'jdbc:derby://localhost:1527/testij2;create=true;user=usr;password=pwd;retrieveMessageText=false';

-- Should not get message text
select * from APP.notthere;

connect 'jdbc:derby://localhost:1527/testij2;create=true;user=usr;password=pwd;retrieveMessageText=true';

-- Should see message text
select * from APP.notthere;

-- just user security mechanism
connect 'jdbc:derby://localhost:1527/testij2;create=true;user=usr;retrieveMessageText=true';

connect 'jdbc:derby://localhost:1527/wombat' USER 'APP';

disconnect all;
