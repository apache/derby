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
driver 'org.apache.derby.jdbc.EmbeddedDriver';
connect 'jdbc:derby:wombat;create=true';

-- can we run a simple query?
values 1;

-- can we disconnect?
disconnect;

-- can we reconnect?
connect 'jdbc:derby:wombat;create=true';

-- can we run a simple query?
values 1;
disconnect;

-- do we get a non-internal error when we try to create
-- over an existing directory? (T#674)
connect 'jdbc:derby:wombat/seg0;create=true';

-- check to ensure an empty database name is taken
-- as the name, over any connection attribute.
-- this should fail.
connect 'jdbc:derby: ;databaseName=wombat';

-- and this should succeed (no database name in URL)
connect 'jdbc:derby:;databaseName=wombat';
disconnect;

-- Doing some simple grant/revoke negative tests in legacy database.
-- All should fail with errors.

connect 'jdbc:derby:wombat';
create table mytab(i int);

grant select on mytab to satheesh;
revoke select on mytab to satheesh;
disconnect;
