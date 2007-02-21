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
-- Specifically test SECURE users and various authentication
-- service/scheme configuration for different databases.
--

-- Configure the 6 different databases with for each
-- of them, a different authentication scheme.
--
-- 'wombat'				- default DERBY scheme &
--						  users known at system level.
--						  Some authorization restriction.
-- 'guestSchemeDB'		- No authentication
-- 'derbySchemeDB'	- BUILTIN authentication
--						  & some db authorization restriction.
-- 'simpleSchemeDB'		- BUILTIN authentication and
--						  some db authorization restriction.
--                        (was the old Cloudscape 1.5 simple scheme)
--
-- let's create all the dbs and configure them.
-- we will authenticate using a default system user that we
-- have configured.
-- A typical bad guy who cannot access any database but guest
-- is Jamie.
--

--
-- 'guestSchemeDB' database authentication/authorization config
--
connect 'guestSchemeDB;create=true;user=system;password=manager';
-- override requireAuthentication to be turned OFF at the database level
autocommit off;
prepare p1 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p1 using 'values(''derby.connection.requireAuthentication'', ''false'')';
commit;
autocommit on;

--
-- 'derbySchemeDB' database authentication/authorization config
--
connect 'derbySchemeDB;create=true;user=system;password=manager';
autocommit off;
prepare p2 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p2 using 'values(''derby.authentication.provider'', ''BUILTIN'')';
execute p2 using 'values(''derby.connection.requireAuthentication'', ''true'')';
-- let's define users in this database (other than the ones
-- known at the system level. This is for the test
-- These 3 users will only be known in this database
execute p2 using 'values(''derby.user.system'', ''manager'')';
execute p2 using 'values(''derby.user.martin'', ''obfuscateIt'')';
execute p2 using 'values(''derby.user.dan'', ''makeItFaster'')';
execute p2 using 'values(''derby.user.mamta'', ''ieScape'')';
execute p2 using 'values(''derby.database.propertiesOnly'', ''true'')';
commit;
autocommit on;

--
-- 'simpleSchemeDB' database authentication/authorization config
--
connect 'simpleSchemeDB;create=true;user=system;password=manager';
autocommit off;
prepare p5 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p5 using 'values(''derby.authentication.provider'', ''BUILTIN'')';

--
--  only allow these 3 users
execute p5 using 'values(''derby.database.fullAccessUsers'', ''system,jeff,howardR'')';
execute p5 using 'values(''derby.database.readOnlyAccessUsers'', ''francois'')';
-- no access to Jamie only as he's a well known hooligan
execute p5 using 'values(''derby.database.defaultConnectionMode'', ''noAccess'')';
commit;
autocommit on;

--
-- Shutdown the system for database properties to take effect
--
disconnect all;
connect 'wombat;user=system;password=manager;shutdown=true';
connect 'guestSchemeDB;user=system;password=manager;shutdown=true';
connect 'derbySchemeDB;user=system;password=manager;shutdown=true';
connect 'simpleSchemeDB;user=system;password=manager;shutdown=true';
disconnect all;

-- shuting down the system causes IJ to loose the protocol, therefore
-- we'd be doomed :(
#connect ';shutdown=true;user=system;password=manager';

-- 1) Valid authentication & authorization requests/ops
-- 
connect 'wombat;create=true;user=kreg;password=IwasBornReady';
connect 'wombat;user=jeff;password=homeRun';
connect 'wombat;user=howardR;password=takeItEasy';
connect 'wombat;user=francois;password=paceesalute';
-- Jamie is allowed here, since he is user at system level
connect 'wombat;user=Jamie;password=theHooligan';
show connections;

connect 'guestSchemeDB;user=kreg;password=IwasBornReady';
connect 'guestSchemeDB;user=jeff;password=homeRun';
connect 'guestSchemeDB;user=howardR;password=takeItEasy';
connect 'guestSchemeDB;user=francois;password=paceesalute';
-- allowed: no authentication
connect 'guestSchemeDB;user=bad;password=guy';
show connections;

connect 'derbySchemeDB;user=mamta;password=ieScape';
connect 'derbySchemeDB;user=dan;password=makeItFaster';
connect 'derbySchemeDB;user=martin;password=obfuscateIt';
-- Invalid ones:
connect 'derbySchemeDB;user=Jamie;password=theHooligan';
connect 'derbySchemeDB;user=francois;password=paceesalute';
show connections;

connect 'simpleSchemeDB;user=jeff;password=homeRun';
connect 'simpleSchemeDB;user=howardR;password=takeItEasy';
connect 'simpleSchemeDB;user=francois;password=paceesalute';
-- Read-only user
create table t1 (c1 int);
-- Invalid ones:
connect 'simpleSchemeDB;user=Jamie;password=theHooligan';
connect 'simpleSchemeDB;user=dan;password=makeItFaster';
connect 'simpleSchemeDB;user=francois;password=corsica';
show connections;
disconnect all;

show connections;

-- Database shutdown - check user - should fail
connect 'derbySchemeDB;shutdown=true';

show connections;

-- Database shutdown - check user - should succeed
connect 'guestSchemeDB;user=kreg;password=IwasBornReady;shutdown=true';

-- Database shutdown - authenticated, so must use owner
connect 'wombat;user=system;password=manager;shutdown=true';
connect 'derbySchemeDB;user=system;password=manager;shutdown=true';
connect 'simpleSchemeDB;user=system;password=manager;shutdown=true';

show connections;

-- Derby system shutdown - check user - should fail
connect ';user=jamie;password=LetMeIn;shutdown=true';

disconnect all;

-- Derby system shutdown - check user - should succeed
connect ';user=system;password=manager;shutdown=true';
