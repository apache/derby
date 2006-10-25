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
-- Specifically test Derby users using DERBY scheme
-- and by only looking at database properties for authentication
-- The only user at the system level is system/manager
--

-- check allowed users in wombat db.
-- initial connection in sysprop was:
-- connect 'wombat;create=true;user=system;password=manager';
--
-- Default to: derby.authentication.provider=BUILTIN
--
autocommit off;
prepare p1 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p1 using 'values(''derby.connection.requireAuthentication'', ''true'')';
commit;
remove p1;
autocommit on;

disconnect all;
connect 'wombat;shutdown=true;user=system;password=manager';

-- beetle 5468
disconnect all;

connect 'wombat;user=system;password=manager';

--
-- set authentication config for 'wombat' database
--
autocommit off;
prepare p2 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p2 using 'values(''derby.user.kreg'', ''visualWhat?'')';
execute p2 using 'values(''derby.user.jeff'', ''HomeRun61'')';
execute p2 using 'values(''derby.user.ames'', ''AnyVolunteer?'')';
execute p2 using 'values(''derby.user.jamie'', ''MrNamePlates'')';
execute p2 using 'values(''derby.user.howardR'', ''IamBetterAtTennis'')';
execute p2 using 'values(''derby.user.francois'', ''paceesalute'')';
execute p2 using 'values(''derby.database.fullAccessUsers'', ''jeff,howardR,ames,francois,kreg'')';
execute p2 using 'values(''derby.database.readOnlyAccessUsers'', ''jamie'')';
execute p2 using 'values(''derby.database.defaultConnectionMode'', ''noAccess'')';
execute p2 using 'values(''derby.database.propertiesOnly'', ''true'')';
commit;
autocommit on;

-- Check that the passwords are encrypted
-- values getDatabaseProperty('derby.user.francois');
-- values getDatabaseProperty('derby.user.ames');
-- values getDatabaseProperty('derby.user.kreg');
-- values getDatabaseProperty('derby.user.jeff');
-- values getDatabaseProperty('derby.user.howardR');
-- values getDatabaseProperty('derby.user.jamie');

connect 'wombat;user=kreg;password=visualWhat?';
connect 'wombat;user=jeff;password=HomeRun61';
connect 'wombat;user=ames;password=AnyVolunteer?';
connect 'wombat;user=howardR;password=IamBetterAtTennis';
-- should succeed
create table APP.t1(c1 char(30));
insert into APP.t1 values CURRENT_USER;

connect 'wombat;user=jamie;password=MrNamePlates';
-- should fail as readOnly user
insert into APP.t1 values CURRENT_USER;
create table APP.t2(c1 char(30));

show connections;

disconnect all;

-- check allowed users in myDB db.
--
connect 'myDB;create=true;user=system;password=manager';
autocommit off;
prepare p3 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p3 using 'values(''derby.connection.requireAuthentication'', ''true'')';
remove p3;
autocommit on;
disconnect all;
connect 'myDB;shutdown=true;user=system;password=manager';

-- beetle 5468
disconnect all;

connect 'myDB;user=system;password=manager';

--
-- set authentication config for 'myDB' database
--
autocommit off;
prepare p4 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p4 using 'values(''derby.user.kreg'', ''visualWhat?'')';
execute p4 using 'values(''derby.user.dan'', ''MakeItFaster'')';
execute p4 using 'values(''derby.user.ames'', ''AnyVolunteer?'')';
execute p4 using 'values(''derby.user.jerry'', ''SacreBleu'')';
execute p4 using 'values(''derby.user.jamie'', ''MrNamePlates'')';
execute p4 using 'values(''derby.user.francois'', ''paceesalute'')';
execute p4 using 'values(''derby.database.fullAccessUsers'', ''jerry,dan,kreg,ames,francois,jamie'')';
execute p4 using 'values(''derby.database.defaultConnectionMode'', ''noAccess'')';
execute p4 using 'values(''derby.database.propertiesOnly'', ''true'')';
commit;
autocommit on;

-- Check that the passwords are encrypted
-- values getDatabaseProperty('derby.user.francois');
-- values getDatabaseProperty('derby.user.ames');
-- values getDatabaseProperty('derby.user.kreg');
-- values getDatabaseProperty('derby.user.dan');
-- values getDatabaseProperty('derby.user.jerry');
-- values getDatabaseProperty('derby.user.jamie');

--
-- also check USER flavors
--
connect 'myDB;user=jerry;password=SacreBleu';
create table APP.t1(c1 char(30) check (UPPER(c1) <> 'JAMIE'));
insert into APP.t1 values CURRENT_USER;
connect 'myDB;user=kreg;password=visualWhat?';
insert into APP.t1 values USER;
connect 'myDB;user=ames;password=AnyVolunteer?';
insert into APP.t1 values SESSION_USER;
connect 'myDB;user=dan;password=MakeItFaster';
select * from APP.t1;
update APP.t1 set c1 = USER;
select * from APP.t1;
connect 'myDB;user=francois;password=paceesalute';
update APP.t1 set c1 = USER;
connect 'myDB;user=jamie;password=MrNamePlates';
select * from APP.t1;
update APP.t1 set c1 = USER;

show connections;

disconnect all;

--
-- some negative cases
--

-- Invalid login's
connect 'wombat';
connect 'wombat;user=badUser1;password=YeeHa!';
connect 'wombat;user=badUser2;password=YeeHa!';
connect 'myDB;user=dan;password=MakeItSlower';
connect 'myDB;user=jamie;password=LetMeIn';
connect 'wombat;user=francois;password=Corsica';

-- Invalid database users
connect 'myDB;user=howardR;password=IamBetterAtTennis';
connect 'wombat;user=jerry;password=SacreBleu';
connect 'wombat;user=jamie;password=MrNamePlates';

show connections;

connect 'wombat;user=francois;password=paceesalute';
connect 'myDB;user=jerry;password=SacreBleu';

-- Database shutdown - check user - should fail
connect 'myDB;shutdown=true';
connect 'myDB;user=jamie;password=LetMeIn;shutdown=true';
connect 'wombat;user=jerry;password=SacreBleu;shutdown=true';

show connections;
disconnect all;
show connections;

-- Database shutdown - check user - should succeed
connect 'wombat;user=francois;password=paceesalute;shutdown=true';

-- beetle 5468
disconnect all;

connect 'myDB;user=jerry;password=SacreBleu;shutdown=true';

-- beetle 5468
disconnect all;

-- there should be no connections left here
show connections;

-- JBMS System shutdown - check user - should fail
connect ';user=jamie;password=LetMeIn;shutdown=true';

-- JBMS System shutdown - check user - should succeed
connect ';user=system;password=manager;shutdown=true';

