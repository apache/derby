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
connect 'wombat;create=true;user=dan;password=MakeItFaster';
autocommit off;
prepare p1 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p1 using 'values(''derby.database.defaultConnectionMode'', ''noAccess'')';
execute p1 using 'values(''derby.database.fullAccessUsers'', ''francois,jeff,howardR,ames,kreg'')';
remove p1;
commit;
autocommit on;
disconnect;

connect 'wombat;shutdown=true;user=francois;password=paceesalute';
connect 'myDB;create=true;user=dan;password=MakeItFaster';
autocommit off;
prepare p2 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p2 using 'values(''derby.database.defaultConnectionMode'', ''noAccess'')';
execute p2 using 'values(''derby.database.fullAccessUsers'', ''jerry,kreg,dan,jamie,ames,francois'')';
remove p2;
commit;
autocommit on;
disconnect;
connect 'myDB;shutdown=true;user=dan;password=MakeItFaster';

-- beetle 5468
disconnect all;

-- Specifically test JBMS users.
--
-- check allowed users in wombat db.
connect 'wombat;user=kreg;password=visualWhat?';
connect 'wombat;user=jeff;password=HomeRun61';
connect 'wombat;user=ames;password=AnyVolunteer?';
connect 'wombat;user=howardR;password=IamBetterAtTennis';
connect 'wombat;user=francois;password=paceesalute';
show connections;
disconnect all;
-- check allowed users in myDB db.
-- also check USER flavors
connect 'myDB;user=jerry;password=SacreBleu';
create table APP.t1(c1 char(30) check (UPPER(c1) <> 'JAMIE'));
insert into APP.t1 values CURRENT_USER;
connect 'myDB;user=kreg;password=visualWhat?';
insert into APP.t1 values USER;
connect 'myDB;user=ames;password=AnyVolunteer?';
insert into APP.t1 values SESSION_USER;
connect 'myDB;user=dan;password=MakeItFaster';
select * from APP.t1;
update APP.t1 set c1 = {fn user() };
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
-- Database shutdown - check user - should succeed
-- beetle 5367
connect 'wombat;user=francois;password=paceesalute;shutdown=true';
connect 'myDB;user=jerry;password=SacreBleu;shutdown=true';
show connections;
-- JBMS System shutdown - check user - should fail
connect ';user=jamie;password=LetMeIn;shutdown=true';
disconnect all;
-- JBMS System shutdown - check user - should succeed
connect ';user=francois;password=paceesalute;shutdown=true';
-- beetle 5390
-- the server does not shut down properly in network server
