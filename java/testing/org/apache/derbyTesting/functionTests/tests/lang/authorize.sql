--
-- Create a database with a table and some data.
--
connect 'authorize;create=true' as c1; 

create table AUTH_TEST.t1 (a int);

CREATE FUNCTION AUTH_TEST.resourcefile(packageName VARCHAR(50), resourceName VARCHAR(50), outputFileName VARCHAR(50)) 
       RETURNS VARCHAR(3200) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.DbFile.mkFileFromResource' 
       LANGUAGE JAVA PARAMETER STYLE JAVA;

-- need two procedures to run this test, otherwise the read-only connection
-- is not even alowed to call a MODIFIES SQL DATA procedure.
create procedure AUTH_TEST.verifyAccessRW(P1 INT) MODIFIES SQL DATA external name 'org.apache.derbyTesting.functionTests.util.T_Authorize.verifyAccessRW' language java parameter style java;
create procedure AUTH_TEST.verifyAccessRO(P1 INT) READS SQL DATA external name 'org.apache.derbyTesting.functionTests.util.T_Authorize.verifyAccessRO' language java parameter style java;

CREATE FUNCTION AUTH_TEST.getPermission(userName VARCHAR(50)) 
       RETURNS VARCHAR(22) EXTERNAL NAME 'org.apache.derby.database.UserUtility.getPermission' 
       LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE PROCEDURE AUTH_TEST.addUserUtility(IN userName VARCHAR(50), IN permission VARCHAR(22)) 
       LANGUAGE JAVA PARAMETER STYLE JAVA
       EXTERNAL NAME 'org.apache.derby.database.UserUtility.add';

CREATE PROCEDURE AUTH_TEST.setUserUtility(IN userName VARCHAR(50), IN permission VARCHAR(22)) 
       LANGUAGE JAVA PARAMETER STYLE JAVA
       EXTERNAL NAME 'org.apache.derby.database.UserUtility.set';

CREATE PROCEDURE AUTH_TEST.dropUserUtility(IN userName VARCHAR(50)) 
       LANGUAGE JAVA PARAMETER STYLE JAVA
       EXTERNAL NAME 'org.apache.derby.database.UserUtility.drop';

--
-- Add a jar file for use in verification that jar replace and drop are not
-- allowed in a read only connection.
call sqlj.install_jar(AUTH_TEST.resourcefile('org.apache.derbyTesting.functionTests.testData.v1','j1v1.jar','extinout/j1v1.jar'),
				'APP.IMMUTABLE', 0);

--
-- Configure the database with an invalid default connection mode (should fail);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode','AsDf');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');

--
-- Add a bad list of read only users (should fail).
--
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers','fred,0IsABadFirstLetter');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');

--
-- Add a bad list of full access users (should fail).
--
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.fullAccessUsers','fred,0IsABadFirstLetter');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

--
-- Connect and verify the user had full access.
connect 'authorize' as c2;
call AUTH_TEST.verifyAccessRW(1);
disconnect;

--
-- Configure the database to disallow access by unknown users
--
set connection c1;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode','NoAcCeSs');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');

--
-- Connect as an unknown user (Should fail)
--
connect 'authorize' user 'fred';

--
-- Connect as a user with an invalid name (Should fail)
--
connect 'authorize' user '!amber' as c2;

--
-- Connect as a known user with a delimited name that is
-- only valid if it is delimited (Should fail)
--
connect 'authorize' user '"!amber"' as c2;

--
-- Delete the defaultAccessMode property. Verify unknown users
-- have full access.
--
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode',null);
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');
connect 'authorize' user '"!amber"' as c2;
call AUTH_TEST.verifyAccessRW(2);
disconnect;

--
-- Configure the database to allow full access by unknown users
-- and verify an unknown user has full access.
set connection c1;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode','fullACCESS');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');
connect 'authorize' user '"!amber"' as c2;
call AUTH_TEST.verifyAccessRW(3);

--
-- Configure the database to allow readOnly access by unknown
-- users. Verify existing connections by unknow users retain 
-- thier full access.
set connection c1;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode','readOnlyACCESS');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');
set connection c2;
call AUTH_TEST.verifyAccessRW(4);
disconnect;

--
-- Connect as an unknown user and verify that the connection has
-- readOnly access.
connect 'authorize' as c2;
call AUTH_TEST.verifyAccessRO(5);
disconnect;

--
-- Configure the database to have some read only and full access
-- users. 
--
set connection c1;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode','NoACCESS');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.fullAccessUsers','fullUser1,"***both","aaa-differByCase"');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

-- The following should fail as user '***both' can only be in 1 list
-- and it is already defined in the fullAccess users list.
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers','readUser1,"***both","AAA-differByCase"');
-- This should succeed
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers','readUser1,"AAA-differByCase"');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');

--
-- Connect as an unknown user - due to case error (should fail);
connect 'authorize' user '"fulluser1"';

--
-- Connect as a read only user and verify access
-- Verify the user can't elevate to full access.
connect 'authorize' user 'readUser1' as c2;
call AUTH_TEST.verifyAccessRO(6);
readonly off; 
call AUTH_TEST.verifyAccessRO(7);

--
-- Connect as a full user and verify access.
--
connect 'authorize' user '"aaa-differByCase"' as c3;
call AUTH_TEST.verifyAccessRW(8);

--
-- Verify the full user can set her connection to readonly 
-- and back.
readonly on;
call AUTH_TEST.verifyAccessRO(9);
readonly off; 
call AUTH_TEST.verifyAccessRW(10);

--
-- Configure the database to have some full users and all unknown
-- users granted read access.
disconnect;
set connection c2;
disconnect;
set connection c1;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode','readOnlyACCESS');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.fullAccessUsers','fullUser1,"***both","aaa-differByCase"');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers',null);
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');

--
-- Connect as a guest user (name differs from a full user by case)
connect 'authorize' user '"fulluser1"' as c2;
call AUTH_TEST.verifyAccessRO(11);
readonly off; 
readonly on;
disconnect;

--
-- Connect as a full user and verify we can do it all
connect 'authorize' user 'fulluser1' as c2;
call AUTH_TEST.verifyAccessRW(12);
readonly on;
call AUTH_TEST.verifyAccessRO(13);
readonly off; 
call AUTH_TEST.verifyAccessRW(14);

--
-- Verfify we cannot set the readonly state in an active connection.
autocommit off;
insert into AUTH_TEST.t1 values 1,2;
readonly off;
readonly on;
rollback;
autocommit on;

--
-- Verify a read only user can perform a query that uses a
-- temp table. 
insert into AUTH_TEST.t1 values 1,2,3,4,5,6,7;
insert into AUTH_TEST.t1 select * from AUTH_TEST.t1;
insert into AUTH_TEST.t1 select * from AUTH_TEST.t1;
insert into AUTH_TEST.t1 select * from AUTH_TEST.t1;
insert into AUTH_TEST.t1 select * from AUTH_TEST.t1;
readonly on;
select * from AUTH_TEST.t1 order by a;
readonly off;
delete from AUTH_TEST.t1;

--
-- Remove all the authorization properties to prepare to test
-- the UserUtility.
--
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode',null);
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.defaultConnectionMode');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers',null);
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.fullAccessUsers',null);
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

--
-- Verify external scalar function getPermission works with no users.
values AUTH_TEST.getPermission('myFriend');

--
-- Verify external scalar function getPermission notices when it is
-- called with a bad id. (should fail.)
values AUTH_TEST.getPermission('***badId');
values AUTH_TEST.getPermission(null);


--
-- Verify external scalar function addUserUtility reports an error when called with
-- a bad user, a null user and a bad permission mode and a
-- null permission mode.
call AUTH_TEST.addUserUtility('***badId','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility(null,'READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('goodUser','badPermission');
call AUTH_TEST.addUserUtility('goodUser',null);

--
-- Add 3 read access users with quoted names and verify they are added.
-- As a negative test we add each user twice
call AUTH_TEST.addUserUtility('"dAda"','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('"dAda"','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('"bUnny"','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('"bUnny"','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('"jAmes"','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('"jAmes"','READ_ACCESS_PERMISSION');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

--
-- Verify external scalar function setUserUtility reports an error when called with
-- a bad user, a null user, a missing user a bad permission 
-- mode and a null permission mode.
call AUTH_TEST.setUserUtility('***badId','READ_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility(null,'READ_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('missingUser','FULL_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('"jAmes"','badPermission');
call AUTH_TEST.setUserUtility('"jAmes"',null);

--
-- Get the access level for our users
values AUTH_TEST.getPermission('"dAda"');
values AUTH_TEST.getPermission('"DADA"'); --wrong case
values AUTH_TEST.getPermission('"bUnny"');
values AUTH_TEST.getPermission('"dAda"');

--
-- Set all our read access users to full access users twice.
call AUTH_TEST.setUserUtility('"dAda"','FULL_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('"dAda"','FULL_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('"bUnny"','FULL_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('"bUnny"','FULL_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('"jAmes"','FULL_ACCESS_PERMISSION');
call AUTH_TEST.setUserUtility('"jAmes"','FULL_ACCESS_PERMISSION');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

--
-- Verify external scalar function dropUserUtility reports an error when called with
-- a bad user, a null user, a missing user.
call AUTH_TEST.dropUserUtility('***badId');
call AUTH_TEST.dropUserUtility(null);
call AUTH_TEST.dropUserUtility('missingUser');

--
-- Drop each user twice
call AUTH_TEST.dropUserUtility('"dAda"');
call AUTH_TEST.dropUserUtility('"dAda"');
call AUTH_TEST.dropUserUtility('"bUnny"');
call AUTH_TEST.dropUserUtility('"bUnny"');
call AUTH_TEST.dropUserUtility('"jAmes"');
call AUTH_TEST.dropUserUtility('"jAmes"');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

--
-- Add set and drop some users with names that
-- are not delimited.
call AUTH_TEST.addUserUtility('dada','READ_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('DADA','READ_ACCESS_PERMISSION'); -- duplicate
call AUTH_TEST.addUserUtility('bunny','FULL_ACCESS_PERMISSION');
call AUTH_TEST.addUserUtility('james','FULL_ACCESS_PERMISSION');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');
call AUTH_TEST.setUserUtility('BUNNY','READ_ACCESS_PERMISSION');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');
values AUTH_TEST.getPermission('dAda');
values AUTH_TEST.getPermission('bunny');
values AUTH_TEST.getPermission('jaMes');
call AUTH_TEST.dropUserUtility('dada');
call AUTH_TEST.dropUserUtility('bunny');
call AUTH_TEST.dropUserUtility('jaMes');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.readOnlyAccessUsers');
VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.fullAccessUsers');

-- clean up
drop function AUTH_TEST.getPermission;
drop procedure AUTH_TEST.verifyAccessRW;
drop procedure AUTH_TEST.verifyAccessRO;
drop procedure AUTH_TEST.setUserUtility;
drop procedure AUTH_TEST.addUserUtility;
drop procedure AUTH_TEST.dropUserUtility;
