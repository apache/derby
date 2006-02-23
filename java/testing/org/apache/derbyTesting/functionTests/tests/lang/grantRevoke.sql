connect 'grantRevoke;create=true' user 'satheesh' as satConnection;

-- Test table privileges
create table satheesh.tsat(i int, j int);
grant select on satheesh.tsat to public;
grant insert on satheesh.tsat to foo;
grant delete on satheesh.tsat to foo;
grant update on satheesh.tsat to foo;
grant update(i) on satheesh.tsat to bar;

select * from sys.systableperms;

connect 'grantRevoke' user 'bar' as barConnection;

-- Following revokes should fail. Only owner can revoke permissions
revoke select on satheesh.tsat from public;
revoke insert on satheesh.tsat from foo;
revoke update(i) on satheesh.tsat from foo;
revoke update on satheesh.tsat from foo;
revoke delete on satheesh.tsat from foo;

set connection satConnection;

-- Revoke permissions not granted already
revoke trigger on satheesh.tsat from foo;
revoke references on satheesh.tsat from foo;

-- Following revokes should revoke permissions
revoke update on satheesh.tsat from foo;
revoke delete on satheesh.tsat from foo;

-- Check success by looking at systableperms directly for now
select * from sys.systableperms;

revoke insert on satheesh.tsat from foo;
revoke select on satheesh.tsat from public;

-- Check success by looking at systableperms directly for now
select * from sys.systableperms;

-- Test routine permissions

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT
NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
LANGUAGE JAVA PARAMETER STYLE JAVA;

grant execute on function F_ABS to foo;
grant execute on function F_ABS(int) to bar;

revoke execute on function F_ABS(int) from bar RESTRICT;

drop function f_abs;

-- Tests with views
create view v1 as select * from tsat;

grant select on v1 to bar;
grant insert on v1 to foo;
grant update on v1 to public;

-- Tests for synonym. Not supported currently.
create synonym mySym for satheesh.tsat;

-- Expected to fail
grant select on mySym to bar;
grant insert on mySym to foo;

-- Test for external security clause
-- Expected to fail
CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
EXTERNAL SECURITY DEFINOR
LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE PROCEDURE AUTH_TEST.addUserUtility(IN userName VARCHAR(50), IN permission VARCHAR(22)) 
LANGUAGE JAVA PARAMETER STYLE JAVA
EXTERNAL SECURITY INVOKER
EXTERNAL NAME 'org.apache.derby.database.UserUtility.add';

CREATE FUNCTION F_ABS(P1 INT)
RETURNS INT NO SQL
RETURNS NULL ON NULL INPUT
EXTERNAL NAME 'java.lang.Math.abs'
EXTERNAL SECURITY DEFINER
LANGUAGE JAVA PARAMETER STYLE JAVA;

values f_abs(-5);

-- Test for AUTHORIZATION option for create schema
-- GrantRevoke TODO: Need to enforce who can create which schema.
-- More negative test cases need to be added once enforcing is done.

CREATE SCHEMA MYDODO AUTHORIZATION DODO;

CREATE SCHEMA AUTHORIZATION DERBY;

select * from sys.sysschemas where schemaname not like 'SYS%';

