--
-- Negative test for SECURE users. This tries to authenticate against an LDAP
-- server on a machine which is not accessible/doesn't exist.
--

-- 'ldapSchemeDB'		- LDAP authentication (on NT thru LDAP)
-- let's create the db and configure it.
-- we will authenticate using a default system user that we have configured.
--
-- 'ldapSchemeDB' database authentication/authorization config
--
connect 'ldapSchemeDB;create=true;user=system;password=manager';
autocommit off;
prepare p1 as 'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)';
execute p1 using 'values(''derby.authentication.provider'', ''LDAP'')';

-- there is no such machine as noSuchMachine and so the authentication will fail
execute p1 using 'values(''derby.authentication.server'', ''noSuchMachine:389'')';
execute p1 using 'values(''derby.authentication.ldap.searchBase'', ''o=opensource.apache.com'')';
-- this is the default search filter
execute p1 using 'values(''derby.authentication.ldap.searchFilter'', ''(&(objectClass=inetOrgPerson)(uid=%USERNAME%))'')';
commit;
autocommit on;
--
-- Shutdown the system for database properties to take effect
--
disconnect all;
connect 'ldapSchemeDB;user=system;password=manager;shutdown=true';
disconnect all;

connect 'ldapSchemeDB;user=mamta;password=yeeHaLdap';
show connections;

disconnect all;

-- Derby system shutdown - check user - should succeed
connect ';user=system;password=manager;shutdown=true';
