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

-- Derby system shutdown - check user (owner) - should succeed
connect ';user=system;password=manager;shutdown=true';
