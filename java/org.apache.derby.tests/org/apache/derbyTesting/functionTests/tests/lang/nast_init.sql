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
-- This script creates a database with NATIVE credentials. The build-test-jars target in the nearby
-- build.xml file creates this database, then puts the database in nast1.jar and nast2.jar for
-- use by NativeAuthenticationServiceTest. The build-test-jars target is invoked by
-- a target by the same name in the top build.xml file.
--
connect 'jdbc:derby:nast/nast;create=true;user=kiwi;password=KIWI_password';

call syscs_util.syscs_create_user( 'KIWI', 'KIWI_password' );
call syscs_util.syscs_create_user( 'GRAPE', 'GRAPE_password' );
call syscs_util.syscs_set_database_property( 'derby.authentication.native.passwordLifetimeMillis', '0' );

create table t( a int );
insert into t( a ) values ( 100 ), ( 200 );

--
-- We use this database to test encryption of the Credentials DB.
--
connect 'jdbc:derby:nast/nastEncrypted;create=true;user=kiwi;password=KIWI_password;dataEncryption=true;bootPassword=clo760uds2caPe';

call syscs_util.syscs_create_user( 'KIWI', 'KIWI_password' );
call syscs_util.syscs_create_user( 'GRAPE', 'GRAPE_password' );
call syscs_util.syscs_set_database_property( 'derby.authentication.native.passwordLifetimeMillis', '0' );

connect 'jdbc:derby:;shutdown=true';


