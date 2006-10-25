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
-- This script tests  configuring an un-encrypted database 
-- for encryption and re-encryption of an encrypted database.
-- with new enryption key/password.


-- if there are any global transactions in the prepared state after
-- recovery; encrypion/re-encryption of a database should fail. 

disconnect;
xa_datasource 'wombat_en' create;
xa_connect ;
xa_start xa_noflags 0;
xa_getconnection;
create table foo (a int);
insert into foo values (0);
insert into foo values (1);
select * from foo;
xa_end xa_success 0;
xa_commit xa_1phase 0;

-- prepare transaction and shutdown
xa_start xa_noflags 1;
insert into foo values (2);
insert into foo values (3);
xa_end xa_success 1;

-- prepare the global tx
xa_prepare 1;


-- shutdown the database
disconnect;
connect 'jdbc:derby:;shutdown=true';


-- configure the database for encrypion with an external encryption key.
-- this should fail because of the global transacton in the prepared state.
 
connect 'jdbc:derby:wombat_en;dataEncryption=true;encryptionKey=6162636465666768';

---attempt to configure the database for encrypion with a boot password.
-- this should fail because of the global transacton in 
-- the prepared state.

connect 'jdbc:derby:wombat_en;dataEncryption=true;bootPassword=xyz1234abc';

-- now reboot the db and commit the transaction in the prepapred state. 
xa_datasource 'wombat_en';
xa_connect ;
xa_start xa_noflags 2;
xa_getconnection;
insert into foo values (4);
xa_recover xa_startrscan;
xa_commit xa_2phase 1;
select * from foo;
xa_end xa_success 2;
xa_commit xa_1phase 2;

-- shutdown the database
disconnect;
connect 'jdbc:derby:;shutdown=true';

--- configure the database for encrypion with a boot password.
--- this should pass.
connect 'jdbc:derby:wombat_en;dataEncryption=true;bootPassword=xyz1234abc';
disconnect;
xa_datasource 'wombat_en';
xa_connect ;
xa_start xa_noflags 3;
xa_getconnection;
insert into foo values (5);
xa_end xa_success 3;

-- prepare the global tx
xa_prepare 3;

-- shutdown the database
disconnect;
connect 'jdbc:derby:;shutdown=true';

-- attempt to reconfigure the database with a new password. 
-- this should fail because of the global transaction in the prepared state
-- after recovery.
connect 'jdbc:derby:wombat_en;bootPassword=xyz1234abc;newBootPassword=new1234xyz';

-- now reboot the db and commit the transaction in the prepared state. 
connect 'jdbc:derby:wombat_en;bootPassword=xyz1234abc';
disconnect;
xa_datasource 'wombat_en';
xa_connect ;
xa_start xa_noflags 4;
xa_getconnection;
insert into foo values (6);
xa_recover xa_startrscan;
xa_commit xa_2phase 3;
select * from foo;
xa_end xa_success 4;
xa_commit xa_1phase 4;

-- shutdown the database
disconnect;
connect 'jdbc:derby:;shutdown=true';

--- re-encrypt the database with a new password. 
--- this should pass. 
connect 'jdbc:derby:wombat_en;bootPassword=xyz1234abc;newBootPassword=new1234xyz';
select * from foo ;
