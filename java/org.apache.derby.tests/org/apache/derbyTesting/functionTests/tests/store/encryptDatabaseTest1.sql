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
-- This script tests configuring an un-enctypted database for encryption and
-- reencryption of an encrypted database with new enryption key/password.

disconnect;
---test configure the database for encrypion with encryption key.
connect 'wombat_key;create=true';
create table t1(a int ) ;
insert into t1 values(1) ;
insert into t1 values(2) ;
insert into t1 values(3) ;
insert into t1 values(4) ;
insert into t1 values(5) ;
disconnect;
connect 'wombat_key;shutdown=true';

-- configure the database for encrypion with external encryption key.
connect 'jdbc:derby:wombat_key;dataEncryption=true;encryptionKey=6162636465666768';
select * from t1;
insert into t1 values(6);
insert into t1 values(7);
disconnect;
connect 'wombat_key;shutdown=true';
connect 'jdbc:derby:wombat_key;encryptionKey=6162636465666768';
select * from t1 ;
disconnect;
connect 'wombat_key;shutdown=true';

--- reencrypt the database with a different encryption key
connect 'jdbc:derby:wombat_key;encryptionKey=6162636465666768;newEncryptionKey=5666768616263646';
select * from t1;
insert into t1 values(7);
insert into t1 values(8);
disconnect;
connect 'wombat_key;shutdown=true';

--- boot the database with the new encyrption key. 
connect 'jdbc:derby:wombat_key;encryptionKey=5666768616263646';
select * from t1;
insert into t1 values(9);
insert into t1 values(10);
disconnect;
connect 'wombat_key;shutdown=true';
--- attempt to boot with the old encrytion key, it should fail.
connect 'jdbc:derby:wombat_key;encryptionKey=6162636465666768';

-- test confugring the database for encrypion with a boot password. 
connect 'wombat_pwd;create=true';
create table t2(a int ) ;
insert into t2 values(1) ;
insert into t2 values(2) ;
insert into t2 values(3) ;
insert into t2 values(4) ;
insert into t2 values(5) ;
disconnect;
connect 'wombat_pwd;shutdown=true';

---configure the database for encrypion with a boot password.
connect 'jdbc:derby:wombat_pwd;dataEncryption=true;bootPassword=xyz1234abc';
select * from t2;
insert into t2 values(6);
insert into t2 values(7);
disconnect;
connect 'wombat_pwd;shutdown=true';
connect 'jdbc:derby:wombat_pwd;bootPassword=xyz1234abc';
select * from t2 ;
disconnect;
connect 'wombat_pwd;shutdown=true';


--- reconfigure the database with a different password. 
connect 'jdbc:derby:wombat_pwd;bootPassword=xyz1234abc;newBootPassword=new1234xyz';
select * from t2 ;
insert into t2 values(8);
insert into t2 values(9);
insert into t2 values(10);
disconnect;
connect 'wombat_pwd;shutdown=true';
-- boot the database with the new password. 
connect 'jdbc:derby:wombat_pwd;bootPassword=new1234xyz';
select * from t2 ;
disconnect;
connect 'wombat_pwd;shutdown=true';
-- attempt to boot the database with the old password, it should fail. 
connect 'jdbc:derby:wombat_pwd;bootPassword=xyz1234abc';



