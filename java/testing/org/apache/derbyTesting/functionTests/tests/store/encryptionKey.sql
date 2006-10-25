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
------------------------------------------------------------------------------------
-- This test file contains test cases for testing encryptionKey  property in the connection
-- url 
-- 
-- Case 1: use external encryption key and create
-- 	   connect using correct key
--	   connect using wrong key ( different length, different key)
--	   connect again using correct key
-- Case 2: backup database
--	   connect to original db after backup
-- Case 3: createFrom backedup database
--	   with wrong key
--	   with right key
--	   with wrong key
--	   with right key
--	   test restoreFrom
-- Case 4: use invalid key when trying to create
--     key length not even
--     key contains invalid character(s)
--	   
------------------------------------------------------------------------------------
-- case1:	give external encryptionKey instead of bootpassword
connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';

create table t1(i1 int);
insert into t1 values(1);
select * from t1;
commit;
connect 'jdbc:derby:encdbcbc_key;shutdown=true';

-- case 1.1 - right key

connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
select * from t1;

connect 'jdbc:derby:encdbcbc_key;shutdown=true';
-- (-ve case) connect without the encryptionKey 
--  connect with encryptionKey and keylength ( will ignore the keylength value)

--  wrong length
connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6163646566676868';
-- wrong key
connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6862636465666768';
select * from t1;

-- correct key
connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
select * from t1;

-- case 2 backup
CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/bkup1');
connect 'jdbc:derby:encdbcbc_key;shutdown=true';

-- connect to original db after backup

connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
select * from t1;

-- case 3 :create db from backup using correct key
connect 'jdbc:derby:encdbcbc_key2;createFrom=extinout/bkup1/encdbcbc_key;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
select * from t1;
connect 'jdbc:derby:encdbcbc_key2;shutdown=true';

-- create db from backup using wrong key
connect 'jdbc:derby:encdbcbc_key3;createFrom=extinout/bkup1/encdbcbc_key;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6122636465666768';
select * from t1;

connect 'jdbc:derby:encdbcbc_key3;shutdown=true';

-- create db from backup using correct key
connect 'jdbc:derby:encdbcbc_12;createFrom=extinout/bkup1/encdbcbc_key;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
select * from t1;

connect 'jdbc:derby:encdbcbc_key12;shutdown=true';

connect 'jdbc:derby:encdb;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
create table t1(i1 int ,c2 char(20));
insert into t1 values(1,'a');
select * from t1;

call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup2');

connect 'jdbc:derby:encdb;shutdown=true';
disconnect;

connect 'jdbc:derby:encdb;restoreFrom=extinout/mybackup2/encdb;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=6162636465666768';
select * from t1;
disconnect;

-- case 4 : invalid keys
-- key length not even
connect 'jdbc:derby:encddbdb_invkey;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=123456789';

-- key contains illegal character(s)
connect 'jdbc:derby:encddbdb_invkey;create=true;dataEncryption=true;encryptionAlgorithm=DES/CBC/NoPadding;encryptionKey=61626364656667XY';

