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
-- beetle6023 : support for AES encryption algorithm
-- Top level testcases grp.
-- Case 1.x	different feedback modes (valid - CBC,ECB,OFB,unsupported - ABC)
-- 		2 cases for each - creating db and recovery mode
-- Case 2.x	padding ( unsupported padding )
-- Case 3.x	key lengths with bootpassword
--		case of 128 bits, 192 bits and 256 bits and unsupported 512 bits
--		mismatch keylengths (case of one keylength during creation and another 
--		during connecting)
-- Case 4.x	case of changing boot password ( covered by test - store/encryptDatabase.sql)
-- Also see store/access.sql for other cases that are run with AES encryption
-- Case 5.x	give external encryptionKey instead of bootpassword
------------------------------------------------------------------------------------

-- 1.1.1 AES - CBC  
connect 'jdbc:derby:encdb;create=true;dataEncryption=true;encryptionAlgorithm=AES/CBC/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-----------------------------------
-- 1.1.2 AES - CBC, recover

connect 'jdbc:derby:encdb;create=true;dataEncryption=true;encryptionAlgorithm=AES/CBC/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;

-----------------------------------
-- 1.2.1 AES - ECB  
connect 'jdbc:derby:encdb_ecb;create=true;dataEncryption=true;encryptionAlgorithm=AES/ECB/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-----------------------------------
-- 1.2.2 AES - ECB, recover

connect 'jdbc:derby:encdb_ecb;create=true;dataEncryption=true;encryptionAlgorithm=AES/ECB/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;

-----------------------------------
-- 1.3.1 AES - OFB  
connect 'jdbc:derby:encdb_ofb;create=true;dataEncryption=true;encryptionAlgorithm=AES/OFB/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-----------------------------------
-- 1.3.2 AES - OFB, recover

connect 'jdbc:derby:encdb_ofb;create=true;dataEncryption=true;encryptionAlgorithm=AES/OFB/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-----------------------------------
-- 1.4.1 AES - CFB  
connect 'jdbc:derby:encdb_cfb;create=true;dataEncryption=true;encryptionAlgorithm=AES/CFB/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-----------------------------------
-- 1.4.2 AES - CFB, recover

connect 'jdbc:derby:encdb_cfb;create=true;dataEncryption=true;encryptionAlgorithm=AES/CFB/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-----------------------------------
-- 1.5.1 -ve cases:  AES - unsupported feedback mode

connect 'jdbc:derby:encdb_abc;create=true;dataEncryption=true;encryptionAlgorithm=AES/ABC/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);

-----------------------------------
-- 2.1 -ve cases:  AES - unsupported padding mode

connect 'jdbc:derby:encdb_pkcs5;create=true;dataEncryption=true;encryptionAlgorithm=AES/ECB/PKCS5Padding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);

------------------------------------
-- 3.x key lengths 
-- 128,192,256 and also unsupported key length

-- 3.1 , 128 key length
connect 'jdbc:derby:encdbcbc_128;create=true;dataEncryption=true;encryptionKeyLength=128
;encryptionAlgorithm=AES/CBC/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
connect 'jdbc:derby:encdbcbc_128;create=true;dataEncryption=true;encryptionKeyLength=128;
encryptionAlgorithm=AES/CBC/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
--------------------------
--------------------------
-- 3.4 unsupported key length
connect 'jdbc:derby:encdbcbc_512;create=true;dataEncryption=true;encryptionKeyLength=512;encryptionAlgorithm=AES/CBC/NoPadding;bootPassword=Thursday';
autocommit off;
create table t1(i1 int);
insert into t1 values ( 1);
select * from t1;
commit;
disconnect;
-------------------------
-------------------------
-- 5.1	give external encryptionKey instead of bootpassword
connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=AES/CBC/NoPadding;encryptionKey=61626364656667686961626364656568';

create table t1(i1 int);
insert into t1 values(1);
select * from t1;
commit;
disconnect;
connect 'jdbc:derby:encdbcbc_key;create=true;dataEncryption=true;encryptionAlgorithm=AES/CBC/NoPadding;encryptionKey=61626364656667686961626364656568';
select * from t1;

