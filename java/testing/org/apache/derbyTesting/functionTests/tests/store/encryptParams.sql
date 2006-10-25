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
-- test database encryption parameters such as the encryption algorithm and the encryption provider


connect 'jdbc:derby:;shutdown=true';

connect 'jdbc:derby:wombatDESede;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionAlgorithm=DESede/CBC/NoPadding';

create table t1 ( a char(20));
insert into t1 values ('hello world');

select * from t1;


disconnect;
connect 'jdbc:derby:;shutdown=true';

-- algorithm is not specified, doesn't matter since algorithm is stored in the database
connect 'jdbc:derby:wombatDESede;bootPassword=ThursdaySaturday';
select * from t1;

disconnect;
connect 'jdbc:derby:;shutdown=true';

-- wrong algorithm, doesn't matter since algorithm is stored in the database
connect 'jdbc:derby:wombatDESede;bootPassword=ThursdaySaturday;encryptionAlgorithm=Blowfish/CBC/NoPadding';

select * from t1;

disconnect;
connect 'jdbc:derby:;shutdown=true';

-- create new databases with different encryption algorithms
connect 'jdbc:derby:wombatDES;create=true;dataEncryption=true;bootPassword=ThursdaySaturdayfoobarpo;encryptionAlgorithm=DES/CBC/NoPadding';

create table t2 ( a char(20));
insert into t2 values ('hot air');

select * from t2;

disconnect;
connect 'jdbc:derby:;shutdown=true';

connect 'jdbc:derby:wombatBlowfish;create=true;dataEncryption=true;bootPassword=SundayMondayFriday;encryptionAlgorithm=Blowfish/CBC/NoPadding';

create table t3 ( a char(20));
insert into t3 values ('blow hot air on fish');

select * from t3;

disconnect;
connect 'jdbc:derby:;shutdown=true';


                
-- have 3 connections open to 3 databases, each datababase uses a different encryption algorithm

connect 'jdbc:derby:wombatDESede;bootPassword=ThursdaySaturday' AS C1;
connect 'jdbc:derby:wombatDES;bootPassword=ThursdaySaturdayfoobarpo' AS C2;
connect 'jdbc:derby:wombatBlowfish;bootPassword=SundayMondayFriday' AS C3;

set connection C1;
select * from t1;

set connection C2;
select * from t2;

set connection C3;
select * from t3;

disconnect;
connect 'jdbc:derby:;shutdown=true';

-- create a new database with an algorithm which uses padding
-- should not work
connect 'jdbc:derby:wombatBad;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionAlgorithm=DESede/CBC/PKCS5Padding';

-- create a new database with a bad algorithm
-- should not work
connect 'jdbc:derby:wombatBad;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionAlgorithm=Fungus/CBC/NoPadding';

-- create a new database with another bad algorithm (bad feedback mode)
-- should not work
connect 'jdbc:derby:wombatBad;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionAlgorithm=DES/CNN/NoPadding';

-- create a new database with a bad provider
-- should not work
connect 'jdbc:derby:wombatBad;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionProvider=com.foo.bar';

-- create a new database with a bad encryption algorithm format
-- should not work
connect 'jdbc:derby:wombatBad;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionAlgorithm=DES';

-- create a new database with a non supported feedback mode (PCBC)
-- should not work
connect 'jdbc:derby:wombatBad;create=true;dataEncryption=true;bootPassword=ThursdaySaturday;encryptionAlgorithm=DES/PCBC/NoPadding';
