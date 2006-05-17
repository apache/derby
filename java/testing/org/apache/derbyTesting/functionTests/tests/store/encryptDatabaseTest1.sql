-- This script tests configuring an un-enctypted database for encryption. 

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
connect 'jdbc:derby:wombat_key;dataEncryption=true;encryptionKey=61626364656667686961626364656568';
select * from t1;
insert into t1 values(6);
insert into t1 values(7);
disconnect;
connect 'wombat_key;shutdown=true';
connect 'jdbc:derby:wombat_key;encryptionKey=61626364656667686961626364656568';
select * from t1 ;
disconnect;
connect 'wombat_key;shutdown=true';

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
