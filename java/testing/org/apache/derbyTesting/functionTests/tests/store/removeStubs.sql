---test which goes through the code path that 
--removes the stub files are no longer necessary
create table t1(a int ) ;
autocommit off ;
drop table t1 ;
commit ;
connect 'jdbc:derby:wombat;shutdown=true';
disconnect all;

connect 'jdbc:derby:wombat';
autocommit on;
create table t1(a int ) ;
drop table t1 ;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
create table t1(a int ) ;
drop table t1 ;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
create table t1(a int ) ;
drop table t1 ;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
connect 'jdbc:derby:wombat;shutdown=true';
disconnect all;
connect 'jdbc:derby:wombat';
create table t2( a int ) ;
connect 'jdbc:derby:wombat;shutdown=true';
disconnect all;
connect 'jdbc:derby:wombat';
drop table t2 ;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
--following create will make sure that the container
--cache does not have the delete stub file entry
create table t3(a int ) ;

--do some inserts/delete and thene remove the stubs
create table t2(b int);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
autocommit off;
delete from t2;
commit;
drop table t2;
commit;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();

-- do some delete and drpop of the container in the same transaction
create table t2(b int);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
commit;
delete from t2;
drop table t2;
commit;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();

--just do a rollback for the heck of it
-- do some delete and drpop of the container in the same transaction
create table t2(b int);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
insert into t2 values (1) , (2) , (3) , (4);
commit;
delete from t2;
drop table t2;
rollback;
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();
CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE();

