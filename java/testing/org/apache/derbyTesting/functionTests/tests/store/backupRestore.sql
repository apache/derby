--CASE: Tests backup/restore with jar files stored inside the database
create table x (x double precision, y int);
insert into x values (1,1),(10,1),(20,1);
call sqlj.install_jar('extin/brtestjar.jar', 'aggjar', 0);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'APP.aggjar');
create function dv(P1 INT) RETURNS INT NO SQL external name 'dbytesting.CodeInAJar.doubleMe' language java parameter style java;
select cast (dv(x) as dec(5,2)) from x;
----take a backup.
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('extinout/mybackup');
connect 'wombat;shutdown=true';
disconnect;

---restore a databases
connect 'wombat;restoreFrom=extinout/mybackup/wombat';
select cast (dv(x) as dec(5,2)) from x;
connect 'wombat;shutdown=true';
disconnect;

---create a new database from backup.
connect 'wombatnew;createFrom=extinout/mybackup/wombat';
select cast (dv(x) as dec(5,2)) from x;
connect 'wombatnew;shutdown=true';
disconnect;

connect 'wombat';
select cast (dv(x) as dec(5,2)) from x;
call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE('extinout/mybackup', 1);
insert into x values (1,1),(10,1),(20,1);
insert into x values (1,1),(10,1),(20,1);
insert into x values (1,1),(10,1),(20,1);
insert into x values (1,1),(10,1),(20,1);
select cast (dv(x) as dec(5,2)) from x;
connect 'wombat;shutdown=true';
disconnect;

---perform a rollforward recovery
connect 'wombat;rollForwardRecoveryFrom=extinout/mybackup/wombat';
select * from x;
select cast (dv(x) as dec(5,2)) from x;
insert into x values (1,1),(10,1),(20,1);
connect 'wombat;shutdown=true';
disconnect;
