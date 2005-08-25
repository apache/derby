--
-- This tests database in a jar
-- and a jar in a database in a jar!
--
;

connect 'jdbc:derby:db1;create=true' AS DB1;
create table t ( i int not null primary key, c char(20));
insert into t values (1, 'hello');
insert into t values (2, 'goodbye');

create function APP.D2ME(VAL INT) RETURNS INT
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'InAJar.doubleMe';

-- should not be found;
values APP.D2ME(2);

CALL sqlj.install_jar('file:extin/dbjar.jar', 'APP.DMJ', 0);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'APP.DMJ');

-- check the class loading is working;
values APP.D2ME(2);

-- shutdown to allow jarring of database
disconnect;
connect 'jdbc:derby:db1;shutdown=true';


-- jar up the database
set connection CONNECTION0;
create procedure CREATEARCHIVE(jarName VARCHAR(20), path VARCHAR(20), dbName VARCHAR(20))
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.createArchive';

call CREATEARCHIVE('ina.jar', 'db1', 'db7');

-- reconnect back to db1 to modify table to ensure we are not seeing db1 unjarred
connect 'jdbc:derby:db1' AS DB1;
insert into t values (4, 'directory version');
disconnect;

connect 'jdbc:derby:jar:(ina.jar)db7' AS DB7;
select * from t;
insert into t values(3, 'is read only');
values APP.D2ME(2);

autocommit off;
select * from t WITH RR;
select TYPE, MODE, TABLENAME from new org.apache.derby.diag.LockTable() AS L ORDER BY 1,2,3;
disconnect;

-- connect to database in jar file via classpath
-- should fail as it is not on the classpath yet.
connect 'jdbc:derby:classpath:db7' AS DB7CLF;

-- create a class loader for this current thread
set connection CONNECTION0;
create procedure setDBContextClassLoader(JARNAME VARCHAR(20))
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.setDBContextClassLoader';

call setDBContextClassLoader('ina.jar');

connect 'jdbc:derby:classpath:db7' AS DB7CL;
select * from t;
insert into t values(3, 'is read only');
values APP.D2ME(2);
autocommit off;
select * from t WITH RR;
select TYPE, MODE, TABLENAME from new org.apache.derby.diag.LockTable() AS L ORDER BY 1,2,3;
disconnect;

-- Beetle 5171.  Don't crash if the contextClassLoader is null
set connection CONNECTION0;
create procedure setNullContextClassLoader()
LANGUAGE JAVA PARAMETER STYLE JAVA
NO SQL
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.setNullContextClassLoader';

call setNullContextClassLoader();

create table t2 (i int);
insert into t2 values(1);
insert into t2 values(2);
select count(*) from t2;
drop table t2;
