-- test getCurConnJdbc20
-- this test will get run under jdk12 only. If run under jdk11x, will get an exception like
-- following for call to newToJdbc20Method
-- ERROR 38000: The exception 'java.lang.NoSuchMethodError: java.sql.Connection: method 
-- createStatement(II)Ljava/sql/Statement; not found' was thrown while evaluating an expression.


-- method alias and table used later
create procedure newToJdbc20Method() PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Jdbc20Test.newToJdbc20Method';
create table T (a int NOT NULL primary key);
insert into T values (1);

-- now lets try a variety of errors
call newToJdbc20Method();

------------------------------------------------------------
-- drop the table
drop table T;

drop procedure newToJdbc20Method;
