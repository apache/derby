-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- make sure that we cannot do a commit/rollback
-- on a nested connection when we are in the middle
-- of something that has to be atomic (e.g. DML).
-- commit/rollback on a nested connection is only
-- permitted when we are doing something simple
-- like CALL myMethod() or VALUES myMethod()

CREATE PROCEDURE doConnCommit() 
       DYNAMIC RESULT SETS 0 LANGUAGE JAVA 
       EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.doConnCommit' 
	   CONTAINS SQL
       PARAMETER STYLE JAVA;

CREATE PROCEDURE doConnRollback() 
       DYNAMIC RESULT SETS 0 LANGUAGE JAVA 
       EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.doConnRollback' 
	   CONTAINS SQL
       PARAMETER STYLE JAVA;

CREATE PROCEDURE doConnStmt(IN TEXT CHAR(50)) 
       DYNAMIC RESULT SETS 0 LANGUAGE JAVA 
       EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.doConnStmtNoRS' 
	   CONTAINS SQL
       PARAMETER STYLE JAVA;

CREATE FUNCTION doConnCommitInt() 
       RETURNS INT EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.doConnCommitInt' 
       LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE FUNCTION doConnStmtInt(TEXT CHAR(50)) 
       RETURNS INT EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Triggers.doConnStmtIntNoRS' 
       LANGUAGE JAVA PARAMETER STYLE JAVA;

create table x (x int);
insert into x values 1,2,3,4;

autocommit off;
-- all the following calls should succeed 
call doConnCommit();
call doConnRollback();
-- No longer supported as language statements.
-- call doConnStmt('commit');
-- call doConnStmt('rollback');
call doConnStmt('call doConnCommit()');
call doConnStmt('call doConnRollback()');
-- call doConnStmt('call doConnStmt(''call doConnStmt(''''commit'''')'')');
values doConnCommitInt();
-- values doConnStmtInt('commit');
-- values doConnStmtInt('rollback');
-- values doConnStmtInt('call doConnStmt(''call doConnStmt(''''commit'''')'')');
values doConnStmtInt('values doConnCommitInt()');

-- fail
insert into x select x+doConnCommitInt() from x;
delete from x where x in (select x+doConnCommitInt() from x);
delete from x where x = doConnCommitInt();
update x set x = doConnCommitInt();
-- insert into x values doConnStmtInt('call doConnStmt(''call doConnStmt(''''commit'''')'')');
-- select doConnStmtInt('call doConnStmt(''call doConnStmt(''''rollback'''')'')') from x;
select doConnStmtInt('call doConnStmt(''call doConnCommit()'')') from x;

-- isolation level always fails in a nested connection
call doConnStmt('set isolation serializable');

-- clean up
drop table x;
drop procedure doConnCommit;
drop procedure doConnRollback;
drop function doConnCommitInt;
drop procedure doConnStmt;
drop function doConnStmtInt;
