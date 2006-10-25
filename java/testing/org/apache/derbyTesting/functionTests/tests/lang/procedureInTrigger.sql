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
--- setup
--- table used in the procedures
create table t1 (i int primary key, b char(15));
--- table used in this test
create table t2 (x integer, y integer);

create procedure proc_no_sql() 
	parameter style java
	language java
	NO SQL
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.zeroArg';

create procedure proc_contains_sql()
	parameter style java
	language java
	CONTAINS SQL
	external name 'org.apache.derbyTesting.functionTests.util.Triggers.getConnection';

create procedure proc_reads_sql(i integer)  
	parameter style java
	language java
	READS SQL DATA
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectRows'
	dynamic result sets 1;

create procedure proc_modifies_sql_insert_op(p1 int, p2 char(10)) 
	parameter style java 
	language java 
	MODIFIES SQL DATA 
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.insertRow';

create procedure proc_modifies_sql_update_op(p1 int) 
	parameter style java 
	language java 
	MODIFIES SQL DATA 
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.updateRow';

create procedure proc_modifies_sql_delete_op(p1 int) 
	parameter style java 
	language java 
	MODIFIES SQL DATA 
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.deleteRow';

create procedure alter_table_proc() 
	parameter style java 
	language java 
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.alterTable';

create procedure drop_table_proc() 
	parameter style java 
	language java 
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.dropTable';

create procedure commit_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.Triggers.doConnCommit'; 
	   
create procedure rollback_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.Triggers.doConnRollback'; 
       
create procedure set_isolation_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.Triggers.doConnectionSetIsolation'; 
       
create procedure create_index_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.createIndex'; 

create procedure drop_index_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.dropIndex'; 

create procedure create_trigger_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.createTrigger'; 

create procedure drop_trigger_proc() 
       parameter style java
       dynamic result sets 0 language java 
       contains sql
       external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.dropTrigger';
       
create procedure proc_wrongly_defined_as_no_sql(p1 int, p2 char(10)) 
	parameter style java 
	language java 
	NO SQL
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.insertRow';       
                     
--- create a new schema and a procedure in it
create schema new_schema;

create procedure new_schema.proc_in_new_schema() 
	parameter style java
	language java
	NO SQL
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.zeroArg';
	
--- procedure which uses a non_existent method	
create procedure proc_using_non_existent_method() 
	parameter style java
	language java
	NO SQL
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.nonexistentMethod';

--- tests

create trigger after_stmt_trig_no_sql AFTER insert on t2 
	for each STATEMENT mode db2sql call proc_no_sql();
--- insert 2 rows. check that trigger is fired - procedure should be called once
insert into t2 values (1,2), (2,4);
--- check inserts are successful
select * from t2;
--- check that trigger firing and database event fail if the procedure referred
--- in the triggered sql statement is dropped
drop procedure proc_no_sql;
--- should fail
insert into t2 values (1,2), (2,4);
--- after recreating the procedure, the trigger should work
create procedure proc_no_sql() 
	parameter style java
	language java
	NO SQL
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.zeroArg';
--- trigger firing should pass now
insert into t2 values (3,6);
--- check inserts are successful
select * from t2;

create trigger after_row_trig_no_sql AFTER delete on t2 
	for each ROW mode db2sql call proc_no_sql();
--- delete all rows. check that trigger is fired - procedure should be called 2 times
delete from t2;
--- check delete is successful
select * from t2;

drop trigger after_stmt_trig_no_sql;
drop trigger after_row_trig_no_sql;

create trigger before_stmt_trig_no_sql no cascade BEFORE insert on t2 
	for each STATEMENT mode db2sql call proc_no_sql();
--- insert 2 rows. check that trigger is fired - procedure should be called once
insert into t2 values (1,2), (2,4);
--- check inserts are successful
select * from t2;
--- check that trigger firing and database event fail if the procedure referred
--- in the triggered sql statement is dropped
drop procedure proc_no_sql;
--- should fail
insert into t2 values (1,2), (2,4);
--- after recreating the procedure, the trigger should work
create procedure proc_no_sql() 
	parameter style java
	language java
	NO SQL
	external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.zeroArg';
--- trigger firing should pass now
insert into t2 values (3,6);
--- check inserts are successful
select * from t2;

create trigger before_row_trig_no_sql no cascade BEFORE delete on t2 
	for each ROW mode db2sql call proc_no_sql();
--- delete and check trigger fired. procedure called twice
delete from t2;
--- check delete is successful. t2 must be empty
select * from t2;

drop trigger before_stmt_trig_no_sql;
drop trigger before_row_trig_no_sql;

insert into t2 values (1,2), (2,4);
create trigger after_row_trig_contains_sql AFTER update on t2 
	for each ROW mode db2sql call proc_contains_sql();
--- update 2 rows. check that trigger is fired - procedure should be called twice
update t2 set x=x*2;
--- check updates are successful
select * from t2;

create trigger before_stmt_trig_contains_sql no cascade BEFORE delete on t2 
	for each STATEMENT mode db2sql call proc_contains_sql();
--- delete 2 rows. check that trigger is fired - procedure should be called once
delete from t2;
--- check delete is successful
select * from t2;

drop trigger after_row_trig_contains_sql;
drop trigger before_stmt_trig_contains_sql;

--- create a row in t1 for use in select in the procedure
insert into t1 values (1, 'one');
create trigger after_stmt_trig_reads_sql AFTER insert on t2 
	for each STATEMENT mode db2sql call proc_reads_sql(1);
--- insert 2 rows. check that trigger is fired - procedure should be called once
insert into t2 values (1,2), (2,4);
--- check inserts are successful
select * from t2;
drop trigger after_stmt_trig_reads_sql;

create trigger before_row_trig_reads_sql no cascade BEFORE delete on t2 
	for each ROW mode db2sql call proc_reads_sql(1);
--- delete 2 rows. check that trigger is fired - procedure should be called twice
delete from t2;
--- check delete is successful
select * from t2;
drop trigger before_row_trig_reads_sql;

--- empty t1
delete from t1;
create trigger after_stmt_trig_modifies_sql_insert_op AFTER insert on t2 
	for each STATEMENT mode db2sql call proc_modifies_sql_insert_op(1, 'one');
--- insert 2 rows
insert into t2 values (1,2), (2,4);
--- check trigger is fired. insertRow should be called once
select * from t1;
--- check inserts are successful
select * from t2;

create trigger after_row_trig_modifies_sql_update_op AFTER update of x on t2 
	for each ROW mode db2sql call proc_modifies_sql_update_op(2);
--- update all rows
update t2 set x=x*2;
--- check row trigger was fired. value of i should be 5
select * from t1;
--- check update successful
select * from t2;

create trigger after_stmt_trig_modifies_sql_delete_op AFTER delete on t2 
	for each STATEMENT mode db2sql call proc_modifies_sql_delete_op(5);
--- delete from t2
delete from t2;
--- check trigger is fired. table t1 should be empty
select * from t1;
--- check delete successful
select * from t2;

drop trigger after_stmt_trig_modifies_sql_insert_op;
drop trigger after_row_trig_modifies_sql_update_op;
drop trigger after_stmt_trig_modifies_sql_delete_op;

create trigger refer_new_row_trig AFTER insert on t2 
	REFERENCING NEW as new
	for each ROW mode db2sql call proc_modifies_sql_insert_op(new.x, 'new');
--- insert a row
insert into t2 values (25, 50);
--- check trigger is fired. insertRow should be called once
select * from t1;
--- check inserts are successful
select * from t2;

create trigger refer_old_row_trig AFTER delete on t2 
	REFERENCING OLD as old
	for each ROW mode db2sql call proc_modifies_sql_delete_op(old.x);
--- delete a row
delete from t2 where x=25;
--- check trigger is fired. deleteRow should be called once
select * from t1;
--- check delete is successful
select * from t2;

drop trigger refer_new_row_trig;
drop trigger refer_old_row_trig;

--- create a before trigger that calls a procedure that modifies sql data. 
--- trigger creation should fail
create trigger before_trig_modifies_sql no cascade BEFORE insert on t2 
	for each STATEMENT mode db2sql call proc_modifies_sql_insert_op(1, 'one');

--- in a BEFORE trigger, call a procedure which actually modifies SQL data	
--- trigger creation will pass but firing should fail
create trigger bad_before_trig no cascade BEFORE insert on t2 
	for each STATEMENT mode db2sql call proc_wrongly_defined_as_no_sql(50, 'fifty');
--- try to insert 2 rows
--- Bug DERBY-1629 -- in JDK 1.6 you only get 38001, not 38000
insert into t2 values (1,2), (2,4);
--- check trigger is not fired.
select * from t1;
--- check inserts failed
select * from t2;
drop trigger bad_before_trig;

--- procedures which insert/update/delete into trigger table
create trigger insert_trig AFTER update on t1 
	for each STATEMENT mode db2sql call proc_modifies_sql_insert_op(1, 'one');
insert into t1 values(2, 'two');
update t1 set i=i+1;
--- Check that update and insert successful. t1 should have 2 rows
select * from t1;
--- causing the trigger to fire again will violate the primary key constraint
--- verify this fails
update t1 set i=i;
--- check that the update failed
select * from t1;
drop trigger insert_trig;

create trigger update_trig AFTER insert on t1 
	for each STATEMENT mode db2sql call proc_modifies_sql_update_op(2);
insert into t1 values (4,'four');
--- Check that insert successful and trigger fired. 
select * from t1;
drop trigger update_trig;

create trigger delete_trig AFTER insert on t1 
	for each STATEMENT mode db2sql call proc_modifies_sql_delete_op(3);
insert into t1 values (8,'eight');
--- Check that insert was successful and trigger was fired
select * from t1;
drop trigger delete_trig;

--- Procedures with schema name
create trigger call_proc_in_default_schema AFTER insert on t2 
	for each STATEMENT mode db2sql call APP.proc_no_sql();
--- insert 2 rows. check that trigger is fired - procedure should be called once
insert into t2 values (1,2), (2,4);
--- check inserts are successful
select * from t2;
drop trigger call_proc_in_default_schema;

create trigger call_proc_in_default_schema no cascade BEFORE delete on t2 
	for each ROW mode db2sql call APP.proc_no_sql();
--- delete 2 rows. check that trigger is fired - procedure should be called twice
delete from t2;
--- check delete is successful
select * from t2;
drop trigger call_proc_in_default_schema;

create trigger call_proc_in_new_schema no cascade BEFORE insert on t2 
	for each STATEMENT mode db2sql call new_schema.proc_in_new_schema();
--- insert 2 rows. check that trigger is fired - procedure should be called once
insert into t2 values (1,2), (2,4);
--- check inserts are successful
select * from t2;
drop trigger call_proc_in_new_schema;

create trigger call_proc_in_new_schema AFTER delete on t2 
	for each ROW mode db2sql call new_schema.proc_in_new_schema();
--- delete 2 rows. check that trigger is fired - procedure should be called twice
delete from t2;
--- check delete is successful
select * from t2;
drop trigger call_proc_in_new_schema;

--- non-existent procedure
create trigger call_non_existent_proc1 AFTER insert on t2 
	for each ROW mode db2sql call non_existent_proc();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_NON_EXISTENT_PROC1';

create trigger call_proc_with_non_existent_proc2 AFTER insert on t2 
	for each ROW mode db2sql call new_schema.non_existent_proc();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_NON_EXISTENT_PROC2';

create trigger call_proc_in_non_existent_schema AFTER insert on t2 
	for each ROW mode db2sql call non_existent_schema.non_existent_proc();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_PROC_IN_NON_EXISTENT_SCHEMA';

create trigger call_proc_using_non_existent_method AFTER insert on t2 
	for each ROW mode db2sql call proc_using_non_existent_method();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_PROC_WITH_NON_EXISTENT_METHOD';

create trigger call_non_existent_proc1 no cascade BEFORE insert on t2 
	for each ROW mode db2sql call non_existent_proc();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_NON_EXISTENT_PROC1';

create trigger call_proc_with_non_existent_proc2 no cascade BEFORE insert on t2 
	for each ROW mode db2sql call new_schema.non_existent_proc();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_NON_EXISTENT_PROC2';

create trigger call_proc_in_non_existent_schema no cascade BEFORE insert on t2 
	for each ROW mode db2sql call non_existent_schema.non_existent_proc();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_PROC_IN_NON_EXISTENT_SCHEMA';

create trigger call_proc_using_non_existent_method no cascade BEFORE insert on t2 
	for each ROW mode db2sql call proc_using_non_existent_method();
select count(*) from SYS.SYSTRIGGERS where triggername='CALL_PROC_WITH_NON_EXISTENT_METHOD';

--- triggers must not allow dynamic parameters (?)
create trigger update_trig AFTER insert on t1 
	for each STATEMENT mode db2sql call proc_modifies_sql_update_op(?);

--- insert some rows into t2
insert into t2 values (1,2), (2,4);

--- use procedure with commit
create trigger commit_trig AFTER delete on t2 
	for each STATEMENT mode db2sql call commit_proc();
--- should fail 
delete from t2;
--- check delete failed
select * from t2;
drop trigger commit_trig;

create trigger commit_trig no cascade BEFORE delete on t2 
	for each STATEMENT mode db2sql call commit_proc();
--- should fail 
delete from t2;
--- check delete failed
select * from t2;
drop trigger commit_trig;

--- use procedure with rollback
create trigger rollback_trig AFTER delete on t2 
	for each STATEMENT mode db2sql call rollback_proc();
--- should fail 
delete from t2;
--- check delete failed
select * from t2;
drop trigger rollback_trig;

create trigger rollback_trig no cascade BEFORE delete on t2 
	for each STATEMENT mode db2sql call rollback_proc();
--- should fail 
delete from t2;
--- check delete failed
select * from t2;
drop trigger rollback_trig;

--- use procedure which changes isolation level
create trigger set_isolation_trig AFTER delete on t2 
	for each STATEMENT mode db2sql call set_isolation_proc();
--- should fail 
delete from t2;
--- check delete failed
select * from t2;
drop trigger set_isolation_trig;

create trigger set_isolation_trig no cascade BEFORE delete on t2 
	for each STATEMENT mode db2sql call set_isolation_proc();
--- should fail 
delete from t2;
--- check delete failed
select * from t2;
drop trigger set_isolation_trig;

--- call procedure that selects from same trigger table
create trigger select_from_trig_table AFTER insert on t1
	for each STATEMENT mode db2sql call proc_reads_sql(1);
--- insert 2 rows. check that trigger is fired - procedure should be called once
insert into t1 values (10, 'ten');
--- check inserts are successful
select * from t1;
drop trigger select_from_trig_table;

create trigger select_from_trig_table no cascade before delete on t1
	for each STATEMENT mode db2sql call proc_reads_sql(1);
--- delete a row. check that trigger is fired - procedure should be called once
delete from t1 where i=10;
--- check delete is successful
select * from t1;
drop trigger select_from_trig_table;

--- use procedures which alter/drop trigger table and some other table
create trigger alter_table_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call alter_table_proc();
--- should fail
delete from t1;
--- check delete failed
select * from t1;
drop trigger alter_table_trig;

create trigger drop_table_trig AFTER delete on t2 
	for each STATEMENT mode db2sql call drop_table_proc();
--- should fail
delete from t2;
--- check delete failed
select * from t2;
drop trigger drop_table_trig;

--- use procedures which create/drop trigger on trigger table and some other table
create trigger create_trigger_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call create_trigger_proc();
--- should fail
delete from t1;
--- check delete failed
select * from t1;
--- check trigger is not created
select count(*) from SYS.SYSTRIGGERS where triggername='TEST_TRIG';
drop trigger create_trigger_trig;

--- create a trigger to test we cannot drop it from a procedure called by a trigger
create trigger test_trig AFTER delete on t1 for each STATEMENT mode db2sql insert into  t1 values(20, 'twenty');

create trigger drop_trigger_trig AFTER delete on t2 
	for each STATEMENT mode db2sql call drop_trigger_proc();
--- should fail
delete from t2;
--- check delete failed
select * from t2;
--- check trigger is not dropped
select count(*) from SYS.SYSTRIGGERS where triggername='TEST_TRIG';
drop trigger drop_trigger_trig;

--- use procedures which create/drop index on trigger table and some other table
create trigger create_index_trig AFTER delete on t2 
	for each STATEMENT mode db2sql call create_index_proc();
--- should fail
delete from t2;
--- check delete failed
select * from t2;
--- check index is not created
select count(*) from SYS.SYSCONGLOMERATES where CONGLOMERATENAME='IX' and ISINDEX=1;
drop trigger create_index_trig;

--- create an index to test we cannot drop it from a procedure called by a trigger
create index ix on t1(i,b);

create trigger drop_index_trig AFTER delete on t1 
	for each STATEMENT mode db2sql call drop_index_proc();
--- should fail
delete from t1;
--- check delete failed
select * from t1;
--- check index is not dropped
select count(*) from SYS.SYSCONGLOMERATES where CONGLOMERATENAME='IX' and ISINDEX=1;
drop trigger drop_index_trig;
