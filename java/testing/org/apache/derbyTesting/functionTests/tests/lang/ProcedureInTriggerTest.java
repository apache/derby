/*
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ProcedureInTriggerTest extends BaseJDBCTestCase {

    public ProcedureInTriggerTest(String name) {
        super(name);
       
    }

    /**
     * Test triggers that fire procedures with no sql
     * 
     * @throws SQLException
     */
    public void testTriggerNoSql() throws SQLException {
        Statement s = createStatement();
        s.execute("create trigger after_stmt_trig_no_sql AFTER insert on t2 for each STATEMENT call proc_no_sql()");
        //insert 2 rows. check that trigger is fired - procedure should be called once
        zeroArgCount = 0;
        s.execute("insert into t2 values (1,2), (2,4)");
        checkAndResetZeroArgCount(1);
        ResultSet rs = s.executeQuery("SELECT * FROM T2");
        JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
        //--- check that trigger firing and database event fail if the procedure referred
        //--- in the triggered sql statement is dropped
        s.execute("drop procedure proc_no_sql");
        assertStatementError("42Y03",s,"insert into t2 values (1,2), (2,4)");
        //--- after recreating the procedure, the trigger should work
        s.execute("create procedure proc_no_sql() parameter style java language java NO SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.zeroArg'");
        s.execute("insert into t2 values (3,6)");
        checkAndResetZeroArgCount(1);
        rs = s.executeQuery("SELECT * FROM T2");  
        JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"},{"3","6"}});
        s.execute("create trigger after_row_trig_no_sql AFTER delete on t2 for each ROW call proc_no_sql()");
        //--- delete all rows. check that trigger is fired - procedure should be called 3 times
        s.execute("delete from t2");
        checkAndResetZeroArgCount(3);
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger after_stmt_trig_no_sql");
        s.execute("drop trigger after_row_trig_no_sql");
        s.execute("create trigger before_stmt_trig_no_sql no cascade BEFORE insert on t2 for each STATEMENT call proc_no_sql()");
        //--- insert 2 rows. check that trigger is fired - procedure should be called once
        s.execute("insert into t2 values (1,2), (2,4)");
        checkAndResetZeroArgCount(1);
        rs = s.executeQuery("select * from t2");
        JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
        //--- check that trigger firing and database event fail if the procedure referred
        //--- in the triggered sql statement is dropped
        s.execute("drop procedure proc_no_sql");
        // --- should fail
        assertStatementError("42Y03",s,"insert into t2 values (1,2), (2,4)");
        //after recreating the procedure, the trigger should work
        s.execute("create procedure proc_no_sql() parameter style java language java NO SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.zeroArg'");
        s.execute("insert into t2 values (3,6)");
        checkAndResetZeroArgCount(1);
        // check inserts are successful
        rs = s.executeQuery("SELECT * FROM T2");  
        JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"},{"3","6"}});
        s.execute("create trigger before_row_trig_no_sql no cascade BEFORE delete on t2 for each ROW call proc_no_sql()");
        // delete and check trigger fired
        s.execute("delete from t2");
        checkAndResetZeroArgCount(3);
        // check delete is successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger before_stmt_trig_no_sql");
        s.execute("drop trigger before_row_trig_no_sql");
        s.execute("insert into t2 values (1,2), (2,4)");
        s.close();
    }
    
    /**
     * Test CONTAINS SQL triggers (neither reads no writes data)
     * procedure does just a getConnection and that's it.
     * 
     * @throws SQLException
     */
    public void testTriggerContainsSql() throws SQLException{
        Statement s = createStatement();
        s.execute("create trigger after_row_trig_contains_sql AFTER update on t2 for each ROW call proc_contains_sql()");
        // --- update 2 rows. check that trigger is fired - procedure should be called twice
        s.execute("update t2 set x = x*2");
        checkAndResetGetConnectionProcCount(2);
        //--- check updates are successful
        ResultSet rs = s.executeQuery("select * from t2");
        JDBC.assertFullResultSet(rs, new String[][] {{"2","2"},{"4","4"}});
        s.execute("create trigger before_stmt_trig_contains_sql no cascade BEFORE delete on t2 for each STATEMENT call proc_contains_sql()");
        //--- delete 2 rows. check that trigger is fired - procedure should be called once
        s.execute("delete from t2");
        checkAndResetGetConnectionProcCount(1);
        //--- check delete is successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger after_row_trig_contains_sql");
        s.execute("drop trigger before_stmt_trig_contains_sql");
        s.close();
    }
   
    /**
     * Test triggers for procedures that READ SQL DATA
     * @throws SQLException
     */
    public void testTriggerReadsSql() throws SQLException {
        Statement s = createStatement();
        //--- create a row in t1 for use in select in the procedure
        s.execute("insert into t1 values (1, 'one')");
        s.execute("create trigger after_stmt_trig_reads_sql AFTER insert on t2 for each STATEMENT call proc_reads_sql(1)");
        //--- insert 2 rows. check that trigger is fired - procedure should be called once
        selectRowsCount = 0;
        s.execute("insert into t2 values (1,2), (2,4)");
        checkAndResetSelectRowsCount(1);
        //--- check inserts are successful
        ResultSet rs = s.executeQuery("select * from t2");
        JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
        s.execute("drop trigger after_stmt_trig_reads_sql");
        s.execute("create trigger before_row_trig_reads_sql no cascade BEFORE delete on t2 for each ROW call proc_reads_sql(1)");
        //--- delete 2 rows. check that trigger is fired - procedure should be called twice
        s.execute("delete from t2");
        checkAndResetSelectRowsCount(2);
        // --- check delete is successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger before_row_trig_reads_sql");
        //--- empty t1
        s.execute("delete from t1");
        s.close();
    }
    
    /**
     * Test triggers that MODIFY SQL DATA
     * 
     * @throws SQLException
     */
    public void testModifiesSql() throws SQLException {
        Statement s = createStatement();
        s.execute("create trigger after_stmt_trig_modifies_sql_insert_op AFTER insert on t2 for each STATEMENT call proc_modifies_sql_insert_op(1, 'one')");
        //--- insert 2 rows
        s.execute("insert into t2 values (1,2), (2,4)");
        //--- check trigger is fired. insertRow should be called once
        ResultSet rs = s.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","one"}});  
        //--- check inserts are successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
        
        s.execute("create trigger after_row_trig_modifies_sql_update_op AFTER update of x on t2 for each ROW call proc_modifies_sql_update_op(2)");
        //--- update all rows
        s.execute("update t2 set x=x*2");
        // --- check row trigger was fired. value of i should be 5
        rs = s.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, new String[][]{{"5","one"}});
        //--- check update successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertFullResultSet(rs, new String[][] {{"2","2"},{"4","4"}});
        s.execute("create trigger after_stmt_trig_modifies_sql_delete_op AFTER delete on t2 for each STATEMENT call proc_modifies_sql_delete_op(5)");
        //--- delete from t2
        s.execute("delete from t2");
        // --- check trigger is fired. table t1 should be empty
        rs = s.executeQuery("select * from t1");
        JDBC.assertEmpty(rs);
        // check delete successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger after_stmt_trig_modifies_sql_insert_op");
        s.execute("drop trigger after_row_trig_modifies_sql_update_op");
        s.execute("drop trigger after_stmt_trig_modifies_sql_delete_op");
        s.execute("create trigger refer_new_row_trig AFTER insert on t2 REFERENCING NEW as new for each ROW call proc_modifies_sql_insert_op(new.x, 'new')");
        //--- insert a row
        s.execute("insert into t2 values (25, 50)");
        //--- check trigger is fired. insertRow should be called once
        rs = s.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, new String[][] {{"25","new"}});
        // --- check inserts are successful
        rs = s.executeQuery("select * from t2");
        JDBC.assertFullResultSet(rs, new String[][] {{"25","50"}});
        s.execute("create trigger refer_old_row_trig AFTER delete on t2 REFERENCING OLD as old for each ROW call proc_modifies_sql_delete_op(old.x)");
        // --- delete a row
        s.execute("delete from t2 where x=25");
        //--- check trigger is fired. deleteRow should be called once
        rs = s.executeQuery("select * from t1");
        JDBC.assertEmpty(rs);
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger refer_new_row_trig");
        s.execute("drop trigger refer_old_row_trig");
        //--- create a before trigger that calls a procedure that modifies sql data. 
        //--- trigger creation should fail
        assertStatementError("42Z9D",s,"create trigger before_trig_modifies_sql no cascade BEFORE insert on t2 for each STATEMENT call proc_modifies_sql_insert_op(1, 'one')");
        //--- in a BEFORE trigger, call a procedure which actually modifies SQL data      
        //--- trigger creation will pass but firing should fail
        s.execute("create trigger bad_before_trig no cascade BEFORE insert on t2 for each STATEMENT call proc_wrongly_defined_as_no_sql(50, 'fifty')");
        //--- try to insert 2 rows
        try {
            s.execute("insert into t2 values (1,2), (2,4)");
        } catch (SQLException se) {
            assertSQLState("38000", se);
            se = se.getNextException();
            assertSQLState("38001", se);
        }
        //--- check trigger is not fired.
        rs = s.executeQuery("select * from t1");
        JDBC.assertEmpty(rs);
        rs = s.executeQuery("select * from t2");
        JDBC.assertEmpty(rs);
        s.execute("drop trigger bad_before_trig");
        //--- procedures which insert/update/delete into trigger table
        s.execute("create trigger insert_trig AFTER update on t1 for each STATEMENT call proc_modifies_sql_insert_op(1, 'one')");
        s.execute("insert into t1 values(2, 'two')");
        s.execute("update t1 set i=i+1");
        //--- Check that update and insert successful. t1 should have 2 rows
        rs = s.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, new String[][] {{"3","two"},{"1","one"}});
       s.execute("drop trigger insert_trig");
       s.execute("create trigger update_trig AFTER insert on t1 for each STATEMENT call proc_modifies_sql_update_op(2)");
       s.execute("insert into t1 values (4,'four')");
       //--- Check that insert successful and trigger fired. 
       rs = s.executeQuery("select * from t1");
       String [][] expectedRows = {{"5","two            "},
                                   {"3","one            "},
                                   {"6","four           "}};
       JDBC.assertFullResultSet(rs, expectedRows);
              
       s.execute("drop trigger update_trig");
       s.execute("create trigger delete_trig AFTER insert on t1 for each STATEMENT call proc_modifies_sql_delete_op(3)");
       s.execute("insert into t1 values (8,'eight')");
       //-- Check that insert was successful and trigger was fired
       rs = s.executeQuery("select * from t1");
       expectedRows = new String [][]
                        {{"5","two            "},{"6","four           "},{"8","eight          "}};
       JDBC.assertFullResultSet(rs, expectedRows);
       s.execute("drop trigger delete_trig");
       //--- Procedures with schema name
       s.execute("create trigger call_proc_in_default_schema AFTER insert on t2 for each STATEMENT call APP.proc_no_sql()");
       //--- insert 2 rows. check that trigger is fired - procedure should be called once
       s.execute("insert into t2 values (1,2), (2,4)");
       //--- check inserts are successful
       rs = s.executeQuery("select * from t2");
       JDBC.assertFullResultSet(rs, new String[][] { {"1","2"}, {"2","4"}});
       s.execute("drop trigger call_proc_in_default_schema");
       s.execute("create trigger call_proc_in_default_schema no cascade BEFORE delete on t2 for each ROW call APP.proc_no_sql()");
       //--- delete 2 rows. check that trigger is fired - procedure should be called twice
       s.execute("delete from t2");
       //--- check delete is successful
       rs = s.executeQuery("select * from t2");
      JDBC.assertEmpty(rs);
      s.execute("drop trigger call_proc_in_default_schema");
      s.execute("create trigger call_proc_in_new_schema no cascade BEFORE insert on t2 for each STATEMENT call new_schema.proc_in_new_schema()");
      //--- insert 2 rows. check that trigger is fired - procedure should be called once
      s.execute("insert into t2 values (1,2), (2,4)");
      //--- check inserts are successful
      rs = s.executeQuery("select * from t2");
      JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
      s.execute("drop trigger call_proc_in_new_schema");
      s.execute("create trigger call_proc_in_new_schema AFTER delete on t2 for each ROW call new_schema.proc_in_new_schema()");
      //--- delete 2 rows. check that trigger is fired - procedure should be called twice
      s.execute("delete from t2");
      //--- check delete is successful
      rs = s.executeQuery("select * from t2");
      JDBC.assertEmpty(rs);
      s.execute("drop trigger call_proc_in_new_schema");
      s.close();
    }

  
    /**
     * Some misc negative tests for procedures in triggers.
     * 
     * @throws SQLException
     */
    public void testTriggerNegative() throws SQLException {
        Statement s = createStatement();
        ResultSet rs;
        assertStatementError("42Y03",s,"create trigger call_non_existent_proc1 AFTER insert on t2 for each ROW call non_existent_proc()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='CALL_NON_EXISTENT_PROC1'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
          assertStatementError("42Y03",s,"create trigger call_proc_with_non_existent_proc2 AFTER insert on t2 for each ROW call new_schema.non_existent_proc()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='CALL_NON_EXISTENT_PROC2'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
          assertStatementError("42Y07",s,"create trigger call_proc_in_non_existent_schema AFTER insert on t2 for each ROW call non_existent_schema.non_existent_proc()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='CALL_PROC_IN_NON_EXISTENT_SCHEMA'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
          assertStatementError("42X50",s,"create trigger call_proc_using_non_existent_method AFTER insert on t2 for each ROW call proc_using_non_existent_method()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername as VARCHAR(128))='CALL_PROC_WITH_NON_EXISTENT_METHOD'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
          assertStatementError("42Y03",s,"create trigger call_non_existent_proc1 no cascade BEFORE insert on t2 for each ROW call non_existent_proc()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='CALL_NON_EXISTENT_PROC1'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
          assertStatementError("42Y07",s,"create trigger call_proc_in_non_existent_schema no cascade BEFORE insert on t2 for each ROW call non_existent_schema.non_existent_proc()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='CALL_PROC_IN_NON_EXISTENT_SCHEMA'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});      
          assertStatementError("42X50",s,"create trigger call_proc_using_non_existent_method no cascade BEFORE insert on t2 for each ROW call proc_using_non_existent_method()");
          rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='CALL_PROC_WITH_NON_EXISTENT_METHOD'");
          JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
          //--- triggers must not allow dynamic parameters (?)
          
          assertStatementError("42Y27",s,"create trigger update_trig AFTER insert on t1 for each STATEMENT call proc_modifies_sql_update_op(?)");
          s.execute("insert into t2 values (1,2), (2,4)");
          // --- use procedure with commit
          s.execute("create trigger commit_trig AFTER delete on t2 for each STATEMENT call commit_proc()");
          assertStatementError("38000",s,"delete from t2");
          rs = s.executeQuery("select * from t2");
          JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
          s.execute("drop trigger commit_trig");
          s.execute("create trigger commit_trig no cascade BEFORE delete on t2 for each STATEMENT call commit_proc()");
          // -- should fail
          assertStatementError("38000",s,"delete from t2");
          //--- check delete failed
          rs = s.executeQuery("select * from t2");
          JDBC.assertFullResultSet(rs,new String[][] {{"1","2"}, {"2","4"}});
          s.execute("drop trigger commit_trig");
          //--- use procedure with rollback
          s.execute("create trigger rollback_trig AFTER delete on t2 for each STATEMENT call rollback_proc()");
          assertStatementError("38000",s,"delete from t2");
          //--- check delete failed
          rs = s.executeQuery("select * from t2");
          JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
          s.execute("drop trigger rollback_trig");
          s.execute("create trigger rollback_trig no cascade BEFORE delete on t2 for each STATEMENT call rollback_proc()");
          //--- should fail
          assertStatementError("38000",s,"delete from t2");
          //--- check delete failed
          rs = s.executeQuery("select * from t2");
          JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
          s.execute("drop trigger rollback_trig");
          //--- use procedure which changes isolation level
          s.execute("create trigger set_isolation_trig AFTER delete on t2 for each STATEMENT call set_isolation_proc()");
          assertStatementError("38000",s,"delete from t2");
          //--- check delete failed
          rs = s.executeQuery("select * from t2");
          JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
          s.execute("drop trigger set_isolation_trig");
          s.execute("create trigger set_isolation_trig no cascade BEFORE delete on t2 for each STATEMENT call set_isolation_proc()");
          assertStatementError("38000",s,"delete from t2");
          //--- check delete failed
          rs = s.executeQuery("select * from t2");
          JDBC.assertFullResultSet(rs, new String[][] {{"1","2"},{"2","4"}});
          s.execute("drop trigger set_isolation_trig");
          // --- call procedure that selects from same trigger table
          s.execute("create trigger select_from_trig_table AFTER insert on t1 for each STATEMENT call proc_reads_sql(1)");
          //--- insert 2 rows check that trigger is fired - procedure should be called once
          s.execute("insert into t1 values (10, 'ten')");
          //--- check inserts are successful
          rs = s.executeQuery("select * from t1");
          String [][] expectedRows = { {"5","two"},{"6","four"},{"8","eight"},{"10","ten"}};
          JDBC.assertFullResultSet(rs,expectedRows );
          s.execute("drop trigger select_from_trig_table");
          s.execute("create trigger select_from_trig_table no cascade before delete on t1 for each STATEMENT call proc_reads_sql(1)");
          //--- delete a row. check that trigger is fired - procedure should be called once
          //RESOLVE: How to check
          s.execute("delete from t1 where i=10");
          // --- check delete is successful
          rs = s.executeQuery("select * from t1");
          expectedRows = new String[][] { {"5","two"},{"6","four"},{"8","eight"}};
          JDBC.assertFullResultSet(rs, expectedRows);
          s.execute("drop trigger select_from_trig_table");
          //--- use procedures which alter/drop trigger table and some other table
          s.execute("create trigger alter_table_trig AFTER delete on t1 for each STATEMENT call alter_table_proc()");
          assertStatementError("38000",s,"delete from t1");
          // check delete failed
          rs = s.executeQuery("select * from t1");
          expectedRows = new String[][] { {"5","two"},{"6","four"},{"8","eight"}};
          JDBC.assertFullResultSet(rs, expectedRows);
          s.execute("create trigger drop_table_trig AFTER delete on t2 for each STATEMENT call drop_table_proc()");
          // should fail
         assertStatementError("38000",s,"delete from t2");
         // check delete failed
         rs = s.executeQuery("select * from t2");
         JDBC.assertFullResultSet(rs,new String[][] {{"1","2"}, {"2","4"}});
         s.execute("drop trigger drop_table_trig");
         //--- use procedures which create/drop trigger on trigger table and some other table
         s.execute("create trigger create_trigger_trig AFTER delete on t1 for each STATEMENT call create_trigger_proc()");
         // -- should fail
         assertStatementError("38000",s,"delete from t1");
         //--- check delete failed
         rs = s.executeQuery("select * from t1");
         expectedRows = new String[][] { {"5","two"},{"6","four"},{"8","eight"}};
         JDBC.assertFullResultSet(rs, expectedRows);
         //--- check trigger is not created
         rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where  CAST(triggername AS VARCHAR(128))='TEST_TRIG'");
         JDBC.assertFullResultSet(rs, new String[][] {{"0"}});
         s.execute("drop trigger create_trigger_trig");
         //--- create a trigger to test we cannot drop it from a procedure called by a trigger
         s.execute("create trigger test_trig AFTER delete on t1 for each STATEMENT insert into  t1 values(20, 'twenty')");
         s.execute("create trigger drop_trigger_trig AFTER delete on t2 for each STATEMENT call drop_trigger_proc()");
         assertStatementError("38000",s,"delete from t2");
         //--- check delete failed
         rs = s.executeQuery("select * from t2");
         JDBC.assertFullResultSet(rs,new String[][] {{"1","2"}, {"2","4"}});
         //--- check trigger is not dropped
         rs = s.executeQuery("select count(*) from SYS.SYSTRIGGERS where CAST(triggername AS VARCHAR(128))='TEST_TRIG'");
         JDBC.assertFullResultSet(rs, new String[][] {{"1"}});
         s.execute("drop trigger drop_trigger_trig");
         //- use procedures which create/drop index on trigger table and some other table
         s.execute("create trigger create_index_trig AFTER delete on t2 for each STATEMENT call create_index_proc()");
         // -- should fail
         assertStatementError("38000",s,"delete from t2");
         // check delete failed
         rs = s.executeQuery("select * from t2");
         JDBC.assertFullResultSet(rs,new String[][] {{"1","2"}, {"2","4"}});
         // -- check index is not created
         rs = s.executeQuery("select count(*) from SYS.SYSCONGLOMERATES where CAST(CONGLOMERATENAME AS VARCHAR(128))='IX' and ISINDEX");
         JDBC.assertFullResultSet(rs, new String [][] {{"0"}});
         s.execute("drop trigger create_index_trig");
         //--- create an index to test we cannot drop it from a procedure called by a trigger
         s.execute("create index ix on t1(i,b)");
         s.execute("create trigger drop_index_trig AFTER delete on t1 for each STATEMENT call drop_index_proc()");
         assertStatementError("38000",s,"delete from t1");
         // -- check delete failed
         rs = s.executeQuery("select * from t1");
         expectedRows = new String[][] { {"5","two"},{"6","four"},{"8","eight"}};
         JDBC.assertFullResultSet(rs, expectedRows);
         // -- check index is not dropped
         rs = s.executeQuery("select count(*) from SYS.SYSCONGLOMERATES where CAST(CONGLOMERATENAME AS VARCHAR(128))='IX' and ISINDEX");
         JDBC.assertFullResultSet(rs, new String[][] {{"1"}});
         s.close();
    }
    
    
    
    private static Test basesuite() {
        Test basesuite = new TestSuite(ProcedureInTriggerTest.class);
        Test clean = new CleanDatabaseTestSetup(basesuite) {
        protected void decorateSQL(Statement s) throws SQLException {
            s.execute("create table t1 (i int primary key, b char(15))");
            s.execute("create table t2 (x integer, y integer)");
            s.execute("create procedure proc_no_sql() parameter style java language java NO SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.zeroArg'");
            s.execute("create procedure proc_contains_sql() parameter style java language java CONTAINS SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.getConnectionProc'");
            s.execute("create procedure proc_reads_sql(i integer) parameter style java language java READS SQL DATA external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.selectRows' dynamic result sets 1");
            s.execute("create procedure proc_modifies_sql_insert_op(p1 int, p2 char(10)) parameter style java language java MODIFIES SQL DATA external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.insertRow'");
            s.execute("create procedure proc_modifies_sql_update_op(p1 int) parameter style java language java  MODIFIES SQL DATA  external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.updateRow'");
            s.execute("create procedure proc_modifies_sql_delete_op(p1 int) parameter style java language java MODIFIES SQL DATA external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.deleteRow'");
            s.execute("create procedure alter_table_proc() parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.alterTable'");
            s.execute("create procedure drop_table_proc() parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.dropTable'");
            s.execute("create procedure commit_proc() parameter style java dynamic result sets 0 language java contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.doConnCommit'");
            s.execute("create procedure rollback_proc() parameter style java dynamic result sets 0 language java contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.doConnRollback'");
            s.execute("create procedure set_isolation_proc() parameter style java dynamic result sets 0 language java contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.doConnectionSetIsolation'");
            s.execute("create procedure create_index_proc() parameter style java dynamic result sets 0 language java  contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.createIndex'");
            s.execute("create procedure drop_index_proc() parameter style java dynamic result sets 0 language java contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.dropIndex'");
            s.execute("create procedure create_trigger_proc() parameter style java dynamic result sets 0 language java contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.createTrigger'");
            s.execute(" create procedure drop_trigger_proc() parameter style java dynamic result sets 0 language java contains sql external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.dropTrigger'");
            s.execute("create procedure proc_wrongly_defined_as_no_sql(p1 int, p2 char(10)) parameter style java language java NO SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.insertRow'");
            // create a new schema and put a procedure in it.
            s.execute("create schema new_schema");
            s.execute("create procedure new_schema.proc_in_new_schema() parameter style java language java NO SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.zeroArg'");
            // procedure which uses non-existant method
            s.execute("create procedure proc_using_non_existent_method() parameter style java language java NO SQL external name 'org.apache.derbyTesting.functionTests.tests.lang.ProcedureInTriggerTest.nonexistentMethod'");
       
            
        }};
        
        return clean;
        }
                
        public static Test suite() { 
            TestSuite suite = new TestSuite();
            if (!JDBC.vmSupportsJSR169()) {
                suite.addTest(basesuite());
                suite.addTest(TestConfiguration.clientServerDecorator(basesuite()));
            }
            return suite;
        }

        private void checkAndResetZeroArgCount(int count) {
            assertEquals(count, zeroArgCount);
            zeroArgCount = 0;
         }      
        
        // PROCEDURES
        private static int zeroArgCount = 0;
        public static void zeroArg() {
            zeroArgCount++;
        }
        
        private static int getConnectionProcCount = 0;
        
        private void checkAndResetGetConnectionProcCount(int count) {
            assertEquals(count, getConnectionProcCount);
            getConnectionProcCount = 0;
         }
        
        public static void getConnectionProc() throws Throwable
        {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                Statement s = conn.createStatement();
                conn.close();
                getConnectionProcCount++;
        }
        
        private static int selectRowsCount = 0;
        
        private void checkAndResetSelectRowsCount(int count) {
            assertEquals(count, selectRowsCount);
            selectRowsCount = 0;
         }
        
        public static void selectRows(int p1, ResultSet[] data) throws SQLException {

                

                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("select * from t1 where i = ?");
                ps.setInt(1, p1);
                data[0] = ps.executeQuery();
                conn.close();
                selectRowsCount++;
        }
        
        public static void selectRows(int p1, int p2, ResultSet[] data1, ResultSet[] data2) throws SQLException {

                

                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("select * from t1 where i = ?");
                ps.setInt(1, p1);
                data1[0] = ps.executeQuery();

                ps = conn.prepareStatement("select * from t1 where i >= ?");
                ps.setInt(1, p2);
                data2[0] = ps.executeQuery();

                if (p2 == 99)
                        data2[0].close();

                // return no results
                if (p2 == 199) {
                        data1[0].close();
                        data1[0] = null;
                        data2[0].close();
                        data2[0] = null;
                }

                // swap results
                if (p2 == 299) {
                        ResultSet rs = data1[0];
                        data1[0] = data2[0];
                        data2[0] = rs;
                }

                conn.close();
                selectRowsCount++;
        }

        // select all rows from a table
        public static void selectRows(String table, ResultSet[] rs)
                throws SQLException
        {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                Statement stmt = conn.createStatement();
                rs[0] = stmt.executeQuery("SELECT * FROM " + table);
                conn.close();
                selectRowsCount++;
        }


        public static void insertRow(int p1) throws SQLException {
                insertRow(p1, "int");
        }

        public static void insertRow(int p1, String p2) throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("insert into t1 values (?, ?)");
                ps.setInt(1, p1);
                ps.setString(2, p2);
                ps.executeUpdate();
                ps.close();
                conn.close();
        }
        
        public static void updateRow(int p1) throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("update t1 set i=i+?");
                ps.setInt(1, p1);
                ps.executeUpdate();
                ps.close();
                conn.close();
        }
        
        public static void deleteRow(int p1) throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("delete from t1 where i=?");
                ps.setInt(1, p1);
                ps.executeUpdate();
                ps.close();
                conn.close();
        }

        public static void alterTable() throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("alter table t1 add column test integer");
                ps.execute();
                ps.close();
                conn.close();
        }

        public static void dropTable() throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("drop table t1");
                ps.execute();
                ps.close();
                conn.close();
        }               
        public static int doConnCommitInt() throws Throwable
        {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                conn.commit();
                return 1;
        }

        public static void doConnCommit() throws Throwable
        {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                conn.commit();
        }        

        public static void doConnRollback() throws Throwable
        {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                conn.rollback();
        }

        public static void doConnectionSetIsolation() throws Throwable
        {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        
        public static void createIndex() throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("create index ix on t1(i,b)");
                ps.execute();
                ps.close();
                conn.close();
        }

        public static void dropIndex() throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("drop index ix");
                ps.execute();
                ps.close();
                conn.close();
        }
        
        public static void createTrigger() throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("create trigger test_trig" +
                                " AFTER delete on t1 for each STATEMENT insert into" +
                                " t1 values(20, 'twenty')");
                ps.execute();
                ps.close();
                conn.close();
        }
        

        public static void dropTrigger() throws SQLException {
                Connection conn = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps = conn.prepareStatement("drop trigger test_trig");
                ps.execute();
                ps.close();
                conn.close();
        }
        
}
