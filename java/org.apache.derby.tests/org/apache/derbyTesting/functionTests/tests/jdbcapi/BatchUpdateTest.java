/*

   Derby - Class 
       org.apache.derbyTesting.functionTests.tests.jdbcapi.BatchUpdateTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test BatchUpdate functionality.
 * <P>
 * This test examines the behavior fo BatchUpdate test.
 * One fixture tests creating tables in batch, the other fixtures can be grouped
 * into 5 rough categories:
 *  - tests that verify that correct usage with Statements work as expected
 *    - testEmptyStatementBatch()
 *      try executing a batch which nothing in it.
 *    - testSingleStatementBatch()
 *      try executing a batch which one statement in it.
 *    - testMultipleStatementsBatch()
 *      try executing a batch with 3 different statements in it.
 *    - test1000StatementsBatch()
 *      try executing a batch with 1000 statements in it.
 *    - testAutoCommitTrueBatch()
 *      try batch with autocommit true
 *    - testCombinationsOfClearBatch()
 *      try clear batch
 *    - testAssociatedParams()
 *      confirm associated parameters run ok with batches
 *   
 *  - tests that verify that incorrect usage with Statments give appropriate
 *    errors
 *    - testStatementWithResultSetBatch()
 *      statements which will return a resultset are not allowed in batch
 *      update. The following case should throw an exception for select.
 *      Below trying various placements of select statement in the batch,
 *      i.e. as 1st stmt, nth stmt and last stmt
 *    - testStatementNonBatchStuffInBatch()
 *      try executing a batch with regular statement intermingled.
 *    - testStatementWithErrorsBatch()
 *      Below trying various placements of overflow update statement
 *      in the batch, i.e. as 1st stmt, nth stat and last stmt
 *    - testTransactionErrorBatch()
 *      try transaction error, i.e. time out while getting the lock
 *    
 *  - tests that verify that usage with callableStatements work as expected
 *    - testCallableStatementBatch()
 *      try callable statements
 *    - testCallableStatementWithOutputParamBatch()
 *      try callable statement with output parameters
 *      
 *  - tests that verify that correct usage with preparedStatements work as
 *    expected
 *    - testEmptyValueSetPreparedBatch()
 *      try executing a batch which nothing in it.
 *    - testNoParametersPreparedBatch()
 *      try executing a batch with no parameters. 
 *    - testSingleValueSetPreparedBatch()
 *      try executing a batch which one parameter set in it.
 *    - testMultipleValueSetPreparedBatch()
 *      try executing a batch with 3 parameter sets in it.
 *    - testMultipleValueSetNullPreparedBatch()
 *      try executing a batch with 2 parameter sets in it and they are set 
 *      to null.
 *    - test1000ValueSetPreparedBatch()
 *      try executing a batch with 1000 statements in it.
 *    - testPreparedStatRollbackAndCommitCombinations()
 *      try executing batches with various rollback and commit combinations.
 *    - testAutoCommitTruePreparedStatBatch()
 *      try prepared statement batch with autocommit true
 *    - testCombinationsOfClearPreparedStatBatch()
 *      try clear batch
 *      
 *  - tests that verify that incorrect use with preparedStatements give 
 *    appropriate errors
 *    - testPreparedStmtWithResultSetBatch()
 *      statements which will return a resultset are not allowed in batch
 *      update. The following case should throw an exception for select.
 *    - testPreparedStmtNonBatchStuffInBatch();
 *      try executing a batch with regular statement intermingled.
 *    - testPreparedStmtWithErrorsBatch();
 *      trying various placements of overflow update statement
 *      in the batch
 *    - testTransactionErrorPreparedStmtBatch()
 *      try transaction error, in this particular case time out while
 *      getting the lock
 * 
 * Almost all fixtures but 1 execute with embedded and 
 * NetworkServer/DerbyNetClient - however, there is a difference in 
 * functionality between the two when an error condition is reaches. Thus,
 * the negative tests have if / else if blocks for embedded and client.
 * 
 * The 1 fixture that ise not running with network server is 
 * identified with //TODO: tags and has an if (usingEmbedded()) block and
 * a JIRA issue attached to it.
 * 
 */

public class BatchUpdateTest extends BaseJDBCTestCase {
	
    /** Creates a new instance of BatchUpdateTest */
    public BatchUpdateTest(String name) {
        super(name);
    }

    /**
     * Set up the conection to the database.
     *  This is itself a test of statements creating tables in batch. 
     */
    public void setUp() throws  Exception {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        s.execute("delete from t1");
        s.close();
        commit();
    }
    
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("BatchUpdateTest");
        suite.addTest(baseSuite("BatchUpdateTest:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("BatchUpdateTest:client")));
        return suite;
    }
    
    
    /**
     * embeddedSuite runs tests only in embedded mode. 
     * Used by CollationTest
     * @return embedded Test suite
     */
    public static Test embeddedSuite() {
        
        BaseTestSuite suite = new BaseTestSuite("BatchUpdateTest");
        suite.addTest(baseSuite("BatchUpdateTest:embedded"));
        return suite;
    }
    
    protected static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(BatchUpdateTest.class);
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4)) 
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                stmt.execute("create table t1(c1 int)");
                // for fixture testCallableStatementBatch
                stmt.execute("create table datetab(c1 date)");
                stmt.execute("create table timetab(c1 time)");
                stmt.execute("create table timestamptab(c1 timestamp)");
                stmt.execute("create table usertypetab(c1 DATE)");
                // for fixture testAssociatedParams
                stmt.execute("create table assoc" +
                    "(x char(10) not null primary key, y char(100))");
                stmt.execute("create table assocout(x char(10))");
            }
        };
    } 
    
    /* 
     * helper method to check each count in the return array of a BatchUpdateException
     */
    public  static   void assertBatchUpdateCounts
        ( long[] expectedBatchResult, BatchUpdateException bue )
    {
        assertBatchUpdateCounts( squashLongs( expectedBatchResult ), bue.getUpdateCounts() );
        
        if (JDBC.vmSupportsJDBC42())
        {
            BatchUpdateExceptionWrapper wrapper = new BatchUpdateExceptionWrapper( bue );

            assertEquals( expectedBatchResult, wrapper.getLargeUpdateCounts() );
        }
    }
   
    /** Squash an array of longs into an array of ints */
    public static  int[]   squashLongs( long[] longs )
    {
        int count = (longs == null) ? 0 : longs.length;
        int[]   ints = new int[ count ];
        for ( int i = 0; i < count; i++ ) { ints[ i ] = (int) longs[ i ]; }

        return ints;
    }
    
    /* 
     * helper method to check each count in the return array of batchExecute
     */
    private static void assertBatchUpdateCounts( 
        int[] expectedBatchResult, int[] executeBatchResult )
    {
        assertEquals("length of array should be identical", 
            expectedBatchResult.length, executeBatchResult.length);
        
        for (int i=0; i<expectedBatchResult.length; i++)
        {
            String msg = "mismatch for array index [" + i + "] ; ";
            assertEquals(msg,expectedBatchResult[i],executeBatchResult[i]);
            println("expectedUpdate result #" + i + " : " +
                expectedBatchResult[i]);
            println("actual result #" + i + " : " + executeBatchResult[i]);
        }
    }
    
    /** 
     * helper method to evaluate negative tests where we expect a 
     * batchExecuteException to be returned.
     * @exception SQLException     Thrown if the expected error occurs
     *                             We expect a BatchUpdateException, and
     *                             verify it is so.
     *
     * @param expectedError The sqlstate to look for.
     * @param stmt The Statement that contains the Batch to
     *                             be executed.
     * @param expectedUpdateCount The expectedUpdateCount array.
     */
    public static void assertBatchExecuteError( 
        String expectedError,
        Statement stmt,
        long[] expectedUpdateCount) 
    throws SQLException 
    { 
       try {
            stmt.executeBatch();
            fail("Expected batchExecute to fail");
        } catch (BatchUpdateException bue) {
            assertSQLState(expectedError, bue);
            assertBatchUpdateCounts(expectedUpdateCount, bue);
        } 
    }
    
    /* Fixture that verifies tables can be created in batch */
    public void testMinimalDDLInBatch() throws SQLException {
        
        Statement stmt = createStatement();
        stmt.addBatch("create table ddltsttable1(c1 int)");
        stmt.addBatch("create procedure ddlinteg() language java " +
            "parameter style java external name 'java.lang.Integer'");
        stmt.addBatch("create table ddltable2(c1 date)");
        int expectedCount[] = {0,0,0};
        assertBatchUpdateCounts(expectedCount, stmt.executeBatch());
        ResultSet rs = stmt.executeQuery(
            "select count(*) from SYS.SYSTABLES where CAST(tablename AS VARCHAR(128)) like 'DDL%'");
        JDBC.assertSingleValueResultSet(rs, "2");
        rs = stmt.executeQuery(
            "select count(*) from SYS.SYSALIASES where CAST(alias AS VARCHAR(128)) like 'DDL%'");
        JDBC.assertSingleValueResultSet(rs, "1");
        stmt.close();

        commit();
    }

    
    /* Fixtures that test correct usage of batch handling with Statements */
    
    // try executing a batch which nothing in it. Should work.
    public void testEmptyStatementBatch() throws SQLException {
        Statement stmt = createStatement();
 
        // try executing a batch which nothing in it. Should work.
        println("Positive Statement: clear the batch and run the empty batch");
        stmt.clearBatch();
        assertBatchUpdateCounts(new int[0], stmt.executeBatch());

        stmt.close();
        commit();
    }

    // try executing a batch which single statement in it. Should work.
    public void testSingleStatementBatch() throws SQLException {

        Statement stmt = createStatement();
        println("Positive Statement: testing 1 statement batch");
        stmt.addBatch("insert into t1 values(2)");

        assertBatchUpdateCounts(new int[] {1}, stmt.executeBatch());
        stmt.close();
        commit();
    }
    
    // try executing a batch with 3 different statements in it.
    public void testMultipleStatementsBatch() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs;

        println("Positive Statement: testing 2 inserts and 1 update batch");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("update t1 set c1=4");
        stmt.addBatch("insert into t1 values(3)");

        assertBatchUpdateCounts(new int[] {1,1,1}, stmt.executeBatch());
        
        rs = stmt.executeQuery("select count(*) from t1 where c1=2");
        rs.next();
        assertEquals("expect 0 rows with c1 = 2", 0, rs.getInt(1));
        rs.close();

        rs = stmt.executeQuery("select count(*) from t1 where c1=4");
        rs.next();
        assertEquals("expect 1 row with c1 = 4", 1, rs.getInt(1));
        rs.close();

        rs = stmt.executeQuery("select count(*) from t1 where c1=3");
        rs.next();
        assertEquals("expect 1 row with c1 = 3", 1, rs.getInt(1));
        rs.close();

        assertTableRowCount("T1", 2);
        
        stmt.close();

        commit();
    }

    /**
     * Regression test case for DERBY-6373.
     */
    public void testMultipleStatementsBatchWithWarnings() throws SQLException {
        Statement s = createStatement();
        s.execute("insert into t1 values 1");

        // Execute a batch of three deletes. All of them should get a warning
        // because no rows matched the WHERE clause.
        s.addBatch("delete from t1 where c1 in (select 0 from t1)");
        s.addBatch("delete from t1 where c1 in (select 0 from t1)");
        s.addBatch("delete from t1 where c1 in (select 0 from t1)");
        s.executeBatch();

        // Used to fail with NullPointerException on the client.
        SQLWarning w = s.getWarnings();

        // Expect one warning per delete on the client. Embedded gives only
        // a single warning.
        assertSQLState("02000", w);
        w = w.getNextWarning();
        if (usingEmbedded()) {
            assertNull(w);
        } else {
            assertSQLState("02000", w);
            w = w.getNextWarning();
            assertSQLState("02000", w);
            w = w.getNextWarning();
            assertNull(w);
        }
    }

    // try executing a batch with 1000 statements in it.
    public void test1000StatementsBatch() throws SQLException {
        int updateCount[];

        Statement stmt = createStatement();

        println("Positive Statement: 1000 statements batch");
        for (int i=0; i<1000; i++){
            stmt.addBatch("insert into t1 values(1)");
        }
        updateCount = stmt.executeBatch();
        
        int[] expectedUpdateCount = new int[1000];
        Arrays.fill(expectedUpdateCount, 1);
        assertBatchUpdateCounts(expectedUpdateCount, updateCount);
        
        assertTableRowCount("T1", 1000);

        stmt.close();

        commit();
    }

    // try batch with autocommit true
    public void testAutoCommitTrueBatch() throws SQLException {

        getConnection().setAutoCommit(true);    
        Statement stmt = createStatement();

        // try batch with autocommit true
        println("Positive Statement: stmt testing with autocommit true");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("delete from t1");
        assertBatchUpdateCounts(new int[] {1,1,2}, stmt.executeBatch());

        assertTableRowCount("T1", 0);
        
        stmt.close();
    }

    //  try combinations of clear batch.
    public void testCombinationsOfClearBatch() throws SQLException {

        Statement stmt = createStatement();

        println("Positive Statement: add 3 statements, clear and execute batch");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.clearBatch();

        // Batch should be cleared, there should be no update count
        assertBatchUpdateCounts(new int[0], stmt.executeBatch());
        assertTableRowCount("T1", 0);

        println("Positive Statement: add 3 statements, clear batch, " +
            "add 3 more statements and execute batch");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.clearBatch();
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");

        assertBatchUpdateCounts(new int[] {1,1,1}, stmt.executeBatch());
        assertTableRowCount("T1", 3);

        stmt.close();
        commit();
    }

    /*
     ** Associated parameters are extra parameters that are created
     ** and associated with the root parameter (the user one) to
     ** improve the performance of like.       For something like
     ** where c1 like ?, we generate extra 'associated' parameters 
     ** that we use for predicates that we give to the access
     ** manager. 
     */
    public void testAssociatedParams() throws SQLException 
    {

        Statement stmt = createStatement();
        int i;
        println("Positive Statement: testing associated parameters");
        PreparedStatement checkps = prepareStatement(
            "select x from assocout order by x");
        PreparedStatement ps = prepareStatement(
            "insert into assoc values (?, 'hello')");
        for ( i = 10; i < 60; i++)
        {
            ps.setString(1, Integer.toString(i));
            ps.executeUpdate();     
        }
        ps.close();

        ps = prepareStatement(
            "insert into assocout select x from assoc where x like ?");
        ps.setString(1, "33%");
        ps.addBatch();
        ps.setString(1, "21%");
        ps.addBatch();
        ps.setString(1, "49%");
        ps.addBatch();
        
        assertBatchUpdateCounts(new int[] {1,1,1}, ps.executeBatch());
        ps.close();
        checkps.execute();
        ResultSet rs = checkps.getResultSet();
        JDBC.assertFullResultSet(
            rs, new String[][] {{"21"},{"33"},{"49"}}, true);
                
        stmt.executeUpdate("delete from assocout");

        ps = prepareStatement(
                "insert into assocout select x from assoc where x like ?");
        ps.setString(1, "3%");
        ps.addBatch(); // expectedCount 10: values 10-19
        ps.setString(1, "2%");
        ps.addBatch(); // values 20-29
        ps.setString(1, "1%");
        ps.addBatch(); // values 30-39

        // set up expected values for check
        String expectedStrArray[][] = new String[30][1];
        for (i=10 ; i < 40 ; i++)
        {
            expectedStrArray[i-10][0] = String.valueOf(i);
        }
   
        assertBatchUpdateCounts( new int[] {10,10,10}, ps.executeBatch());
        ps.close();
        checkps.execute();
        rs = checkps.getResultSet();
        JDBC.assertFullResultSet(rs, expectedStrArray, true);
                
        stmt.executeUpdate("delete from assocout");
        ps = prepareStatement(
            "insert into assocout select x from assoc where x like ?");
        ps.setString(1, "%");// values 10-59
        ps.addBatch();
        ps.setString(1, "666666");
        ps.addBatch();
        ps.setString(1, "%");// values 10-59
        ps.addBatch();
        
        // set up expected values for check
        String expectedStrArray2[][] = new String[100][1];
        int j = 0;
        for (i=10 ; i < 60 ; i++)
        {  
            for (int twice = 0; twice < 2; twice++)
            {
                expectedStrArray2[j][0] = String.valueOf(i);
                j++;
            }
        }
        
        assertBatchUpdateCounts (new int[] {50,0,50}, ps.executeBatch());
        ps.close();
        checkps.execute();
        rs = checkps.getResultSet();
        JDBC.assertFullResultSet(rs, expectedStrArray2, true);
        checkps.close();
        stmt.close();
    }

    /* Fixtures that test incorrect batch usage with Statements */

    // statements which will return a resultset are not allowed in batch
    // update. The following case should throw an exception for select. 
    // Below trying various placements of select statement in the batch,
    // i.e. as 1st stmt, nth stat and last stmt
    public void testStatementWithResultSetBatch() throws SQLException {
        
        Statement stmt = createStatement();

        // trying select as the first statement
        println("Negative Statement: statement testing select as first " +
            "statement in the batch");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        stmt.addBatch("insert into t1 values(1)");
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            assertBatchExecuteError("X0Y79", stmt, new long[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new long[] {-3, 1});
        
        assertTableRowCount("T1",
                usingEmbedded() ? 0 : 1);
        
        // trying select as the nth statement
        println("Negative Statement: " +
            "statement testing select as nth stat in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        stmt.addBatch("insert into t1 values(1)");
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            assertBatchExecuteError("X0Y79", stmt, new long[] {1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new long[] {1,-3,1});
            
        assertTableRowCount("T1",
                usingEmbedded() ? 1 : 3);

        // trying select as the last statement
        println("Negative Statement: statement testing select" +
            " as last stat in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            assertBatchExecuteError("X0Y79", stmt, new long[] {1,1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new long[] {1,1,-3});

        assertTableRowCount("T1",
                usingEmbedded() ? 3 : 5);

        rollback();

        assertTableRowCount("T1", 0);

        stmt.close();

        commit();
    }
    
    // try executing a batch with regular statement intermingled.
    public void testStatementNonBatchStuffInBatch() throws SQLException {
        
        Statement stmt = createStatement();
        int[] updateCount=null;

        // trying execute after addBatch
        println("Negative Statement:" +
            " statement testing execute in the middle of batch");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        /* Check to be sure the exception is the MIDDLE_OF_BATCH */
        /* assertStatementError will do the execute() */
        if (usingEmbedded())
            assertStatementError("XJ068",stmt,"insert into t1 values(1)");
        else if (usingDerbyNetClient())
        {
            stmt.addBatch("insert into t1 values(1)"); 
            assertBatchExecuteError("XJ208",stmt, new long[] {-3,1});           
            // pull level with embedded situation
            rollback();
        }
        // do clearBatch so we can proceed
        stmt.clearBatch();

        assertTableRowCount("T1", 0);

        // trying executeQuery after addBatch
        println("Negative Statement: " +
            "statement testing executeQuery in the middle of batch");
        stmt.addBatch("insert into t1 values(1)");
        if (usingEmbedded())
        {
            try
            {
                stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");
                fail("Expected executeQuerywith embedded");
            } catch (SQLException sqle) {
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
                // do clearBatch so we can proceed
                stmt.clearBatch();
            }
        }
        else if (usingDerbyNetClient())
        {
            stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");
            updateCount = stmt.executeBatch();
            assertBatchUpdateCounts(new int[] {1}, updateCount);
            // set to same spot as embedded
            rollback();
        }

        assertTableRowCount("T1", 0);

        println("Negative Statement: " +
            "statement testing executeUpdate in the middle of batch");
        // trying executeUpdate after addBatch
        println("Negative Statement: " +
        "statement testing executeUpdate in the middle of batch");
        stmt.addBatch("insert into t1 values(1)");
        try
        {
            stmt.executeUpdate("insert into t1 values(1)");
            stmt.addBatch("insert into t1 values(1)");
            stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
            if (usingDerbyNetClient())
            {
                assertBatchExecuteError("XJ208", stmt, new long[] {1,1,-3});
            }
            else if (usingEmbedded())
            {
                updateCount = stmt.executeBatch();
                fail("Expected executeBatch to fail");
            }
        } catch (SQLException sqle) {
            /* Check to be sure the exception is the MIDDLE_OF_BATCH */
            if (usingEmbedded())
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XJ208", sqle);
            
            stmt.clearBatch();
        }
        
        assertTableRowCount("T1",
                usingEmbedded() ? 0 : 3);

        rollback();

        assertTableRowCount("T1", 0);
        stmt.close();

        commit();
    }
    
    // Below trying various placements of overflow update statement in the 
    // batch, i.e. as 1st stmt, nth stmt and last stmt
    public void testStatementWithErrorsBatch() throws SQLException {
        
        Statement stmt = createStatement();

        stmt.executeUpdate("insert into t1 values(1)");

        // trying update as the first statement
        println("Negative Statement: statement testing overflow error" +
            " as first statement in the batch");
        stmt.addBatch("update t1 set c1=2147483647 + 1");
        stmt.addBatch("insert into t1 values(1)");
        /* Check to be sure the exception is the one we expect */
        /* Overflow is first stmt in the batch, so expect no update count */
        if (usingEmbedded())
            assertBatchExecuteError("22003", stmt, new long[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new long[] {-3,1});

        assertTableRowCount("T1",
                usingEmbedded() ? 1 : 2);

        // trying update as the nth statement
        println("Negative Statement: statement testing overflow error" +
            " as nth statement in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("update t1 set c1=2147483647 + 1");
        stmt.addBatch("insert into t1 values(1)");
        /* Check to be sure the exception is the one we expect */
        /* Update is second statement in the batch, expect 1 update count */
        if (usingEmbedded())
            assertBatchExecuteError("22003", stmt, new long[] {1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new long[] {1,-3,1});

        assertTableRowCount("T1",
                usingEmbedded() ? 2 : 4);

        // trying select as the last statement
        println("Negative Statement: statement testing overflow error" +
            " as last stat in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("update t1 set c1=2147483647 + 1");
        /* Check to be sure the exception is the one we expect */
        /* Update is last statement in the batch, expect 2 update counts */
        if (usingEmbedded())
            assertBatchExecuteError("22003", stmt, new long[] {1,1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new long[] {1,1,-3});

        assertTableRowCount("T1",
                usingEmbedded() ? 4 : 6);
        stmt.close();

        commit();
    }
    
    // try transaction error, in this particular case time out while
    // getting the lock
    public void testTransactionErrorBatch() throws SQLException {

        // conn is just default connection
        Connection conn = getConnection();
        Connection conn2 = openDefaultConnection();
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);        
        Statement stmt = conn.createStatement();
        Statement stmt2 = conn2.createStatement();
        
        int[] updateCount = null;

        println("Negative Statement: statement testing time out" +
            " while getting the lock in the batch");

        stmt.execute("insert into t1 values(1)");
        stmt2.execute("insert into t1 values(2)");

        stmt.addBatch("update t1 set c1=3 where c1=2");
        stmt2.addBatch("update t1 set c1=4 where c1=1");

        try
        {
            stmt.executeBatch();
            fail ("Batch is expected to fail");
            updateCount = stmt2.executeBatch();
        } catch (BatchUpdateException bue) {
            /* Ensure the exception is time out while getting lock */
            if (usingEmbedded())
                assertSQLState("40XL1", bue);
            else if (usingDerbyNetClient())
                assertSQLState("XJ208", bue);
            updateCount = ((BatchUpdateException)bue).getUpdateCounts();
            if (updateCount != null) {
                if (usingEmbedded())
                    assertEquals("first statement in the batch caused time out" +
                        " while getting the lock, there should be no update count", 
                        0, updateCount.length);
                else if (usingDerbyNetClient())
                    /* first statement in the batch caused time out while getting
                     *  the lock, there should be 1 update count of -3 */
                    assertBatchUpdateCounts(new int[] {-3}, updateCount);
            }
        }
        conn.rollback();
        conn2.rollback();
        stmt.clearBatch();
        stmt2.clearBatch();
        stmt.close();
        stmt2.close();
        commit();
        conn2.close();
    }
    
    /* Fixtures that test batch updates with CallableStatements */

    // try callable statements
    public void testCallableStatementBatch() throws SQLException {

        println("Positive Callable Statement: " +
            "statement testing callable statement batch");
        CallableStatement cs = prepareCall("insert into t1 values(?)");

        cs.setInt(1, 1);
        cs.addBatch();
        cs.setInt(1,2);
        cs.addBatch();
        executeBatchCallableStatement(cs);

        cleanUpCallableStatement(cs, "t1");

        /* For 'beetle' bug 2813 - setDate/setTime/setTimestamp
         * calls on callableStatement throws ClassNotFoundException 
         * verify setXXXX() works with Date, Time and Timestamp 
         * on CallableStatement.
         */
        cs = prepareCall("insert into datetab values(?)");

        cs.setDate(1, Date.valueOf("1990-05-05"));
        cs.addBatch();
        cs.setDate(1,Date.valueOf("1990-06-06"));
        cs.addBatch();

        executeBatchCallableStatement(cs);

        cleanUpCallableStatement(cs, "datetab");

        cs = prepareCall("insert into timetab values(?)");

        cs.setTime(1, Time.valueOf("11:11:11"));
        cs.addBatch();
        cs.setTime(1, Time.valueOf("12:12:12"));
        cs.addBatch();
        executeBatchCallableStatement(cs);

        cleanUpCallableStatement(cs, "timestamptab");

        cs = prepareCall("insert into timestamptab values(?)");

        cs.setTimestamp(1, Timestamp.valueOf("1990-05-05 11:11:11.1"));
        cs.addBatch();
        cs.setTimestamp(1, Timestamp.valueOf("1992-07-07 12:12:12.2"));
        cs.addBatch();
        executeBatchCallableStatement(cs);

        cleanUpCallableStatement(cs, "timestamptab");

        // Try with a user type
        cs = prepareCall("insert into usertypetab values(?)");

        cs.setObject(1, Date.valueOf("1990-05-05"));
        cs.addBatch();
        cs.setObject(1,Date.valueOf("1990-06-06"));
        cs.addBatch();
        executeBatchCallableStatement(cs);

        cleanUpCallableStatement(cs, "usertypetab");
    }
    
    // helper method to testCallableStatementBatch 
    // executes and evaluates callable statement
    private static void executeBatchCallableStatement(CallableStatement cs)
    throws SQLException
    {
        int updateCount[];

        updateCount = cs.executeBatch();
        assertEquals("there were 2 statements in the batch", 
            2, updateCount.length);
        for (int i=0; i<updateCount.length; i++) 
        {
            assertEquals("update count should be 1", 1, updateCount[i]);
        }
    }

    // helper method to testCallableStatementBatch - 
    // removes all rows from table
    protected void cleanUpCallableStatement(
        CallableStatement cs, String tableName)
    throws SQLException
    {
        cs.close();
        rollback();
        cs = prepareCall("delete from " + tableName);
        cs.executeUpdate();
        cs.close();
        commit();
    }
    
    // try callable statements with output parameters
    public void testCallableStatementWithOutputParamBatch() 
    throws SQLException {

        println("Negative Callable Statement: " +
            "callable statement with output parameters in the batch");
        Statement s = createStatement();

        s.execute("CREATE PROCEDURE " +
            "takesString(OUT P1 VARCHAR(40), IN P2 INT) " +
            "EXTERNAL NAME '" + this.getClass().getName() + ".takesString'" +
        " NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");

        CallableStatement cs = prepareCall("call takesString(?,?)");
        cs.registerOutParameter(1, Types.CHAR);
        cs.setInt(2, Types.INTEGER);
        try
        {
            cs.addBatch();
            if (usingEmbedded())
                fail("Expected to see error XJ04C");
            else if (usingDerbyNetClient()) {
                executeBatchCallableStatement(cs);       
            }
        }
        catch (SQLException sqle)
        {
            // Check to be sure the exception is callback related
            assertSQLState("XJ04C", sqle);
        }

        cs.close();
        s.execute("drop procedure takesString");
        s.close();
        rollback();
    }
    
    // helper method to be used as procedure in test 
    // testCallableStatementWithOutputParamBatch
    public static void takesString(String[] outparam, int type) 
    throws Throwable
    {
        // method is stripped from takesString in jdbcapi/outparams.java
        outparam[0] = "3";
    }

    /* Fixtures that test correct usage with PreparedStatements */    
    
    // try executing a batch which nothing in it. Should work.
    public void testEmptyValueSetPreparedBatch() throws SQLException {
        
        // try executing a batch which nothing in it. Should work.
        println("Positive Prepared Stat: " +
            "set no parameter values and run the batch");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");

        assertBatchUpdateCounts(new int[] {}, pStmt.executeBatch());

        pStmt.close();
        commit();
    }
    
    // try prepared statement batch without settable parameters.
    public void testNoParametersPreparedBatch() throws SQLException {

        // Note: also tests for fix of NPE of DERBY-2112
     
        Statement stmt = createStatement();
        ResultSet rs;

        println("Positive Prepared Stat: no settable parameters");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(5)");
        pStmt.addBatch();
        pStmt.addBatch();
        pStmt.addBatch();
        /* 3 parameters were set in the batch, update count length
         *  should be 3 */
        assertBatchUpdateCounts(new int[] {1,1,1}, pStmt.executeBatch());

        pStmt.close();
        rs = stmt.executeQuery("select count(*) from t1 where c1=5");
        rs.next();
        assertEquals("There should be 3 rows with c1 = 5", 3, rs.getInt(1));
        rs.close();

        assertTableRowCount("T1", 3);
        stmt.close();

        commit();
    }
    
    // try prepared statement batch with just one set of values.
    public void testSingleValueSetPreparedBatch() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs;

        // try prepared statement batch with just one set of values
        println("Positive Prepared Stat: " +
            "set one set of parameter values and run the batch");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* 1 parameter was set in batch, update count length should be 1 */
        assertBatchUpdateCounts(new int[] {1}, pStmt.executeBatch());

        pStmt.close();
        rs = stmt.executeQuery("select count(*) from t1 where c1=1");
        rs.next();
        assertEquals("There should be 1 row with c1=1", 1, rs.getInt(1));
        
        rs.close();

        assertTableRowCount("T1", 1);

        stmt.close();

        commit();
    }
    
    // try executing a batch with 3 different parameter sets in it.
    public void testMultipleValueSetPreparedBatch() throws SQLException {

        // try prepared statement batch with just one set of values
        println("Positive Prepared Stat: " +
            "set 3 set of parameter values and run the batch");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 2);
        pStmt.addBatch();
        pStmt.setInt(1, 3);
        pStmt.addBatch();
        /* 3 parameters were set , update count length should be 3 */
        assertBatchUpdateCounts(new int[] {1,1,1}, pStmt.executeBatch());

        pStmt.close();
        
        assertTableRowCount("T1", 3);

        commit();
    }
    
    // try prepared statement batch with just 2 set of values 
    // and there value is null. 
    // tests fix for 'beetle' bug 4002: Execute batch for
    // preparedStatement gives nullPointerException
    public void testMultipleValueSetNullPreparedBatch() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs;

        // try prepared statement batch with just one set of values
        println("Positive Prepared Stat: " +
            "set one set of parameter values to null and run the batch");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        pStmt.setNull(1, Types.INTEGER);
        pStmt.addBatch();
        pStmt.setNull(1, Types.INTEGER);
        pStmt.addBatch();
        /* 2 parameters were set in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        pStmt.close();
        rs = stmt.executeQuery("select count(*) from t1 where c1 is null");
        rs.next();
        assertEquals("There should be 2 rows with c1 is null",
            2, rs.getInt(1));
        rs.close();

        assertTableRowCount("T1", 2);

        stmt.close();

        commit();
    }
    
    // try executing a batch with 1000 statements in it.
    public void test1000ValueSetPreparedBatch() throws SQLException {
        
        int updateCount[];

        println("Positive Prepared Stat: 1000 parameter set batch");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        for (int i=0; i<1000; i++){
            pStmt.setInt(1, 1);
            pStmt.addBatch();
        }
        updateCount = pStmt.executeBatch();

        int[] expectedUpdateCount = new int[1000];
        Arrays.fill(expectedUpdateCount, 1);
        assertBatchUpdateCounts(expectedUpdateCount, updateCount);
        
        assertTableRowCount("T1", 1000);

        pStmt.close();
        commit();
    }

    // try executing batches with various rollback and commit combinations.
    public void testPreparedStatRollbackAndCommitCombinations() 
    throws SQLException {

        println("Positive Prepared Stat: batch, rollback," +
            " batch and commit combinations");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        rollback();

        assertTableRowCount("T1", 0);

        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        commit();
        
        assertTableRowCount("T1", 2);

        // try batch and commit
        println("Positive Prepared Stat: batch and commit combinations");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        commit();

        assertTableRowCount("T1", 4);

        // try batch, batch and rollback
        println("Positive Prepared Stat: batch, " +
            "batch and rollback combinations");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        rollback();

        assertTableRowCount("T1", 4);

        // try batch, batch and commit
        println("Positive Prepared Stat: " +
            "batch, batch and commit combinations");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        commit();
        
        assertTableRowCount("T1", 8);

        pStmt.close();

        commit();
    }

    // try prepared statement batch with autocommit true
    public void testAutoCommitTruePreparedStatBatch() throws SQLException {

        getConnection().setAutoCommit(true);    

        // prepared statement batch with autocommit true
        println("Positive Prepared Stat: testing batch with autocommit true");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 3 statements in the batch, 
         * update count length should be 3 */
        assertBatchUpdateCounts(new int[] {1,1,1}, pStmt.executeBatch());

        assertTableRowCount("T1", 3);

        pStmt.close();       
    }
    
    // try combinations of clear batch.
    public void testCombinationsOfClearPreparedStatBatch() 
    throws SQLException {

        int updateCount[];

        println("Positive Prepared Stat: add 3 statements, " +
            "clear batch and execute batch");
        PreparedStatement pStmt = 
            prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 2);
        pStmt.addBatch();
        pStmt.setInt(1, 3);
        pStmt.addBatch();
        pStmt.clearBatch();
        /* there were 0 statements in the batch, 
         * update count length should be 0 */
        assertBatchUpdateCounts(new int[] {}, pStmt.executeBatch());

        println("Positive Prepared Stat: " +
            "add 3 statements, clear batch, add 3 and execute batch");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 2);
        pStmt.addBatch();
        pStmt.setInt(1, 3);
        pStmt.addBatch();
        pStmt.clearBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 2);
        pStmt.addBatch();
        pStmt.setInt(1, 3);
        pStmt.addBatch();

        assertBatchUpdateCounts(new int[] {1,1,1}, pStmt.executeBatch());
        
        assertTableRowCount("T1", 3);

        pStmt.close();

        commit();
    }
    
    /* Fixtures that test incorrect usage with PreparedStatements */
    
    // statements which will return a resultset are not allowed in
    // batch Updates. Following case should throw an exception for select.
    public void testPreparedStmtWithResultSetBatch() throws SQLException {


        println("Negative Prepared Stat: testing select in the batch");
        PreparedStatement pStmt = 
            prepareStatement("select * from t1 where c1=?");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            /* "Select is first statement in the batch, 
             * so there should not be any update counts */
            assertBatchExecuteError("X0Y79", pStmt, new long[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ117", pStmt, new long[] {-3});
        pStmt.close();

        assertTableRowCount("T1", 0);

        commit();
    }
    
    // try executing a batch with regular statement intermingled.
    public void testPreparedStmtNonBatchStuffInBatch() throws SQLException {
        
        int updateCount[] = null;

        // trying execute in the middle of batch
        println("Negative Prepared Stat: " +
            "testing execute in the middle of batch");
        PreparedStatement pStmt = 
            prepareStatement("select * from t1 where c1=?");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        try
        {
            pStmt.execute();
            if (usingEmbedded())
                fail("Expected executeBatch to fail");
            else if (usingDerbyNetClient())
                updateCount = pStmt.executeBatch();
        } catch (SQLException sqle) {
            if (usingEmbedded())
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XJ117", sqle);
        }
        pStmt.close();
        
        
        assertTableRowCount("T1", 0);

        // trying executeQuery in the middle of batch
        println("Negative Prepared Statement: " +
            "testing executeQuery in the middle of batch");
        pStmt = 
            prepareStatement("select * from t1 where c1=?");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        try
        {
            pStmt.executeQuery();
            if (usingEmbedded())
                fail("Expected executeBatch to fail");
            else if (usingDerbyNetClient())
                updateCount = pStmt.executeBatch();
        } catch (SQLException sqle) {
            if (usingEmbedded())
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XJ117", sqle);
        }
        pStmt.close();

        assertTableRowCount("T1", 0);

        //  trying executeUpdate in the middle of batch
        println("Negative Prepared Stat: " +
            "testing executeUpdate in the middle of batch");
        pStmt = 
            prepareStatement("select * from t1 where c1=?");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        try
        {
            pStmt.executeUpdate();
            if (usingEmbedded())
                fail("Expected executeBatch to fail");
            else if (usingDerbyNetClient())
                updateCount = pStmt.executeBatch();
        } catch (SQLException sqle) {
            if (usingEmbedded())
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("X0Y79", sqle);
        }
        pStmt.close();

        assertTableRowCount("T1", 0);

        commit();
    }
    
    // Below trying placements of overflow update statement in the batch
    public void testPreparedStmtWithErrorsBatch() throws SQLException {

        Statement stmt = createStatement();

        PreparedStatement pStmt = null;

        stmt.executeUpdate("insert into t1 values(1)");

        println("Negative Prepared Stat: " +
            "testing overflow as first set of values");
        pStmt = prepareStatement("update t1 set c1=(? + 1)");
        pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
        pStmt.addBatch();
        if (usingEmbedded())
            /* Check to be sure the exception is the one we expect */
            /* Overflow is first statement in the batch, 
             * so there should not be any update count */
            assertBatchExecuteError("22003", pStmt, new long[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", pStmt, new long[] {-3});
        pStmt.close();

        assertTableRowCount("T1", 1);

        println("Negative Prepared Stat: " +
            "testing overflow as nth set of values");
        pStmt = prepareStatement("update t1 set c1=(? + 1)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        if (usingEmbedded())
            /* Check to be sure the exception is the one we expect */
            /* Overflow is second statement in the batch, 
             * so there should be only 1 update count */
            assertBatchExecuteError("22003", pStmt, new long[] {1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", pStmt, new long[] {1,-3,1});
        pStmt.close();

        assertTableRowCount("T1", 1);

        // trying select as the last statement
        println("Negative Prepared Stat: " +
            "testing overflow as last set of values");
        pStmt = prepareStatement("update t1 set c1=(? + 1)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
        pStmt.addBatch();
        if (usingEmbedded())
            /* Check to be sure the exception is the one we expect */
            /* Overflow is last statement in the batch, 
             * so there should be 2 update counts */
            assertBatchExecuteError("22003", pStmt, new long[] {1,1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", pStmt, new long[] {1,1,-3});

        assertTableRowCount("T1", 1);
        
        pStmt.close();
        stmt.close();

        commit();
    }
    
    // try transaction error, in this particular case 
    // time out while getting the lock
    public void testTransactionErrorPreparedStmtBatch() throws SQLException {

        Connection conn = getConnection();
        Connection conn2 = openDefaultConnection();
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);        
        Statement stmt = createStatement();
        Statement stmt2 = conn2.createStatement();

        int updateCount[] = null;

        println("Negative Prepared Statement: " +
            "testing transaction error, time out while getting the lock");

        stmt.execute("insert into t1 values(1)");
        stmt2.execute("insert into t1 values(2)");

        PreparedStatement pStmt1 = 
            prepareStatement("update t1 set c1=3 where c1=?");
        pStmt1.setInt(1, 2);
        pStmt1.addBatch();

        PreparedStatement pStmt2 = 
            conn.prepareStatement("update t1 set c1=4 where c1=?");
        pStmt2.setInt(1, 1);
        pStmt2.addBatch();

        try
        {
            pStmt1.executeBatch();
            updateCount = pStmt2.executeBatch();
            fail ("Batch is expected to fail");
        } catch (BatchUpdateException bue) {
            /* Check that the exception is time out while 
             * getting the lock */
            if (usingEmbedded())
                assertSQLState("40XL1", bue);
            else if (usingDerbyNetClient())
                assertSQLState("XJ208", bue);
            updateCount = ((BatchUpdateException)bue).getUpdateCounts();
            if (updateCount != null) {
                if (usingEmbedded())
                    assertEquals("first statement in the batch caused time out" +
                        " while getting the lock, there should be no update count", 
                        0, updateCount.length);
                else if (usingDerbyNetClient())
                    /* first statement in the batch caused time out while getting
                     *  the lock, there should be 1 update count of -3 */
                    assertBatchUpdateCounts(new int[] {-3}, updateCount);
            }
        }
        
        pStmt1.close();
        pStmt2.close();
        
        stmt.close();
        stmt2.close();

        rollback();
        conn2.rollback();
        conn2.close();
    }

    /**
     * Test that the underlying exception is included in the output when we
     * call printStackTrace() on a BatchUpdateException. Earlier, with the
     * client driver, the underlying cause of a BatchUpdateException could not
     * be seen without calling getNextException().
     */
    public void testUnderlyingExceptionIsVisible() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.addBatch("create table t(x int unique not null)");
        for (int i = 0; i < 3; i++) {
            s.addBatch("insert into t values 1");
        }

        BatchUpdateException bue = null;
        try {
            s.executeBatch();
        } catch (BatchUpdateException e) {
            bue = e;
        }
        assertNotNull("Did not get duplicate key exception", bue);

        StringWriter w = new StringWriter();
        bue.printStackTrace(new PrintWriter(w, true));

        String stackTrace = w.toString();
        if (stackTrace.indexOf("duplicate key") == -1) {
            fail("Could not see 'duplicate key' in printStackTrace()", bue);
        }
    }

    /**
     * Test the behaviour when one of the statements in a batch fails. The
     * embedded driver stops executing the batch when that happens, whereas
     * the client driver continues. The difference between embedded and
     * client is logged as DERBY-4316.
     */
    public void testContinueAfterError() throws SQLException {
        // Turn off auto-commit so that the tables added by the test can be
        // rolled back in tearDown().
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table a(x int)");
        s.execute("create table b(x int primary key)");
        s.execute("create table c(x int references b(x))");

        // Drop the three tables in a batch. Since B is referenced by C, it
        // cannot be dropped before C is dropped. Hence DROP TABLE B will fail.
        s.addBatch("drop table a");
        s.addBatch("drop table b");
        s.addBatch("drop table c");

        // Embedded stops processing the batch on the first failure, and only
        // the update count from the successful statement is returned. The
        // client driver continues after the failure, so it'll also drop C.
        long[] expectedCounts = usingEmbedded() ?
            new long[]{0} : new long[]{0, Statement.EXECUTE_FAILED, 0};

        assertBatchExecuteError("X0Y25", s, expectedCounts);

        // Table A should not exist after the batch was executed.
        assertStatementError("42X05", s, "select * from a");
        // Table B should still exist, since DROP TABLE B failed.
        assertTableRowCount("B", 0);

        // Embedded driver stops after failure, so expect table C to exist,
        // whereas the client driver continues after failure, so expect that
        // it does not exist.
        if (usingEmbedded()) {
            assertTableRowCount("C", 0);
        } else {
            assertStatementError("42X05", s, "select * from c");
        }
    }
    ////////////////////////////////////////////////////////////////////////
    //
    // NESTED JDBC 4.2 WRAPPER AROUND A BatchUpdateException
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This wrapper is used to expose JDBC 4.2 methods which can run on
     * VM rev levels lower than Java 8.
     * </p>
     */
    public  static  class   BatchUpdateExceptionWrapper
    {
        private BatchUpdateException    _wrappedException;

        public BatchUpdateExceptionWrapper( BatchUpdateException wrappedException )
        {
            _wrappedException = wrappedException;
        }

        public  BatchUpdateException   getWrappedException() { return _wrappedException; }

        // New methods added by JDBC 4.2
        public  long[] getLargeUpdateCounts()
        {
            return ((long[]) invoke
                (
                 "getLargeUpdateCounts",
                 new Class[] {},
                 new Object[] {}
                 ));
        }

        // Reflection minion
        protected Object  invoke( String methodName, Class[] argTypes, Object[] argValues )
        {
            try {
                Method  method = _wrappedException.getClass().getMethod( methodName, argTypes );

                return method.invoke( _wrappedException, argValues );
            }
            catch (Exception nsme) { printException( nsme ); }

            return null;
        }
        private void    printException( Throwable t ) { BaseJDBCTestCase.println( t.getMessage() ); }
    }
    
}
