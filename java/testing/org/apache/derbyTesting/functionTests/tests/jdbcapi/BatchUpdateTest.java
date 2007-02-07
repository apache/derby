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

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Test BatchUpdate functionality.
 * <P>
 * This test examines the behavior fo BatchUpdate test.
 * There are 5 actual fixtures, and even the setup is executing in batch, to 
 * verify basic create table and insert statements also work in batch.
 * The five actual fixtures are:
 * testStatementBatchUpdatePositive - verifies correct usage with Statements 
 *                                work as expected 
 * testStatementBatchUpdateNegative - verifies incorrect usage with Statments
 *                                gives appropriate errors
 * testCallableStatementBatchUpdate - verifies usage with callableStatements
 *                                works as expected
 * testPreparedStatementBatchUpdatePositive
 *                              - verifies correct usage with preparedStatements
 * testPreparedStatementBatchUpdateNegative
 *                              - verifies incorrect use with preparedStatements
 * 
 * The test executes almost all submethods of these fixtures with both
 * embedded and NetworkServer/DerbyNetClient - however, there is a difference
 * in functionality between the two when an error condition is reaches. Thus,
 * the negative tests have if / else if blocks for embedded and client.
 * 
 * The three subtests that are not running with network server are 
 * identified with //TODO: tags and have an if (usingEmbedded()) block.
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
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement stmt = createStatement();
        stmt.addBatch("create table t1(c1 int)");
        stmt.addBatch("create procedure Integ() language java " +
            "parameter style java external name 'java.lang.Integer'");
        stmt.addBatch("create table datetab(c1 date)");
        stmt.addBatch("create table timetab(c1 time)");
        stmt.addBatch("create table timestamptab(c1 timestamp)");
        stmt.addBatch("create table usertypetab(c1 DATE)");

        int expectedCount[] = {0,0,0,0,0,0};
        assertBatchUpdateCounts(expectedCount, stmt.executeBatch());
        
        // for method checkAssociatedParams
        stmt.executeUpdate("create table assoc" +
                "(x char(10) not null primary key, y char(100))");
        stmt.executeUpdate("create table assocout(x char(10))");
        
        conn.commit();
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate("DROP TABLE datetab");
        stmt.executeUpdate("DROP TABLE timetab");
        stmt.executeUpdate("DROP TABLE timestamptab");
        stmt.executeUpdate("DROP TABLE usertypetab");
        stmt.executeUpdate("DROP PROCEDURE Integ");
        stmt.executeUpdate("DROP TABLE t1");
        // for method checkAssociatedParams
        stmt.executeUpdate("drop table assoc");
        stmt.executeUpdate("drop table assocout");
        commit();
        super.tearDown();
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("BatchUpdateTest");
        suite.addTest(
            TestConfiguration.defaultSuite(BatchUpdateTest.class));

        return new CleanDatabaseTestSetup(
            DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4));
     }
    
    /* 
     * helper method to check each count in the return array of batchExecute
     */
    public void assertBatchUpdateCounts( 
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
     * @param String               The sqlstate to look for.
     * @param Statement            The Statement that contains the Batch to
     *                             be executed.
     * @param int[]                The expectedUpdateCount array.
     */
    protected void assertBatchExecuteError( 
            String expectedError,
            Statement stmt,
            int[] expectedUpdateCount) throws SQLException 
    {
        int[] updateCount;    
        try {
            updateCount = stmt.executeBatch();
            fail("Expected stmt.batchExecute to fail");
        } catch (SQLException sqle) {
            assertSQLState(expectedError, sqle);
            assertTrue("Expect BatchUpdateException", 
                (sqle instanceof BatchUpdateException));
            updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
            assertBatchUpdateCounts(expectedUpdateCount, updateCount);
        }
    }


    /** 
     * helper method to evaluate negative tests where we expect a
     * batchExecuteException to be returned
     * 
     * @exception SQLException     Thrown if the expected error occurs.
     *                             We expect a BatchUpdateException, and
     *                             verify it is so.
     *
     * @param String               The sqlstate to look for.
     * @param PreparedStatement    The PreparedStatement that contains the 
     *                             batch to be executed.
     * @param int[]                The expectedUpdateCount array.
     *                             
     */
    protected void assertBatchExecuteError( 
            String expectedError,
            PreparedStatement pstmt,
            int[] expectedUpdateCount) throws SQLException 
    {
        int[] updateCount;    
        try {
            updateCount = pstmt.executeBatch();
            fail("Expected pstmt.batchExecute to fail");
        } catch (SQLException sqle) {
            assertSQLState(expectedError, sqle);
            assertTrue("Expect BatchUpdateException", 
                (sqle instanceof BatchUpdateException));
            updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
            assertBatchUpdateCounts(expectedUpdateCount, updateCount);
        }
    }
    
    /**
     * Positive tests for statement batch update.
     *
     * @exception SQLException      Thrown if some unexpected error happens
     */
    public void testStatementBatchUpdatePositive()
    throws SQLException
    {
        Connection conn = getConnection();
        Statement stmt = createStatement();
        // try executing a batch which nothing in it.
        runEmptyStatementBatch(conn, stmt);
        // try executing a batch which one statement in it.
        runSingleStatementBatch(conn, stmt);
        // try executing a batch with 3 different statements in it.
        runMultipleStatementsBatch(conn, stmt);
        // try executing a batch with 1000 statements in it.
        run1000StatementsBatch(conn, stmt);
        // try batch with autocommit true
        runAutoCommitTrueBatch(conn, stmt);
        // try clear batch
        runCombinationsOfClearBatch(conn, stmt);
        // confirm associated parameters run ok with batches
        checkAssociatedParams(conn, stmt);
        conn.commit();
    }

    /**
     * Negative tests for statement batch update.
     * 
     * @exception SQLException      Thrown if some unexpected error happens
     */
    public void testStatementBatchUpdateNegative() throws SQLException 
    {
        Connection conn = getConnection();
        Connection conn2 = openDefaultConnection();
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);        
        Statement stmt = conn.createStatement();
        Statement stmt2 = conn2.createStatement();

        // statements which will return a resultset are not allowed in batch
        // update. The following case should throw an exception for select.
        // Below trying various placements of select statement in the batch,
        // i.e. as 1st stmt, nth stmt and last stmt
        runStatementWithResultSetBatch(conn, stmt);

        // try executing a batch with regular statement intermingled.
        runStatementNonBatchStuffInBatch(conn, stmt);

        // Below trying various placements of overflow update statement 
        // in the batch, i.e. as 1st stmt, nth stat and last stmt
        runStatementWithErrorsBatch(conn, stmt);

        // TODO: When running this with networkserver, we won't be able
        // to drop t1 afterwards. Needs researching.
        if (usingEmbedded())
            // try transaction error, i.e. time out while getting the lock
            runTransactionErrorBatch(conn, stmt, conn2, stmt2);
        
     }

    /**
     * Tests for callable statement batch update.
     *
     * @exception SQLException      Thrown if some unexpected error happens
     */
    public void testCallableStatementBatchUpdate()
    throws SQLException
    {
        Connection conn = getConnection();
        
        // try callable statements
        runCallableStatementBatch(conn);

        // try callable statement with output parameters
        runCallableStatementWithOutputParamBatch(conn);
    }
    
    /**
     * Positive tests for prepared statement batch update.
     *
     *  @exception SQLException      Thrown if some unexpected error happens
     */
    public void testPreparedStatementBatchUpdatePositive()
    throws SQLException 
    {
        Connection conn = getConnection();
        Statement stmt = createStatement();

        //try executing a batch which nothing in it.
        runEmptyValueSetPreparedBatch(conn, stmt);

        // following fails with NullPointerException with NetworkServer
        // see DERBY-2112
        if (usingEmbedded())
        // try executing a batch with no parameters.
            runNoParametersPreparedBatch(conn, stmt);

        // try executing a batch which one parameter set in it.
        runSingleValueSetPreparedBatch(conn, stmt);

        // try executing a batch with 3 parameter sets in it.
        runMultipleValueSetPreparedBatch(conn, stmt);

        // try executing a batch with 2 parameter sets in it 
        // and they are set to null.
        runMultipleValueSetNullPreparedBatch(conn, stmt);

        // try executing a batch with 1000 statements in it.
        run1000ValueSetPreparedBatch(conn, stmt);

        // try executing batches with various rollback and commit combinations.
        runPreparedStatRollbackAndCommitCombinations(conn, stmt);

        // try prepared statement batch with autocommit true
        runAutoCommitTruePreparedStatBatch(conn, stmt);

        // try clear batch
        runCombinationsOfClearPreparedStatBatch(conn, stmt);

    }
       
    /**
     * Negative tests for prepared statement batch update.
     *
     *  @exception SQLException      Thrown if some unexpected error happens
     */
    public void testPreparedStatementBatchUpdateNegative() throws SQLException 
    {

        Connection conn = getConnection();
        Connection conn2 = openDefaultConnection();
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);        
        Statement stmt = conn.createStatement();
        Statement stmt2 = conn2.createStatement();
        
        // statements which will return a resultset are not allowed in batch
        // update. The following case should throw an exception for select.
        runPreparedStmtWithResultSetBatch(conn, stmt);

        // try executing a batch with regular statement intermingled.
        runPreparedStmtNonBatchStuffInBatch(conn, stmt);

        // Below trying various placements of overflow update statement 
        // in the batch
        runPreparedStmtWithErrorsBatch(conn, stmt);

        // TODO: when running this test with NetworkServer, t1 can
        //       no longer be dropped. Needs research.
        if (usingEmbedded())
            // try transaction error, in this particular case time out while 
            // getting the lock
            runTransactionErrorPreparedStmtBatch(conn, stmt, conn2, stmt2);
    }
	
    /* Following are methods used in testStatementUpdateBatchPositive */
    
    // try executing a batch which nothing in it. Should work.
    protected void runEmptyStatementBatch(Connection conn, Statement stmt) 
    throws SQLException {
        int updateCount[];

        // try executing a batch which nothing in it. Should work.
        println("Positive Statement: clear the batch and run the empty batch");
        stmt.clearBatch();
        updateCount = stmt.executeBatch();
        assertEquals("expected updateCount of 0", 0, updateCount.length);

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    // try executing a batch which single statement in it. Should work.
    protected void runSingleStatementBatch(Connection conn, Statement stmt) 
    throws SQLException {

        println("Positive Statement: testing 1 statement batch");
        stmt.addBatch("insert into t1 values(2)");

        assertBatchUpdateCounts(new int[] {1}, stmt.executeBatch());
            
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try executing a batch with 3 different statements in it.
    protected void runMultipleStatementsBatch(
            Connection conn, Statement stmt) throws SQLException {
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

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("expect 2 rows total", 2, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    // try executing a batch with 1000 statements in it.
    protected void run1000StatementsBatch(Connection conn, Statement stmt) 
    throws SQLException {
        int updateCount[];
        ResultSet rs;

        println("Positive Statement: 1000 statements batch");
        for (int i=0; i<1000; i++){
            stmt.addBatch("insert into t1 values(1)");
        }
        updateCount = stmt.executeBatch();
        assertEquals("1000 statement in the batch, expect update count 1000", 
            1000, updateCount.length);

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("1000 statement in the batch, expect 1000 rows",
            1000, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    // try batch with autocommit true
    protected void runAutoCommitTrueBatch(Connection conn, Statement stmt) 
    throws SQLException {
        ResultSet rs;

        conn.setAutoCommit(true);
        // try batch with autocommit true
        println("Positive Statement: stmt testing with autocommit true");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("delete from t1");
        assertBatchUpdateCounts(new int[] {1,1,2}, stmt.executeBatch());

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("expect 0 rows", 0,rs.getInt(1));
        rs.close();

        //turn it true again after the above negative test
        conn.setAutoCommit(false);
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    //  try combinations of clear batch.
    protected void runCombinationsOfClearBatch(
            Connection conn, Statement stmt) throws SQLException {
        ResultSet rs;

        println("Positive Statement: add 3 statements, clear and execute batch");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.addBatch("insert into t1 values(2)");
        stmt.clearBatch();

        assertEquals("Batch should be cleared, there should be no update count",
            0, stmt.executeBatch().length);
        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        JDBC.assertEmpty(rs);
        rs.close();

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
        rs = stmt.executeQuery("select count(*) from t1");
        JDBC.assertFullResultSet(rs, new String[][] {{"3"}}, true);

        rs.close();
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    /*
     ** Associated parameters are extra parameters that are created
     ** and associated with the root parameter (the user one) to
     ** improve the performance of like.       For something like
     ** where c1 like ?, we generate extra 'associated' parameters 
     ** that we use for predicates that we give to the access
     ** manager. 
     */
    protected void checkAssociatedParams(Connection conn, Statement stmt)
    throws SQLException 
    {
        int i;
        conn.setAutoCommit(false);
        println("Positive Statement: testing associated parameters");
        PreparedStatement checkps = conn.prepareStatement(
            "select x from assocout order by x");
        PreparedStatement ps = conn.prepareStatement(
            "insert into assoc values (?, 'hello')");
        for ( i = 10; i < 60; i++)
        {
            ps.setString(1, new Integer(i).toString());
            ps.executeUpdate();     
        }

        ps = conn.prepareStatement(
            "insert into assocout select x from assoc where x like ?");
        ps.setString(1, "33%");
        ps.addBatch();
        ps.setString(1, "21%");
        ps.addBatch();
        ps.setString(1, "49%");
        ps.addBatch();
        
        assertBatchUpdateCounts(new int[] {1,1,1}, ps.executeBatch());
        checkps.execute();
        ResultSet rs = checkps.getResultSet();
        JDBC.assertFullResultSet(
            rs, new String[][] {{"21"},{"33"},{"49"}}, true);
                
        stmt.executeUpdate("delete from assocout");

        ps = conn.prepareStatement(
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
        checkps.execute();
        rs = checkps.getResultSet();
        JDBC.assertFullResultSet(rs, expectedStrArray, true);
                
        stmt.executeUpdate("delete from assocout");
        ps = conn.prepareStatement(
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
        checkps.execute();
        rs = checkps.getResultSet();
        JDBC.assertFullResultSet(rs, expectedStrArray2, true);
    }

    /* Following are methods used in testStatementBatchUpdateNegative */

    // statements which will return a resultset are not allowed in batch
    // update. The following case should throw an exception for select. 
    // Below trying various placements of select statement in the batch,
    // i.e. as 1st stmt, nth stat and last stmt
    protected void runStatementWithResultSetBatch(
        Connection conn, Statement stmt) throws SQLException {
        
        ResultSet rs;

        // trying select as the first statement
        println("Negative Statement: statement testing select as first " +
            "statement in the batch");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        stmt.addBatch("insert into t1 values(1)");
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            assertBatchExecuteError("X0Y79", stmt, new int[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new int[] {-3, 1});
        
        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals(
                "There should be no rows in the table", 0, rs.getInt(1));
        else if (usingDerbyNetClient())
            assertEquals("There will be 1 row in the table", 1, rs.getInt(1));
        rs.close();
        
        // trying select as the nth statement
        println("Negative Statement: " +
            "statement testing select as nth stat in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        stmt.addBatch("insert into t1 values(1)");
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            assertBatchExecuteError("X0Y79", stmt, new int[] {1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new int[] {1,-3,1});
            
        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals(
                "There should be 1 row in the table", 1, rs.getInt(1));
        else if (usingDerbyNetClient())
            assertEquals("There are 3 rows in the table", 3, rs.getInt(1));
        rs.close();

        // trying select as the last statement
        println("Negative Statement: statement testing select" +
            " as last stat in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            assertBatchExecuteError("X0Y79", stmt, new int[] {1,1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new int[] {1,1,-3});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals(
                "There should now be 3 rows in the table", 3, rs.getInt(1));
        else if (usingDerbyNetClient())
            assertEquals(
                "There should now be 5 rows in the table", 5, rs.getInt(1));
        rs.close();

        conn.rollback();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table after rollback", 
                0, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try executing a batch with regular statement intermingled.
    protected void runStatementNonBatchStuffInBatch(
        Connection conn, Statement stmt) throws SQLException {
        
        int[] updateCount=null;
        ResultSet rs;

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
            assertBatchExecuteError("XJ208",stmt, new int[] {-3,1});           
            // pull level with embedded situation
            conn.rollback();
        }
        // do clearBatch so we can proceed
        stmt.clearBatch();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table", 0, rs.getInt(1));
        rs.close();

        try
        {
            // trying executeQuery after addBatch
            println("Negative Statement: " +
                "statement testing executeQuery in the middle of batch");
            stmt.addBatch("insert into t1 values(1)");
            stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");
            updateCount = stmt.executeBatch();
            if (usingEmbedded())
                fail("Expected executeBatch to fail with embedded");
            else if (usingDerbyNetClient())
            {   
                assertBatchUpdateCounts(new int[] {1}, updateCount);
                // set to same spot as embedded
                conn.rollback();
            }
        } catch (SQLException sqle) {
            /* Check to be sure the exception is the MIDDLE_OF_BATCH */
            assertSQLState("XJ068", sqle);
            // do clearBatch so we can proceed
            stmt.clearBatch();
        }

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table", 0, rs.getInt(1));
        rs.close();

        println("Negative Statement: " +
            "statement testing executeUpdate in the middle of batch");
        try
        {
            // trying executeUpdate after addBatch
            println("Negative Statement: " +
                "statement testing executeUpdate in the middle of batch");
            stmt.addBatch("insert into t1 values(1)");
            stmt.executeUpdate("insert into t1 values(1)");
            stmt.addBatch("insert into t1 values(1)");
            stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
            if (usingDerbyNetClient())
            {
                assertBatchExecuteError("XJ208", stmt, new int[] {1,1,-3});
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
        
        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals("There should be no rows in the table", 
                0, rs.getInt(1));
        else if (usingDerbyNetClient())
            assertEquals("There should be 3 rows in the table", 
                3, rs.getInt(1));
        rs.close();

        conn.rollback();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table", 0, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // Below trying various placements of overflow update statement in the 
    // batch, i.e. as 1st stmt, nth stmt and last stmt
    protected void runStatementWithErrorsBatch(
        Connection conn, Statement stmt) throws SQLException {
        
        ResultSet rs;

        stmt.executeUpdate("insert into t1 values(1)");

        // trying update as the first statement
        println("Negative Statement: statement testing overflow error" +
            " as first statement in the batch");
        stmt.addBatch("update t1 set c1=2147483647 + 1");
        stmt.addBatch("insert into t1 values(1)");
        /* Check to be sure the exception is the one we expect */
        /* Overflow is first stmt in the batch, so expect no update count */
        if (usingEmbedded())
            assertBatchExecuteError("22003", stmt, new int[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new int[] {-3,1});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals("there should be 1 row in the table", 
                    1, rs.getInt(1));
        if (usingDerbyNetClient())
            assertEquals("there should be 2 rows in the table", 
                    2, rs.getInt(1));
        rs.close();

        // trying update as the nth statement
        println("Negative Statement: statement testing overflow error" +
            " as nth statement in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("update t1 set c1=2147483647 + 1");
        stmt.addBatch("insert into t1 values(1)");
        /* Check to be sure the exception is the one we expect */
        /* Update is second statement in the batch, expect 1 update count */
        if (usingEmbedded())
            assertBatchExecuteError("22003", stmt, new int[] {1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new int[] {1,-3,1});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals("expected: 2 rows", 2, rs.getInt(1));
        if (usingDerbyNetClient())
            assertEquals("expected: 4 rows", 4, rs.getInt(1));
        rs.close();

        // trying select as the last statement
        println("Negative Statement: statement testing overflow error" +
            " as last stat in the batch");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("insert into t1 values(1)");
        stmt.addBatch("update t1 set c1=2147483647 + 1");
        /* Check to be sure the exception is the one we expect */
        /* Update is last statement in the batch, expect 2 update counts */
        if (usingEmbedded())
            assertBatchExecuteError("22003", stmt, new int[] {1,1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", stmt, new int[] {1,1,-3});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        if (usingEmbedded())
            assertEquals("expected: 4 rows", 4, rs.getInt(1));
        if (usingDerbyNetClient())
            assertEquals("expected: 6 rows", 6, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try transaction error, in this particular case time out while
    // getting the lock
    protected void runTransactionErrorBatch(
        Connection conn, Statement stmt,
        Connection conn2, Statement stmt2) throws SQLException {

        int[] updateCount = null;

        try
        {
            println("Negative Statement: statement testing time out" +
                " while getting the lock in the batch");

            stmt.execute("insert into t1 values(1)");
            stmt2.execute("insert into t1 values(2)");

            stmt.addBatch("update t1 set c1=3 where c1=2");
            stmt2.addBatch("update t1 set c1=4 where c1=1");
                      
            stmt.executeBatch();
            updateCount = stmt2.executeBatch();
            fail ("Batch is expected to fail");
        } catch (SQLException sqle) {
            /* Ensure the exception is time out while getting lock */
            assertSQLState("40XL1",sqle);
            assertTrue("we should get a BatchUpdateException", 
                (sqle instanceof BatchUpdateException));
            updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
            if (updateCount != null) {
                assertEquals("first statement in the batch caused time out" +
                    " while getting the lock, expect no update count",
                    0, updateCount.length);
            }
        }
        conn.rollback();
        conn2.rollback();
        stmt.clearBatch();
        stmt2.clearBatch();
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    /* Following are methods used in testCallableStatementBatchUpdate */

    // try callable statements
    protected void runCallableStatementBatch(Connection conn)
    throws SQLException {
        conn.setAutoCommit(false);
        
        println("Positive Callable Statement: " +
            "statement testing callable statement batch");
        CallableStatement cs = conn.prepareCall("insert into t1 values(?)");

        cs.setInt(1, 1);
        cs.addBatch();
        cs.setInt(1,2);
        cs.addBatch();
        try
        {
            executeBatchCallableStatement(cs);
        }
        catch (SQLException sqle)
        {   
            fail("The executeBatch should have succeeded");
        }
        cleanUpCallableStatement(conn, cs, "t1");

        /* For 'beetle' bug 2813 - setDate/setTime/setTimestamp
         * calls on callableStatement throws ClassNotFoundException 
         * verify setXXXX() works with Date, Time and Timestamp 
         * on CallableStatement.
         */
        cs = conn.prepareCall("insert into datetab values(?)");

        cs.setDate(1, Date.valueOf("1990-05-05"));
        cs.addBatch();
        cs.setDate(1,Date.valueOf("1990-06-06"));
        cs.addBatch();
        try
        {
            executeBatchCallableStatement(cs);
        }
        catch (SQLException sqle)
        {   
            fail("The executeBatch should have succeeded");
        }
        cleanUpCallableStatement(conn, cs, "datetab");

        cs = conn.prepareCall("insert into timetab values(?)");

        cs.setTime(1, Time.valueOf("11:11:11"));
        cs.addBatch();
        cs.setTime(1, Time.valueOf("12:12:12"));
        cs.addBatch();
        try
        {
            executeBatchCallableStatement(cs);
        }
        catch (SQLException sqle)
        {   
            fail("The executeBatch should have succeeded");
        }
        cleanUpCallableStatement(conn, cs, "timestamptab");

        cs = conn.prepareCall("insert into timestamptab values(?)");

        cs.setTimestamp(1, Timestamp.valueOf("1990-05-05 11:11:11.1"));
        cs.addBatch();
        cs.setTimestamp(1, Timestamp.valueOf("1992-07-07 12:12:12.2"));
        cs.addBatch();
        try
        {
            executeBatchCallableStatement(cs);
        }
        catch (SQLException sqle)
        {   
            fail("The executeBatch should have succeeded");
        }
        cleanUpCallableStatement(conn, cs, "timestamptab");

        // Try with a user type
        cs = conn.prepareCall("insert into usertypetab values(?)");

        cs.setObject(1, Date.valueOf("1990-05-05"));
        cs.addBatch();
        cs.setObject(1,Date.valueOf("1990-06-06"));
        cs.addBatch();
        try
        {
            executeBatchCallableStatement(cs);
        }
        catch (SQLException sqle)
        {   
            fail("The executeBatch should have succeeded");
        }
        cleanUpCallableStatement(conn, cs, "usertypetab");
    }
    
    // helper method to runCallableStatementBatch 
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

    // helper method to runCallableStatementBatch - 
    // removes all rows from table
    protected static void cleanUpCallableStatement(
        Connection conn, CallableStatement cs, String tableName)
    throws SQLException
    {
        cs.close();
        conn.rollback();
        cs = conn.prepareCall("delete from " + tableName);
        cs.executeUpdate();
        cs.close();
        conn.commit();
    }
    
    // try callable statements with output parameters
    // TODO: isolate the procedure(s) from lang/outparams? 
    protected void runCallableStatementWithOutputParamBatch(Connection conn) 
    throws SQLException {
        String CLASS_NAME;
        if(JDBC.vmSupportsJDBC3())
            CLASS_NAME = 
               "org.apache.derbyTesting.functionTests.tests.lang.outparams30.";
        else
            CLASS_NAME = 
                "org.apache.derbyTesting.functionTests.tests.lang.outparams.";

        println("Negative Callable Statement: " +
            "callable statement with output parameters in the batch");
        Statement s = conn.createStatement();

        s.execute("CREATE PROCEDURE " +
                "takesString(OUT P1 VARCHAR(40), IN P2 INT) " +
                "EXTERNAL NAME '" + CLASS_NAME + "takesString'" +
        " NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");

        CallableStatement cs = conn.prepareCall("call takesString(?,?)");
        try
        {
            cs.registerOutParameter(1, Types.CHAR);
            cs.setInt(2, Types.INTEGER);
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
        conn.rollback();
        conn.commit();
    }

    /* Following are methods used in 
     * testPreparedStatementBatchUpdatePositive */    
    
    // try executing a batch which nothing in it. Should work.
    protected void runEmptyValueSetPreparedBatch(
        Connection conn, Statement stmt) throws SQLException {

        // try executing a batch which nothing in it. Should work.
        println("Positive Prepared Stat: " +
            "set no parameter values and run the batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");

        assertBatchUpdateCounts(new int[] {}, pStmt.executeBatch());

        pStmt.close();
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try prepared statement batch with just no settable parameters.
    protected void runNoParametersPreparedBatch(
        Connection conn, Statement stmt) throws SQLException {
        ResultSet rs;

        println("Positive Prepared Stat: no settable parameters");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(5)");
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

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 3 rows", 3, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try prepared statement batch with just one set of values.
    protected void runSingleValueSetPreparedBatch(
        Connection conn, Statement stmt) throws SQLException {
        ResultSet rs;

        // try prepared statement batch with just one set of values
        println("Positive Prepared Stat: " +
            "set one set of parameter values and run the batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* 1 parameter was set in batch, update count length should be 1 */
        assertBatchUpdateCounts(new int[] {1}, pStmt.executeBatch());

        pStmt.close();
        rs = stmt.executeQuery("select count(*) from t1 where c1=1");
        rs.next();
        assertEquals("There should be 1 row with c1=1", 1, rs.getInt(1));
        
        rs.close();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 1 row", 1, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try executing a batch with 3 different parameter sets in it.
    protected void runMultipleValueSetPreparedBatch(
            Connection conn, Statement stmt) throws SQLException {
        ResultSet rs;

        // try prepared statement batch with just one set of values
        println("Positive Prepared Stat: " +
            "set 3 set of parameter values and run the batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 2);
        pStmt.addBatch();
        pStmt.setInt(1, 3);
        pStmt.addBatch();
        /* 3 parameters were set , update count length should be 3 */
        assertBatchUpdateCounts(new int[] {1,1,1}, pStmt.executeBatch());

        pStmt.close();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 3 rows", 3, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try prepared statement batch with just 2 set of values 
    // and there value is null. 
    // tests fix for 'beetle' bug 4002: Execute batch for
    // preparedStatement gives nullPointerException
    protected void runMultipleValueSetNullPreparedBatch(
            Connection conn, Statement stmt) throws SQLException {
        ResultSet rs;

        // try prepared statement batch with just one set of values
        println("Positive Prepared Stat: " +
            "set one set of parameter values to null and run the batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
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

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 2 rows", 2, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try executing a batch with 1000 statements in it.
    protected void run1000ValueSetPreparedBatch(
        Connection conn, Statement stmt) 
    throws SQLException {
        int updateCount[];
        ResultSet rs;

        println("Positive Prepared Stat: 1000 parameter set batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
        for (int i=0; i<1000; i++){
            pStmt.setInt(1, 1);
            pStmt.addBatch();
        }
        updateCount = pStmt.executeBatch();

        assertEquals("there were 1000 parameters set in the batch," +
            " update count length should be 1000",
            1000, updateCount.length);
        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 1000 rows in the table",
                1000, rs.getInt(1));
        rs.close();

        pStmt.close();
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    // try executing batches with various rollback and commit combinations.
    protected void runPreparedStatRollbackAndCommitCombinations(
        Connection conn, Statement stmt) throws SQLException {

        ResultSet rs;

        println("Positive Prepared Stat: batch, rollback," +
                " batch and commit combinations");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        conn.rollback();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 0 rows after rollback", 0, rs.getInt(1));
        rs.close();

        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        conn.commit();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 2 rows", 2, rs.getInt(1));
        
        rs.close();

        // try batch and commit
        println("Positive Prepared Stat: batch and commit combinations");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 2 statements in the batch, 
         * update count length should be 2 */
        assertBatchUpdateCounts(new int[] {1,1}, pStmt.executeBatch());

        conn.commit();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 4 rows", 4, rs.getInt(1));
        rs.close();

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

        conn.rollback();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 4 rows", 4, rs.getInt(1));
        rs.close();

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

        conn.commit();

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 8 rows", 8, rs.getInt(1));

        rs.close();
        pStmt.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }

    // try prepared statement batch with autocommit true
    protected void runAutoCommitTruePreparedStatBatch(
        Connection conn, Statement stmt) throws SQLException {
        ResultSet rs;

        conn.setAutoCommit(true);
        // prepared statement batch with autocommit true
        println("Positive Prepared Stat: testing batch with autocommit true");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        /* there were 3 statements in the batch, 
         * update count length should be 3 */
        assertBatchUpdateCounts(new int[] {1,1,1}, pStmt.executeBatch());

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 3 rows in the table", 3, rs.getInt(1));
        rs.close();
        pStmt.close();

        // turn it true again after the above negative test
        conn.setAutoCommit(false);

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try combinations of clear batch.
    protected void runCombinationsOfClearPreparedStatBatch(
        Connection conn, Statement stmt) throws SQLException {

        int updateCount[];
        ResultSet rs;

        println("Positive Prepared Stat: add 3 statements, " +
            "clear batch and execute batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("insert into t1 values(?)");
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
        updateCount = pStmt.executeBatch();

        assertEquals("there were 3 statements in the batch, " +
            "update count should be 3",
            3, updateCount.length);
        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 3 rows in the table", 3, rs.getInt(1));
        rs.close();
        pStmt.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    /* Methods used in testPreparedStatementBatchUpdateNegative */
    
    // statements which will return a resultset are not allowed in
    // batch Updates. Following case should throw an exception for select.
    protected void runPreparedStmtWithResultSetBatch(
        Connection conn, Statement stmt) throws SQLException {

        ResultSet rs;

        println("Negative Prepared Stat: testing select in the batch");
        PreparedStatement pStmt = 
            conn.prepareStatement("select * from t1 where c1=?");
        pStmt.setInt(1, 1);
        pStmt.addBatch();
        if (usingEmbedded())
            /* Ensure the exception is the ResultSetReturnNotAllowed */
            /* "Select is first statement in the batch, 
             * so there should not be any update counts */
            assertBatchExecuteError("X0Y79", pStmt, new int[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ117", pStmt, new int[] {-3});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table",
            0, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try executing a batch with regular statement intermingled.
    protected void runPreparedStmtNonBatchStuffInBatch(
            Connection conn, Statement stmt) throws SQLException {
        int updateCount[] = null;
        ResultSet rs;

        try
        {
            // trying execute in the middle of batch
            println("Negative Prepared Stat: " +
                "testing execute in the middle of batch");
            PreparedStatement pStmt = 
                conn.prepareStatement("select * from t1 where c1=?");
            pStmt.setInt(1, 1);
            pStmt.addBatch();
            pStmt.execute();
            updateCount = pStmt.executeBatch();
            fail("Expected executeBatch to fail");
        } catch (SQLException sqle) {
            if (usingEmbedded())
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XJ117", sqle);
            // do clearBatch so we can proceed
            stmt.clearBatch();
        }

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table", 
            0, rs.getInt(1));
        rs.close();

        try
        {
            // trying executeQuery in the middle of batch
            println("Negative Prepared Statement: " +
                "testing executeQuery in the middle of batch");
            PreparedStatement pStmt = 
                conn.prepareStatement("select * from t1 where c1=?");
            pStmt.setInt(1, 1);
            pStmt.addBatch();
            pStmt.executeQuery();
            updateCount = pStmt.executeBatch();
            fail("Expected executeBatch to fail");
        } catch (SQLException sqle) {
            if (usingEmbedded())
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XJ117", sqle);
            // do clearBatch so we can proceed
            stmt.clearBatch();
        }

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table", 
            0, rs.getInt(1));
        rs.close();

        try
        {
            //  trying executeUpdate in the middle of batch
            println("Negative Prepared Stat: " +
                        "testing executeUpdate in the middle of batch");
            PreparedStatement pStmt = 
                conn.prepareStatement("select * from t1 where c1=?");
            pStmt.setInt(1, 1);
            pStmt.addBatch();
            pStmt.executeUpdate();
            updateCount = pStmt.executeBatch();
            fail("Expected executeBatch to fail");
        } catch (SQLException sqle) {
            if (usingEmbedded())
                /* Check to be sure the exception is the MIDDLE_OF_BATCH */
                assertSQLState("XJ068", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("X0Y79", sqle);
            // do clearBatch so we can proceed
            stmt.clearBatch();
        }

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be no rows in the table", 
            0, rs.getInt(1));
        rs.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // Below trying placements of overflow update statement in the batch
    protected void runPreparedStmtWithErrorsBatch(
        Connection conn, Statement stmt) throws SQLException {
        
        ResultSet rs;
        PreparedStatement pStmt = null;

        stmt.executeUpdate("insert into t1 values(1)");

        println("Negative Prepared Stat: " +
            "testing overflow as first set of values");
        pStmt = conn.prepareStatement("update t1 set c1=(? + 1)");
        pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
        pStmt.addBatch();
        if (usingEmbedded())
            /* Check to be sure the exception is the one we expect */
            /* Overflow is first statement in the batch, 
             * so there should not be any update count */
            assertBatchExecuteError("22003", pStmt, new int[] {});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", pStmt, new int[] {-3});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 1 row in the table", 1, rs.getInt(1));
        rs.close();

        println("Negative Prepared Stat: " +
            "testing overflow as nth set of values");
        pStmt = conn.prepareStatement("update t1 set c1=(? + 1)");
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
            assertBatchExecuteError("22003", pStmt, new int[] {1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", pStmt, new int[] {1,-3,1});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 1 row in the table", 1, rs.getInt(1));
        rs.close();

        // trying select as the last statement
        println("Negative Prepared Stat: " +
            "testing overflow as last set of values");
        pStmt = conn.prepareStatement("update t1 set c1=(? + 1)");
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
            assertBatchExecuteError("22003", pStmt, new int[] {1,1});
        else if (usingDerbyNetClient())
            assertBatchExecuteError("XJ208", pStmt, new int[] {1,1,-3});

        rs = stmt.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals("There should be 1 row in the table", 1, rs.getInt(1));
        rs.close();
        pStmt.close();

        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
    
    // try transaction error, in this particular case 
    // time out while getting the lock
    protected void runTransactionErrorPreparedStmtBatch(
            Connection conn, Statement stmt,
            Connection conn2, Statement stmt2) throws SQLException {

        int updateCount[] = null;
        
        try
        {
            println("Negative Prepared Statement: " +
                "testing transaction error, time out while getting the lock");

            stmt.execute("insert into t1 values(1)");
            stmt2.execute("insert into t1 values(2)");

            PreparedStatement pStmt1 = 
                conn.prepareStatement("update t1 set c1=3 where c1=?");
            pStmt1.setInt(1, 2);
            pStmt1.addBatch();

            PreparedStatement pStmt2 = 
                conn.prepareStatement("update t1 set c1=4 where c1=?");
            pStmt2.setInt(1, 1);
            pStmt2.addBatch();

            pStmt1.executeBatch();
            updateCount = pStmt2.executeBatch();
            fail ("Batch is expected to fail");
        } catch (SQLException sqle) {
            /* Check that the exception is time out while 
             * getting the lock */
            assertSQLState("40XL1", sqle);
            assertTrue("Expect BatchUpdateException", 
                (sqle instanceof BatchUpdateException));
            updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
            if (updateCount != null) {
                assertEquals("first statement in the batch caused time out" +
                    " while getting the lock, there should be no update count", 
                    0, updateCount.length);
            }
        }

        conn.rollback();
        conn2.rollback();
        stmt.executeUpdate("delete from t1");
        conn.commit();
    }
}
