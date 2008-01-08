/*
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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests of stored procedures.
 */
public class ProcedureTest extends BaseJDBCTestCase {

    /**
     * Creates a new <code>ProcedureTest</code> instance.
     *
     * @param name name of the test
     */
    public ProcedureTest(String name) {
        super(name);
    }

    // TESTS

    /**
     * Tests that <code>Statement.executeQuery()</code> fails when no
     * result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithNoDynamicResultSets() throws SQLException {
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(0)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>Statement.executeQuery()</code> succeeds when
     * one result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithOneDynamicResultSet() throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(1)");
        assertNotNull("executeQuery() returned null.", rs);
        JDBC.assertDrainResultsHasData(rs);
    }

    /**
     * Tests that <code>Statement.executeQuery()</code> fails when
     * multiple result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithMoreThanOneDynamicResultSet()
        throws SQLException
    {
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(2)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>Statement.executeUpdate()</code> succeeds when
     * no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithNoDynamicResultSets()
        throws SQLException
    {
        Statement stmt = createStatement();
        assertUpdateCount(stmt, 0, "CALL RETRIEVE_DYNAMIC_RESULTS(0)");
        JDBC.assertNoMoreResults(stmt);
    }

    /**
     * Tests that <code>Statement.executeUpdate()</code> fails when a
     * result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithOneDynamicResultSet() throws SQLException {
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("CALL RETRIEVE_DYNAMIC_RESULTS(1)");
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
    }

    /**
     * Tests that <code>PreparedStatement.executeQuery()</code> fails
     * when no result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithNoDynamicResultSets_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 0);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>PreparedStatement.executeQuery()</code>
     * succeeds when one result set is returned from a stored
     * procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertNotNull("executeQuery() returned null.", rs);
        JDBC.assertDrainResultsHasData(rs);

    }

    /**
     * Tests that <code>PreparedStatement.executeQuery()</code> fails
     * when multiple result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithMoreThanOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 2);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>PreparedStatement.executeUpdate()</code>
     * succeeds when no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithNoDynamicResultSets_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 0);
        assertUpdateCount(ps, 0);
        JDBC.assertNoMoreResults(ps);
    }

    /**
     * Tests that <code>PreparedStatement.executeUpdate()</code> fails
     * when a result set is returned from a stored procedure.
     *
     * <p>Currently, this test fails with
     * JCC. However, the corresponding tests for
     * <code>Statement</code> and <code>CallableStatement</code>
     * succeed. Strange...
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 1);
        try {
            ps.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
    }

    /**
     * Tests that <code>CallableStatement.executeQuery()</code> fails
     * when no result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithNoDynamicResultSets_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 0);
        try {
            cs.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>CallableStatement.executeQuery()</code>
     * succeeds when one result set is returned from a stored
     * procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithOneDynamicResultSet_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 1);
        ResultSet rs = cs.executeQuery();
        assertNotNull("executeQuery() returned null.", rs);
        JDBC.assertDrainResultsHasData(rs);
    }

    /**
     * Tests that <code>CallableStatement.executeQuery()</code> fails
     * when multiple result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithMoreThanOneDynamicResultSet_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 2);
        try {
            cs.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>CallableStatement.executeUpdate()</code>
     * succeeds when no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithNoDynamicResultSets_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 0);
        assertUpdateCount(cs, 0);
        JDBC.assertNoMoreResults(cs);
    }

    /**
     * Tests that <code>CallableStatement.executeUpdate()</code> fails
     * when a result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithOneDynamicResultSet_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 1);
        try {
            cs.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when
     * <code>Connection.rollback()</code> is called.
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWithExecuteQuery() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(1)");
        rs.close();
        rollback();
        
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeUpdate()</code> are correctly rolled back when
     * <code>Connection.rollback()</code> is called.
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWithExecuteUpdate() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(0)");
        rollback();
        
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is zero.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsNothing()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(0)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }

        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is more
     * than one.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsTooMuch()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(2)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeUpdate()</code> are correctly rolled back when the
     * query fails because the stored procedure returned a result set.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteUpdateReturnsResults()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(1)");
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is zero.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsNothing_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 0);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
        Statement stmt = createStatement();
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is more
     * than one.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsTooMuch_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 2);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        Statement stmt = createStatement();
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
     }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeUpdate()</code> are correctly rolled back when the
     * query fails because the stored procedure returned a result set.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void
        testRollbackStoredProcWhenExecuteUpdateReturnsResults_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 1);
        try {
            ps.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        Statement stmt = createStatement();
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that closed result sets are not returned when calling
     * <code>executeQuery()</code>.
     * @exception SQLException if a database error occurs
     */
    public void testClosedDynamicResultSetsFromExecuteQuery()
        throws SQLException
    {
        Statement stmt = createStatement();
        try {
            ResultSet rs = stmt.executeQuery("CALL RETRIEVE_CLOSED_RESULT()");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that closed result sets are ignored when calling
     * <code>executeUpdate()</code>.
     * @exception SQLException if a database error occurs
     */
    public void testClosedDynamicResultSetsFromExecuteUpdate()
        throws SQLException
    {
        Statement stmt = createStatement();
        stmt.executeUpdate("CALL RETRIEVE_CLOSED_RESULT()");
        JDBC.assertNoMoreResults(stmt);
    }

    /**
     * Tests that dynamic result sets from other connections are
     * ignored when calling <code>executeQuery</code>.
     * @exception SQLException if a database error occurs
     */
    public void testDynamicResultSetsFromOtherConnectionWithExecuteQuery()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_EXTERNAL_RESULT(?,?,?)");
        
        ps.setString(1, getTestConfiguration().getDefaultDatabaseName());
        ps.setString(2, getTestConfiguration().getUserName());
        ps.setString(3, getTestConfiguration().getUserPassword());
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that dynamic result sets from other connections are
     * ignored when calling <code>executeUpdate</code>.
     * @exception SQLException if a database error occurs
     */
    public void testDynamicResultSetsFromOtherConnectionWithExecuteUpdate()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_EXTERNAL_RESULT(?,?,?)");
        
        ps.setString(1, getTestConfiguration().getDefaultDatabaseName());
        ps.setString(2, getTestConfiguration().getUserName());
        ps.setString(3, getTestConfiguration().getUserPassword());
        
        ps.executeUpdate();
        
        JDBC.assertNoMoreResults(ps);
    }

    // UTILITY METHODS

    /**
     * Raises an exception if the exception is not caused by
     * <code>executeQuery()</code> returning no result set.
     *
     * @param sqle a <code>SQLException</code> value
     */
    private void assertNoResultSetFromExecuteQuery(SQLException sqle) {
        if (usingDerbyNet()) {
            assertNull("Unexpected SQL state.", sqle.getSQLState());
        } else {
            assertSQLState("Unexpected SQL state.", "X0Y78", sqle);
        }
    }

    /**
     * Raises an exception if the exception is not caused by
     * <code>executeQuery()</code> returning multiple result sets.
     *
     * @param sqle a <code>SQLException</code> value
     */
    private void assertMultipleResultsFromExecuteQuery(SQLException sqle)
    {
        if (usingDerbyNet()) {
            assertNull("Unexpected SQL state.", sqle.getSQLState());
        } else {
            assertSQLState("Unexpected SQL state.", "X0Y78", sqle);
        }
    }

    /**
     * Raises an exception if the exception is not caused by
     * <code>executeUpdate()</code> returning result sets.
     *
     * @param sqle a <code>SQLException</code> value
     */
    private void assertResultsFromExecuteUpdate(SQLException sqle) {
        if (usingDerbyNet()) {
            assertNull("Unexpected SQL state.", sqle.getSQLState());
        } else {
            assertSQLState("Unexpected SQL state.", "X0Y79", sqle);
        }

    }

    // SETUP

    /**
     * Runs the test fixtures in embedded and client.
     * @return test suite
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("ProcedureTest");

        suite.addTest(baseSuite("ProcedureTest:embedded"));

        suite.addTest(
                TestConfiguration.clientServerDecorator(
                        baseSuite("ProcedureTest:client")));    
        return suite;
    }

    /**
     * Creates the test suite and wraps it in a <code>TestSetup</code>
     * instance which sets up and tears down the test environment.
     * @return test suite
     */
    private static Test baseSuite(String name)
    {
        TestSuite suite = new TestSuite(name);
        
        // Need JDBC DriverManager to run these tests
        if (!JDBC.vmSupportsJDBC3())
            return suite;
        
        suite.addTestSuite(ProcedureTest.class);
        
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables and the stored procedures used in the test
             * cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement s) throws SQLException
            {
                for (int i = 0; i < PROCEDURES.length; i++) {
                    s.execute(PROCEDURES[i]);
                }
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute(TABLES[i][1]);
                }
            }
        };
    }

    /**
     * Sets up the connection for a test case and clears all tables
     * used in the test cases.
     * @exception SQLException if a database error occurs
     */
    public void setUp() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement s = createStatement();
        for (int i = 0; i < TABLES.length; i++) {
            s.execute("DELETE FROM " + TABLES[i][0]);
        }
        commit();
    }

    /**
     * Procedures that should be created before the tests are run and
     * dropped when the tests have finished. First element in each row
     * is the name of the procedure, second element is SQL which
     * creates it.
     */
    private static final String[] PROCEDURES = {
       
          "CREATE PROCEDURE RETRIEVE_DYNAMIC_RESULTS(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveDynamicResults' " +
          "DYNAMIC RESULT SETS 4",


          "CREATE PROCEDURE RETRIEVE_CLOSED_RESULT() LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveClosedResult' " +
          "DYNAMIC RESULT SETS 1",

          "CREATE PROCEDURE RETRIEVE_EXTERNAL_RESULT(" +
          "DBNAME VARCHAR(128), DBUSER VARCHAR(128), DBPWD VARCHAR(128)) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveExternalResult' " +
          "DYNAMIC RESULT SETS 1",

          "CREATE PROCEDURE PROC_WITH_SIDE_EFFECTS(ret INT) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".procWithSideEffects' " +
          "DYNAMIC RESULT SETS 2",
          
          "CREATE PROCEDURE NESTED_RESULT_SETS(proctext VARCHAR(128)) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".nestedDynamicResultSets' " +
          "DYNAMIC RESULT SETS 6"

    };

    /**
     * Tables that should be created before the tests are run and
     * dropped when the tests have finished. The tables will be
     * cleared before each test case is run. First element in each row
     * is the name of the table, second element is the SQL text which
     * creates it.
     */
    private static final String[][] TABLES = {
        // SIMPLE_TABLE is used by PROC_WITH_SIDE_EFFECTS
        { "SIMPLE_TABLE", "CREATE TABLE SIMPLE_TABLE (id INT)" },
    };

    // PROCEDURES

    /**
     * Stored procedure which returns 0, 1, 2, 3 or 4 <code>ResultSet</code>s.
     *
     * @param number the number of <code>ResultSet</code>s to return
     * @param rs1 first <code>ResultSet</code>
     * @param rs2 second <code>ResultSet</code>
     * @param rs3 third <code>ResultSet</code>
     * @param rs4 fourth <code>ResultSet</code>
     * @exception SQLException if a database error occurs
     */
    public static void retrieveDynamicResults(int number,
                                              ResultSet[] rs1,
                                              ResultSet[] rs2,
                                              ResultSet[] rs3,
                                              ResultSet[] rs4)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        if (number > 0) {
            rs1[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (number > 1) {
            rs2[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (number > 2) {
            rs3[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (number > 3) {
            rs4[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        c.close();
    }

    /**
     * Stored procedure which produces a closed result set.
     *
     * @param closed holder for the closed result set
     * @exception SQLException if a database error occurs
     */
    public static void retrieveClosedResult(ResultSet[] closed)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        closed[0] = c.createStatement().executeQuery("VALUES(1)");
        closed[0].close();
        c.close();
    }

    /**
     * Stored procedure which produces a result set in another
     * connection.
     *
     * @param external result set from another connection
     * @exception SQLException if a database error occurs
     */
    public static void retrieveExternalResult(String dbName, 
            String user, String password, ResultSet[] external)
        throws SQLException
    {
        // Use a server-side connection to the same database.
        String url = "jdbc:derby:" + dbName;
        
        Connection conn = DriverManager.getConnection(url, user, password);
        
        external[0] =
            conn.createStatement().executeQuery("VALUES(1)");
    }

    /**
     * Stored procedure which inserts a row into SIMPLE_TABLE and
     * optionally returns result sets.
     *
     * @param returnResults if one, return one result set; if greater
     * than one, return two result sets; otherwise, return no result
     * set
     * @param rs1 first result set to return
     * @param rs2 second result set to return
     * @exception SQLException if a database error occurs
     */
    public static void procWithSideEffects(int returnResults,
                                           ResultSet[] rs1,
                                           ResultSet[] rs2)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = c.createStatement();
        stmt.executeUpdate("INSERT INTO SIMPLE_TABLE VALUES (42)");
        if (returnResults > 0) {
            rs1[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (returnResults > 1) {
            rs2[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        c.close();
    }
    
    /**
     * Method for a Java procedure that calls another procedure
     * and just passes on the dynamic results from that call.
     */
    public static void nestedDynamicResultSets(String procedureText,
            ResultSet[] rs1, ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
            ResultSet[] rs5, ResultSet[] rs6)
    throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        
        CallableStatement cs = c.prepareCall("CALL " + procedureText);
        
        cs.execute();
        
        // Mix up the order of the result sets in the returned
        // parameters, ensures order is defined by creation
        // and not parameter order.
        rs6[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs3[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs4[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs2[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs1[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs5[0] = cs.getResultSet();
    
    }

    
        /**
         * Test various combinations of getMoreResults
         * 
         * @throws SQLException
         */
        public void testGetMoreResults() throws SQLException {

                Statement s = createStatement();
                

                s.executeUpdate("create table MRS.FIVERS(i integer)");
                PreparedStatement ps = prepareStatement("insert into MRS.FIVERS values (?)");
                for (int i = 1; i <= 20; i++) {
                        ps.setInt(1, i);
                        ps.executeUpdate();
                }

                // create a procedure that returns 5 result sets.
                        
                s.executeUpdate("create procedure MRS.FIVEJP() parameter style JAVA READS SQL DATA dynamic result sets 5 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.fivejp'");


                CallableStatement cs = prepareCall("CALL MRS.FIVEJP()");
                ResultSet[] allRS = new ResultSet[5];

                defaultGetMoreResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                closeCurrentGetMoreResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                keepCurrentGetMoreResults(cs, allRS);                              
                java.util.Arrays.fill(allRS, null);
                mixedGetMoreResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                checkExecuteClosesResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                checkCSCloseClosesResults(cs,allRS);
                java.util.Arrays.fill(allRS, null);
                
                // a procedure that calls another procedure that returns
                // dynamic result sets, see if the result sets are handled
                // correctly through the nesting.
                CallableStatement nestedCs = prepareCall(
                        "CALL NESTED_RESULT_SETS('MRS.FIVEJP()')");
                defaultGetMoreResults(nestedCs, allRS);
                
        }

        
        /**
         * Check that CallableStatement.execute() closes results
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void checkExecuteClosesResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            //Fetching result sets with getMoreResults(Statement.KEEP_CURRENT_RESULT) and checking that cs.execute() closes them");          
            cs.execute();
            int pass = 0;
            do {

                    allRS[pass++] = cs.getResultSet();                
                    // expect everything to stay open.                        

            } while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            //fetched all results
            // All should still be open.
            for (int i = 0; i < 5; i++)
                JDBC.assertDrainResults(allRS[i]);                
            
            cs.execute();
            // all should be closed.
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

        /**
         * Check that CallableStatement.close() closes results
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void checkCSCloseClosesResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            cs.execute();
            int pass = 0;
            do {

                    allRS[pass++] = cs.getResultSet();                
                    // expect everything to stay open.                        

            } while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            //fetched all results
            // All should still be open.
            for (int i = 0; i < 5; i++)
                JDBC.assertDrainResults(allRS[i]);                
            
            cs.close();
            // all should be closed.
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

        private void mixedGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            //Fetching result sets with getMoreResults(<mixture>)"
            cs.execute();

            //first two with KEEP_CURRENT_RESULT"
            allRS[0] = cs.getResultSet();
            boolean moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            if (!moreRS)
                    fail("FAIL - no second result set");
            allRS[1] = cs.getResultSet();                
            // two open
            allRS[0].next();
            assertEquals(2,allRS[0].getInt(1));
            allRS[1].next();
            assertEquals(3,allRS[1].getInt(1));
            
            //third with CLOSE_CURRENT_RESULT"
            moreRS = cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            if (!moreRS)
                    fail("FAIL - no third result set");
            // first and third open
            allRS[2] = cs.getResultSet();
            assertEquals(2,allRS[0].getInt(1));
            JDBC.assertClosed(allRS[1]);
            allRS[2].next();
            assertEquals(4,allRS[2].getInt(1));

            
            //fourth with KEEP_CURRENT_RESULT"
            moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            if (!moreRS)
                    fail("FAIL - no fourth result set");
            allRS[3] = cs.getResultSet();
            allRS[3].next();
            // first, third and fourth open, second closed
            assertEquals(2,allRS[0].getInt(1));
            JDBC.assertClosed(allRS[1]);
            assertEquals(4,allRS[2].getInt(1));
            assertEquals(5,allRS[3].getInt(1));
            
            //fifth with CLOSE_ALL_RESULTS"
            moreRS = cs.getMoreResults(Statement.CLOSE_ALL_RESULTS);
            if (!moreRS)
                   fail("FAIL - no fifth result set");
            allRS[4] = cs.getResultSet();
            allRS[4].next();
            // only fifth open
            JDBC.assertClosed(allRS[0]);
            JDBC.assertClosed(allRS[1]);
            JDBC.assertClosed(allRS[2]);
            JDBC.assertClosed(allRS[3]);
            assertEquals(6,allRS[4].getInt(1));
            
            //no more results with with KEEP_CURRENT_RESULT"
            moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            if (moreRS)
                    fail("FAIL - too many result sets");
            // only fifth open
            JDBC.assertClosed(allRS[0]);
            JDBC.assertClosed(allRS[1]);
            JDBC.assertClosed(allRS[2]);
            JDBC.assertClosed(allRS[3]);
            assertEquals(6,allRS[4].getInt(1));
            
            allRS[4].close();
        }

        /**
         * Check getMoreResults(Statement.KEEP_CURRENT_RESULT)  
         * 
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void keepCurrentGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            cs.execute();
            
            for (int i = 0; i < 5; i++)
            {
                allRS[i] = cs.getResultSet();
                allRS[i].next();
                assertEquals(2+i, allRS[i].getInt(1));
                
                if (i < 4)
                    assertTrue(cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
                else
                    assertFalse(cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            }            
            
            // resultSets should still be open
            for (int i = 0; i < 5; i++)
                JDBC.assertDrainResults(allRS[i]);
        }

        private void closeCurrentGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            cs.execute();
            
            for (int i = 0; i < 5; i++)
            {
                allRS[i] = cs.getResultSet();
                allRS[i].next();
                assertEquals(2+i, allRS[i].getInt(1));
                
                if (i < 4)
                    assertTrue(cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
                else
                    assertFalse(cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            }
            
            // verify resultSets are closed
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

        /**
         * Test default getMoreResults() closes result set.
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void defaultGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            // execute the procedure that returns 5 result sets and then use the various
            // options of getMoreResults().

            cs.execute();
            
            for (int i = 0; i < 5; i++)
            {
                allRS[i] = cs.getResultSet();
                allRS[i].next();
                assertEquals(2+i, allRS[i].getInt(1));
                
                if (i < 4)
                    assertTrue(cs.getMoreResults());
                else
                    assertFalse(cs.getMoreResults());
            } 
                        
            // verify resultSets are closed
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

}
