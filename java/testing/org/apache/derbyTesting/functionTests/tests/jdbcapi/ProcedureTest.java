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
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

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
        stmt.close();
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
        assertTrue("Result set has no data.", rs.next());
        rs.close();
        stmt.close();
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
        stmt.close();
    }

    /**
     * Tests that <code>Statement.executeUpdate()</code> succeeds when
     * no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void xtestExecuteUpdateWithNoDynamicResultSets()
        throws SQLException
    {
        Statement stmt = getConnection().createStatement();
        int count = stmt.executeUpdate("CALL RETRIEVE_DYNAMIC_RESULTS(0)");
        assertEquals("Wrong update count.", 0, count);
        stmt.close();
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
        stmt.close();
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
            getConnection().prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 0);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
        ps.close();
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
            getConnection().prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertNotNull("executeQuery() returned null.", rs);
        assertTrue("Result set has no data.", rs.next());
        rs.close();
        ps.close();
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
            getConnection().prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 2);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        ps.close();
    }

    /**
     * Tests that <code>PreparedStatement.executeUpdate()</code>
     * succeeds when no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void xtestExecuteUpdateWithNoDynamicResultSets_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            getConnection().prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 0);
        int count = ps.executeUpdate();
        assertEquals("Wrong update count.", 0, count);
        ps.close();
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
    public void xtestExecuteUpdateWithOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            getConnection().prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 1);
        try {
            ps.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        ps.close();
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
            getConnection().prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
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
            getConnection().prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 1);
        ResultSet rs = cs.executeQuery();
        assertNotNull("executeQuery() returned null.", rs);
        assertTrue("Result set has no data.", rs.next());
        rs.close();
        cs.close();
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
            getConnection().prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
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
    public void xtestExecuteUpdateWithNoDynamicResultSets_callable()
        throws SQLException
    {
        CallableStatement cs =
            getConnection().prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 0);
        int count = cs.executeUpdate();
        assertEquals("Wrong update count.", 0, count);
        cs.close();
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
            getConnection().prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 1);
        try {
            cs.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        cs.close();
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
        stmt.getConnection().rollback();
        ResultSet tableRs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        // table should be empty after rollback
        assertFalse("Side effects from stored procedure not rolled back.",
                    tableRs.next());
        tableRs.close();
        stmt.close();
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
        stmt.getConnection().rollback();
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        // table should be empty after rollback
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        stmt.close();
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
    public void xtestRollbackStoredProcWhenExecuteQueryReturnsNothing()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(0)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        stmt.close();
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
    public void xtestRollbackStoredProcWhenExecuteQueryReturnsTooMuch()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(2)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        stmt.close();
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
    public void xtestRollbackStoredProcWhenExecuteUpdateReturnsResults()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(1)");
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        stmt.close();
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
    public void xtestRollbackStoredProcWhenExecuteQueryReturnsNothing_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            conn.prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 0);
        try {
            ResultSet rs = ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        ps.close();
        stmt.close();
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
    public void xtestRollbackStoredProcWhenExecuteQueryReturnsTooMuch_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            conn.prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 2);
        try {
            ResultSet rs = ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        ps.close();
        stmt.close();
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
        xtestRollbackStoredProcWhenExecuteUpdateReturnsResults_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            conn.prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 1);
        try {
            ps.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM SIMPLE_TABLE");
        assertFalse("Side effects from stored procedure not rolled back.",
                    rs.next());
        rs.close();
        ps.close();
        stmt.close();
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
        stmt.close();
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
        stmt.close();
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
            getConnection().prepareStatement("CALL RETRIEVE_EXTERNAL_RESULT(?,?,?)");
        
        ps.setString(1, getTestConfiguration().getDatabaseName());
        ps.setString(2, getTestConfiguration().getUserName());
        ps.setString(3, getTestConfiguration().getUserPassword());
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
        ps.close();
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
            getConnection().prepareStatement("CALL RETRIEVE_EXTERNAL_RESULT(?,?,?)");
        
        ps.setString(1, getTestConfiguration().getDatabaseName());
        ps.setString(2, getTestConfiguration().getUserName());
        ps.setString(3, getTestConfiguration().getUserPassword());
        
        ps.executeUpdate();
        ps.close();
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
     * Creates the test suite and wraps it in a <code>TestSetup</code>
     * instance which sets up and tears down the test environment.
     * @return test suite
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("ProcedureTest");
        
        // Need JDBC 2 DriverManager to run these tests
        if (JDBC.vmSupportsJDBC2()) {        	
        
        suite.addTestSuite(ProcedureTest.class);
        if (!usingDerbyNet()) {
            suite.addTest
                (new ProcedureTest
                 ("xtestExecuteUpdateWithNoDynamicResultSets"));
            suite.addTest
                (new ProcedureTest
                 ("xtestExecuteUpdateWithNoDynamicResultSets_prepared"));
            suite.addTest
                (new ProcedureTest
                 ("xtestExecuteUpdateWithOneDynamicResultSet_prepared"));
            suite.addTest
                (new ProcedureTest
                 ("xtestExecuteUpdateWithNoDynamicResultSets_callable"));
            suite.addTest
                (new ProcedureTest
                 ("xtestRollbackStoredProcWhenExecuteQueryReturnsNothing"));
            suite.addTest
                (new ProcedureTest
                 ("xtestRollbackStoredProcWhenExecuteQueryReturnsTooMuch"));
            suite.addTest
                (new ProcedureTest
                 ("xtestRollbackStoredProcWhenExecuteUpdateReturnsResults"));
            suite.addTest
                (new ProcedureTest
                 ("xtestRollbackStoredProcWhenExecuteQueryReturnsNothing" +
                  "_prepared"));
            suite.addTest
                (new ProcedureTest
                 ("xtestRollbackStoredProcWhenExecuteQueryReturnsTooMuch" +
                  "_prepared"));
            suite.addTest
                (new ProcedureTest
                 ("xtestRollbackStoredProcWhenExecuteUpdateReturnsResults" +
                  "_prepared"));
        }
        }
        Test test = new BaseJDBCTestSetup(suite) {
            /**
             * Creates the tables and the stored procedures used in the test
             * cases.
             * @exception SQLException if a database error occurs
             */
        	protected void setUp() throws SQLException {
                Connection c = getConnection();
                c.setAutoCommit(false);
                Statement s = c.createStatement();
                for (int i = 0; i < PROCEDURES.length; i++) {
                    s.execute(PROCEDURES[i][1]);
                }
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute(TABLES[i][1]);
                }
                s.close();
                c.commit();
                c.close();
            }
            /**
             * Drops the stored procedures used in the tests.
             * @exception SQLException if a database error occurs
             */
        	protected void tearDown() throws Exception {
                Connection c = getConnection();
                c.setAutoCommit(false);
                Statement s = c.createStatement();
                for (int i = 0; i < PROCEDURES.length; i++) {
                    s.execute("DROP PROCEDURE " + PROCEDURES[i][0]);
                }
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute("DROP TABLE " + TABLES[i][0]);
                }
                s.close();
                c.commit();
                c.close();
                
                super.tearDown();
            }
        };
        
        return new CleanDatabaseTestSetup(test);
    }

    /**
     * Sets up the connection for a test case and clears all tables
     * used in the test cases.
     * @exception SQLException if a database error occurs
     */
    public void setUp() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        for (int i = 0; i < TABLES.length; i++) {
            s.execute("DELETE FROM " + TABLES[i][0]);
        }
        s.close();
        conn.commit();
    }

    /**
     * Procedures that should be created before the tests are run and
     * dropped when the tests have finished. First element in each row
     * is the name of the procedure, second element is SQL which
     * creates it.
     */
    private static final String[][] PROCEDURES = {
        { "RETRIEVE_DYNAMIC_RESULTS",
          "CREATE PROCEDURE RETRIEVE_DYNAMIC_RESULTS(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveDynamicResults' " +
          "DYNAMIC RESULT SETS 4"
        },
        { "RETRIEVE_CLOSED_RESULT",
          "CREATE PROCEDURE RETRIEVE_CLOSED_RESULT() LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveClosedResult' " +
          "DYNAMIC RESULT SETS 1"
        },
        { "RETRIEVE_EXTERNAL_RESULT",
          "CREATE PROCEDURE RETRIEVE_EXTERNAL_RESULT(" +
          "DBNAME VARCHAR(128), DBUSER VARCHAR(128), DBPWD VARCHAR(128)) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveExternalResult' " +
          "DYNAMIC RESULT SETS 1"
        },
        { "PROC_WITH_SIDE_EFFECTS",
          "CREATE PROCEDURE PROC_WITH_SIDE_EFFECTS(ret INT) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".procWithSideEffects' " +
          "DYNAMIC RESULT SETS 2"
        },
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
}
