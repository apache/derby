/*
 *
 * Derby - Class StatementTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.*;

import java.sql.*;

/**
 * Tests for new methods added for Statement in JDBC4.
 */
public class StatementTest
    extends BaseJDBCTestCase {

   /** Default statement used by the tests. */
    private Statement stmt = null;
    
    /**
     * Create a new test with the given name.
     *
     * @param name name of the test.
     */
    public StatementTest(String name) {
        super(name);
    }

    /**
     * Create default connection and statement.
     *
     * @throws SQLException if setAutoCommit, createStatement or 
     *                      BaseJDBCTestCase.getConnection fails.
     */
    protected void setUp() 
        throws SQLException {
        getConnection().setAutoCommit(false);
        // Create a default statement.
        stmt = createStatement();
        assertFalse("First statement must be open initially", 
                stmt.isClosed());
    }

    /**
     * Close default connection and statement if necessary.
     *
     * @throws SQLException if a database access exception occurs.
     */
    protected void tearDown() 
        throws Exception {
        // Close default statement
        if (stmt != null) {
            stmt.close();
            stmt = null;
        }

        super.tearDown();
    }

    /**
     * Check that <code>isClosed</code> returns <code>true</code> after
     * the statement has been explicitly closed.
     */
    public void testIsClosedBasic()
        throws SQLException {
        ResultSet rs = stmt.executeQuery("select count(*) from stmtTable");
        assertFalse("Statement should still be open", stmt.isClosed());
        rs.close();
        assertFalse("Statement should be open after ResultSet has been " +
                "closed", stmt.isClosed());
        stmt.close();
        assertTrue("Statement should be closed, close() has been called", 
                stmt.isClosed());
    }
    
    /**
     * Test that creating two statements on the same connection does not
     * cause side effects on the statements.
     */
    public void testIsClosedWithTwoStatementsOnSameConnection()
        throws SQLException {
        // Create a second statement on the default connection.
        Statement stmt2 = createStatement();
        assertFalse("Second statement must be open initially", 
                stmt2.isClosed());
        assertFalse("First statement should not be closed when " +
                "creating a second statement", stmt.isClosed());
        ResultSet rs = stmt2.executeQuery("select count(*) from stmtTable");
        assertFalse("Second statement should be open after call to " +
                "execute()", stmt2.isClosed());
        assertFalse("First statement should be open after call to " +
                "second statment's execute()", stmt.isClosed());
        stmt2.close();
        assertTrue("Second statement should be closed, close() has " +
                "been called!", stmt2.isClosed());
        assertFalse("First statement should be open after call to " +
                "second statment's close()", stmt.isClosed());
    }

    /**
     * Test that the two statements created on the connection are closed
     * when the connection itself is closed.
     */
    public void testIsClosedWhenClosingConnection()
        throws SQLException {
        // Create an extra statement for good measure.
        Statement stmt2 = createStatement();
        assertFalse("Second statement must be open initially",
                stmt2.isClosed());
        // Exeute something on it, as opposed to the default statement.
        stmt2.execute("select count(*) from stmtTable");
        assertFalse("Second statement should be open after call to " +
                "execute()", stmt2.isClosed());
        // Close the connection. We must commit/rollback first, or else a
        // "Invalid transaction state" exception is raised.
        rollback();
        Connection con = getConnection();
        con.close();
        assertTrue("Connection should be closed after close()", 
                con.isClosed());
        assertTrue("First statement should be closed, as parent " +
                "connection has been closed", stmt.isClosed());
        assertTrue("Second statement should be closed, as parent " +
                "connection has been closed", stmt2.isClosed());
    }
    
    /**
     * Check the state of the statement when the connection is first attempted
     * closed when in an invalid transaction state, then closed after a
     * commit. According to the JDBC 4 API documentation: </i>"It is strongly 
     * recommended that an application explictly commits or rolls back an 
     * active transaction prior to calling the close method. If the close 
     * method is called and there is an active transaction, 
     * the results are implementation-defined."</i>
     * Derby throws an exception and keeps the connection open.
     */
    public void testIsClosedWhenClosingConnectionInInvalidState()
        throws SQLException {
        stmt.executeQuery("select count(*) from stmtTable");
        // Connection should now be in an invalid transaction state.
        Connection con = stmt.getConnection();
        try {
            con.close();
            fail("Invalid transaction state exception was not thrown");
        } catch (SQLException sqle) {
            // The SQL State is incorrect in the embedded client, see
            // JIRA id DERBY-1168
            String expectedState;
            if ( this.usingDerbyNetClient() )
                expectedState = SQLStateConstants.INVALID_TRANSACTION_STATE_ACTIVE_SQL_TRANSACTION;
            else
                expectedState = SQLStateConstants.INVALID_TRANSACTION_STATE_NO_SUBCLASS;
            
            if ( ! expectedState.equals(sqle.getSQLState()) )
            {
                System.err.println("ERROR: Unexpected SQL State encountered; "
                    + "got " + sqle.getSQLState() + ", expected " 
                    + expectedState +
                    ". Unexpected exception message is " + sqle.getMessage());
                
                throw sqle;
            }
        }
        assertFalse("Statement should still be open, because " +
                "Connection.close() failed", stmt.isClosed());
        assertFalse("Connection should still be open", con.isClosed());
        // Do a commit here, since we do a rollback in another test.
        con.commit();
        con.close();
        assertTrue("Connection should be closed after close()", 
                con.isClosed());
        assertTrue("Statement should be closed, because " +
                "the connection has been closed", stmt.isClosed()); 
        stmt.close();
        assertTrue("Statement should still be closed", stmt.isClosed()); 
    }
        
    /**
     * Execute a query on a statement after the parent connection has been
     * closed.
     */
    public void testStatementExecuteAfterConnectionClose() 
        throws SQLException {
        Connection con = stmt.getConnection();
        con.close();
        assertTrue("Connection should be closed after close()", 
                con.isClosed());
        try {
            stmt.executeQuery("select count(*) from stmtTable");
        } catch (SQLException sqle) {
            assertEquals("Unexpected SQL state for performing " +
                    "operations on a closed statement.",
                    SQLStateConstants.CONNECTION_EXCEPTION_CONNECTION_DOES_NOT_EXIST,
                    sqle.getSQLState());
        }
        assertTrue("Statement should be closed, because " +
                "the connection has been closed", stmt.isClosed()); 
    }

    public void testIsWrapperForStatement() throws SQLException {
        assertTrue(stmt.isWrapperFor(Statement.class));
    }

    public void testIsNotWrapperForPreparedStatement() throws SQLException {
        assertFalse(stmt.isWrapperFor(PreparedStatement.class));
    }

    public void testIsNotWrapperForCallableStatement() throws SQLException {
        assertFalse(stmt.isWrapperFor(CallableStatement.class));
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(stmt.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapStatement() throws SQLException {
        Statement stmt2 = stmt.unwrap(Statement.class);
        assertSame("Unwrap returned wrong object.", stmt, stmt2);
    }

    public void testUnwrapPreparedStatement() {
        try {
            PreparedStatement ps = stmt.unwrap(PreparedStatement.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapCallableStatement() {
        try {
            CallableStatement cs = stmt.unwrap(CallableStatement.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapResultSet() throws SQLException {
        try {
            ResultSet rs = stmt.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     * Tests isPoolable, setPoolable, and the default poolability.
     */
    public void testPoolable() throws SQLException {
        assertFalse("Statement cannot be poolable by default", 
                    stmt.isPoolable()); 
        stmt.setPoolable(true);
        assertTrue("Statement must be poolable", stmt.isPoolable());

        stmt.setPoolable(false);
        assertFalse("Statement cannot be poolable", stmt.isPoolable());
    }

    /**
     * Create test suite for StatementTest.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("StatementTest suite");
        // Decorate test suite with a TestSetup class.
        suite.addTest(new StatementTestSetup(
                        new TestSuite(StatementTest.class)));
        suite.addTest(TestConfiguration.clientServerDecorator(
            new StatementTestSetup(new TestSuite(StatementTest.class))));
        return suite;
    }
    
} // End class StatementTest
