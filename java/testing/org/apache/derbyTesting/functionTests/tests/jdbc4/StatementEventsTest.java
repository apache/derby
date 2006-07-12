/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.StatementEventsTest
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.*;
import javax.sql.*;
import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.TestDataSourceFactory;
import java.util.Enumeration;

/*
    This class is used to test the JDBC4 statement event 
    support 
*/
public class StatementEventsTest extends BaseJDBCTestCase {

    /**
     * Type of data source to use. If <code>true</code>, use
     * <code>XADataSource</code>; otherwise, use
     * <code>ConnectionPoolDataSource</code>.
     */
    private boolean xa;
    /**
     * Type of statement to use. If <code>true</code>, use
     * <code>CallableStatement</code>; otherwise, use
     * <code>PreparedStatement</code>.
     */
    private boolean callable;

    /** The statement that caused the last statementClosed event. */
    private Statement closedStatement;
    /** Number of times statementClosed events have been raised. */
    private int closedCount;
    /** The statement that caused the last statementError event. */
    private Statement errorStatement;
    /** Number of times statementError events have been raised. */
    private int errorCount;

    /**
     * The pooled connection to use in the test (could also be an XA
     * connection).
     */
    private PooledConnection pooledConnection;
    /** The connection object to use in the test. */
    private Connection connection;

    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    public StatementEventsTest(String name) {
        super(name);
    }

    /**
     * Set whether the test should use <code>XADataSource</code> or
     * <code>ConnectionPoolDataSource</code>.
     *
     * @param xa if <code>true</code>, use XA
     */
    private void setXA(boolean xa) {
        this.xa = xa;
    }

    /**
     * Set whether the test should use <code>CallableStatement</code> or
     * <code>PreparedStatement</code>.
     *
     * @param callable if <code>true</code>, use callable statement; otherwise,
     * use prepared statement
     */
    private void setCallable(boolean callable) {
        this.callable = callable;
    }

    /**
     * Return the name of the test.
     *
     * @return name of the test
     */
    public String getName() {
        return super.getName() + (xa ? "_xa" : "_pooled") +
            (callable ? "_callable" : "_prepared");
    }

    // TEST SETUP

    /**
     * Set up the connection to the database and register a statement event
     * listener.
     *
     * @exception SQLException if a database error occurs
     */
    public void setUp() throws SQLException {
        if (xa) {
            XADataSource ds = TestDataSourceFactory.getXADataSource();
            pooledConnection = ds.getXAConnection();
        } else {
            ConnectionPoolDataSource ds =
                TestDataSourceFactory.getConnectionPoolDataSource();
            pooledConnection = ds.getPooledConnection();
        }
        StatementEventListener listener = new StatementEventListener() {
                public void statementClosed(StatementEvent event) {
                    closedStatement = event.getStatement();
                    closedCount++;
                }
                public void statementErrorOccurred(StatementEvent event) {
                    errorStatement = event.getStatement();
                    errorCount++;
                }
            };
        pooledConnection.addStatementEventListener(listener);
        connection = pooledConnection.getConnection();
    }

    /**
     * Free resources used in the test.
     *
     * @exception SQLException if a database error occurs
     */
    public void tearDown() throws SQLException {
        connection.close();
        pooledConnection.close();
    }

    /**
     * Return suite with all tests of the class for all combinations of
     * pooled/xa connection and prepared/callable statement.
     *
     * @return a test suite
     */
    public static Test suite() {
        TestSuite suites = new TestSuite();
        boolean[] truefalse = new boolean[] { true, false };
        for (boolean xa : truefalse) {
            for (boolean callable : truefalse) {
                suites.addTest(new Suite(xa, callable));
            }
        }
        return suites;
    }

    /**
     * Test suite class which contains all test cases in
     * <code>StatementEventsTest</code> for a given configuration.
     */
    private static class Suite extends TestSuite {
        private Suite(boolean xa, boolean callable) {
            super(StatementEventsTest.class);
            for (Enumeration e = tests(); e.hasMoreElements(); ) {
                StatementEventsTest test =
                    (StatementEventsTest) e.nextElement();
                test.setXA(xa);
                test.setCallable(callable);
            }
        }
    }

    // UTILITIES

    /**
     * Prepare a statement.
     *
     * @param sql SQL text
     * @return a <code>PreparedStatement</code> or
     * <code>CallableStatement</code> object
     * @exception SQLException if a database error occurs
     */
    private PreparedStatement prepare(String sql) throws SQLException {
        if (callable) {
            return connection.prepareCall(sql);
        }
        return connection.prepareStatement(sql);
    }

    // TEST CASES

    /**
     * Test that a close event is raised when a statement is closed.
     *
     * @exception SQLException if a database error occurs
     */
    public void testCloseEvent() throws SQLException {
        PreparedStatement ps = prepare("VALUES (1)");
        ps.close();
        assertSame("Close event raised on wrong statement.",
                   ps, closedStatement);
        assertEquals("Incorrect close count.", 1, closedCount);
    }

    /**
     * Test whether a close event is raised when a connection is
     * closed. (Client should raise a close event since the connection calls
     * <code>close()</code> on its statements. Embedded should not raise a
     * close event since the connection does not call <code>close()</code> on
     * its statements.)
     *
     * @exception SQLException if a database error occurs
     */
    public void testCloseEventOnClosedConnection() throws SQLException {
        PreparedStatement ps = prepare("VALUES (1)");
        connection.close();
        if (usingDerbyNetClient()) {
            assertSame("Close event raised on wrong statement.",
                       ps, closedStatement);
            assertEquals("Incorrect close count.", 1, closedCount);
        } else if (usingEmbedded()) {
            assertNull("Didn't expect close event.", closedStatement);
            assertEquals("Incorrect close count.", 0, closedCount);
        } else {
            fail("Unknown framework.");
        }
    }

    /**
     * Test that an error event is raised when <code>execute()</code> fails
     * because the connection is closed.
     *
     * @exception SQLException if a database error occurs
     */
    public void testErrorEventOnClosedConnection() throws SQLException {
        PreparedStatement ps = prepare("VALUES (1)");
        connection.close();
        try {
            ps.execute();
            fail("No exception thrown.");
        } catch (SQLException e) {
            assertSQLState("Unexpected SQL state.", "08003", e);
            assertSame("Error event raised on wrong statement.",
                       ps, errorStatement);
            assertEquals("Incorrect error count.", 1, errorCount);
        }
    }
}
