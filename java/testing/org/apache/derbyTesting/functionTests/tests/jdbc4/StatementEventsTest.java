/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.StatementEventsTest
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.*;
import java.util.Arrays;
import javax.sql.*;
import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

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
            XADataSource ds = J2EEDataSource.getXADataSource();
            J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");
            pooledConnection = ds.getXAConnection();
        } else {
            ConnectionPoolDataSource ds =
                J2EEDataSource.getConnectionPoolDataSource();
            J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");
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
     * @exception Exception if an error occurs
     */
    protected void tearDown() throws Exception {
        connection.close();
        pooledConnection.close();
        connection = null;
        pooledConnection = null;
        closedStatement = null;
        errorStatement = null;
        super.tearDown();
    }

    /**
     * Return suite with all tests of the class for all combinations of
     * pooled/xa connection and prepared/callable statement.
     *
     * @param name name of the test suite
     * @return a test suite
     */
    private static Test baseSuite(String name) {
        TestSuite suites = new TestSuite(name);
        boolean[] truefalse = new boolean[] { true, false };
        for (boolean xa : truefalse) {
            for (boolean callable : truefalse) {
                suites.addTest(new Suite(xa, callable));
            }
        }
        return suites;
    }

    /** Create a test suite with all tests in the class. */
    public static Test suite() {
        TestSuite suite = new TestSuite("StatementEventsTest suite");
        suite.addTest(baseSuite("StatementEventsTest:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
              baseSuite("StatementEventsTest:client")));
        return suite;
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

    /**
     * Test that removing a listener from a listener works. (DERBY-3401)
     */
    public void testRemoveListenerFromListener() throws SQLException {

        // First element is number of times the close listeners below have
        // been triggered, second element is number of times the error
        // listeners have been triggered.
        final int[] counters = new int[2];

        // Add some listeners that remove themselves
        for (int i = 0; i < 5; i++) {
            StatementEventListener close = new StatementEventListener() {

                public void statementClosed(StatementEvent event) {
                    pooledConnection.removeStatementEventListener(this);
                    counters[0]++;
                }

                public void statementErrorOccurred(StatementEvent event) {
                }
            };
            pooledConnection.addStatementEventListener(close);

            StatementEventListener error = new StatementEventListener() {

                public void statementClosed(StatementEvent event) {
                }

                public void statementErrorOccurred(StatementEvent event) {
                    pooledConnection.removeStatementEventListener(this);
                    counters[1]++;
                }
            };
            pooledConnection.addStatementEventListener(error);
        }

        // Generate close event twice. The close listeners remove themselves
        // in the first iteration, so no updates of the counters are expected
        // in the second iteration.
        for (int i = 0; i < 2; i++) {
            prepare("VALUES (1)").close();
            assertEquals("unexpected number of close events", 5, counters[0]);
            assertEquals("unexpected number of error events", 0, counters[1]);
        }

        // reset counters
        Arrays.fill(counters, 0);

        // Generate error event twice. Only expect counters to be updated in
        // the first iteration since the listeners remove themselves.
        for (int i = 0; i < 2; i++) {
            PreparedStatement ps = prepare("VALUES (1)");
            connection.close();
            try {
                ps.execute();
                fail("Execute on closed connection should fail");
            } catch (SQLNonTransientConnectionException e) {
                assertSQLState("08003", e);
            }
            assertEquals("unexpected number of close events", 0, counters[0]);
            assertEquals("unexpected number of error events", 5, counters[1]);
            connection = pooledConnection.getConnection();
        }

        // The listeners that are automatically added for all test cases have
        // been active all the time.
        assertEquals("Incorrect error count", 2, errorCount);
        // Embedded doesn't receive close events when the connection is
        // closed, whereas the client driver does. This is therefore an
        // expected difference.
        if (usingEmbedded()) {
            assertEquals("Incorrect close count", 2, closedCount);
        } else if (usingDerbyNetClient()) {
            assertEquals("Incorrect close count", 4, closedCount);
        } else {
            fail("unknown framework");
        }
    }

    /**
     * Test that adding a listener from a listener works. (DERBY-3401)
     */
    public void testAddListenerFromListener() throws SQLException {

        // First element is number of times the close listeners below have
        // been triggered, second element is number of times the error
        // listeners have been triggered. Third element is the number of
        // times listeners added by close listeners have been triggered,
        // fourth element is the number of times listeners added by error
        // listeners have been triggered.
        final int[] counters = new int[4];

        // Add some listeners that add another listener
        for (int i = 0; i < 5; i++) {
            StatementEventListener close = new StatementEventListener() {

                public void statementClosed(StatementEvent event) {
                    counters[0]++;
                    pooledConnection.addStatementEventListener(
                            new StatementEventListener() {
                        public void statementClosed(StatementEvent e) {
                            counters[2]++;
                        }
                        public void statementErrorOccurred(StatementEvent e) {
                            counters[2]++;
                        }
                    });
                }

                public void statementErrorOccurred(StatementEvent event) {
                }
            };

            pooledConnection.addStatementEventListener(close);

            StatementEventListener error = new StatementEventListener() {

                public void statementClosed(StatementEvent event) {
                }

                public void statementErrorOccurred(StatementEvent event) {
                    counters[1]++;
                    pooledConnection.addStatementEventListener(
                            new StatementEventListener() {
                        public void statementClosed(StatementEvent e) {
                            counters[3]++;
                        }
                        public void statementErrorOccurred(StatementEvent e) {
                            counters[3]++;
                        }
                    });
                }
            };

            pooledConnection.addStatementEventListener(error);
        }

        // Generate close event
        prepare("VALUES (1)").close();
        assertEquals("unexpected number of close events", 5, counters[0]);
        assertEquals("unexpected number of error events", 0, counters[1]);
        assertEquals("unexpected number of added close listeners triggered",
                     0, counters[2]);
        assertEquals("unexpected number of added error listeners triggered",
                     0, counters[3]);

        // Generate another close event
        prepare("VALUES (1)").close();
        assertEquals("unexpected number of close events", 10, counters[0]);
        assertEquals("unexpected number of error events", 0, counters[1]);
        assertEquals("unexpected number of added close listeners triggered",
                     5, counters[2]);
        assertEquals("unexpected number of added error listeners triggered",
                     0, counters[3]);

        // Generate a statement that doesn't work
        PreparedStatement ps = prepare("VALUES (1)");
        connection.close();
        // reset counters
        Arrays.fill(counters, 0);

        // Generate an error event
        try {
            ps.execute();
            fail("Execute on closed connection should fail");
        } catch (SQLNonTransientConnectionException e) {
            assertSQLState("08003", e);
        }

        assertEquals("unexpected number of close events", 0, counters[0]);
        assertEquals("unexpected number of error events", 5, counters[1]);
        // difference between embedded and client because client gets
        // statement-closed event when the connection is closed, whereas
        // embedded doesn't
        assertEquals("unexpected number of added close listeners triggered",
                     usingEmbedded() ? 10 : 15, counters[2]);
        assertEquals("unexpected number of added error listeners triggered",
                     0, counters[3]);

        // reset counters
        Arrays.fill(counters, 0);

        // Generate another error event, now with more listeners active
        try {
            ps.execute();
            fail("Execute on closed connection should fail");
        } catch (SQLNonTransientConnectionException e) {
            assertSQLState("08003", e);
        }

        assertEquals("unexpected number of close events", 0, counters[0]);
        assertEquals("unexpected number of error events", 5, counters[1]);
        // difference between embedded and client because client gets
        // statement-closed event when the connection is closed, whereas
        // embedded doesn't
        assertEquals("unexpected number of added close listeners triggered",
                     usingEmbedded() ? 10 : 15, counters[2]);
        assertEquals("unexpected number of added error listeners triggered",
                     5, counters[3]);

        // The listeners that are automatically added for all test cases have
        // been active all the time.
        assertEquals("Incorrect error count", 2, errorCount);
        // Embedded doesn't receive close events when the connection is
        // closed, whereas the client driver does. This is therefore an
        // expected difference.
        if (usingEmbedded()) {
            assertEquals("Incorrect close count", 2, closedCount);
        } else if (usingDerbyNetClient()) {
            assertEquals("Incorrect close count", 3, closedCount);
        } else {
            fail("unknown framework");
        }
    }

}
