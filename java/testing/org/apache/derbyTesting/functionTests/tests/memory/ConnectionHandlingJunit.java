/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory.ConnectionHandlingJunit
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
package org.apache.derbyTesting.functionTests.tests.memory;

import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

/**
 *  This class tests Derby's ability to handle multiple connection attempts
 *  against one or more databases, which may or may not exist. Such repeated
 *  connection attempts have been known to cause OutOfMemoryErrors in the past,
 *  see for example DERBY-2480.
 */
public class ConnectionHandlingJunit extends BaseJDBCTestCase {

    /**
     * Returns a log writer that discards all the data written to it.
     *
     * @return Writer discarding the log.
     */
    public static Writer getLogDiscarder() {
        // Writer discarding all data written to it.
        return new Writer() {
            public void write(char[] cbuf, int off, int len) {
                // Do nothing.
            }

            public void flush() {
                // Do nothing.
            }

            public void close() {
                // Do nothing.
            }
        };
    }

    /** Creates a new instance of this test class 
     *  @param name The name of this test instance; may determine which test
     *         fixture to run.
     */
    public ConnectionHandlingJunit(String name) {
        super(name);
    }
    
    /**
     * Creates a Test Suite to be run by a JUnit TestRunner. The elements of
     * the test suite may depend on the environment in which the TestRunner
     * is running (classpath, JVM, etc.).
     *
     * @return JUnit test suite containing appropriate tests from this class.
     */
    public static Test suite() {
        
        BaseTestSuite suite = new BaseTestSuite("ConnectionHandlingJUnit");
        
        // Only support for java.sql.DriverManager has been implemented.
        if (JDBC.vmSupportsJDBC3()) {
            /* driverMgrTestConnectionsToNonexistentDb:
             * Only support for DriverManager (JDBC2 and above) for now, for
             * simplicity (connecting to DB and possibly also loading the driver
             * manually, to have control over url attributes (database 
             * creation)). This adheres to the following advice in 
             * ...derbyTesting.junit.Connector.java:
             * "Tests that need finer control over the connection handling
             * should use the JDBC classes directly, such as DriverManager
             * or DataSource."
             */

            TestCase nonExistentDbTest = new ConnectionHandlingJunit(
                    "driverMgrTestConnectionsToNonexistentDb");
            TestCase nonExistentDbTestInMem = new ConnectionHandlingJunit(
                    "driverMgrTestConnectionsToNonexistentDbInMemory");
            
            /* run "driverMgrTestConnectionsToNonexistentDb" in embedded mode only
             * by default, since it is not very useful to continue running in
             * client/server mode (server in the same JVM) if the JVM's memory 
             * resources are <i>almost</i> exhausted from the embedded test.
             */
            suite.addTest(nonExistentDbTest);
            suite.addTest(nonExistentDbTestInMem);
            // to run the test in client/server mode, comment the above line,
            // uncomment the next and recompile.
            //suite.addTest(TestConfiguration.clientServerDecorator(nonExistentDbTest));
            
        }
        
        return suite;
    }    

    
    /**
     * <p>This fixture tries a number of times to connect to a non-existent
     * database, in order to test Derby's ability to handle this situation
     * without running out of resources (for example Java heap space (memory)).
     * See 
     * <a href="https://issues.apache.org/jira/browse/DERBY-2480">DERBY-2480</a>
     * for details.</p>
     * <p>This test fixture is currently not part of any large JUnit suite
     * because <b>1)</b> the default number of connection attempts is rather
     * large, and takes some time to complete (depending on hardware), and 
     * <b>2)</b> if the tested Derby version is still vulnerable to DERBY-2480
     * or similar issues the JVM will most likely run out of memory (depending
     * on heap settings), causing subsequent tests to fail, hang or not run at
     * all.</p>
     * <p>
     * <b>Note:</b> The JVM may slow down significantly (even appear to hang)
     * before an OOME is thrown. Depending on the avaliable resources, the error
     * may or may not be reported in the logs (derby.log, server console).</p>
     * <p>
     * This fixture requires java.sql.DriverManager. This is because simple
     * and easy control of the connection handling and database creation is 
     * desired (see implementation comments). However, the test logic itself 
     * should also work with other connection mechanisms.</p>
     *
     * @throws SQLException if an unexpected exception occurs that is not
     *         examined using assertions.
     */
    public void driverMgrTestConnectionsToNonexistentDb() throws SQLException {
        String url = getTestConfiguration().getJDBCUrl("nonexistentDatabase");
        driverMgrConnectionInitiator(url, false);
    }

    public void driverMgrTestConnectionsToNonexistentDbInMemory()
            throws SQLException {
        driverMgrConnectionInitiator("jdbc:derby:memory:noDbHere", true);
    }

    private void driverMgrConnectionInitiator(String url, boolean appendId)
            throws SQLException {
        Connection myInvalidConn = null;
        // Not using the regular helper methods in super class because
        // we don't want to actually create a database, or connect to an
        // existing one (current helper classes add ";create=true" if the DB
        // does not exist). Hence, the JDBC driver will not necessarily be 
        // loaded automatically.
        loadDriver(url);
        
        Runtime runtime = Runtime.getRuntime();
        double memTotalNow; // Total amount of heap memory committed to this JVM
        
        // ~110k attempts is enough for a 64 MB heap (prior to DERBY-2480 fix)
        int maxCount = 130000;  // max number of connection attempts to try
        int count = 0;  // number of connection attempts so far
        
        println("Trying " + maxCount + " connection attempts...");
        
        try {
            while (count < maxCount) {

                try {
                    // We are expecting an exception here because we are trying to 
                    // connect to a DB that does not exist.
                    myInvalidConn = DriverManager.getConnection(
                            appendId ? url + count : url);
                    // The following may happen because of changes to helper methods
                    // such as TestConfiguration.getJDBCUrl(dbName).
                    fail("Got connection to a DB that should not exist");
                } catch (SQLException e) {
                    // Expected SQLState for "database not found"...
                    // For the client driver the generic 08004 is returned.
                    String expectedState;
                    if (getTestConfiguration().getJDBCClient().isEmbedded()) {
                        // embedded driver
                        expectedState = "XJ004";
                        // in embedded mode, OOMEs are usually wrapped in 
                        // SQLExceptions with SQLState 08004
                        if (e.getSQLState().equals("08004") 
                                && e.getMessage().matches(".*OutOfMemoryError.*")) {
                            alarm("OutOfMemoryError after " + count 
                                    + " connection attempts to a "
                                    + "non-existing database!");
                            // test should fail on next assertSQLState call,
                            // but may not have enough resources to get that far
                            printStackTrace(e);
                            //fail("OutOfMemoryError: " + e.getMessage());
                        }
                    } else {
                        // client driver
                        expectedState = "08004";
                        // with client driver, OOMEs are often wrapped in
                        // SQLExceptions with SQLState XJ001
                        if (e.getSQLState().equals("XJ001") 
                                && e.getMessage().matches(".*OutOfMemoryError.*")) {
                            alarm("OutOfMemoryError after " + count 
                                    + " connection attempts to a "
                                    + "non-existing database!");
                            // test should fail on next assertSQLState call,
                            // but may not have enough resources to do so
                            printStackTrace(e);
                        }
                    }
                    assertSQLState("Wrong SQLState for non-existent database", 
                            expectedState, e);
                }

                count++;
                // print informational messages (mem) if debug property is true
                if (getTestConfiguration().isVerbose()) {
                    if (count % 1000 == 0) {
                        memTotalNow = runtime.totalMemory()/(double)(1024*1024);
                        println("Iteration: " + count + "\tTotal memory (MB): " 
                                + memTotalNow);
                    }
                }
            }
        } catch(OutOfMemoryError oome) {
            alarm("OutOfMemory after " + count + " connection attempts!");
            alarm(oome.getMessage());
            throw oome;
        }
    }
    
    /**
     * Will check if the JDBC driver has been loaded and load it if that is not 
     * the case.
     * Any other exception messages than "No suitable driver" on the first
     * attempt to get the JDBC driver will result in an assertion failure.
     * 
     * @param url a valid connection URL for the desired JDBC driver
     * @throws SQLException if an unexpected exception is thrown
     */
    private void loadDriver(String url) throws SQLException {
        // Attempt to make Derby discard the log, as a log message will be
        // written for every failed connection attempt.
        // To take effect, the property must be set before the driver is
        // loaded, which means this test should be run separately.
        setSystemProperty("derby.stream.error.method",
                "org.apache.derbyTesting.functionTests.tests.memory." +
                "ConnectionHandlingJunit.getLogDiscarder");
        try {
            DriverManager.getDriver(url);
        } catch (SQLException e) {
            // getDriver() failed, JDBC driver probably not loaded.
            // Expecting SQLState 08001 and message "No suitable driver"...
            assertSQLState("Unexpected SQLState from getDriver().", "08001", e);
            assertEquals("Unexpected exception message from getDriver(), ",
                    "No suitable driver", e.getMessage());
            String driverClass = 
                    getTestConfiguration().getJDBCClient().getJDBCDriverName();
            println("Loading JDBC driver " + driverClass);
            // load the driver
            try {
                Class<?> clazz = Class.forName(driverClass);
                clazz.getConstructor().newInstance();
            } catch (ClassNotFoundException cnfe) {
                throw new SQLException("Failed to load JDBC driver '" 
                        + driverClass + "', ClassNotFoundException: " 
                        + cnfe.getMessage());
            } catch (IllegalAccessException iae) {
                throw new SQLException("Failed to load JDBC driver '" 
                        + driverClass + "', IllegalAccessException: " 
                        + iae.getMessage());
            } catch (InstantiationException ie) {
                throw new SQLException("Failed to load JDBC driver '" 
                        + driverClass + "', InstantiationException: " 
                        + ie.getMessage());
            } catch (NoSuchMethodException ie) {
                throw new SQLException("Missing constructor for JDBC driver '" 
                        + driverClass + "', NoSuchMethodException: " 
                        + ie.getMessage());
            } catch (java.lang.reflect.InvocationTargetException ie) {
                throw new SQLException("Could not invoke the constructor for JDBC driver '" 
                        + driverClass + "', InvocationTargetException: " 
                        + ie.getMessage());
            }
        }
    }

}
