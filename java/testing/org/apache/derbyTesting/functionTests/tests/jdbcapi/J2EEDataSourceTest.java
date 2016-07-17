/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.J2EEDataSourceTest

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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import org.apache.derby.jdbc.BasicClientDataSource40;
import org.apache.derby.jdbc.ClientConnectionPoolDataSourceInterface;
import org.apache.derby.jdbc.ClientXADataSourceInterface;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.SecurityCheck;
import org.apache.derbyTesting.functionTests.util.TestRoutines;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the ConnectionPoolDataSource and XADataSource implementations of Derby.
 * DataSource functionality common to DataSources including what is
 * supported with JSR169 is tested in DataSourceTest. 
 * 
 * Performs SecurityCheck analysis on the JDBC objects returned.
 * This is because this test returns to the client a number of
 * different implementations of Connection, Statement etc.
 * 
 * @see org.apache.derbyTesting.functionTests.util.SecurityCheck
 *
 */
public class J2EEDataSourceTest extends BaseJDBCTestCase {

    private static final String dbName = 
        TestConfiguration.getCurrent().getDefaultDatabaseName();
    
    /**
     * A hashtable of opened connections.  This is used when checking to
     * make sure connection strings are unique; we need to make sure all
     * the connections are closed when we are done, so they are stored
     * in this hashtable
     */
    protected static Hashtable<String, Connection> conns =
            new Hashtable<String, Connection>();
    
    /** The expected format of a connection string. In English:
     * "<classname>@<hashcode> (XID=<xid>), (SESSION = <sessionid>),
     *  (DATABASE=<dbname>), (DRDAID = <drdaid>)"
     */
    private static final String CONNSTRING_FORMAT = 
        "\\S+@\\-?[0-9]+.* \\(XID = .*\\), \\(SESSIONID = [0-9]+\\), " +
        "\\(DATABASE = [A-Za-z]+\\), \\(DRDAID = .*\\) "; 
    
    
    /**
     * Hang onto the SecurityCheck class while running the
     * tests so that it is not garbage collected during the
     * test and lose the information it has collected,
     * in case it should get printed out.
     */
    private final Object nogc = SecurityCheck.class;
    
    public J2EEDataSourceTest(String name) {
        super(name);
    }
    
    /**
     * Return a suite of tests that are run with a lower lock timeout.
     *
     * @param postfix suite name postfix
     * @return A suite of tests being run with a lower lock timeout.
     */
    private static Test getTimeoutSuite(String postfix) {
        BaseTestSuite suite =
            new BaseTestSuite("Lower lock timeout" + postfix);

        suite.addTest(new J2EEDataSourceTest("timeoutTestDerby1144PooledDS"));
        suite.addTest(new J2EEDataSourceTest("timeoutTestDerby1144XADS"));
        // Reduce the timeout threshold to make the tests run faster.
        return DatabasePropertyTestSetup.setLockTimeouts(suite, 3, 5);
    }
    
    /**
     * Return a suite of tests that are run with both client and embedded
     * 
     * @param postfix suite name postfix
     * @return A suite of tests to be run with client and/or embedded
     */
    private static Test baseSuite(String postfix) {
        BaseTestSuite suite =
            new BaseTestSuite("ClientAndEmbedded" + postfix);

        suite.addTest(new J2EEDataSourceTest("testGlobalLocalInterleaf"));
        suite.addTest(new J2EEDataSourceTest("testSetIsolationWithStatement"));
        suite.addTest(new J2EEDataSourceTest("testJira95pds"));
        suite.addTest(new J2EEDataSourceTest("testJira95xads"));
        suite.addTest(new J2EEDataSourceTest("testBadConnectionAttributeSyntax"));
        suite.addTest(new J2EEDataSourceTest("testCloseActiveConnection_DS"));
        suite.addTest(new J2EEDataSourceTest("testCloseActiveConnection_CP"));
        suite.addTest(
            new J2EEDataSourceTest("testCloseActiveConnection_XA_local"));
        suite.addTest(
            new J2EEDataSourceTest("testCloseActiveConnection_XA_global"));
        suite.addTest(new J2EEDataSourceTest("testDescriptionProperty"));
        suite.addTest(new J2EEDataSourceTest("testConnectionErrorEvent"));
        suite.addTest(new J2EEDataSourceTest("testIsolationWithFourConnections"));
        suite.addTest(new J2EEDataSourceTest(
                              "testConnectionEventListenerIsNull"));
        suite.addTest(new J2EEDataSourceTest("testReadOnlyToWritableTran"));
        suite.addTest(new J2EEDataSourceTest("testAutoCommitOnXAResourceStart"));
        suite.addTest(new J2EEDataSourceTest("testAllDataSources"));
        suite.addTest(new J2EEDataSourceTest("testClosedCPDSConnection"));
        suite.addTest(new J2EEDataSourceTest("testClosedXADSConnection"));
        suite.addTest(new J2EEDataSourceTest("testSetSchemaInXAConnection"));
        suite.addTest(new J2EEDataSourceTest("testPooledReuseOnClose"));
        suite.addTest(new J2EEDataSourceTest("testSchemaIsReset"));
        suite.addTest(new J2EEDataSourceTest("testSchemaIsResetWhenDeleted"));
        suite.addTest(new J2EEDataSourceTest("testDerby3799"));
        return suite;
    }

    /**
     * Return a suite of tests that are run with client only
     * 
     * @return A suite of tests being run with client only
     */
    private static Test getClientSuite() {
        BaseTestSuite suite = new BaseTestSuite("Client/Server");
        suite.addTest(new J2EEDataSourceTest("testClientDSConnectionAttributes"));
        suite.addTest(new J2EEDataSourceTest(
                "testClientTraceFileDSConnectionAttribute"));
        suite.addTest(new J2EEDataSourceTest(
                "testClientMessageTextConnectionAttribute"));
        suite.addTest(new J2EEDataSourceTest("testConnectionFlowCommit"));
        suite.addTest(new J2EEDataSourceTest("testConnectionFlowCommitAlt"));
        suite.addTest(new J2EEDataSourceTest("testDerby2026LoginTimeout"));
        // Disabled because rollback flow optimization hasn't been implemented.
        // See DERBY-4687
        //suite.addTest(new J2EEDataSourceTest("testConnectionFlowRollbackAlt"));
        return suite;
    }
    
    /**
     * Return a suite of tests that are run with embedded only
     * 
     * @param postfix suite name postfix
     * @return A suite of tests being run with embedded only
     */
    private static Test getEmbeddedSuite(String postfix) {
        BaseTestSuite suite = new BaseTestSuite("Embedded" + postfix);
        suite.addTest(new J2EEDataSourceTest("testDSRequestAuthentication"));
        // Following cannot run with client because of DERBY-2533; it hangs
        // when fixed, this can be moved to baseSuite.
        suite.addTest(new J2EEDataSourceTest("testReuseAcrossGlobalLocal"));
        suite.addTest(new J2EEDataSourceTest("testXAHoldability"));
        return suite;
    }
    
    public static Test suite() {
        if (JDBC.vmSupportsJSR169())
        {
            // test uses unsupported classes like DriverManager, XADataSource,
            // ConnectionPoolDataSource, ConnectionEvenListenere, as well as
            // unsupported methods, like Connection.setTypeMap()...
            BaseTestSuite suite =
                new BaseTestSuite("J2EEDatasourceTest cannot run with JSR169");
            return suite;
        }
        else
        {
            BaseTestSuite suite =
                new BaseTestSuite("J2EEDataSourceTest suite");

            // Add tests that will run with both embedded
            suite.addTest(baseSuite(":embedded"));
            //  and network server/client
            suite.addTest(TestConfiguration.clientServerDecorator(
                    baseSuite(":client")));
            // Add the tests that only run with client
            suite.addTest(new SupportFilesSetup(
                    TestConfiguration.clientServerDecorator(getClientSuite())));
            // Add the tests that only run with embedded
            suite.addTest(getEmbeddedSuite("embedded"));
            // Add the tests relying on getting timeouts.
            suite.addTest(getTimeoutSuite(":embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                    getTimeoutSuite(":client")));
            // wrap all in CleanDatabaseTestSetup that creates all database
            // objects any fixture might need.
            // Note that not all fixtures need (all of) these.
            return new CleanDatabaseTestSetup(suite) {
                /**
                 * Create and populate database objects
                 * 
                 * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
                 */
                protected void decorateSQL(Statement s) throws SQLException {
                    TestRoutines.installRoutines(getConnection());
                    s.executeUpdate("create table autocommitxastart(i int)");
                    s.executeUpdate("insert into autocommitxastart values 1,2,3,4,5");
                    s.executeUpdate("create schema SCHEMA_Patricio");
                    s.executeUpdate("create table " +
                    "SCHEMA_Patricio.Patricio (id VARCHAR(255), value INTEGER)");
                    s.executeUpdate("create table intTable(i int)");
                    s.executeUpdate("create table hold_30 " +
                    "(id int not null primary key, b char(30))");
                    s.executeUpdate(
                            "create procedure checkConn2(in dsname varchar(20)) " +
                            "parameter style java language java modifies SQL DATA " +
                            "external name " +
                            "'org.apache.derbyTesting.functionTests.tests.jdbcapi.J2EEDataSourceTest." +
                            getNestedMethodName() +
                    "'");
                    s.execute("create table derby3799 (dClob clob)");
                    s.executeUpdate("insert into derby3799 values (" +
                            "'myLittleTestClob')");
                }
            };
        }
    }
    
    public void tearDown() throws Exception {
        // attempt to get rid of any left-over trace files
        for (int i = 0; i < 6; i++) {
            String traceFileName = "trace" + (i + 1) + ".out";
            File traceFile = new File(traceFileName);
            if (PrivilegedFileOpsForTests.exists(traceFile)) {
                // if it exists, attempt to get rid of it
                PrivilegedFileOpsForTests.delete(traceFile);
            }
        }
        super.tearDown();
    }

    /* comment out. leaving in, just in case it's ever relevant.
     * when uncommented, this will run when network server tests are
     * started, and then reflect the results of the embedded checks.
    // perform security analysis of the public api for the embedded engine
    public void testDataSourceAPI() throws SQLException, ClassNotFoundException
    {
        SecurityCheck.report();
    }
     */
    
    /**
     * Test case for DERBY-3172
     * When the Derby engine is shutdown or Network Server is brought down, any
     * api on JDBC Connection object should generate a Connection error event.
     */
    public void testConnectionErrorEvent() throws SQLException, Exception
    {
        AssertEventCatcher aes12 = new AssertEventCatcher(12);
        
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();

        PooledConnection pc = ds.getPooledConnection();
        //Add a connection event listener to ConnectionPoolDataSource
        pc.addConnectionEventListener(aes12);
        Connection conn = pc.getConnection();
        
        dropTable(conn, "TAB1");

        //No event should have been generated at this point
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertFalse(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        //Shutdown the Derby engine or Network Server depending on what 
        //mode we are running in.
        if (usingEmbedded())
        {
        	getTestConfiguration().shutdownDatabase();
        } else
        {
        	getTestConfiguration().stopNetworkServer();
        }
        //Now try to use various apis on the JDBC Connection object created 
        //before shutdown and they all should generate connection error event.
        try {
            conn.prepareStatement("CREATE TABLE TAB1(COL1 INT NOT NULL)");
            fail("SQLException should be thrown!");
        } catch (SQLException e) {
            //The first call on JDBC Connection object after Network Server
            //shutdown will generate a communication error and that's why we
            //are checking for SQL State 08006 rather than No current connection
            //SQL State 08003. In embedded mode, we will get SQL State 08003
        	//meaning No current connection
            if (usingEmbedded())
                assertSQLState("08003", e);
            else
                assertSQLState("08006", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.prepareStatement("CREATE TABLE TAB1(COL1 INT NOT NULL)", 1);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
        	int[] columnIndexes = {1};
            conn.prepareStatement("CREATE TABLE TAB1(COL1 INT NOT NULL)", 
            		columnIndexes);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
        	String[] columnNames = {"col1"};
            conn.prepareStatement("CREATE TABLE TAB1(COL1 INT NOT NULL)", 
            		columnNames);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.prepareStatement("CREATE TABLE TAB1(COL1 INT NOT NULL)",
            		ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.prepareStatement("CREATE TABLE TAB1(COL1 INT NOT NULL)",
            		ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            		ResultSet.CLOSE_CURSORS_AT_COMMIT);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.createStatement();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
            		ResultSet.CONCUR_READ_ONLY,
            		ResultSet.CLOSE_CURSORS_AT_COMMIT);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                    ResultSet.CONCUR_READ_ONLY);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.prepareCall("CREATE TABLE TAB1(COL1 INT NOT NULL)",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.prepareCall("CREATE TABLE TAB1(COL1 INT NOT NULL)");
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.prepareCall("CREATE TABLE TAB1(COL1 INT NOT NULL)",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.nativeSQL("CREATE TABLE TAB1(COL1 INT NOT NULL)");
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getAutoCommit();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setAutoCommit(false);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getHoldability();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setHoldability(1);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.commit();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.rollback();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setSavepoint();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setSavepoint("savept1");
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.rollback((Savepoint)null);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.releaseSavepoint((Savepoint)null);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getTransactionIsolation();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getWarnings();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.clearWarnings();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getMetaData();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.isReadOnly();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setReadOnly(true);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setCatalog(null);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getCatalog();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.getTypeMap();
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        try {
            conn.setTypeMap(null);
            fail("SQLException of 08003 should be thrown!");
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();
        if (usingEmbedded())
        {
            Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            clazz.getConstructor().newInstance();
        }else
        {
        	getTestConfiguration().startNetworkServer();
        }

        // Get a new connection to the database
        conn = getConnection();
        conn.close();
    }

    /**
     * Test that event notification doesn't fail when a null listener has
     * been registered (DERBY-3307).
     */
    public void testConnectionEventListenerIsNull() throws SQLException {
        ConnectionPoolDataSource cpds =
            J2EEDataSource.getConnectionPoolDataSource();
        subtestCloseEventWithNullListener(cpds.getPooledConnection());
        subtestErrorEventWithNullListener(cpds.getPooledConnection());

        XADataSource xads = J2EEDataSource.getXADataSource();
        subtestCloseEventWithNullListener(xads.getXAConnection());
        subtestErrorEventWithNullListener(xads.getXAConnection());
    }

    /**
     * Test that notification of a close event doesn't fail when the
     * listener is null.
     */
    private void subtestCloseEventWithNullListener(PooledConnection pc)
        throws SQLException
    {
        pc.addConnectionEventListener(null);
        // Trigger a close event
        pc.getConnection().close();
        pc.close();
    }

    /**
     * Test that notification of an error event doesn't fail when the
     * listener is null.
     */
    private void subtestErrorEventWithNullListener(PooledConnection pc)
        throws SQLException
    {
        pc.addConnectionEventListener(null);
        Connection c = pc.getConnection();
        // Shut down the database to invalidate all connections
        getTestConfiguration().shutdownDatabase();
        try {
            // Should trigger an error event since the connection is no
            // longer valid
            c.prepareStatement("VALUES 1");
            fail("Statement should fail after database shutdown");
        } catch (SQLException e) {
            if (usingEmbedded()) {
                // No current connection is expected on embedded
                assertSQLState("08003", e);
            } else {
                // The client driver reports communication error
                assertSQLState("08006", e);
            }
        }
        c.close();
        pc.close();
    }

    /**
     * Test that {@code Connection.close()} behaves as expected when the
     * transaction is active (DERBY-3319).
     *
     * @param c the connection to test
     * @param autoCommit the expected auto-commit value. When auto-commit is
     * on, {@code close()} shouldn't fail when the transaction is active.
     * @param global tells whether the connection is part of a global XA
     * transaction. If it is, {@code close()} shouldn't fail, since the
     * transaction can be finished later without using the connection.
     */
    private void testCloseActiveConnection(Connection c, boolean autoCommit,
                                           boolean global)
        throws SQLException
    {
        if (global) {
            assertFalse("auto-commit should be false in XA", autoCommit);
        }
        assertEquals("auto-commit", autoCommit, c.getAutoCommit());
        Statement s = c.createStatement();
        JDBC.assertDrainResults(s.executeQuery("SELECT * FROM SYS.SYSTABLES"));
        s.close();
        try {
            c.close();
            // should not fail in auto-commit or global XA, but should fail
            // otherwise
            assertTrue("close() should fail", autoCommit || global);
        } catch (SQLException e) {
            // no exception expected in auto-commit or global XA, re-throw
            if (autoCommit || global) {
                throw e;
            }
            assertSQLState("25001", e);
        }
        if (!autoCommit && !global) {
            c.rollback();
        }
        c.close();
    }

    /**
     * Test that connections retrieved from {@code DataSource} behave as
     * expected when {@code close()} is called and the transaction is active.
     */
    public void testCloseActiveConnection_DS() throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource();
        testCloseActiveConnection(ds.getConnection(), true, false);
        Connection c = ds.getConnection();
        c.setAutoCommit(false);
        testCloseActiveConnection(c, false, false);
    }

    /**
     * Test that connections retrieved from {@code ConnectionPoolDataSource}
     * behave as expected when {@code close()} is called and the transaction is
     * active.
     */
    public void testCloseActiveConnection_CP() throws SQLException {
        ConnectionPoolDataSource ds =
            J2EEDataSource.getConnectionPoolDataSource();
        PooledConnection pc = ds.getPooledConnection();
        testCloseActiveConnection(pc.getConnection(), true, false);
        Connection c = pc.getConnection();
        c.setAutoCommit(false);
        testCloseActiveConnection(c, false, false);
        pc.close();
    }

    /**
     * Test that connections retrieved from {@code XADataSource} that are not
     * part of a global XA transaction, behave as expected when {@code close()}
     * is called and the transaction is active.
     */
    public void testCloseActiveConnection_XA_local() throws SQLException {
        XADataSource ds = J2EEDataSource.getXADataSource();
        XAConnection xa = ds.getXAConnection();
        testCloseActiveConnection(xa.getConnection(), true, false);
        Connection c = xa.getConnection();
        c.setAutoCommit(false);
        testCloseActiveConnection(c, false, false);
        xa.close();
    }

    /**
     * Test that connections retrieved from {@code XADataSource} that are part
     * of a global XA transaction, behave as expected when {@code close()} is
     * called and the transaction is active.
     */
    public void testCloseActiveConnection_XA_global()
        throws SQLException, XAException
    {
        XADataSource ds = J2EEDataSource.getXADataSource();
        XAConnection xa = ds.getXAConnection();
        XAResource xar = xa.getXAResource();
        Xid xid = new cdsXid(1, (byte) 2, (byte) 3);
        xar.start(xid, XAResource.TMNOFLAGS);
        // auto-commit is always off in XA transactions, so we expect
        // getAutoCommit() to return false without having set it explicitly
        testCloseActiveConnection(xa.getConnection(), false, true);
        Connection c = xa.getConnection();
        c.setAutoCommit(false);
        testCloseActiveConnection(c, false, true);
        xar.end(xid, XAResource.TMSUCCESS);
        xa.close();
    }

    /**
     * Test that a connection to XADataSource can be established
     * successfully while creating a database using setDatabaseName()
     * with create=true
     *  
     * @throws SQLException
     * @throws XAException
     */
    public void testCreateInDatabaseName_XA() throws SQLException, XAException
    {
    	//test with XADataSource
    	XADataSource xads = J2EEDataSource.getXADataSource();
    	String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();    	
    	J2EEDataSource.setBeanProperty(xads,"databaseName",dbName +";create=true");       
    	XAConnection xa = xads.getXAConnection();
    	Connection c = xa.getConnection();  
    	c.setAutoCommit(false); 
    	c.close();
    }
    
    /**
     * Test that a connection to PoolDataSource can be established
     * successfully while creating a database using setDatabaseName()
     * with create=true
     * 
     * @throws SQLException
     */
    
    public void testCreateInDatabaseName_Pooled() throws SQLException
    {
    	//test with PooledConnection
    	ConnectionPoolDataSource cpds = J2EEDataSource.getConnectionPoolDataSource();
    	PooledConnection pc = cpds.getPooledConnection();
    	String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
    	J2EEDataSource.setBeanProperty(cpds, "databaseName",dbName +";create=true");
    	Connection c;
    	c = pc.getConnection();
    	c.setAutoCommit(false);
    	c.close();
    	pc.close();
    }
    
    /**
     * Test that a connection to JDBCDataSource can be established
     * successfully while creating a database using setDatabaseName()
     * with create=true
     * 
     * @throws SQLException
     */
    
    public void testCreateInDatabaseName_DS() throws SQLException
    {
    	DataSource ds = JDBCDataSource.getDataSource();
    	String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
    	JDBCDataSource.setBeanProperty(ds, "databaseName", dbName +";create=true");
        Connection c = ds.getConnection();
        c.setAutoCommit(false);
        c.close();
    }
        
    
    /**
     * Test that a PooledConnection can be reused and closed
     * (separately) during the close event raised by the
     * closing of its logical connection.
     * DERBY-2142.
     * @throws SQLException 
     *
     */
    public void testPooledReuseOnClose() throws SQLException
    {
    	// PooledConnection from a ConnectionPoolDataSource
    	ConnectionPoolDataSource cpds =
    		J2EEDataSource.getConnectionPoolDataSource();
    	subtestPooledReuseOnClose(cpds.getPooledConnection());
        subtestPooledCloseOnClose(cpds.getPooledConnection());
        // DERBY-3401 - removing a callback during a close causes problems.
        subtestPooledRemoveListenerOnClose(cpds.getPooledConnection());
        subtestPooledAddListenerOnClose(cpds.getPooledConnection());

    	// PooledConnection from an XDataSource
    	XADataSource xads = J2EEDataSource.getXADataSource();
    	subtestPooledReuseOnClose(xads.getXAConnection());
        subtestPooledCloseOnClose(xads.getXAConnection());
        // DERBY-3401 - removing a callback during a close causes problems.
        subtestPooledRemoveListenerOnClose(xads.getXAConnection());
        subtestPooledAddListenerOnClose(xads.getXAConnection());
    }
    
    /**
     * Tests that a pooled connection can successfully be reused
     * (a new connection obtained from it) during the processing
     * of its close event by its listener.
     * Sections 11.2 and 12.5 of JDBC 4 specification indicate that the
     * connection can be returned to the pool when the
     * ConnectionEventListener.connectionClosed() is called.
     */
    private void subtestPooledReuseOnClose(final PooledConnection pc) throws SQLException
    {
    	final Connection[] newConn = new Connection[1];
    	pc.addConnectionEventListener(new ConnectionEventListener() {

    		/**
    		 * Mimic a pool handler that returns the PooledConnection
    		 * to the pool and then reallocates it to a new logical connection.
    		 */
			public void connectionClosed(ConnectionEvent event) {
				PooledConnection pce = (PooledConnection) event.getSource();
				assertSame(pc, pce);
				try {
					// open a new logical connection and pass
					// back to the fixture.
					newConn[0] = pce.getConnection();
				} catch (SQLException e) {
                    // Need to catch the exception here because
                    // we cannot throw a checked exception through
                    // the api method. Wrap it in a RuntimeException.
                    throw new RuntimeException(e);
				}
			}

			public void connectionErrorOccurred(ConnectionEvent event) {
			}
    		
    	});
    	
    	// Open a connection then close it to trigger the
    	// fetching of a new connection in the callback.
    	Connection c1 = pc.getConnection();
    	c1.close();
    	
    	// Fetch the connection created in the close callback
    	Connection c2 = newConn[0];
    	assertNotNull(c2);
    	
    	// Ensure the connection is useable, this hit a NPE before DERBY-2142
    	// was fixed (for embedded).
    	c2.createStatement().close();
    	
    	pc.close();
    }
    
    /**
     * Tests that a pooled connection can successfully be closed
     * during the processing of its close event by its listener.
     */
    private void subtestPooledCloseOnClose(final PooledConnection pc) throws SQLException
    {
        pc.addConnectionEventListener(new ConnectionEventListener() {

            /**
             * Mimic a pool handler that closes the PooledConnection
             * (say it no longer needs it, pool size being reduced)
             */
            public void connectionClosed(ConnectionEvent event) {
                PooledConnection pce = (PooledConnection) event.getSource();
                assertSame(pc, pce);
                try {
                    pce.close();
                } catch (SQLException e) {
                    // Need to catch the exception here because
                    // we cannot throw a checked exception through
                    // the api method. Wrap it in a RuntimeException.
                    throw new RuntimeException(e);
                }
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
            }
            
        });
        
        // Open and close a connection to invoke the logic above
        // through the callback
        pc.getConnection().close();
                
        // The callback closed the actual pooled connection
        // so subsequent requests to get a logical connection
        // should fail.
        try {
            pc.getConnection();
            fail("PooledConnection should be closed");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
    }
    
    /**
     * Tests that a listener of a pooled connection can successfully
     * remove itself during the processing of its close event by its listener.
     * Failed before DERBY-3401 was fixed.
     */
    private void subtestPooledRemoveListenerOnClose(final PooledConnection pc) throws SQLException
    {
        
        final int[] count1 = new int[1];
        pc.addConnectionEventListener(new ConnectionEventListener() {

            /**
             * Mimic a pool handler that removes the listener during
             * a logical close.
             */
            public void connectionClosed(ConnectionEvent event) {
                PooledConnection pce = (PooledConnection) event.getSource();
                assertSame(pc, pce);
                count1[0]++;
                pce.removeConnectionEventListener(this);
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
            }
            
        });
        
        // and have another listener to ensure removing one leaves
        // the other working and intact.
        final int[] count2 = new int[1];
        pc.addConnectionEventListener(new ConnectionEventListener() {

            /**
             * Mimic a pool handler that closes the PooledConnection
             * (say it no longer needs it, pool size being reduced)
             */
            public void connectionClosed(ConnectionEvent event) {             
                PooledConnection pce = (PooledConnection) event.getSource();
                assertSame(pc, pce);
                count2[0]++;
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
            }
            
        });        
        // no callback yet
        assertEquals(0, count1[0]);
        assertEquals(0, count2[0]);
        
        // Open and close a connection to invoke the logic above
        // through the callback
        pc.getConnection().close();
        
        // one callback for each
        assertEquals(1, count1[0]);
        assertEquals(1, count2[0]);
              
        // the callback (count1) that was removed is not called on the
        // second close but the second callback (count2) is called.
        pc.getConnection().close();
        assertEquals(1, count1[0]);
        assertEquals(2, count2[0]);
        
        pc.close();
    }

    /**
     * Tests that a listener of a pooled connection can successfully add
     * another listener when processing a close event. Failed before DERBY-3401
     * was fixed.
     */
    private void subtestPooledAddListenerOnClose(final PooledConnection pc)
            throws SQLException {

        // Holder for the two counts { number of times the main listener
        // has been triggered, number of times added listeners have been
        // triggered }.
        final int[] count = new int[2];

        // Register the main listener
        pc.addConnectionEventListener(new ConnectionEventListener() {

            public void connectionClosed(ConnectionEvent event) {
                assertSame(pc, event.getSource());
                count[0]++;
                // Register a new listener
                pc.addConnectionEventListener(new ConnectionEventListener() {
                    public void connectionClosed(ConnectionEvent e) {
                        assertSame(pc, e.getSource());
                        count[1]++;
                    }
                    public void connectionErrorOccurred(ConnectionEvent e) {
                    }
                });
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
            }
        });

        // Number of times we expect the added listener to have been called.
        int expectedAdded = 0;

        // Trigger some close events and check the count between each event.
        for (int i = 0; i < 5; i++) {
            assertEquals("close count (main)", i, count[0]);
            assertEquals("close count (added)", expectedAdded, count[1]);

            // In the next iteration, we expect that the number of times the
            // listeners added by the main listener have been called, has
            // increased by the number of times the main listener has been
            // called (i).
            expectedAdded = expectedAdded + i;

            // Trigger a close event
            pc.getConnection().close();
        }

        pc.close();
    }

    public void testAllDataSources() throws SQLException, Exception
    {
        Connection dmc = getConnection();
        CallableStatement cs = dmc.prepareCall("call checkConn2(?)");
        cs.setString(1,"Nested");
        try {
            cs.execute();
            fail("SQLException of 40XC0 should be thrown!");
        } catch (SQLException sqle) {
            assertSQLState("40XC0", sqle);
        }
        cs.setString(1,"Nested2");
        cs.execute();
        
        String EmptyMapValue = "OK";
        String NullMapValue = "XJ081";
        String MapMapValue = "0A000";
        Object[] expectedValues = {
            ResultSet.HOLD_CURSORS_OVER_COMMIT, "XJ010",
            2, true, false, 
            EmptyMapValue, NullMapValue, MapMapValue};

        assertConnectionOK(expectedValues, "DriverManager ", dmc);
    
        if (usingEmbedded())
            assertTenConnectionsUnique();

        DataSource dscs = JDBCDataSource.getDataSource();
        if (usingEmbedded()) 
                assertToString(dscs);

        DataSource ds = dscs;
        assertConnectionOK(expectedValues, "DataSource", ds.getConnection());
        
        ConnectionPoolDataSource dsp = 
            J2EEDataSource.getConnectionPoolDataSource();      
        
        if (usingEmbedded()) 
            assertToString(dsp);

        PooledConnection pc = dsp.getPooledConnection();
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(
                pc, "javax.sql.PooledConnection");
        }
        AssertEventCatcher aes1 = new AssertEventCatcher(1);
        pc.addConnectionEventListener(aes1);

        // DERBY-2531
        // with Network Server / DerbyNetClient, the assertConnectionOK check
        // returns a different connection object...
        assertConnectionOK(
            expectedValues, "ConnectionPoolDataSource", pc.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes1.didConnectionClosedEventHappen());
        assertFalse(aes1.didConnectionErrorEventHappen());
        aes1.resetState();
        assertConnectionOK(
            expectedValues, "ConnectionPoolDataSource", pc.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes1.didConnectionClosedEventHappen());
        assertFalse(aes1.didConnectionErrorEventHappen());
        aes1.resetState();

        XADataSource dsx = J2EEDataSource.getXADataSource();

        if (usingEmbedded())
            assertToString(dsx);

        // shutdown db and check all's still ok thereafter
        TestConfiguration.getCurrent().shutdownDatabase();

        dmc = getConnection();
        cs = dmc.prepareCall("call checkConn2(?)");
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(
                cs, "java.sql.CallableStatement");
        }
        cs.setString(1,"Nested");
        try {
            cs.execute();
            fail("SQLException of 40XC0 should be thrown!");
        } catch (SQLException sqle) {
            assertSQLState("40XC0", sqle);
        }
        cs.setString(1, "Nested2");
        cs.execute();

        XAConnection xac = dsx.getXAConnection();
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(xac, "javax.sql.XAConnection");
        }
        AssertEventCatcher aes3 = new AssertEventCatcher(3);
        xac.addConnectionEventListener(aes3);
        assertConnectionOK(
            expectedValues, "XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes3.didConnectionClosedEventHappen());
        assertFalse(aes3.didConnectionErrorEventHappen());
        aes3.resetState();
                       
        pc = dsp.getPooledConnection();
        AssertEventCatcher aes2 = new AssertEventCatcher(2);
        pc.addConnectionEventListener(aes2);
        assertConnectionOK(
            expectedValues, "ConnectionPoolDataSource", pc.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes2.didConnectionClosedEventHappen());
        assertFalse(aes2.didConnectionErrorEventHappen());
        aes2.resetState();

        // test "local" XAConnections
        xac = dsx.getXAConnection();
        AssertEventCatcher aes4 = new AssertEventCatcher(4);
        xac.addConnectionEventListener(aes4);
        assertConnectionOK(
            expectedValues, "XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes4.didConnectionClosedEventHappen());
        assertFalse(aes4.didConnectionErrorEventHappen());
        aes4.resetState();
        assertConnectionOK(
            expectedValues, "XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes4.didConnectionClosedEventHappen());
        assertFalse(aes4.didConnectionErrorEventHappen());
        aes4.resetState();
        xac.close();

        // test "global" XAConnections
        xac = dsx.getXAConnection();
        AssertEventCatcher aes5 = new AssertEventCatcher(5);
        xac.addConnectionEventListener(aes5);
        XAResource xar = xac.getXAResource();
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
           SecurityCheck.assertSourceSecurity(
                xar, "javax.transaction.xa.XAResource");
        }
        Xid xid = new cdsXid(1, (byte) 35, (byte) 47);
        xar.start(xid, XAResource.TMNOFLAGS);
        Connection xacc = xac.getConnection();
        xacc.close();
        expectedValues[0] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        if (usingEmbedded())
            expectedValues[1] = "XJ058";
        expectedValues[3] = false;
        assertConnectionOK(
            expectedValues, "Global XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();
        assertConnectionOK(
            expectedValues, "Global XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();

        xar.end(xid, XAResource.TMSUCCESS);

        expectedValues[0] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        expectedValues[3] = true;
        assertConnectionOK(expectedValues, 
            "Switch to local XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();
        assertConnectionOK(expectedValues, 
            "Switch to local XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();

        Connection backtoGlobal = xac.getConnection();

        xar.start(xid, XAResource.TMJOIN);
        expectedValues[0] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        expectedValues[3] = false;
        assertConnectionOK(expectedValues, 
            "Switch to global XADataSource", backtoGlobal);
        //Check if got connection closed event but not connection error event
        assertTrue(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();
        assertConnectionOK(expectedValues, 
            "Switch to global XADataSource", xac.getConnection());
        //Check if got connection closed event but not connection error event
        assertTrue(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();
        xar.end(xid, XAResource.TMSUCCESS);
        xar.commit(xid, true);

        xac.close();
    }
    
    public void testClosedCPDSConnection() throws SQLException, Exception {
        // verify that outstanding updates from a closed connection, obtained
        // from a ConnectionPoolDataSource, are not committed, but rolled back.
        ConnectionPoolDataSource dsp = 
            J2EEDataSource.getConnectionPoolDataSource();       
        PooledConnection pc = dsp.getPooledConnection();
        Connection c1 = pc.getConnection();
        Statement s = c1.createStatement();
        // start by deleting all rows from intTable
        s.executeUpdate("delete from intTable");
        c1.setAutoCommit(false);

        // this update should get rolled back later
        s.executeUpdate("insert into intTable values(1)");
        // this should automatically close the original connection
        c1 = pc.getConnection();

        ResultSet rs = 
            c1.createStatement().executeQuery("select count(*) from intTable");
        rs.next();
        assertEquals(0, rs.getInt(1));
        c1.close();
        
        // check connection objects are closed once connection is closed
        try {
            rs.next();
            fail("ResultSet is open for a closed connection obtained from PooledConnection");
        } catch (SQLException sqle) {
            // 08003 - No current connection; XCL16 - ResultSet not open
            if (usingEmbedded())
                assertSQLState("08003", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XCL16", sqle);
        }

        try {
            s.executeUpdate("update intTable set i = 1");
            fail("Statement is open for a closed connection " +
                "obtained from PooledConnection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }

        pc.close();
        pc = null;
        PoolReset("ConnectionPoolDataSource", dsp.getPooledConnection());
        s.close();
        rs.close();
        c1.close();
    }

    public void testClosedXADSConnection() throws SQLException, Exception {
        // verify that outstanding updates from a closed connection, obtained
        // from an XADataSource, are not committed, but rolled back.
        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac = dsx.getXAConnection();
        Connection c1 = xac.getConnection();
        Statement s = c1.createStatement();

        c1.setAutoCommit(false);

        // this update should be rolled back
        s.executeUpdate("insert into intTable values(2)");
        
        c1 = xac.getConnection();

        ResultSet rs = c1.createStatement().executeQuery(
           "select count(*) from intTable");
        rs.next();

        assertEquals(0, rs.getInt(1));

        rs.close();
        c1.close();
        xac.close();
        xac = null;

        PoolReset("XADataSource", dsx.getXAConnection());
    }

    public void testGlobalLocalInterleaf() throws SQLException, XAException {    
        // now some explicit tests for how connection state behaves
        // when switching between global transactions and local
        // and setting connection state.
        // some of this may be tested elsewhere too.

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac = dsx.getXAConnection();
        AssertEventCatcher aes6 = new AssertEventCatcher(6);
        xac.addConnectionEventListener(aes6);
        XAResource xar = xac.getXAResource();
        Xid xid = new cdsXid(1, (byte) 93, (byte) 103);

        // series 1 - Single connection object
        Connection cs1 = xac.getConnection();
        // initial local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        xar.start(xid, XAResource.TMNOFLAGS);
        // initial X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            false, false, cs1);
        cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        cs1.setReadOnly(true);
        setHoldability(cs1, false); // close cursors
        // modified X1
        boolean ReadOnly = false;
        // see DERBY-911, ReadOnly state different for Embedded/DerbyNetClient
        if (usingEmbedded())
            ReadOnly = true;
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        xar.end(xid, XAResource.TMSUCCESS);
        // the underlying local transaction/connection must pick up the
        // state of the Connection handle cs1
        // modified local:
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, ReadOnly, cs1);
        
        cs1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        cs1.setReadOnly(false);
        setHoldability(cs1, false); // close cursors

        // reset local
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);

        // now re-join the transaction, should pick up the read-only
        // and isolation level from the transaction,
        // holdability remains that of this handle.
        xar.start(xid, XAResource.TMJOIN);
        // re-join X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        xar.end(xid, XAResource.TMSUCCESS);

        // back to local - should be the same as the reset local
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        
        // test suspend/resume
        // now re-join the transaction (X1) for the second time, should pick
        // up the read-only and isolation level from the transaction,
        // holdability remains that of this handle.
        xar.start(xid, XAResource.TMJOIN);
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        
        xar.end(xid, XAResource.TMSUSPEND);
        // local after suspend
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        
        xar.start(xid, XAResource.TMRESUME);
        // resume X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        
        xar.end(xid, XAResource.TMSUCCESS);
        // back to local (second time)
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        
        cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        cs1.setReadOnly(true);
        setHoldability(cs1, true); // hold
        //Confirm - no connection closed event & connection error event
        assertFalse(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();
        cs1.close();
        //Check if got connection closed event but not connection error event
        assertTrue(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();
        
        cs1 = xac.getConnection();
        // new handle - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        cs1.close();
        //Check if got connection closed event but not connection error event
        assertTrue(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();
        
        xar.start(xid, XAResource.TMJOIN);
        cs1 = xac.getConnection();
        // re-join with new handle X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        cs1.close();
        xar.end(xid, XAResource.TMSUCCESS);
        //Check if got connection closed event but not connection error event
        assertTrue(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();

        // now get a connection (attached to a local)
        // attach to the global and commit it.
        // state should be that of the local after the commit.
        cs1 = xac.getConnection();
        cs1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        // pre-X1 commit - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, cs1);
        xar.start(xid, XAResource.TMJOIN);
        // pre-X1 commit - X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        xar.end(xid, XAResource.TMSUCCESS);
        // post-X1 end - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, cs1);
        xar.commit(xid, true);
        // post-X1 commit - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, cs1);
        //Confirm - no connection closed event & connection error event
        assertFalse(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();
        cs1.close();
        //Check if got connection closed event but not connection error event
        assertTrue(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();
    }
    
    // really part of testGlobalLocalInterLeaf:
    /**
     * @throws SQLException
     * @throws XAException
     */
    public void testSetIsolationWithStatement() 
    throws SQLException, XAException {
        // DERBY-421 Setting isolation level with SQL was not getting 
        // handled correctly 
        // Some more isolation testing using SQL and JDBC api
        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac = dsx.getXAConnection();
        AssertEventCatcher aes6 = new AssertEventCatcher(6);
        xac.addConnectionEventListener(aes6);
        XAResource xar = xac.getXAResource();
        Connection conn = xac.getConnection();
        Statement s = conn.createStatement();
        // initial local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, conn);

        // Issue setTransactionIsolation in local transaction
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        // setTransactionIsolation in local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);

        Xid xid;
        //Issue SQL to change isolation in local transaction
        s.executeUpdate("set current isolation = RR");
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            true, false, conn);

        xid = new cdsXid(1, (byte) 35, (byte) 47);
        xar.start(xid, XAResource.TMNOFLAGS);
        // 1st global (new)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);

        // local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT,
            Connection.TRANSACTION_SERIALIZABLE,
            true, false, conn);
        //Issue SQL to change isolation in local transaction
        s.executeUpdate("set current isolation = RS");
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, conn);

        // DERBY-1325 - Isolation level of local connection does not get reset after ending 
        // a global transaction that was joined/resumed if the isolation level was changed 
        // using SQL 
        xar.start(xid, XAResource.TMJOIN);
        // 1st global(existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);
        // local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, conn);
        // DERBY-1325 end test 

        Xid xid2 = new cdsXid(1, (byte) 93, (byte) 103);
        xar.start(xid2, XAResource.TMNOFLAGS);
        // 2nd global (new)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            false, false, conn);
        //DERBY-4314 create a statement(s2) and execute ddl sql
        Statement s2 = conn.createStatement();
        s2.executeUpdate("create table testglobal (i int)");
        //DERBY-4314 end test
        xar.end(xid2, XAResource.TMSUCCESS);

        xar.start(xid, XAResource.TMJOIN);
        // 1st global (existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);

        //local 
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, conn);

        xar.start(xid, XAResource.TMJOIN);
        
        // 1st global (existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        // Issue SQL to change isolation in 1st global transaction
        s.executeUpdate("set current isolation = UR");
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);

        // local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);

        xar.start(xid2, XAResource.TMJOIN);
        // 2nd global (existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            false, false, conn);
        xar.end(xid2, XAResource.TMSUCCESS);
        xar.rollback(xid2);
        // (After 2nd global rollback ) local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);

        xar.rollback(xid);
        // (After 1st global rollback) local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);
        //Confirm - no connection closed event & connection error event
        assertFalse(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();
    }

    // This test includes some short-hand descriptions of the test cases
    // left in for reference to the original non-junit test
    public void testReuseAcrossGlobalLocal() throws SQLException, XAException {

        // DERBY-2533 -
        // network server cannot run this test - it hits a protocol error
        // on tearDown. Embedded requires a database shutdown
        if (usingDerbyNetClient())
            return;
        
        int[] onetwothree = {1,2,3};
        int[] three = {3};
        int[] pspc = {1, 4}; // expected parameter count for prepared statements
        int[] cspc = {2, 12, 12}; // for callable statements
        
        // statics for testReuseAcrossGlobalLocal
        int[] StatementExpectedValues = {
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
            ResultSet.FETCH_REVERSE, 444, 713, 19, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT};
            //ResultSet.CLOSE_CURSORS_AT_COMMIT};
        int[] PreparedStatementExpectedValues = { 
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
            ResultSet.FETCH_REVERSE, 888, 317, 91,
            ResultSet.HOLD_CURSORS_OVER_COMMIT};
        int[] CallableStatementExpectedValues = { 
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
            ResultSet.FETCH_REVERSE, 999, 137, 85,
            ResultSet.HOLD_CURSORS_OVER_COMMIT};

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac = dsx.getXAConnection();
        AssertEventCatcher aes6 = new AssertEventCatcher(6);
        xac.addConnectionEventListener(aes6);
        XAResource xar = xac.getXAResource();
        Xid xid = new cdsXid(1, (byte) 103, (byte) 119);

        // now check re-use of *Statement objects across local/global 
        // connections.
        Connection cs1 = xac.getConnection();

        // ensure read locks stay around until end-of transaction
        cs1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        cs1.setAutoCommit(false);
        assertLocks(null, cs1);

        Statement sru1 = cs1.createStatement();
        sru1.setCursorName("SN1");
        sru1.executeUpdate("insert into intTable values 1,2,3");
        Statement sruBatch = cs1.createStatement();
        sruBatch.setCursorName("sruBatch");
        Statement sruState = createFloatStatementForStateChecking(
            StatementExpectedValues, cs1);
        PreparedStatement psruState = createFloatStatementForStateChecking(
            new int[] {1, 4}, PreparedStatementExpectedValues, cs1, 
            "select i from intTable where i = ?");
        CallableStatement csruState = createFloatCallForStateChecking(
            new int[] {2, 12, 12}, CallableStatementExpectedValues, cs1, 
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)");
        PreparedStatement psParams = 
            cs1.prepareStatement("select * from intTable where i > ?");
        psParams.setCursorName("params");
        psParams.setInt(1, 2);
        // Params-local-1
        resultSetQuery("params", three, psParams.executeQuery());

        sruBatch.addBatch("insert into intTable values 4");
        // sru1-local-1
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        cs1.commit(); // need to commit to switch to an global connection;

        // simple case - underlying connection is re-used for global.
        xar.start(xid, XAResource.TMNOFLAGS); 
        // Expecting downgrade because global transaction sru1-global-2 is 
        // using a statement with holdability true
        // sru1-global-2
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        sruBatch.addBatch("insert into intTable values 5");
        Statement sru2 = cs1.createStatement();
        sru2.setCursorName("OAK2");
        //sru2-global-3
        queryOnStatement("OAK2", onetwothree, cs1, sru2);
        // Expecting downgrade because global transaction sru1-global-4 is
        // using a statement with holdability true
        // sru1-global-4
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        // Global statement
        StatementExpectedValues[6] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        PreparedStatementExpectedValues[6] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        CallableStatementExpectedValues[6] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        assertStatementState(null, StatementExpectedValues ,sruState);
        // Global PreparedStatement
        assertStatementState(pspc, PreparedStatementExpectedValues, psruState);
        // Global CallableStatement
        assertStatementState(cspc, CallableStatementExpectedValues, csruState);
        // Params-global-1
        resultSetQuery("params", three, psParams.executeQuery());

        xar.end(xid, XAResource.TMSUCCESS);
        // now a new underlying connection is created
        // sru1-local-5
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        // sru2-local-6
        queryOnStatement("OAK2", onetwothree, cs1, sru2);
        sruBatch.addBatch("insert into intTable values 6,7");
        Statement sru3 = cs1.createStatement();
        sru3.setCursorName("SF3");
        // sru3-local-7
        queryOnStatement("SF3", onetwothree, cs1, sru3);
        // Two transactions should hold locks (global and the current XA);
        // LOCAL
        StatementExpectedValues[6] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        PreparedStatementExpectedValues[6] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        CallableStatementExpectedValues[6] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        assertStatementState(null, StatementExpectedValues, sruState); 
        assertStatementState(pspc, PreparedStatementExpectedValues, psruState);
        assertStatementState(cspc, CallableStatementExpectedValues, csruState);
        // Params-local-2
        resultSetQuery("params", three, psParams.executeQuery());
        assertLocks(new int[] {14,14}, cs1);
        cs1.commit();
        //Confirm - no connection closed event & connection error event
        assertFalse(aes6.didConnectionClosedEventHappen());
        assertFalse(aes6.didConnectionErrorEventHappen());
        aes6.resetState();

        // attach the XA transaction to another connection and see what happens
        XAConnection xac2 = dsx.getXAConnection();
        AssertEventCatcher aes5 = new AssertEventCatcher(5);
        xac2.addConnectionEventListener(aes5);
        XAResource xar2 = xac2.getXAResource();

        xar2.start(xid, XAResource.TMJOIN);
        Connection cs2 = xac2.getConnection();

        // these statements were generated by cs1 and thus are still
        // in a local connection.
        // sru1-local-8
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        // sru2-local-9
        queryOnStatement("OAK2", onetwothree, cs1, sru2);
        // sru3-local-10
        queryOnStatement("SF3", onetwothree, cs1, sru3);
        sruBatch.addBatch("insert into intTable values 8");
        // LOCAL 2
        assertStatementState(null, StatementExpectedValues, sruState);
        assertStatementState(pspc, PreparedStatementExpectedValues, psruState);
        assertStatementState(cspc, CallableStatementExpectedValues, csruState);

        assertLocks(new int[] {14, 12}, cs1);

        int[] updateCounts = sruBatch.executeBatch();
        int[] expectedUpdateCounts = {1, 1, 2, 1};
        // sruBatch update counts: 
        for (int i = 0; i < updateCounts.length; i++) {
            assertEquals(expectedUpdateCounts[i], updateCounts[i]);
        }
        // sruBatch
        queryOnStatement(
            "sruBatch", new int[] {1,2,3,4,5,6,7,8}, cs1, sruBatch);

        xar2.end(xid, XAResource.TMSUCCESS);
        //Confirm - no connection closed event & connection error event
        assertFalse(aes5.didConnectionClosedEventHappen());
        assertFalse(aes5.didConnectionErrorEventHappen());
        aes5.resetState();
        xac2.close();

        // allow close on already closed XAConnection
        xac2.close();
        xac2.addConnectionEventListener(null);
        xac2.removeConnectionEventListener(null);

        // test methods against a closed XAConnection and its resource
        try {
            // (DERBY-2532)
            xac2.getXAResource();
            fail("expected SQLException on closed XAConnection.getXAResource");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        try {
            xac2.getConnection();
            fail ("expected SQLException on XAConnection.getConnection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        try {
            xar2.start(xid, XAResource.TMJOIN);
            fail ("expected XAException on XAResource.TMJOIN");
        } catch (XAException xae) {
            assertXAException("XAResource.start", xae);
        }
        try {
            xar2.end(xid, XAResource.TMJOIN);
            fail ("expected XAException on XAResource.TMJOIN");
        } catch (XAException xae) {
            assertXAException("XAResource.end", xae);
        }
        try {
            xar2.commit(xid, true);
            fail ("expected XAException on XAResource.commit");
        } catch (XAException xae) {
            assertXAException("XAResource.commit", xae);
        }
        try {
            xar2.prepare(xid);
            fail ("expected XAException on XAResource.prepare");
        } catch (XAException xae) {
            assertXAException("XAResource.prepare", xae);
        }
        try {
            xar2.recover(0);
            fail ("expected XAException on XAResource.recover");
        } catch (XAException xae) {
            assertXAException("XAResource.recover", xae);
        }
        try {
            xar2.prepare(xid);
            fail ("expected XAException on XAResource.prepare");
        } catch (XAException xae) {
            assertXAException("XAResource.prepare", xae);
        }
        try {
            xar2.isSameRM(xar2);
            fail ("expected XAException on XAResource.isSameRM");
        } catch (XAException xae) {
            assertXAException("XAResource.isSameRM", xae);
        }
        
        // close everything
        cs1.rollback();
        sruState.close();
        psruState.close();
        csruState.close();
        psParams.close();
        sruBatch.close();
        sru1.close();
        sru2.close();
        sru3.close();
        cs1.close();
        cs2.close();
        xac.removeConnectionEventListener(null);
        xac.close();
        xac2.close();
        
        // but, still not enough.
        // what with all the switching between global and local transactions
        // we still have a lock open on intTable, which will interfere with
        // our tearDown efforts. Bounce the database.
        TestConfiguration.getCurrent().shutdownDatabase();
    }
    
    public void testSetSchemaInXAConnection() throws SQLException {
        // tests that set schema works correctly in an XA connection.

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac3 = dsx.getXAConnection();
        Connection conn3 = xac3.getConnection();
        Statement st3 = conn3.createStatement();
        st3.execute("SET SCHEMA SCHEMA_Patricio");
        st3.close();

        PreparedStatement ps3 = 
            conn3.prepareStatement("INSERT INTO Patricio VALUES (?, ?)");
        ps3.setString(1, "Patricio");
        ps3.setInt(2, 3);
        ps3.executeUpdate();

        assertEquals(1, ps3.getUpdateCount());
        ps3.close();
        conn3.close();
        xac3.close();
    }

    /**
     * Verifies that the schema is reset when creating a new logical connection.
     * <p>
     * The test is run in a non-statement pooling configuration first,
     * and then with statement pooling enabled if the environment supports it.
     * <p>
     * Relevant Jira issue: DERBY-3690.
     * <p>
     * The current role also needs to be reset, but can't be tested here since
     * we need to run with SQL authorization.
     *
     * @see org.apache.derbyTesting.functionTests.tests.lang.RolesTest#testCurrentRoleIsReset
     *
     * @throws SQLException if something goes wrong
     */
    public void testSchemaIsReset()
            throws SQLException {
        final String userSchema = "USERSCHEMA";
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpDs, "createDatabase", "create");
        // Connect with a user specified, which should cause the schema to be
        // set to the user name.
        // Test without statement pooling first.
        doTestSchemaIsReset(cpDs.getPooledConnection(userSchema, "secret"),
                userSchema);

        // Try to enable statement pooling.
        // This is currently only implemented in the client driver.
        if (usingDerbyNetClient()) {
            J2EEDataSource.setBeanProperty(
                    cpDs, "maxStatements",7);
            doTestSchemaIsReset(cpDs.getPooledConnection(userSchema, "secret"),
                    userSchema);
        }
    }

    /**
     * Executes a test sequence to make sure the schema (and with DERBY-4551,
     * current user) is correctly reset between logical connections.
     *
     * @param pc pooled connection to get logical connections from
     * @param userSchema name of the default schema for the connection (user)
     * @throws SQLException if something goes wrong...
     */
    private void doTestSchemaIsReset(PooledConnection pc, String userSchema)
            throws SQLException {
        Connection con1 = pc.getConnection();
        JDBC.assertCurrentSchema(con1, userSchema);
        JDBC.assertCurrentUser(con1, userSchema);
        Statement stmt1 = con1.createStatement();
        // Change the schema.
        stmt1.execute("set schema APP");
        stmt1.close();
        JDBC.assertCurrentSchema(con1, "APP");
        JDBC.assertCurrentUser(con1, userSchema);
        // Close the logical connection and get a new one.
        con1.close();
        Connection con2 = pc.getConnection();
        // Make sure the schema has been reset from APP to the user name.
        JDBC.assertCurrentSchema(con2, userSchema);
        JDBC.assertCurrentUser(con2, userSchema);
        con2.close();
        // Try a third time, but don't change the schema now.
        Connection con3 = pc.getConnection();
        JDBC.assertCurrentSchema(con3, userSchema);
        JDBC.assertCurrentUser(con3, userSchema);
        con3.close();
        pc.close();
    }

    /**
     * Tests that deleting the current / default schema doesn't cause the next
     * logical connection to fail.
     * <p>
     * Relevant Jira issue: DERBY-3690.
     *
     * @throws SQLException if something goes wrong
     */
    public void testSchemaIsResetWhenDeleted()
            throws SQLException {
        final String userSchema = "AUSER";
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpDs, "createDatabase", "create");
        PooledConnection pc = cpDs.getPooledConnection(userSchema, "secret");
        // Get first connection, create a table, then drop schema.
        Connection con = pc.getConnection();
        JDBC.assertCurrentSchema(con, userSchema);
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table schematest (id int)");
        stmt.executeUpdate("drop table schematest");
        stmt.executeUpdate("drop schema " + userSchema + " restrict");
        stmt.close();
        con.close();
        // Get second connection.
        con = pc.getConnection();
        JDBC.assertCurrentSchema(con, userSchema);
        stmt = con.createStatement();
        stmt.executeUpdate("create table schematest (id int)");
        stmt.executeUpdate("drop table schematest");
        stmt.close();
        JDBC.assertCurrentSchema(con, userSchema);
        con.close();
        pc.close();
    }
    /**
     * check whether commit without statement will flow by checking its transaction id
     * on client. This test is run only for client where commits without an
     * active transactions will not flow to the server.
     * DERBY-4653
     * 
     * @throws SQLException
     **/
    public void testConnectionFlowCommit()
            throws SQLException {
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();

        PooledConnection pc = ds.getPooledConnection();
        Connection conn = pc.getConnection();

        testConnectionFlowCommitWork(conn);
        conn.close();
        
        //Test for XADataSource
        XADataSource xs = J2EEDataSource.getXADataSource();
        XAConnection xc = xs.getXAConnection();
        conn = xc.getConnection();
        testConnectionFlowCommitWork(conn);
        conn.close();
        
        //Test for DataSource
        DataSource jds = JDBCDataSource.getDataSource();
        conn = jds.getConnection();
        testConnectionFlowCommitWork(conn);
        conn.close();       
    }

    /**
     * DERBY-2026 - Make sure login timeout does not impact 
     * queries.
     */
    public void testDerby2026LoginTimeout() throws SQLException {
        DataSource jds = null;
        try {
            jds = JDBCDataSource.getDataSource();
            jds.setLoginTimeout(10);
            Connection conn = jds.getConnection();
            CallableStatement cs = conn.prepareCall("CALL TESTROUTINE.SLEEP(20000)");
            cs.execute();
            //rollback to make sure our connection is ok.
            conn.rollback();
        } finally {
            if (jds != null)
                jds.setLoginTimeout(0);
        }

        ConnectionPoolDataSource cpds = null;
        try {
            cpds = J2EEDataSource.getConnectionPoolDataSource();        
            cpds.setLoginTimeout(10);
            PooledConnection pc = cpds.getPooledConnection();
            Connection conn = pc.getConnection();
            CallableStatement cs = conn.prepareCall("CALL TESTROUTINE.SLEEP(20000)");
            cs.execute();
            //rollback to make sure our connection is ok.
            conn.rollback();

            // Close the logical connection and get a new one.
            // This will invoke reset which also needs its timeout reset
            conn.close();
            conn = pc.getConnection();
            cs = conn.prepareCall("CALL TESTROUTINE.SLEEP(20000)");
            cs.execute();
            //rollback to make sure our connection is ok.
            conn.rollback();
        } finally {
            if (cpds != null)
                cpds.setLoginTimeout(0);
        }

        XADataSource xads = null;
        try {
            xads = J2EEDataSource.getXADataSource();        
            xads.setLoginTimeout(10);
            XAConnection xac = xads.getXAConnection();
            Connection conn = xac.getConnection();
            CallableStatement cs = conn.prepareCall("CALL TESTROUTINE.SLEEP(20000)");
            cs.execute();
            //rollback to make sure our connection is ok.
            conn.rollback();

            // Close the logical connection and get a new one.
            // This will invoke reset which also needs its timeout reset
            conn.close();
            conn = xac.getConnection();
            cs = conn.prepareCall("CALL TESTROUTINE.SLEEP(20000)");
            cs.execute();
            //rollback to make sure our connection is ok.
            conn.rollback();
        } finally {
            if (xads != null)
                xads.setLoginTimeout(0);
        }
    }
    
    /**
     * Doing the work for test Connection.flowcommit() and Connection.flowrollback()
     * @param conn
     * @throws SQLException
     **/  
    private void testConnectionFlowCommitWork(Connection conn) throws SQLException {
        //DERBY 4653 - make sure commit with no work does not flow in client
        conn.setAutoCommit(false);
        int startXactId = getClientTransactionID(conn);
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("values 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();
        conn.commit();
        // second commit should not flow
        conn.commit();
        int endXactId = getClientTransactionID(conn);
        
        //to verify the fix for DERBY-4653. Only one transaction
        assertTrue("Should have had 1 xact, startXactId = "
                    + startXactId + " endXactId = " + endXactId ,
                    ((endXactId - startXactId) == 1) );
        
        s.close();
    }

    /**
     * Alternative test for making sure "redundant" rollbacks, i.e. rollbacks
     * invoked when no transaction is in progress, don't result in a rollback
     * command being flowed to the server.
     *
     * @throws IOException if reading/parsing the trace file fails
     * @throws SQLException if something goes wrong
     */
    public void testConnectionFlowRollbackAlt()
            throws IOException, SQLException {
        Object[] dataSources = new Object[] {
            JDBCDataSource.getDataSource(),
            J2EEDataSource.getConnectionPoolDataSource(),
            J2EEDataSource.getXADataSource()
        };
        for (int i=0; i < dataSources.length; i++) {
            Object ds = dataSources[i];
            // Run test sequence without invoking extra rollbacks.
            int flowsBase = testConnectionFlowCommitRollback(ds, false, false);
            // Run test sequence with extra rollbacks - these should not result
            // in a rollback command being flowed to the server.
            int flowsExtra = testConnectionFlowCommitRollback(ds, true, false);
            assertEquals("Rollback optimization not working for connections " +
                    "originating from " + ds.getClass().getName(),
                    flowsBase, flowsExtra);
        }
    }

    /**
     * Alternative test for making sure "redundant" commits, i.e. commits
     * invoked when no transaction is in progress, don't result in a commit
     * command being flowed to the server.
     *
     * @throws IOException if reading/parsing the trace file fails
     * @throws SQLException if something goes wrong
     */
    public void testConnectionFlowCommitAlt()
            throws IOException, SQLException {
        Object[] dataSources = new Object[] {
            JDBCDataSource.getDataSource(),
            J2EEDataSource.getConnectionPoolDataSource(),
            J2EEDataSource.getXADataSource()
        };
        for (int i=0; i < dataSources.length; i++) {
            Object ds = dataSources[i];
            // Run test sequence without invoking extra commits.
            int flowsBase = testConnectionFlowCommitRollback(ds, false, true);
            // Run test sequence with extra commits - these should not result in
            // a commit command being flowed to the server.
            int flowsExtra = testConnectionFlowCommitRollback(ds, true, true);
            assertEquals("Commit optimization not working for connections " +
                    "originating from " + ds.getClass().getName(),
                    flowsBase, flowsExtra);
        }
    }

    /**
     * Performs a test sequence accessing the server, then parses the client
     * connection trace file to obtain the number of commit or rollback
     * commands flowed from the client to the server.
     *
     * @param ds data source used to obtain a connection to the database
     *      (must be using the test framework defaults)
     * @param invokeExtra if {@code true} extra invocations of either commit or
     *      rollback are performed (depending on value of {@code isCommit})
     * @param isCommit if {@code true}, commits are invoked, otherwise
     *      rollbacks are invoked
     * @return The number of wire flows detected (depending on value of
     *      {@code isCommit}).
     * @throws IOException if reading/parsing the trace file fails
     * @throws SQLException if something goes wrong
     */
    private int testConnectionFlowCommitRollback(
                            Object ds, boolean invokeExtra, boolean isCommit)
            throws IOException, SQLException {
        final int extraInvokations = invokeExtra ? 25 : 0;
        final int rowCount = 10;
        final boolean isXA = ds instanceof XADataSource;
        final boolean isCP = ds instanceof ConnectionPoolDataSource;
        // Generate trace file name and define trace behavior.
        String dsType = (isXA ? "xa_" : (isCP ? "cp_" : ""));
        String tbl = "ds_" + dsType +
                (invokeExtra ? "base_" : "extra_") +
                (isCommit ? "commit" : "rollback");
        File traceFile = SupportFilesSetup.getReadWrite(tbl + ".trace");
        J2EEDataSource.setBeanProperty(ds, "traceFile",
                PrivilegedFileOpsForTests.getAbsolutePath(traceFile));
        J2EEDataSource.setBeanProperty(ds, "traceFileAppend", Boolean.FALSE);
        J2EEDataSource.setBeanProperty( ds, "traceLevel",
                BasicClientDataSource40.TRACE_ALL);

        // Obtain connection.
        PooledConnection physicalCon = null;
        Connection con;
        if (isXA) {
            physicalCon = ((XADataSource)ds).getXAConnection();
            con = physicalCon.getConnection();
        } else if (isCP) {
            physicalCon = ((ClientConnectionPoolDataSourceInterface)ds).
                    getPooledConnection();
            con = physicalCon.getConnection();
        } else {
            con = ((DataSource)ds).getConnection();
        }
        con.setAutoCommit(false);

        // Run test sequence.
        // step 0: create table
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table " + tbl + " (id int)");
        con.commit(); // Unconditional commit to persist table
        endTranscation(con, isCommit, extraInvokations);
        // step 1: insert data
        PreparedStatement ps =
                con.prepareStatement("insert into " + tbl + " values (?)");
        for (int i=0; i < rowCount; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();
            endTranscation(con, isCommit, extraInvokations);
        }
        ps.close();
        // Unconditional commit, should catch "missed" rollbacks above when we
        // do a select with another connection at the end.
        con.commit();
        // step 2: select data
        ResultSet rs = stmt.executeQuery("select count(*) from " + tbl);
        rs.next();
        rs.getInt(1);
        rs.close();
        endTranscation(con, isCommit, extraInvokations);
        // step 3: values clause
        rs = stmt.executeQuery("values 7");
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        rs.close();
        stmt.close();
        endTranscation(con, isCommit, extraInvokations);
        con.close();
        if (physicalCon != null) {
            physicalCon.close();
        }

        // step 4: table content validation
        stmt = createStatement();
        rs = stmt.executeQuery("select count(*) from " + tbl);
        rs.next();
        assertEquals("Potential COMMIT/ROLLBACK protocol error",
                isCommit ? rowCount : 0, rs.getInt(1));

        // Parse the trace file for commits or rollbacks.
        String token = "SEND BUFFER: " + (isXA ? "SYNCCTL" :
            (isCommit ? "RDBCMM" : "RDBRLLBCK"));
        int tokenCount = 0;
        BufferedReader r = new BufferedReader(
                PrivilegedFileOpsForTests.getFileReader(traceFile));
        String line;
        while ((line = r.readLine()) != null) {
            if (line.startsWith("[derby]") && line.indexOf(token) != -1) {
                println((isCommit ? "COMMIT: " : "ROLLBACK: ") + line);
                tokenCount++;
            }
        }
        r.close();
        assertTrue("Parsing failed, no COMMITS/ROLLBACKS detected",
                tokenCount > 0);
        println(ds.getClass().getName() + ", invokeExtra=" + invokeExtra +
                ", isCommit=" + isCommit + ", tokenCount=" + tokenCount);
        return tokenCount;
    }

    /**
     * Ends a transaction by invoking at least one commit or rollback.
     *
     * @param con the connection to work on
     * @param isCommit if {@code true} commit is invoked, otherwise rollback
     *      is invoked
     * @param count the number of extra commits or rollbacks to invoke
     * @throws SQLException if a commit/rollback fails
     */
    private void endTranscation(Connection con, boolean isCommit, int count)
            throws SQLException {
        if (isCommit) {
            con.commit();
            for (int i=0; i < count; i++) {
                con.commit();
            }
        } else {
            con.rollback();
            for (int i=0; i < count; i++) {
                con.rollback();
            }
        }
    }

    /**
     * Check setTransactioIsolation and with four connection in connection pool
     * for DERBY-4343 case
     * 
     * @throws SQLException
     */
    public void testIsolationWithFourConnections()
            throws SQLException {
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();

        PooledConnection pc = ds.getPooledConnection();
        //First connection
        Connection conn = pc.getConnection();
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM SYS.SYSTABLES");
        rs.next();
        int ri = rs.getInt(1);
        rs.close();
        conn.rollback();
        conn.close();
        
        //Second connection
        conn = pc.getConnection();
        conn.close();
        
        //Third connection
        conn = pc.getConnection();
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());
        conn.close();
        
        //Fourth connetion
        conn = pc.getConnection();
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());
        conn.close();
    
    }
    
    // test that an xastart in auto commit mode commits the existing work.
    // test fix of a bug ('beetle 5178') wherein XAresource.start() when 
    // auto-commit is true did not implictly commit any transaction
    // Also tests DERBY-1025, same description, but for client.
    public void testAutoCommitOnXAResourceStart() throws SQLException, XAException {

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac4 = dsx.getXAConnection();
        Xid xid4a= null;

        // We get an XAID_DUP error from networkserver when attempting
        // the XAResource.start below if we use the same xid.
        // Possibly because we're in the same jvm.
        // When the test is run with clientserverSuite, rather than default,
        // this wasn't needed, so just create a different id for client
        if (usingEmbedded())
            xid4a = new cdsXid(4, (byte) 23, (byte) 76);
        else if (usingDerbyNetClient())
            xid4a = new cdsXid(5, (byte) 23, (byte) 76);
            
        Connection conn4 = xac4.getConnection();
        assertTrue(conn4.getAutoCommit());

        Statement s4 = conn4.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs4 = s4.executeQuery("select i from autocommitxastart");
        rs4.next();
        assertEquals(1, rs4.getInt(1));
        rs4.next();
        assertEquals(2, rs4.getInt(1));

        // XAResource().start should commit the transaction
        xac4.getXAResource().start(xid4a, XAResource.TMNOFLAGS);
        xac4.getXAResource().end(xid4a, XAResource.TMSUCCESS);

        try {
            rs4.next();
            fail ("expected an exception indicating resultset is closed.");
        } catch (SQLException sqle) {
            // Embedded gets 08003. No current connection (DERBY-2620)        	
        	if (usingDerbyNetClient())
        		assertSQLState("XCL16",sqle);
        }

        conn4.setAutoCommit(false);
        assertFalse(conn4.getAutoCommit());

        rs4 = s4.executeQuery("select i from autocommitxastart");
        rs4.next();
        assertEquals(1, rs4.getInt(1));
        rs4.next();
        assertEquals(2, rs4.getInt(1));
        
         // Get a new xid to begin another transaction. 
        if (usingEmbedded())
            xid4a = new cdsXid(4, (byte) 93, (byte) 103);
        else if (usingDerbyNetClient())
            xid4a = new cdsXid(5, (byte) 93, (byte) 103);

        try {
            xac4.getXAResource().start(xid4a, XAResource.TMNOFLAGS);
        } catch (XAException xae) {
            if (usingEmbedded())
                assertNull(xae.getMessage());
            else if (usingDerbyNetClient())
            {
                // This should give XAER_OUTSIDE exception because
                // the resource manager is busy in the local transaction
                assertTrue(xae.getMessage().indexOf("XAER_OUTSIDE") >=0 );
            }
            assertEquals(-9, xae.errorCode);
        }
        
        rs4.next();
        assertEquals(3, rs4.getInt(1));
        rs4.close();

        conn4.rollback();
        conn4.close();
        xac4.close();
    }

    public void testReadOnlyToWritableTran() throws SQLException, Exception
    {
        // This fixture will run twice, once with embedded, once with client,
        // and insert 2 rows in addition to the 5 rows inserted during setup. 
        // The fixture tests a commit, so before running, try to remove row 
        // 6 and 7 in case this is the second run of the fixture.
        Statement s = createStatement();
        s.executeUpdate("delete from autocommitxastart where i = 6");
        s.executeUpdate("delete from autocommitxastart where i = 7");
        
        // TESTING READ_ONLY TRANSACTION FOLLOWED BY WRITABLE TRANSACTION
        // Test following sequence of steps
        // 1)start a read-only global transaction 
        // 2)finish that read-only transaction
        // 3)start another global transaction 

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac5 = dsx.getXAConnection();
        Xid xid5a = new cdsXid(5, (byte) 119, (byte) 129);
        Connection conn5 = xac5.getConnection();
        Statement sru5a = conn5.createStatement();
        XAResource xar = xac5.getXAResource();
        xar.start(xid5a, XAResource.TMNOFLAGS);
        conn5.setReadOnly(true);

        // Read-Only XA transaction;
        // holdability: (hold, or close cursors over commit) , 
        // transaction isolation: read-committed, 
        // auto-commit false, read-only true (with embedded)
        if (usingEmbedded()) 
        {
            assertConnectionState(
                ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                Connection.TRANSACTION_READ_COMMITTED,
                false, true, conn5);
        }
        // Note: the original test had no comments about this difference
        //       between Embedded and DerbyNetClient, this has apparently
        //       been accepted behavior.
        else if (usingDerbyNetClient())
        {
            assertConnectionState(
                ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                Connection.TRANSACTION_READ_COMMITTED,
                false, false, conn5);
        }
        
        ResultSet rs5 = sru5a.executeQuery(
            "select count(*) from autocommitxastart");
        rs5.next();
        assertEquals(5, rs5.getInt(1));
        rs5.close();
        xar.end(xid5a, XAResource.TMSUCCESS);
        xar.commit(xid5a, true);
        conn5.close();
        
        //now start a new transaction
        conn5 = xac5.getConnection();
        sru5a = conn5.createStatement();
        xar.start(xid5a, XAResource.TMNOFLAGS);
        
        // Writeable XA transaction
        // holdability: (hold, or close cursors over commit) , 
        // transaction isolation: read-committed, 
        // auto-commit false, read-only false
        assertConnectionState(
                ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                Connection.TRANSACTION_READ_COMMITTED,
                false, false, conn5);
        sru5a.executeUpdate("insert into autocommitxastart values 6,7");
        rs5 = sru5a.executeQuery("select count(*) from autocommitxastart");
        rs5.next();
        assertEquals(7, rs5.getInt(1));
        xar.end(xid5a, XAResource.TMSUCCESS);
        xar.commit(xid5a, true);
        conn5.close();
        xac5.close();
        sru5a.close();
    }
    
    // test jira-derby 95 - a NullPointerException was returned when passing
    // an incorrect database name, should now give error XCY00   
    // with ConnectionPoolDataSource
    public void testJira95pds() throws Exception {
        try {
            ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
            JDBCDataSource.setBeanProperty(pds, "databaseName", "jdbc:derby:boo");
            pds.getPooledConnection();
            fail ("expected an SQLException!");
        } catch (SQLException sqle) {
            assertSQLState("XCY00", sqle);
        } catch (Exception e) {
            throw e;
        }
    }
    
    // test jira-derby 95 - a NullPointerException was returned when passing
    // an incorrect database name, should now give error XCY00   
    // with XADataSource
    public void testJira95xads() throws SQLException {
        try {
            XADataSource dxs = J2EEDataSource.getXADataSource();
            JDBCDataSource.setBeanProperty(dxs, "databaseName", "jdbc:derby:boo");
            dxs.getXAConnection().getConnection();
            fail ("expected an SQLException!");
        } catch (SQLException sqle) {
            assertSQLState("XCY00", sqle);
        }
    }
    
    // there is a corresponding fixture for datasources in DataSourceTest
    public void testBadConnectionAttributeSyntax() throws SQLException {
        
        // ConnectionPoolDataSource - bad connatr syntax
        ConnectionPoolDataSource cpds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(cpds, "ConnectionAttributes", "bad");
        try {
            cpds.getPooledConnection();
            fail ("should have seen an error");
        } catch (SQLException e) {
            assertSQLState("XJ028", e);
        } 

        // XADataSource - bad connattr syntax");
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "ConnectionAttributes", "bad");
        try {
            xads.getXAConnection();
            fail ("should have seen an error");
        } catch (SQLException e) {
            assertSQLState("XJ028", e);
        } 
    } // End testBadConnectionAttributeSyntax
        
    /**
     * Check that database name set using setConnectionAttributes is not used
     * by ClientDataSource. This method tests DERBY-1130.
     * 
     * @throws SQLException
     */
    public void testClientDSConnectionAttributes()
            throws SQLException, ClassNotFoundException,
                   IllegalAccessException, InstantiationException,
                   NoSuchMethodException, InvocationTargetException
    {
        if (usingEmbedded())
            return;

        // now with ConnectionPoolDataSource
        ClientConnectionPoolDataSourceInterface cpds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientConnectionPoolDataSource");
            cpds = (ClientConnectionPoolDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40");
            cpds = (ClientConnectionPoolDataSourceInterface) clazz.getConstructor().newInstance();
        }

        cpds.setPortNumber(TestConfiguration.getCurrent().getPort());
        
        // ConnectionPoolDataSource - EMPTY
        dsCPConnectionRequests(new String[]
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, 
            cpds);

        // ConnectionPoolDataSource 
        // - connectionAttributes=databaseName=<valid dbname>
        cpds.setConnectionAttributes("databaseName=" + dbName);
        dsCPConnectionRequests(new String[]
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"},
            cpds);
        cpds.setConnectionAttributes(null);

        // Test that database name specified in connection attributes is 
        // not used
        // ConnectionPoolDataSource - databaseName=wombat and 
        // connectionAttributes=databaseName=kangaroo
        cpds.setConnectionAttributes("databaseName=kangaroo");
        cpds.setDatabaseName(dbName);
        dsCPConnectionRequests(new String[]
            {"OK","08001","OK","OK","08001","08001","OK","OK","OK"},
            cpds);
        cpds.setConnectionAttributes(null);
        cpds.setDatabaseName(null);

        // now with XADataSource
        ClientXADataSourceInterface xads;
        
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientXADataSource");
            xads = (ClientXADataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientXADataSource40");
            xads = (ClientXADataSourceInterface) clazz.getConstructor().newInstance();
        }

        xads.setPortNumber(TestConfiguration.getCurrent().getPort());
        
        // XADataSource - EMPTY
        dsXAConnectionRequests(new String[]
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, 
            xads);

        // XADataSource - connectionAttributes=databaseName=<valid dbname>
        xads.setConnectionAttributes("databaseName=wombat");
        dsXAConnectionRequests(new String[]
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"},
            xads);
        xads.setConnectionAttributes(null);

        // Test that database name specified in connection attributes is not used
        // XADataSource - databaseName=wombat and 
        // connectionAttributes=databaseName=kangaroo
        xads.setConnectionAttributes("databaseName=kangaroo");
        xads.setDatabaseName("wombat");
        dsXAConnectionRequests(new String[]
            {"OK","08001","OK","OK","08001","08001","OK","OK","OK"},
            xads);
        xads.setConnectionAttributes(null);
        xads.setDatabaseName(null);
    } // End testClientDSConnectionAttributes
            
    // Following test is similar to testClientDSConnectionAttributes, but
    // for embedded datasources.
    // This subtest does not run for network server, it uses
    // setAttributesAsPassword, which isn't supported for client datasources.
    //
    // Note that DataSourceTest has some more basic testing of
    // an empty DataSource in a fixture with similar name - however
    // that fixture does not test setAttributesAsPassword
    public void testDSRequestAuthentication() throws Exception {

//        if (usingDerbyNetClient())
//            return;
        
        JDBCClient dsclient = getTestConfiguration().getJDBCClient();
        String dsName = dsclient.getDataSourceClassName();
        Class<?> clazz = Class.forName(dsName);
        DataSource ds = (DataSource) clazz.getConstructor().newInstance();

        // DataSource - attributesAsPassword=true");
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.FALSE);

        // DataSource - attributesAsPassword=true, 
        // connectionAttributes=databaseName=kangaroo");
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", "databaseName=kangaroo");
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.FALSE);
        JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");

        // Enable Authentication;

        setDatabaseProperty("derby.user.fred", "wilma");
        setDatabaseProperty("derby.user.APP", "APP");
        setDatabaseProperty("derby.authentication.provider", "BUILTIN");
        setDatabaseProperty("derby.connection.requireAuthentication", "true");
        
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection();
            fail("shutdown should raise exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ015", sqle);
        }

        JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
        JDBCDataSource.clearStringBeanProperty(ds, "shutdownDatabase");

        // "AUTHENTICATION NOW ENABLED");

        // DataSource - attributesAsPassword=true
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.FALSE);

        // ensure the DS property password is not treated as a set of 
        // attributes.
        // DataSource - attributesAsPassword=true, user=fred, 
        //     password=databaseName=wombat;password=wilma
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        JDBCDataSource.setBeanProperty(ds, "user", "fred");
        JDBCDataSource.setBeanProperty(ds, "password", "databaseName=" + dbName + ";password=wilma");
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.FALSE);
        JDBCDataSource.clearStringBeanProperty(ds, "user");
        JDBCDataSource.clearStringBeanProperty(ds, "password");
        ds = null;

        // now with ConnectionPoolDataSource
        String cpdsName = dsclient.getConnectionPoolDataSourceClassName();
        clazz = Class.forName(cpdsName);
        ConnectionPoolDataSource cpds =(ConnectionPoolDataSource) clazz.getConstructor().newInstance();

        // ConnectionPoolDataSource - EMPTY
        dsCPConnectionRequests(new String[] {
            "XJ004","XJ004","XJ004","XJ004",
            "XJ004","XJ004","XJ004","XJ004","XJ004"},
            cpds);

        // ConnectionPoolDataSource - 
        // connectionAttributes=databaseName=wombat
        JDBCDataSource.setBeanProperty(cpds, "connectionAttributes", "databaseName=" + dbName);
        dsCPConnectionRequests(new String[] {
            "XJ004","XJ004","XJ004","XJ004",
            "XJ004","XJ004","XJ004","XJ004","XJ004"},
            cpds);
        JDBCDataSource.clearStringBeanProperty(cpds, "connectionAttributes");

        // ConnectionPoolDataSource - attributesAsPassword=true
        JDBCDataSource.setBeanProperty(cpds, "attributesAsPassword", Boolean.TRUE);
        dsCPConnectionRequests(new String[] {
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"},
            cpds);
        JDBCDataSource.setBeanProperty(cpds, "attributesAsPassword", Boolean.FALSE);
        
        // ensure the DS property password is not treated as a set of
        // attributes.
        // ConnectionPoolDataSource - attributesAsPassword=true, 
        //     user=fred, password=databaseName=wombat;password=wilma");
        JDBCDataSource.setBeanProperty(cpds, "attributesAsPassword", Boolean.TRUE);
        JDBCDataSource.setBeanProperty(cpds, "user", "fred");
        JDBCDataSource.setBeanProperty(cpds, "password", "databaseName=" + dbName + ";password=wilma");
        dsCPConnectionRequests(new String[] {
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"},
            cpds);
        JDBCDataSource.setBeanProperty(cpds, "attributesAsPassword", Boolean.FALSE);
        JDBCDataSource.clearStringBeanProperty(cpds, "user");
        JDBCDataSource.clearStringBeanProperty(cpds, "password");
        cpds = null;

        // now with XADataSource
//        EmbeddedXADataSource xads = new EmbeddedXADataSource();
        String xadsName = dsclient.getXADataSourceClassName();
        clazz = Class.forName(xadsName);
        XADataSource xads =
            (XADataSource) clazz.getConstructor().newInstance();

        // XADataSource - EMPTY
        dsXAConnectionRequests(new String[] {
            "08006","08006","08006","08006",
            "08006","08006","08006","08006","08006"},
            xads);

        // XADataSource - databaseName=wombat
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        dsXAConnectionRequests(new String[] {
            "08004","08004","08004","OK",
            "08004","08004","08004","08004","08004"},
            xads);
        JDBCDataSource.clearStringBeanProperty(xads, "databaseName");

        // XADataSource - connectionAttributes=databaseName=wombat");
        JDBCDataSource.setBeanProperty(xads, "connectionAttributes", "databaseName=" + dbName);
        dsXAConnectionRequests(new String[] {
            "08006","08006","08006","08006",
            "08006","08006","08006","08006","08006"},
            xads);
        JDBCDataSource.clearStringBeanProperty(xads, "connectionAttributes");

        // XADataSource - attributesAsPassword=true
        JDBCDataSource.setBeanProperty(xads, "attributesAsPassword", Boolean.TRUE);
        dsXAConnectionRequests(new String[] {
            "08006","08006","08006","08006",
            "08006","08006","08006","08006","08006"},
            xads);
        JDBCDataSource.setBeanProperty(xads, "attributesAsPassword", Boolean.FALSE);

        // XADataSource - databaseName=wombat, attributesAsPassword=true
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "attributesAsPassword", Boolean.TRUE);
        dsXAConnectionRequests(new String[] {
            "08004","08004","08004","XJ028",
            "XJ028","08004","08004","OK","08004"},
            xads);
        JDBCDataSource.setBeanProperty(xads, "attributesAsPassword", Boolean.FALSE);
        JDBCDataSource.clearStringBeanProperty(xads, "databaseName");
        
        setDatabaseProperty("derby.connection.requireAuthentication", "false");
        TestConfiguration.getCurrent().shutdownDatabase();
    }
    
    /**
     * Check that traceFile connection attribute functions correctly.
     * tracefile was tested in checkDriver, but not for DataSources.
     * tracefile= was used in datasourcepermissions_net, but that's 
     * incorrect syntax. Note that we're not checking the contents of
     * the tracefile.
     *
     * Note also that this test cannot run against a remote server.
     * <p>
     * This is also a regression test for DERBY-4717.
     *  
     * @throws SQLException
     */
    public void testClientTraceFileDSConnectionAttribute() throws SQLException
    {
        String traceFile;

        // with ConnectionPoolDataSource
        ConnectionPoolDataSource cpds = J2EEDataSource.getConnectionPoolDataSource();

        traceFile = "trace3.out";
        JDBCDataSource.setBeanProperty(cpds, "connectionAttributes",
        		"traceFile="+traceFile);
        // DERBY-2468 - trace3.out does not get created
        ((PooledConnection)getPhysicalConnection(cpds)).close();
        JDBCDataSource.clearStringBeanProperty(cpds, "connectionAttributes");

        traceFile = "trace4.out";
        JDBCDataSource.setBeanProperty(cpds, "traceFile", traceFile);
        ((PooledConnection)getPhysicalConnection(cpds)).close();
        cpds = null;

        // now with XADataSource
        XADataSource xads = J2EEDataSource.getXADataSource();

        traceFile = "trace5.out";
        JDBCDataSource.setBeanProperty(xads, "connectionAttributes",
        		"traceFile="+traceFile);
        ((XAConnection)getPhysicalConnection(xads)).close();
        // DERBY-2468 - trace5.out does not get created
        JDBCDataSource.clearStringBeanProperty(xads, "connectionAttributes");

        traceFile = "trace6.out";
        JDBCDataSource.setBeanProperty(xads, "traceFile", traceFile);
        ((XAConnection)getPhysicalConnection(xads)).close();

        assertTraceFilesExistAndCanBeDeleted();
    }
        
    /* -- Helper Methods for testClientTraceFileDSConnectionAttribute -- */
    
    /**
     * Check that trace file exists in <framework> directory
     */
    private static void assertTraceFilesExistAndCanBeDeleted()
    {
        for (int i = 3; i <= 6; i++) {
            File traceFile = new File("trace" + i + ".out");
            assertTrue("Doesn't exist",
                    PrivilegedFileOpsForTests.exists(traceFile));
            assertTrue("Delete failed",
                    PrivilegedFileOpsForTests.delete(traceFile));
        }
    }

    /**
     * Check that messageText connection attribute functions correctly.
     * retrievemessagetext was tested in checkdriver, and derbynet/testij,
     * but not tested for datasources, and in datasourcepermissions_net,
     * but as it has nothing to do with permissions/authentication,
     * this test seems a better place for it. 
     * 
     * There is a corresponding fixture for clientDataSource in DataSourceTest
     *  
     * @throws SQLException
     */
    public void testClientMessageTextConnectionAttribute()
            throws SQLException, ClassNotFoundException, IllegalAccessException,
                   InstantiationException, NoSuchMethodException, InvocationTargetException
    {
        String retrieveMessageTextProperty = "retrieveMessageText";
        // with ConnectionPoolDataSource
        // ConnectionPoolDataSource - retrieveMessageTextProperty
        ClientConnectionPoolDataSourceInterface cpds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientConnectionPoolDataSource");
            cpds = (ClientConnectionPoolDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40");
            cpds = (ClientConnectionPoolDataSourceInterface) clazz.getConstructor().newInstance();
        }

        cpds.setPortNumber(TestConfiguration.getCurrent().getPort());
        
        cpds.setDatabaseName(dbName);
        cpds.setConnectionAttributes(
                retrieveMessageTextProperty + "=false");
        PooledConnection cpConn = cpds.getPooledConnection();
        assertMessageText(cpConn.getConnection(), "false");
        cpConn.close();
        cpds.setConnectionAttributes(
                retrieveMessageTextProperty + "=true");
        cpConn = cpds.getPooledConnection();
        assertMessageText(cpConn.getConnection(), "true");
        cpds.setConnectionAttributes(null);
        cpConn.close();

        // now with XADataSource
        ClientXADataSourceInterface xads;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientXADataSource");
            xads = (ClientXADataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientXADataSource40");
            xads = (ClientXADataSourceInterface) clazz.getConstructor().newInstance();
        }
        //XADataSource - retrieveMessageTextProperty
        xads.setPortNumber(TestConfiguration.getCurrent().getPort());
        xads.setDatabaseName(dbName);
        xads.setConnectionAttributes(
                retrieveMessageTextProperty + "=false");
        XAConnection xaConn = xads.getXAConnection();
        assertMessageText(xaConn.getConnection(), "false");
        xaConn.close();
        xads.setConnectionAttributes(
                retrieveMessageTextProperty + "=true");
        xaConn = xads.getXAConnection();
        assertMessageText(xaConn.getConnection(), "true");
        xaConn.close();
        xads.setConnectionAttributes(null);
    }

    /* -- Helper Method for testClientMessageTextDSConnectionAttribute -- */

    /**
     * Checks if <tt>retrieveMessageText</tt> takes effect.
     *
     * @param conn the connection to use, will be closed
     * @param retrieveMessageTextValue the current value
     * @throws SQLException if something goes wrong
     */
    private static void assertMessageText(
            Connection conn, String retrieveMessageTextValue) 
    throws SQLException
    {
        try {
            conn.createStatement().executeQuery("SELECT * FROM APP.NOTTHERE");
            fail("SQLException of 42X05 should be thrown!");
        }
        catch (SQLException e)
        {
            assertSQLState("42X05", e);
            if (retrieveMessageTextValue.equals("true") )
            {
                assertTrue(e.getMessage().indexOf("does not exist") >= 0);
            }
            else
            {
                // retrieveMessageTextValue is false
                assertTrue(e.getMessage().indexOf("does not exist") == -1);
            }
        } finally {
            try {
                conn.close();
            } catch (SQLException ignore) {
                // Ignore error on close
                println("Ignored error on close: " + ignore.getMessage());
            }
        }
    }

    /**
     * Check that messageText connection attribute functions correctly.
     * retrievemessagetext was tested in checkdriver, and derbynet/testij
     * (but not tested for datasources), and in datasourcepermissions_net,
     * but as it has nothing to do with permissions/authentication,
     * this test seems a better place for it. 
     * 
     * there is a corresponding method call for datasources in DataSourceTest
     *  
     * @throws SQLException
     */
    public void testDescriptionProperty() 
    throws SQLException, Exception {
        
        // ConnectionPoolDataSource - setDescription
        subTestDataSourceDescription(
        		(DataSource) J2EEDataSource.getConnectionPoolDataSource());

        // XADataSource - setDescription
        subTestDataSourceDescription(
        		(DataSource) J2EEDataSource.getXADataSource());

    }
    
    /**
     * Utility method for testing setting and fetching the description
     * property on a data source.
     */
    private void subTestDataSourceDescription(DataSource ds) throws Exception
    {
        String setDescription = 
            "Everything you ever wanted to know about this datasource";
        
        JDBCDataSource.setBeanProperty(ds, "description", setDescription);
        getPhysicalConnection(ds);
        assertEquals(setDescription, JDBCDataSource.getBeanProperty(ds, "description"));
        JDBCDataSource.clearStringBeanProperty(ds, "description");
        assertNull(JDBCDataSource.getBeanProperty(ds, "description"));    	
    }

    /* ------------------ JDBC30 (and up) Fixtures ------------------ */
    
    public void testXAHoldability() throws SQLException, XAException {
        // DERBY-2533 - 
        // This test, when run with Network server / DerbyNetClient
        // leaves the database is a bad state which results in a
        // network protocol error
        if (usingDerbyNetClient())
            return;
        // START XA HOLDABILITY TEST
        XADataSource dscsx = J2EEDataSource.getXADataSource();

        XAConnection xac = dscsx.getXAConnection();
        XAResource xr = xac.getXAResource();
        Xid xid = new cdsXid(25, (byte) 21, (byte) 01);
        Connection conn1 = xac.getConnection();
        // check that autocommit is true; default for a connection
        assertTrue(conn1.getAutoCommit());
        // check that holdability is HOLD_CURSORS_OVER_COMMIT in a default
        // CONNECTION(not in xa transaction yet)
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, conn1.getHoldability());
        // start a global transaction and default holdability and 
        // autocommit will be switched to match Derby XA restrictions
        xr.start(xid, XAResource.TMNOFLAGS);
        // So, now autocommit should be false for connection because it is
        // part of the global transaction
        assertFalse(conn1.getAutoCommit());
        // Connection's holdability is now CLOSE_CURSORS_AT_COMMIT because
        // it is part of the global transaction
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, conn1.getHoldability());
        
        xr.end(xid, XAResource.TMSUCCESS);
        conn1.commit();
        conn1.close();

        xid = new cdsXid(27, (byte) 21, (byte) 01);
        xr.start(xid, XAResource.TMNOFLAGS);
        conn1 = xac.getConnection();
        // CONNECTION(in xa transaction) HOLDABILITY:
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, conn1.getHoldability());
        // Autocommit on Connection inside global transaction should be false
        assertFalse(conn1.getAutoCommit());
        xr.end(xid, XAResource.TMSUCCESS);
        conn1.rollback();

        Connection conn = xac.getConnection();
        conn.setAutoCommit(false);
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        // CONNECTION(non-xa transaction) HOLDABILITY: 
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

        Statement s = conn.createStatement();
        // STATEMENT HOLDABILITY: 
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());

        s.executeUpdate("insert into hold_30 values " +
            "(1,'init2'), (2, 'init3'), (3,'init3')");
        s.executeUpdate("insert into hold_30 values " +
            "(4,'init4'), (5, 'init5'), (6,'init6')");
        s.executeUpdate("insert into hold_30 values " +
            "(7,'init7'), (8, 'init8'), (9,'init9')");

        // STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());

        Statement sh = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        PreparedStatement psh = conn.prepareStatement(
            "select id from hold_30 for update", ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        CallableStatement csh = conn.prepareCall(
            "select id from hold_30 for update", ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

        // STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, sh.getResultSetHoldability());
        // PREPARED STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, psh.getResultSetHoldability());
        // CALLABLE STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, csh.getResultSetHoldability());

        ResultSet rsh = sh.executeQuery("select id from hold_30 for update");
        rsh.next();  
        assertEquals(1, rsh.getInt(1)); // H@1 id
        rsh.next(); 
        assertEquals(2, rsh.getInt(1)); // H@2 id 
        conn.commit();
        rsh.next(); 
        assertEquals(3, rsh.getInt(1)); // H@3 id 
        conn.commit();

        xid = new cdsXid(23, (byte) 21, (byte) 01);
        xr.start(xid, XAResource.TMNOFLAGS);
        Statement stmtInsideGlobalTransaction = conn.createStatement();
        PreparedStatement prepstmtInsideGlobalTransaction = 
            conn.prepareStatement("select id from hold_30");
        CallableStatement callablestmtInsideGlobalTransaction = 
            conn.prepareCall("select id from hold_30");

        // CONNECTION(xa) HOLDABILITY:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        // STATEMENT(this one was created with holdability false, outside the 
        // global transaction. Check its holdability inside global transaction
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());
        // STATEMENT(this one was created with holdability true, 
        // outside the global transaction. Check its holdability inside 
        // global transaction:
        // DERBY-2531: network server / DerbyNetClient has a different value 
        // than embedded.
        if (usingEmbedded())
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                sh.getResultSetHoldability());
        else if (usingDerbyNetClient())
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
                sh.getResultSetHoldability());
        // STATEMENT(this one was created with default holdability inside this
        // global transaction. Check its holdability:
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            stmtInsideGlobalTransaction.getResultSetHoldability());
        // PREPAREDSTATEMENT(this one was created with default holdability
        // inside this global transaction. Check its holdability:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
        prepstmtInsideGlobalTransaction.getResultSetHoldability());
        // CALLABLESTATEMENT(this one was created with default holdability 
        // inside this global transaction. Check its holdability:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
        callablestmtInsideGlobalTransaction.getResultSetHoldability()); 

        ResultSet rsx = s.executeQuery("select id from hold_30 for update");

        rsx.next(); 
        assertEquals(1, rsx.getInt(1)); // X@1 id
        rsx.next(); 
        assertEquals(2, rsx.getInt(1)); // X@2 id
        xr.end(xid, XAResource.TMSUCCESS);

        // result set should not be useable, since it is part of a detached
        // XAConnection
        try {
            rsx.next();
            fail("rsx's connection not active id ");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }

        // result set should not be useable, it should have been closed by
        // the xa start.
        try {
            rsh.next();
            fail("rsh's connection not active id ");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }

        // resume XA transaction and keep using rs");
        xr.start(xid, XAResource.TMJOIN);
        Statement stmtAfterGlobalTransactionResume = conn.createStatement();
        PreparedStatement prepstmtAfterGlobalTransactionResume = 
            conn.prepareStatement("select id from hold_30");
        CallableStatement callablestmtAfterGlobalTransactionResume = 
            conn.prepareCall("select id from hold_30");

        // Check holdability of various jdbc objects after resuming XA 
        // transaction
        // CONNECTION(xa) HOLDABILITY:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,conn.getHoldability());
        // STATEMENT(this one was created with holdability false, outside the
        // global transaction. Check its holdability inside global transaction
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());
        // STATEMENT(this one was created with holdability true, outside the 
        // global transaction. Check its holdability inside global transaction
        if (usingEmbedded())
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                sh.getResultSetHoldability());
        else if (usingDerbyNetClient())
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
                sh.getResultSetHoldability());
        // STATEMENT(this one was created with default holdability inside the
        // global transaction when it was first started. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            stmtInsideGlobalTransaction.getResultSetHoldability());
        // PREPAREDSTATEMENT(this one was created with default holdability 
        // inside the global transaction when it was first started. Check its
        // holdability) 
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            prepstmtInsideGlobalTransaction.getResultSetHoldability());
        // CALLABLESTATEMENT(this one was created with default holdability 
        // inside the global transaction when it was first started. Check its
        // holdability) HOLDABILITY
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            callablestmtInsideGlobalTransaction.getResultSetHoldability());
        // STATEMENT(this one was created with default holdability after the
        // global transaction was resumed. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            stmtAfterGlobalTransactionResume.getResultSetHoldability());
        // PREPAREDSTATEMENT(this one was created with default holdability 
        // after the global transaction was resumed. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            prepstmtAfterGlobalTransactionResume.getResultSetHoldability());
        // CALLABLESTATEMENT(this one was created with default holdability
        // after the global transaction was resumed. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            callablestmtAfterGlobalTransactionResume.getResultSetHoldability());
        // DERBY-1370           
        if (usingEmbedded())
        {
            // Network XA BUG gives result set closed
            rsx.next();  
            assertEquals(3, rsx.getInt(1)); // X@3 id
        }
        xr.end(xid, XAResource.TMSUCCESS);

        if (xr.prepare(xid) != XAResource.XA_RDONLY)
            xr.commit(xid, false);

        // try again once the xa transaction has been committed.            
        try {
            rsx.next(); 
            fail("rsx's connection not active id (B)");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }
        try {
            rsh.next(); 
            fail ("rsh's should be closed (B)");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }

        // Set connection to hold
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // CONNECTION(held) HOLDABILITY:
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            conn.getHoldability());

        xid = new cdsXid(24, (byte) 21, (byte) 01);
        xr.start(xid, XAResource.TMNOFLAGS);
        // CONNECTION(xa) HOLDABILITY:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        try {
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            fail("allowed to set hold mode in xa transaction");
        } catch (SQLException sqle) {
            assertSQLState("XJ05C", sqle);
        }

        // JDBC 4.0 (proposed final draft) section 16.1.3.1 allows Statements 
        // to be created with a different holdability if the driver cannot 
        // support it. In this case the driver does not support holdability in
        // a global transaction, so a valid statement is returned with close 
        // cursors on commit.
        Statement shxa = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // HOLDABLE Statement in global xact " 
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            s.getResultSetHoldability());
        assertErrorCode(10000, conn.getWarnings());
        shxa.close();

        shxa = conn.prepareStatement("select id from hold_30",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // HOLDABLE PreparedStatement in global xact 
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            s.getResultSetHoldability());
        assertErrorCode(10000, conn.getWarnings());
        shxa.close();

        shxa = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // HOLDABLE CallableStatement in global xact:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            s.getResultSetHoldability());
        assertErrorCode(10000, conn.getWarnings());
        shxa.close();

        // check we can use a holdable statement set up in local mode.
        // holdability is downgraded, tested in XATest.java
        // DERBY-1370           
        if(usingEmbedded()) {
            // STATEMENT HOLDABILITY:
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                sh.getResultSetHoldability());
            sh.executeQuery("select id from hold_30").close();
            sh.execute("select id from hold_30");
            sh.getResultSet().close();

            // PREPARED STATEMENT HOLDABILITY:
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                psh.getResultSetHoldability());
            psh.executeQuery().close();
            psh.execute();
            psh.getResultSet().close();

            // CALLABLE STATEMENT HOLDABILITY:
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                csh.getResultSetHoldability());
            csh.executeQuery().close();
            csh.execute();
            csh.getResultSet().close();
        }        

        // but an update works
        sh.executeUpdate("insert into hold_30 values(10, 'init10')");

        xr.end(xid, XAResource.TMSUCCESS);

        // CONNECTION(held) HOLDABILITY:
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

        s.close();
        sh.close();
        csh.close();
        psh.close();
        rsx.close();
        stmtInsideGlobalTransaction.close();
        prepstmtInsideGlobalTransaction.close();
        callablestmtInsideGlobalTransaction.close();
        stmtAfterGlobalTransactionResume.close();
        prepstmtAfterGlobalTransactionResume.close();
        callablestmtAfterGlobalTransactionResume.close();
        conn.close();
        xac.close();
        TestConfiguration.getCurrent().shutdownDatabase();
        // END XA HOLDABILITY TEST");
    }
    
    /**
     * Tests that DatabaseMetaData.getConnection does not leak references to
     * physical connections or other logical connections.
     *
     * @throws SQLException if something goes wrong
     */
    public void testConnectionLeakInDatabaseMetaData()
            throws SQLException {
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        PooledConnection pc = cpDs.getPooledConnection();
        // Get first logical connection and a meta data object.
        Connection con1 = pc.getConnection();
        DatabaseMetaData dmd1 = con1.getMetaData();
        assertSame(con1, dmd1.getConnection());
        con1.close();
        // Get second logical connection and a meta data object.
        Connection con2 = pc.getConnection();
        DatabaseMetaData dmd2 = con2.getMetaData();
        // The first meta data object should not return a reference to the
        // second logical connection.
        assertSame(con2, dmd2.getConnection());
        try {
            dmd1.getConnection();
            fail("Should have thrown no current connection exception");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        con2.close();
        pc.close();
        try {
            dmd2.getConnection();
            fail("Should have thrown no current connection exception");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
    }

    /**
     * Regression test for a NullPointerException when trying to use the LOB
     * stored procedures after closing and then getting a new logical
     * connection. The problem was that the LOB stored procedure objects on the
     * server side were closed and not reprepared.
     * See Jira issue DERBY-3799.
     */
    public void testDerby3799() throws SQLException {
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        PooledConnection pc = cpDs.getPooledConnection();
        // Get first logical connection.
        Connection con1 = pc.getConnection();
        Statement stmt = con1.createStatement();
        ResultSet rs = stmt.executeQuery("select dClob from derby3799");
        assertTrue(rs.next());
        rs.getString(1);
        rs.close();
        con1.close();
        // Get second logical connection.
        Connection con2 = pc.getConnection();
        stmt = con2.createStatement();
        rs = stmt.executeQuery("select dClob from derby3799");
        assertTrue(rs.next());
        rs.getString(1); // NPE happened here.
        con2.close();
    }

    /**
     * Tests for DERBY-1144
     * 
     * This test tests that holdability, autocomit, and transactionIsolation 
     * are reset on getConnection for PooledConnections obtaind from 
     * connectionPoolDataSources 
     * 
     * DERBY-1134 has been filed for more comprehensive testing of client 
     * connection state. 
     * 
     * @throws SQLException
     */
    public void timeoutTestDerby1144PooledDS() throws SQLException {
    
        PooledConnection pc1 = null;

        // Test holdability   
        ConnectionPoolDataSource ds = 
            J2EEDataSource.getConnectionPoolDataSource();
        pc1 = ds.getPooledConnection();
        assertPooledConnHoldability("PooledConnection", pc1);
        pc1.close();
        
        // Test autocommit
        pc1 = ds.getPooledConnection();
        assertPooledConnAutoCommit("PooledConnection", pc1);
        pc1.close();
        
        // Test pooled connection isolation
        pc1 = ds.getPooledConnection();
        assertPooledConnIso("PooledConnection" , pc1);   
        pc1.close();
    }
    
    public void timeoutTestDerby1144XADS() throws SQLException {
       
        XADataSource xds = J2EEDataSource.getXADataSource();
        // Test xa connection isolation
        XAConnection xpc1 = xds.getXAConnection();        
        assertPooledConnIso("XAConnection", xpc1);                 
        xpc1.close();
    }

    /* -------------- Helper Methods for testDerby1144 -------------- */
    
    /**
     * Make sure autocommit gets reset on PooledConnection.getConnection()
     * @param desc      description of connection
     * @param pc1       pooled connection to test
     * @throws SQLException
     */
    private static void assertPooledConnAutoCommit(
        String desc, PooledConnection pc1) throws SQLException 
    {
        // ** Verify autoCommit state
        Connection conn  = pc1.getConnection();
        conn.setAutoCommit(true);
        // reset the connection and see if the autocommit has changed 
        conn = pc1.getConnection();
        boolean autocommit  = conn.getAutoCommit();
        // autocommit should get reset on getConnection
        assertTrue(autocommit);
        conn.close();
    }


    /**
     * Checks that Holdability gets reset on PooledConnection.getConnection()
     * @param desc
     * @param pc1
     * @throws SQLException
     */
    private static void assertPooledConnHoldability(
        String desc, PooledConnection pc1) throws SQLException 
    { 
        // **Test holdability state
        Connection conn  = pc1.getConnection();
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        // reset the connection and see if the holdability gets reset
        // to HOLD_CURSORS_OVER_COMMIT
        conn = pc1.getConnection();
        assertConnHoldability(conn, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        conn.close();
    }


    /**
     * Verify connection holdablity is expected holdability
     * @param conn
     * @param expectedHoldability 
     *   * @throws SQLException
     */
    private static void assertConnHoldability(
        Connection conn, int expectedHoldability) throws SQLException 
    {
        int holdability = conn.getHoldability();
        assertEquals (expectedHoldability, holdability);
    }

    /**
     * Test that isolation is reset on PooledConnection.getConnection()
     * @param pooledConnType   Descripiton of the type of pooled connection
     * @param pc               PooledConnection or XAConnection  
     * @throws SQLException
     */
    private void assertPooledConnIso(
        String pooledConnType, PooledConnection pc) throws SQLException {
        Connection conn = pc.getConnection();

        setupDerby1144Table(conn);

        // *** Test isolation level reset on conntype.getConnection()          
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertIsoLocks(conn, Connection.TRANSACTION_READ_UNCOMMITTED);

        conn.close();
        //Get a new connection with pooledConnType.getConnection()
        // Isolation level should be reset to READ_COMMITTED
        Connection newconn = pc.getConnection();
        assertIsoLocks(newconn, Connection.TRANSACTION_READ_COMMITTED);
    }

    /**
     * insert two rows into the simple table for DERBY-1144 tests
     * @param conn
     * @throws SQLException
     */
    private static void  setupDerby1144Table(Connection conn) 
    throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO intTable VALUES(1)");
        stmt.executeUpdate("INSERT INTO intTable VALUES(2)");

        conn.commit ();
    }
    
    /**
     * Checks locks for designated isolation level on the connection.
     * Currently only supports TRANSACTION_READ_COMMITTED and 
     * TRANSACTION_READ_UNCOMMITTED
     * @param conn   Connection to test
     * @param expectedIsoLevel expected isolation level
     *
     */
    private void assertIsoLocks(Connection conn, int expectedIsoLevel)
    throws SQLException {
        int conniso = conn.getTransactionIsolation();
        assertEquals(expectedIsoLevel, conniso);

        boolean selectTimedOut = selectTimesoutDuringUpdate(conn);
        // expect a lock timeout for READ_COMMITTED
        switch (conniso) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                assertFalse(selectTimedOut); break;
            case Connection.TRANSACTION_READ_COMMITTED:
                assertTrue(selectTimedOut); break;
            default:
                System.out.println("No test support for isolation level");
        }
    }
    
    /**
     * Determine if a select on this connection during update will timeout.
     * Used to establish isolation level.  If the connection isolation level
     * is <code> Connection.TRANSACTION_READ_UNCOMMITTED </code> it will not
     * timeout.  Otherwise it should.  
     * 
     * @param conn   Connection to test.
     * @return  true if the select got a lock timeout, false otherwise.
     */
    private boolean selectTimesoutDuringUpdate(Connection conn) 
    throws SQLException {

        Connection updateConn=null;
        conn.setAutoCommit(false);

        try {
            // create another connection and do an update but don't commit
            updateConn = openDefaultConnection();
            updateConn.setAutoCommit(false);

            // First update the rows on the update connection
            Statement upStmt = updateConn.createStatement();
            upStmt.executeUpdate("update intTable set i = 3");

            // now see if we can select them
            Statement stmt = conn.createStatement();
            JDBC.assertDrainResults(
                stmt.executeQuery("Select * from intTable"));
            stmt.close();
        }
        catch (SQLException e)
        {
            if (e.getSQLState().equals("40XL1"))
            {
                // If we got a lock timeout this is not read uncommitted
                return true;
            }   
        }
        finally {
            try {
                conn.rollback();
                updateConn.rollback();
            }catch (SQLException  se) {
                se.printStackTrace();
            }
        }
        return false;
    }
    
    /* -------------------- Other Helper Methods -------------------- */
    
    private void assertConnectionState(
        int expectedHoldability, int expectedIsolation,
        boolean expectedCommitSetting, boolean expectedReadOnly, 
        Connection conn) throws SQLException 
    {
        assertEquals(expectedHoldability, conn.getHoldability());
        assertEquals(expectedIsolation, conn.getTransactionIsolation());
        assertEquals(expectedCommitSetting, conn.getAutoCommit());
        assertEquals(expectedReadOnly, conn.isReadOnly());
    }

    private static void setDatabaseProperty(String property, String value) 
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource();
        Connection cadmin = ds.getConnection();
        CallableStatement cs = cadmin.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        cs.setString(1, property);
        cs.setString(2, value);
        cs.execute();
        
        cs.close();
        cadmin.close();
    }
    
    private void setHoldability(Connection conn, boolean hold) throws SQLException {

        conn.setHoldability(hold ? ResultSet.HOLD_CURSORS_OVER_COMMIT : ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }
    
    private static void dsConnectionRequests(
        String[] expectedValues, DataSource ds) {

        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(ds, "javax.sql.DataSource");
        }
        
        try {
            ds.getConnection();
            if (!expectedValues[0].equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValues[0], sqle);
        }
        dsConnectionRequest(expectedValues[1], ds, null, null);
        dsConnectionRequest(expectedValues[2], ds, "fred", null);
        dsConnectionRequest(expectedValues[3], ds, "fred", "wilma");
        dsConnectionRequest(expectedValues[4], ds, null, "wilma");
        dsConnectionRequest(
            expectedValues[5], ds, null, "databaseName=wombat");
        dsConnectionRequest(
            expectedValues[6], ds, "fred", "databaseName=wombat");
        dsConnectionRequest(expectedValues[7], 
            ds, "fred", "databaseName=wombat;password=wilma");
        dsConnectionRequest(expectedValues[8], 
            ds, "fred", "databaseName=wombat;password=betty");
    }

    private static void dsConnectionRequest(
        String expectedValue, DataSource ds, String user, String ConnAttr)
    {
        try {
            ds.getConnection(user, ConnAttr);
            if (!expectedValue.equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValue, sqle);
        }
    }
    
    private static void dsCPConnectionRequests(
        String[] expectedValues, ConnectionPoolDataSource ds) {
        try {
            ds.getPooledConnection();
            if (!expectedValues[0].equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValues[0], sqle);
        }

        dsConnectionRequest(expectedValues[1], ds, null, null);
        dsConnectionRequest(expectedValues[2], ds, "fred", null);
        dsConnectionRequest(expectedValues[3], ds, "fred", "wilma");
        dsConnectionRequest(expectedValues[4], ds, null, "wilma");
        dsConnectionRequest(
            expectedValues[5], ds, null, "databaseName=wombat");
        dsConnectionRequest(
            expectedValues[6], ds, "fred", "databaseName=wombat");
        dsConnectionRequest(expectedValues[7], 
            ds, "fred", "databaseName=wombat;password=wilma");
        dsConnectionRequest(expectedValues[8], 
            ds, "fred", "databaseName=wombat;password=betty");
    }
    
    private static void dsConnectionRequest(String expectedValue, 
        ConnectionPoolDataSource ds, String user, String ConnAttr)
    {
        try {
            ds.getPooledConnection(user, ConnAttr);
            if (!expectedValue.equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValue, sqle);
        }
    }
        
    private static void dsXAConnectionRequests(
        String[] expectedValues, XADataSource ds) {
        try {
            ds.getXAConnection();
            if (!expectedValues[0].equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValues[0], sqle);
        }

        dsConnectionRequest(expectedValues[1], ds, null, null);
        dsConnectionRequest(expectedValues[2], ds, "fred", null);
        dsConnectionRequest(expectedValues[3], ds, "fred", "wilma");
        dsConnectionRequest(expectedValues[4], ds, null, "wilma");
        dsConnectionRequest(
            expectedValues[5], ds, null, "databaseName=" + dbName);
        dsConnectionRequest(
            expectedValues[6], ds, "fred", "databaseName=" + dbName);
        dsConnectionRequest(expectedValues[7], 
            ds, "fred", "databaseName=" + dbName + ";password=wilma");
        dsConnectionRequest(expectedValues[8], 
            ds, "fred", "databaseName=" + dbName + ";password=betty");
    }
    
    private static void dsConnectionRequest(String expectedValue, 
            XADataSource ds, String user, String ConnAttr)
    {
        try {
            ds.getXAConnection(user, ConnAttr);
            if (!expectedValue.equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValue, sqle);
        }
    }

    protected void assertXAException(String tag, XAException xae) {

        // for all our cases, we expect some kind of closed con error
        // but the message is different for embedded vs. network server
        if (usingEmbedded())
            assertEquals("No current connection.", xae.getMessage());
        else if (usingDerbyNetClient())
            assertEquals(
                "XAER_RMFAIL : No current connection.", xae.getMessage());
        Throwable t = xae.getCause();
        if (t instanceof SQLException)
            assertSQLState("08003", (SQLException)t);
    }

    private static void queryOnStatement(String expectedCursorName,
        int[] expectedValues, Connection conn, Statement s) 
    throws SQLException {

        // DERBY-2531
        // network server gives mismatched connections. See also
        // comment in testAllDataSources()
        if (usingEmbedded()) {
            assertEquals(conn, s.getConnection());
        }
        resultSetQuery(expectedCursorName, expectedValues,
                       s.executeQuery("select * from intTable"));
    }

    private static void resultSetQuery(String expectedCursorName, 
        int[] expectedValues, ResultSet rs) throws SQLException 
    {
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(rs, "java.sql.ResultSet");
        }
        assertEquals(expectedCursorName, rs.getCursorName());
        int index=0;
        while (rs.next()) {
            assertEquals(expectedValues[index], rs.getInt(1));
            index++;
        }
        assertEquals(expectedValues.length, index++);
        rs.close();
    }

    private static void assertLocks(int[] expectedValues, Connection conn) 
    throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(
            "SELECT XID, sum(cast (LOCKCOUNT AS INT)) " +
            "FROM SYSCS_DIAG.LOCK_TABLE AS L GROUP BY XID");

        // Don't output actual XID's as they tend for every catalog change
        // to the system.
        int xact_index = 0;
        while (rs.next()) {
            if (expectedValues != null)
                assertEquals(expectedValues[xact_index], rs.getInt(2));
            else 
                fail("expected no locks");
            xact_index++;
        }
        if (expectedValues != null)
            assertEquals(expectedValues.length, xact_index);
        
        rs.close();
        s.close();
    }

    private void assertStatementState(int[] parameterExpectedValues, 
        int[] expectedValues, Statement s) 
    throws SQLException {
        assertEquals(expectedValues[0], s.getResultSetType());
        assertEquals(
            expectedValues[1], s.getResultSetConcurrency());
        assertEquals(
            expectedValues[2], s.getFetchDirection());
        assertEquals(expectedValues[3], s.getFetchSize());
        assertEquals(expectedValues[4], s.getMaxFieldSize());
        assertEquals(expectedValues[5], s.getMaxRows());
        assertEquals(expectedValues[6], s.getResultSetHoldability());

        if (s instanceof PreparedStatement) {
            PreparedStatement ps = (PreparedStatement) s;
            ParameterMetaData psmd = ps.getParameterMetaData();
            // Parameter count:
            assertEquals(parameterExpectedValues[0], psmd.getParameterCount());
            for (int i = 1; i <= psmd.getParameterCount(); i++) {
                assertEquals(parameterExpectedValues[i], psmd.getParameterType(i));
            }
        }
    }

    /**
    Create a statement with modified State.
     */
    private Statement createFloatStatementForStateChecking(
        int[] StatementExpectedValues, Connection conn)
    throws SQLException {
        Statement s = internalCreateFloatStatementForStateChecking(conn);
        s.setCursorName("StokeNewington");
        s.setFetchDirection(ResultSet.FETCH_REVERSE);
        s.setFetchSize(444);
        s.setMaxFieldSize(713);
        s.setMaxRows(19);

        // Create
        assertStatementState(null, StatementExpectedValues, s);
        return s;
    }

    private Statement internalCreateFloatStatementForStateChecking(
        Connection conn) throws SQLException {
        return conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private PreparedStatement createFloatStatementForStateChecking(
        int[] parameterExpectedValues, int[] PreparedStatementExpectedValues,
        Connection conn, String sql) 
    throws SQLException {
        PreparedStatement s = 
            internalCreateFloatStatementForStateChecking(conn, sql);
        s.setCursorName("StokeNewington");
        s.setFetchDirection(ResultSet.FETCH_REVERSE);
        s.setFetchSize(888);
        s.setMaxFieldSize(317);
        s.setMaxRows(91);

        // PreparedStatement Create        
        assertStatementState(
            parameterExpectedValues, PreparedStatementExpectedValues, s);
        return s;
    }
    
    private PreparedStatement internalCreateFloatStatementForStateChecking(
        Connection conn, String sql) throws SQLException {
        return conn.prepareStatement(sql, 
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private CallableStatement createFloatCallForStateChecking(
        int[] parameterExpectedValues, int[] CallableStatementExpectedValues,
        Connection conn, String sql) 
    throws SQLException 
    {
        CallableStatement s = 
            internalCreateFloatCallForStateChecking(conn, sql);
        s.setCursorName("StokeNewington");
        s.setFetchDirection(ResultSet.FETCH_REVERSE);
        s.setFetchSize(999);
        s.setMaxFieldSize(137);
        s.setMaxRows(85);

        // Callable Statement Create
        assertStatementState(
            parameterExpectedValues, CallableStatementExpectedValues, s);
        return s;
    }
    
    private CallableStatement internalCreateFloatCallForStateChecking(
        Connection conn, String sql) throws SQLException {
        return conn.prepareCall(sql, 
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private void assertConnectionOK(
        Object[] expectedValues, String dsName, Connection conn) 
    throws SQLException { 
        
        assertEquals(
            ((Integer)expectedValues[0]).intValue(), conn.getHoldability());

        // check it's a 3.0 connection object by checking if 
        // set & release Savepoint is ok.
        try {
            conn.releaseSavepoint(conn.setSavepoint());
            if (conn.getAutoCommit())
                fail("expected a SQLExpection (savepoint with autocommit on");
            if (!((String)expectedValues[1]).equals("OK"))
                fail("expected a SQLExpection (savepoint with autocommit on");
        } catch (SQLException sqle) {
            // we expect savepoints exceptions because either
            // it's a global transaction, or it's in auto commit mode.
            if (conn.getAutoCommit())
                assertSQLState("XJ010", sqle);
            else if (((String)expectedValues[1]).equals("OK"))
                throw sqle;
            else 
                assertSQLState((String)expectedValues[1], sqle);
        }

        // Running connection checks
        // connection checks currently only implemented for Embedded
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(conn, "java.sql.Connection");
            SecurityCheck.assertSourceSecurity(
                conn.getMetaData(), "java.sql.DatabaseMetaData");
        }

        assertEquals(((Integer)expectedValues[2]).intValue(), 
            conn.getTransactionIsolation());
        assertEquals(((Boolean)expectedValues[3]).booleanValue(), 
            conn.getAutoCommit());
        assertEquals(((Boolean)expectedValues[4]).booleanValue(), 
            conn.isReadOnly());

        if (dsName.endsWith("DataSource"))
            assertNull(conn.getWarnings());

        Statement s1 = conn.createStatement();
        assertStatementOK(dsName, conn, s1);
        assertStatementOK(dsName, conn, conn.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));

        Connection c1 = conn.getMetaData().getConnection();
        // c1 and conn should be the same connection object.
        if (!usingDerbyNetClient() && dsName.indexOf("DataSource")>=0)
            assertEquals(c1, conn);

        // Derby-33 - setTypeMap on connection
        try {
            Map<String, Class<?>> empty = Collections.emptyMap();
            conn.setTypeMap(empty);
            if (!((String)expectedValues[5]).equals("OK"))
                fail (" expected an sqlexception on setTypeMap(EMPTY_MAP)");
        } catch (SQLException sqle) {
            if (((String)expectedValues[5]).equals("OK"))
                throw sqle;
            else
                assertSQLState((String)expectedValues[5], sqle);
        }
        try {
            // expect 0A000 - not implemented for client,
            //        XJ081 - invalid null value passed as map for embedded
            conn.setTypeMap(null);
            fail ("setTypeMap(null) should throw exception");
        } catch (SQLException sqle) {
            assertSQLState((String)expectedValues[6], sqle);
        }
        try {
            // a populated map, not implemented
            Map<String, Class<?>> map = new HashMap<String, Class<?>>();
            map.put("name", Class.class);
            conn.setTypeMap(map);
            if (!((String)expectedValues[7]).equals("OK"))
                fail (" expected an sqlexception on setTypeMap(map)");
        } catch (SQLException sqle) {
            if (((String)expectedValues[7]).equals("OK"))
                throw sqle;
            else
                assertSQLState((String)expectedValues[7], sqle);
        }

        assertConnectionPreClose(dsName, conn);
        conn.close();

        // method calls on a closed connection
        conn.close(); // expect no error

        try {
            conn.createStatement();
            fail (dsName + " <closedconn>.createStatement(), " +
                "expected 08003 - No current connection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        try {
            s1.execute("values 1");
            fail(dsName + " <closedstmt>.execute(), " +
                "expected 08003 - No current connection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
    }

    private void assertConnectionPreClose(String dsName, Connection conn) 
    throws SQLException {

        // before closing the connection, attempt to change holdability
        // and readOnly
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

        if (!dsName.equals("Nested2"))
        {
            try {
                conn.setReadOnly(true);
            } catch (SQLException sqle) {
                // cannot set read-only in an active transaction, & sometimes
                // connections are active at this point.
                assertSQLState("25501", sqle);
            }
        }
    }
    
    private void assertStatementOK(String dsName, Connection conn, Statement s)
    throws SQLException {

        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(s, "java.sql.Statement");
        }

        Connection c1 = s.getConnection();
        if (c1 != conn)
        {
            // with DerbyNetClient and any kind of DataSource, this goes wrong
            if (!usingDerbyNetClient() && (dsName.indexOf("DataSource") >= 0))
                fail ("incorrect connection object returned for Statement.getConnection()");
        }

        s.addBatch("insert into intTable values 1");
        s.addBatch("insert into intTable values 2,3");
        int[] states = s.executeBatch();
        if (states[0] != 1)
            fail ("invalid update count for first batch statement");
        if (states[1] != 2)
            fail ("invalid update count for second batch statement");

        ResultSet rs = s.executeQuery("VALUES 1");
        if (rs.getStatement() != s)
            fail ("incorrect Statement object returned for ResultSet.getStatement for " + dsName);
        rs.close();
        s.close();
    }

    /**
    When a connection is being pooled, the underlying JDBC embedded
    connection object is re-used. As each application gets a new
    Connection object, that is really a wrapper around the old connection
    it should reset any connection spoecific state on the embedded connection
    object.
     */
    private static void PoolReset(String type, PooledConnection pc) throws SQLException
    {
        PoolResetWork("1", "C", pc.getConnection());
        PoolResetWork("2", "", pc.getConnection());
        PoolResetWork("3", "D", pc.getConnection());

        pc.close();
    }

    private static void PoolResetWork(
        String expectedID, String tableAction, Connection conn) 
    throws SQLException
    {
        Statement s = conn.createStatement();
        if (tableAction.equals("C"))
        {
            s.execute("CREATE TABLE PoolResetWork (id int generated always as identity, name varchar(25))");
        }

        ResultSet rs = s.executeQuery("VALUES IDENTITY_VAL_LOCAL()");
        rs.next();
        String val = rs.getString(1);
        if (!rs.wasNull() || (val != null))
            fail ("initial call to IDENTITY_VAL_LOCAL is not NULL!" + val);
        rs.close();

        s.executeUpdate("INSERT INTO PoolResetWork(name) values ('derby-222')");

        rs = s.executeQuery("VALUES IDENTITY_VAL_LOCAL()");
        rs.next();
        val = rs.getString(1);
        assertEquals(expectedID, val);
        rs.close();

        if (tableAction.equals("D"))
        {
            s.execute("DROP TABLE PoolResetWork");
        }

        s.close();
        conn.close();
    }

    /**
     * Make sure this connection's string is unique (DERBY-243)
     */
    private static void assertToString(Connection conn) throws Exception
    {
        assertStringFormat(conn);
        String str = conn.toString();

        if ( conns.containsKey(str))
        {
            throw new Exception("ERROR: Connection toString() is not unique: " 
                    + str);
        }
        conns.put(str, conn);
    }

    /** 
     * Check the format of a pooled connection
     **/
    private static void assertStringFormat(PooledConnection pc) throws Exception
    {
        String prefix = assertStringPrefix(pc);
        String connstr = pc.toString();
        String format = prefix + " \\(ID = [0-9]+\\), Physical Connection = " +
            "<none>|" + CONNSTRING_FORMAT;
        assertTrue(connstr.matches(format));
    }

    /**
     * Check the format of the connection string.  This is the default test
     * to run if this is not a BrokeredConnection class
     */
    private static void assertStringFormat(Connection conn) //throws Exception
    {
        assertStringPrefix(conn);
        String str = conn.toString(); 
        assertTrue("\nexpected format:\n " + CONNSTRING_FORMAT + "\nactual value:\n " + str,
            str.matches(CONNSTRING_FORMAT));
    }

    /**
     * Make sure the connection string starts with the right prefix, which
     * is the classname@hashcode.
     *
     * @return the expected prefix string, this is used in further string
     *   format checking
     */
    private static String assertStringPrefix(Object conn) //throws Exception
    {
        String connstr = conn.toString();
        String prefix = conn.getClass().getName() + "@" + conn.hashCode();
        // Connection class and has code for connection string should
        // match prefix
        assertTrue(connstr.startsWith(prefix));

        return prefix;
    }

    /**
     * Check uniqueness of connection strings coming from a
     * DataSouce
     */
    private static void assertToString(DataSource ds) throws Exception
    {
        clearConnections();

        int numConnections = 10;
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            Connection conn = ds.getConnection();
            assertToString(conn);
        }

        clearConnections();
    }

    /**
     * Clear out and close connections in the connections
     * hashtable. 
     */
    private static void clearConnections() throws SQLException
    {
        for (Connection conn : conns.values()) {
            conn.close();
        }
        conns.clear();
    }

    /**
     * Get connections  using getConnection() and make sure
     * they're unique
     */
    private void assertTenConnectionsUnique() throws Exception
    {
        clearConnections();
        // Open ten connections rather than just two to
        // try and catch any odd uniqueness bugs.  Still
        // no guarantee but is better than just two.
        int numConnections = 10;
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            Connection conn = openDefaultConnection();
            assertToString(conn);
        }

        // Now close the connections
        clearConnections();
    }

    /**
     * Check uniqueness of strings for an XA data source
     */
    private static void assertToString(XADataSource xds) throws Exception
    {
        int numConnections = 10;

        //  First get a bunch of pooled connections
        //  and make sure they're all unique
        HashMap<String, XAConnection> xaConns =
                new HashMap<String, XAConnection>();
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            XAConnection xc = xds.getXAConnection();
            assertStringFormat(xc);
            String str = xc.toString();
            // XA connection toString should be unique
            assertNull(xaConns.get(str));
            xaConns.put(str, xc);
        }

        // Now check that connections from each of these
        // pooled connections have different string values
        Iterator it = xaConns.values().iterator();
        clearConnections();
        while ( it.hasNext() )
        {
            XAConnection xc = (XAConnection)it.next();
            Connection conn = xc.getConnection();
            assertToString(conn);
        }
        clearConnections();

        // Now clear out the pooled connections
        it = xaConns.values().iterator();
        while ( it.hasNext() )
        {
            XAConnection xc = (XAConnection)it.next();
            xc.close();
        }
        xaConns.clear();
    }

    /**
     * Check uniqueness of strings with a pooled data source.
     * We want to check the PooledConnection as well as the
     * underlying physical connection. 
     */
    private static void assertToString(ConnectionPoolDataSource pds)
    throws Exception
    {
        int numConnections = 10;

        //  First get a bunch of pooled connections
        //  and make sure they're all unique
        HashMap<String, PooledConnection> pooledConns =
                new HashMap<String, PooledConnection>();
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            PooledConnection pc = pds.getPooledConnection();
            assertStringFormat(pc);
            String str = pc.toString();
            // Pooled connection toString should be unique
            assertNull( pooledConns.get(str));
            pooledConns.put(str, pc);
        }

        // Now check that connections from each of these
        // pooled connections have different string values
        Iterator it = pooledConns.values().iterator();
        clearConnections();
        while ( it.hasNext() )
        {
            PooledConnection pc = (PooledConnection)it.next();
            Connection conn = pc.getConnection();
            assertToString(conn);
        }
        clearConnections();

        // Now clear out the pooled connections
        it = pooledConns.values().iterator();
        while ( it.hasNext() )
        {
            PooledConnection pc = (PooledConnection)it.next();
            pc.close();
        }
        pooledConns.clear();
    }

    /**
     * Return the Java class and method for the procedure
     * for the nested connection test.
     */
    private static String getNestedMethodName()
    {
        return "checkNesConn";
    }

    // calling checkConnection 
    // - for use in a procedure to get a nested connection.
    public static void checkNesConn (String dsName) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        String EmptyMapValue=null;
        // Note: currently, not supported
        String NullMapValue=null;
        String MapMapValue=null;
        if (usingEmbedded())
        {
            EmptyMapValue="OK"; NullMapValue="XJ081"; MapMapValue="0A000";
        }
        else if (usingDerbyNetClient())
        {
            EmptyMapValue="0A000"; NullMapValue="0A000"; MapMapValue="0A000";
        }
        Object[] expectedValues = { 
            ResultSet.HOLD_CURSORS_OVER_COMMIT, "OK",
            2, false, false, 
            EmptyMapValue, NullMapValue, MapMapValue};

        new J2EEDataSourceTest("J2EEDataSourceTest").assertConnectionOK(
            expectedValues, dsName, conn);
    }

    /**
     * Creates a physical connection from the given data source.
     * <p>
     * For a XADataSource, <tt>getXAConnection</tt> is invoked, for a
     * ConnectionPoolDataSource <tt>getPooledConnection</tt> is invoked, and
     * for a DataSource <tt>getConnection</tt> is invoked.
     *
     * @param ds the data source to get the physical connection from
     * @return A pysical connection, which can be an instance of
     *      <tt>XAConnection</tt>, <tt>PooledConnection</tt>, or
     *      <tt>Connection</tt>
     * @throws SQLException if getting a connection fails
     * @throws IllegalArgumentException if the object isn't a data source
     */
    public static Object getPhysicalConnection(Object ds)
            throws SQLException {
        if (ds instanceof XADataSource) {
            return ((XADataSource)ds).getXAConnection();
        } else if (ds instanceof ConnectionPoolDataSource) {
            return ((ConnectionPoolDataSource)ds).getPooledConnection();
        } else if (ds instanceof DataSource) {
            return ((DataSource)ds).getConnection();
        } else {
            throw new IllegalArgumentException(
                    "Not a data source: " + ds.getClass());
        }
    }
}

class cdsXid implements Xid, Serializable
{
  private static final long serialVersionUID = 64467338100036L;

    private final int format_id;
    private byte[] global_id;
    private byte[] branch_id;


    cdsXid(int xid, byte b1, byte b2)
    {
        format_id = xid;
        global_id = new byte[Xid.MAXGTRIDSIZE];
        branch_id = new byte[Xid.MAXBQUALSIZE];

        for (int i = 0; i < global_id.length; i++) {
            global_id[i] = b1;
        }

        for (int i = 0; i < branch_id.length; i++) {
            branch_id[i] = b2;
        }
    }

    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
     * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
     * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier()
    {
        return(branch_id);
    }
}
