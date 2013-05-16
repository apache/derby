/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DataSourceTest

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

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.jdbc.ClientDataSourceInterface;
import org.apache.derby.jdbc.BasicEmbeddedDataSource40;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.SecurityCheck;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the various DataSource implementations of Derby, but not
 * ConnectionPoolDataSource or XADataSource; those are tested in
 * J2EEDataSourceTest.
 * 
 * Performs SecurityCheck analysis on the JDBC objects returned.
 * This is because this test returns to the client a number of
 * different implementations of Connection, Statement etc.
 * 
 * @see org.apache.derbyTesting.functionTests.util.SecurityCheck
 *
 */
public class DataSourceTest extends BaseJDBCTestCase {

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
    
    public DataSourceTest(String name) {
        super(name);
    }
    
    /**
     * Return a suite of tests that are run with both client and embedded
     * 
     * @param postfix suite name postfix
     * @return A suite of tests to be run with client and/or embedded
     */
    private static Test baseSuite(String postfix) {
        TestSuite suite = new TestSuite("ClientAndEmbedded" + postfix);
        suite.addTest(new DataSourceTest("testBadConnectionAttributeSyntax"));
        suite.addTest(new DataSourceTest("testDescriptionProperty"));
        suite.addTest(new DataSourceTest("testAllDataSources"));
        suite.addTest(new DataSourceTest("testJira95ds"));
        return suite;
    }

    /**
     * Return a suite of tests that are run with client only
     * 
     * @return A suite of tests being run with client only
     */
    private static Test getClientSuite() {
        TestSuite suite = new TestSuite("Client/Server");
        suite.addTest(new DataSourceTest("testClientDSConnectionAttributes"));
        suite.addTest(new DataSourceTest(
                "testClientTraceFileDSConnectionAttribute"));
        suite.addTest(new DataSourceTest(
                "testClientMessageTextConnectionAttribute"));
        return suite;
    }
    
    /**
     * Return a suite of tests that are run with embedded only
     * 
     * @param postfix suite name postfix
     * @return A suite of tests being run with embedded only
     */
    private static Test getEmbeddedSuite(String postfix) {
        TestSuite suite = new TestSuite("Embedded" + postfix);
        suite.addTest(new DataSourceTest("testDSRequestAuthentication"));
        return suite;
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("DataSourceTest suite");
        // Add the tests that only run with embedded
        suite.addTest(getEmbeddedSuite("embedded"));
        // Add tests that will run with embedded
        suite.addTest(baseSuite(":embedded"));
        if (!JDBC.vmSupportsJSR169()) {
            //  and network server/client
            suite.addTest(TestConfiguration.clientServerDecorator(
                    baseSuite(":client")));
            // Add the tests that only run with client
            suite.addTest(TestConfiguration.clientServerDecorator(
                    getClientSuite()));
        }
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
                s.executeUpdate("create table intTable(i int)");
            }
        };
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

    // the J2EEDataSourceTest version of this includes testing for
    // setTypeMap, which is supported for both EmbeddedDataSource and
    // ClientDataSource
    public void testAllDataSources() throws SQLException, Exception
    {
        //Connection dmc = getConnection();
        
        Object[] expectedValues = {
            new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT), "XJ010",
            new Integer(2), new Boolean(true), new Boolean(false)};

        if (usingEmbedded())
            assertTenConnectionsUnique();

        DataSource dscs = JDBCDataSource.getDataSource();
        if (usingEmbedded()) 
                assertToString(dscs);

        DataSource ds = dscs;
        assertConnectionOK(expectedValues, "DataSource", ds.getConnection());
        
        if (JDBC.vmSupportsJDBC4()) {
            BasicEmbeddedDataSource40 nds = new BasicEmbeddedDataSource40();
            nds.setDatabaseName(dbName);
            assertConnectionOK(
                    expectedValues, "BasicDataSource", nds.getConnection());
        }
    }            
    
    // test jira-derby 95 - a NullPointerException was returned when passing
    // an incorrect database name, should now give error:
    // XCY00 - invalid valid for property ...  
    // with DataSource
    public void testJira95ds() throws SQLException {
        try {
            DataSource ds = JDBCDataSource.getDataSource();
            // non-existent database
            JDBCDataSource.setBeanProperty(ds, "databaseName", "jdbc:derby:wombat");
            ds.getConnection();
            fail ("expected an SQLException!");
        } catch (SQLException sqle) {
            assertSQLState("XCY00", sqle);
        } catch (Exception e) {
            fail ("unexpected exception: " + e.toString());
        }
    } 

    // this fixture has a counterpart for Pooled and XA DataSources in
    // J2EEDataSourceTest
    public void testBadConnectionAttributeSyntax() throws SQLException {
        
        // DataSource - bad connattr syntax
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "ConnectionAttributes", "bad");
        try {
            ds.getConnection();
            fail ("should have seen an error");
        } catch (SQLException e) {
            assertSQLState("XJ028", e);
        } 
    } // End testBadConnectionAttributeSyntax
        
    /**
     * Check that database name set using setConnectionAttributes is not used
     * by ClientDataSource. This method tests DERBY-1130.
     * this fixture has a counterpart for Pooled and XA DataSources in 
     * J2EEDataSourceTest
     * 
     * @throws SQLException
     */
    public void testClientDSConnectionAttributes() throws Exception {
        if (usingEmbedded())
            return;
        
        ClientDataSourceInterface ds = null;

        if (JDBC.vmSupportsJNDI()) {
            // Use reflection to avoid class not found in non-JNDI context
            ds = (ClientDataSourceInterface)Class.forName(
                    "org.apache.derby.jdbc.ClientDataSource").newInstance();
        } else {
            ds = (ClientDataSourceInterface)Class.forName(
                    "org.apache.derby.jdbc.BasicClientDataSource40").
                    newInstance();
        }

        ds.setPortNumber(TestConfiguration.getCurrent().getPort());
        
        // DataSource - EMPTY; expect error 08001 in all cases
        // 08001: Required Derby DataSource property databaseName not set.
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, ds);

        // DataSource - connectionAttributes=databaseName=<valid name>
        ds.setConnectionAttributes("databaseName=" + dbName);
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, ds);
        ds.setConnectionAttributes(null);

        // Test that (invalid) database name specified in connection
        // attributes is not used
        // DataSource - databaseName=<valid db> and 
        // connectionAttributes=databaseName=kangaroo
        ds.setConnectionAttributes("databaseName=kangaroo");
        ds.setDatabaseName(dbName);
        dsConnectionRequests(new String[]  
            {"OK","08001","OK","OK",
             "08001","08001","OK","OK","OK"}, ds);
        ds.setConnectionAttributes(null);
        ds.setDatabaseName(null);

    } // End testClientDSConnectionAttributes
            
    // Following test is similar to testClientDSConnectionAttributes, but
    // for embedded datasources.
    // This fixture has a counterpart for Pooled and XA DataSources in
    // J2EEDataSourceTest. In that test, the corresponding fixture
    // also includes testing for setAttributesAsPassword.
    // Note that in this simple shape, there is no authentication done
    // but the fixture name is kept the same for reference to the J2EE one.
    public void testDSRequestAuthentication() throws SQLException {

        // Create an empty datasource of the type befitting the jvm/client/server
        JDBCClient dsclient = getTestConfiguration().getJDBCClient();
        String dsName = dsclient.getDataSourceClassName();
        DataSource ds = null;
        try {
            ds = (javax.sql.DataSource) Class.forName(dsName).newInstance();
        } catch (Exception e) {
            fail("unable to complete test because unable to create new instance of datasource");
        }

        // DataSource - EMPTY
        dsConnectionRequests(new String[] {  
             "XJ004","XJ004","XJ004","XJ004",
             "XJ004","XJ004","XJ004","XJ004","XJ004"}, ds);
 
        // DataSource - connectionAttributes=databaseName=wombat");
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", "databaseName=" + dbName);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ004",
            "XJ004","XJ004","XJ004","XJ004","XJ004"}, ds);
        JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        ds = null;

        TestConfiguration.getCurrent().shutdownDatabase();
    }
    
    /**
     * Check that traceFile connection attribute functions correctly.
     * tracefile was tested in checkDriver, but not for DataSources.
     * tracefile= was used in datasourcepermissions_net, but that's 
     * incorrect syntax. Note that we're not checking the contents of
     * the tracefile.
     * 
     * This fixture has a counterpart for Pooled and XA DataSources in
     * J2EEDataSourceTest
     *
     * Note also that this test cannot run against a remote server.
     *  
     * @throws SQLException
     */
    public void testClientTraceFileDSConnectionAttribute() throws SQLException
    {
        if (usingEmbedded())
            return;

        String traceFile;

        // DataSource
        DataSource ds = JDBCDataSource.getDataSource();

        // DataSource - setTransationAttributes
        traceFile = "trace1.out";
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
        		"traceFile="+traceFile);

        // In this scenario, we *only* get a tracefile, if we first get a 
        // successful connection, followed by an unsuccessful connection. 
        // So, we cannot just use ds.getConnection()
        dsGetBadConnection(ds);
        JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");

        // DataSource - setTraceFile
        traceFile = "trace2.out";
        JDBCDataSource.setBeanProperty(ds, "traceFile", traceFile);
        ds.getConnection();
        ds = null;

        assertTraceFilesExist();
    }
        
    /* -- Helper Methods for testClientTraceFileDSConnectionAttribute -- */
    
    private static void dsGetBadConnection(DataSource ds) {
        // first get a good connection, or we don't get a
        // traceFile when using connectionattributes.
        // also, we do not get a tracefile that way unless
        // we see an error.
        // with setTraceFile, we *always* get a file, even
        // with just a successful connection.
        try {
            ds.getConnection();
            ds.getConnection(null, null);
            fail("expected an sqlException");
        } catch (SQLException sqle) {
            assertSQLState("08001", sqle);
        }
    }
    
    /**
     * Check that trace file exists in <framework> directory
     */
    private static void assertTraceFilesExist() 
    {
        for (int i = 0; i < 2; i++) {
            String traceFileName = "trace" + (i + 1) + ".out";
            File traceFile = new File(traceFileName);
            assertTrue(PrivilegedFileOpsForTests.exists(traceFile));
        }
    }

    /**
     * Check that messageText connection attribute functions correctly.
     * retrievemessagetext was tested in checkdriver, and derbynet/testij,
     * but not tested for datasources, and in datasourcepermissions_net,
     * but as it has nothing to do with permissions/authentication,
     * this test seems a better place for it. 
     * 
     * This fixture has a counterpart for Pooled and XA DataSources in
     * J2EEDataSourceTest
     * 
     * @throws SQLException
     */
    public void testClientMessageTextConnectionAttribute() throws Exception
    {
        if (usingEmbedded())
            return;
        
        String retrieveMessageTextProperty = "retrieveMessageText";
        Connection conn;

        // DataSource
        // DataSource - retrieveMessageTextProperty
        ClientDataSourceInterface ds = null;

        if (JDBC.vmSupportsJNDI()) {
            // Use reflection to avoid class not found in non-JNDI context
            ds = (ClientDataSourceInterface)Class.forName(
              "org.apache.derby.jdbc.ClientDataSource").newInstance();
        } else {
            ds = (ClientDataSourceInterface)Class.forName(
              "org.apache.derby.jdbc.BasicClientDataSource40").newInstance();
        }

        ds.setPortNumber(TestConfiguration.getCurrent().getPort());
        ds.setDatabaseName(dbName);
        ds.setConnectionAttributes(retrieveMessageTextProperty + "=false");
        conn = ds.getConnection();
        assertMessageText(conn,"false");
        conn.close();
        // now try with retrieveMessageText = true
        ds.setConnectionAttributes(retrieveMessageTextProperty + "=true");
        conn = ds.getConnection();
        assertMessageText(conn,"true");
        ds.setConnectionAttributes(null);
        conn.close();
    }

    /* -- Helper Method for testClientMessageTextDSConnectionAttribute -- */

    private static void assertMessageText(
            Connection conn, String retrieveMessageTextValue) 
    throws SQLException
    {
        try {
            conn.createStatement().executeQuery("SELECT * FROM APP.NOTTHERE");
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
        }
    }

    /**
     * Check that messageText connection attribute functions correctly.
     * retrievemessagetext was tested in checkdriver, and derbynet/testij
     * (but not tested for datasources), and in datasourcepermissions_net,
     * but as it has nothing to do with permissions/authentication,
     * this test seems a better place for it. 
     * 
     * This fixture has a counterpart for Pooled and XA DataSources in
     * J2EEDataSourceTest
     *  
     * @throws SQLException
     */
    public void testDescriptionProperty() 
    throws SQLException, Exception {
        
        // DataSource - setDescription
        subTestDataSourceDescription(JDBCDataSource.getDataSource());
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
        ds.getConnection();
        assertEquals(setDescription, JDBCDataSource.getBeanProperty(ds, "description"));
        JDBCDataSource.clearStringBeanProperty(ds, "description");
        assertNull(JDBCDataSource.getBeanProperty(ds, "description"));    	
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
                fail ("unexpected JDBC 3.0 savepoint SQL Exception");
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

        // if this fixture runs without the default db being created,
        // there will not be a warning. Otherwise,  the warning will be,
        // cannot create db, connecting to existing db
        if (conn.getWarnings() != null)
            assertSQLState("01J01", conn.getWarnings());

        Statement s1 = conn.createStatement();
        assertStatementOK(dsName, conn, s1);
        assertStatementOK(dsName, conn, conn.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));

        Connection c1 = conn.getMetaData().getConnection();
        // c1 and conn should be the same connection object.
        if (!usingDerbyNetClient() && dsName.indexOf("DataSource")>=0)
            assertEquals(c1, conn);

        assertConnectionPreClose(dsName, conn);
        conn.close();

        // method calls on a closed connection
        try {
            conn.close(); // expect no error
        } catch (SQLException sqle) {
            fail(" unexpected exception on <closedconn>.close() ");
        }
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
     * Check the format of the connection string.  This is the default test
     * to run if this is not a BrokeredConnection class
     */
    private static void assertStringFormat(Connection conn) //throws Exception
    {
        assertStringPrefix(conn);
        String str = conn.toString(); 
        // matches is not a supported method with JSR169
        if (!JDBC.vmSupportsJSR169())
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
}
