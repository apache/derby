/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.DataSourceTest

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

import junit.framework.*;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.AssertEventCatcher;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.net.InetAddress;
import java.sql.*;

import javax.sql.*;

/**
 * Tests of the <code>javax.sql.DataSource</code> JDBC40 API.
 */

public class DataSourceTest extends BaseJDBCTestCase {
    
    //Default DataSource that will be used by the tests
    private DataSource ds = null;
    protected static String dbName = 
        TestConfiguration.getCurrent().getDefaultDatabaseName();
    
    /**
     *
     * Create a test with the given name.
     *
     * @param name name of the test.
     *
     */
    public DataSourceTest(String name) {
        super(name);
    }
    
    /**
     * Create a default DataSource
     */
    public void setUp() {
        ds = JDBCDataSource.getDataSource();
    }
    
    /**
     * 
     * Initialize the ds to null once the tests that need to be run have been 
     * run
     */
    public void tearDown() throws Exception {
        ds = null;
        super.tearDown();
    }

    public void testIsWrapperForDataSource() throws SQLException {
        assertTrue(ds.isWrapperFor(DataSource.class));
    }

    public void testIsNotWrapperForPoolDataSource() throws SQLException {
        assertFalse(ds.isWrapperFor(ConnectionPoolDataSource.class));
    }

    public void testIsNotWrapperForXADataSource() throws SQLException {
        assertFalse(ds.isWrapperFor(XADataSource.class));
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(ds.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapDataSource() throws SQLException {
        DataSource ds2 = ds.unwrap(DataSource.class);
        assertSame("Unwrap returned wrong object.", ds, ds2);
    }

    public void testUnwrapConnectionPoolDataSource() {
        try {
            ConnectionPoolDataSource cpds =
                ds.unwrap(ConnectionPoolDataSource.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapXADataSource() {
        try {
            XADataSource xads = ds.unwrap(XADataSource.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = ds.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }
    
    /**
     * Test case for DERBY-3172
     * When the Derby engine is shutdown or Network Server is brought down, any
     * api on JDBC Connection object should generate a Connection error event.
     */
    public void testConnectionErrorEvent() throws SQLException, Exception
    {
    	Connection conn;
    	ConnectionPoolDataSource ds;
    	PooledConnection pc;
    	Statement st;
        AssertEventCatcher aes12 = new AssertEventCatcher(12);
        //Get the correct ConnectionPoolDataSource object
        if (usingEmbedded())
        {
        	ds = new EmbeddedConnectionPoolDataSource();
            ((EmbeddedConnectionPoolDataSource)ds).setDatabaseName(dbName);
        } else
        {
            ds = new ClientConnectionPoolDataSource();
            ((ClientConnectionPoolDataSource)ds).setDatabaseName(dbName);
        }
        pc = ds.getPooledConnection();
        //Add a connection event listener to ConnectionPoolDataSource
        pc.addConnectionEventListener(aes12);
        conn = pc.getConnection();
        st = conn.createStatement();
        //TAB1 does not exist and hence catch the expected exception
        try {
            st.executeUpdate("drop table TAB1");
        } catch (SQLException sqle) {
            assertSQLState("42Y55", sqle);
        }
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
        	stopNetworkServer();
        }
        //Now try to use various apis on the JDBC Connection object created 
        //before shutdown and they all should generate connection error event.
        try {
            conn.createArrayOf("junk", null);
        } catch (SQLException e) {
            assertSQLState("0A000", e);
        }
        try {
            conn.createNClob();
        } catch (SQLException e) {
            assertSQLState("0A000", e);
        }
        try {
            conn.createSQLXML();
        } catch (SQLException e) {
            assertSQLState("0A000", e);
        }
        try {
            conn.createStruct("junk", null);
        } catch (SQLException e) {
            assertSQLState("0A000", e);
        }
        try {
            conn.createBlob();
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
            conn.createClob();
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.getClientInfo();
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.getClientInfo("junk");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.setClientInfo(null);
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.setClientInfo("junk1", "junk2");
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        assertTrue(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.isWrapperFor(this.getClass());
        } catch (SQLException e) {
                assertSQLState("08003", e);
        }
        assertFalse(aes12.didConnectionClosedEventHappen());
        if (usingEmbedded())
        	assertTrue(aes12.didConnectionErrorEventHappen());
        else
        	//We do not make any call on underneath JDBC Connection
        	//object for isWrapperFor and hence never get Connection
        	//Error event
        	assertFalse(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.unwrap(this.getClass());
        } catch (SQLException e) {
            if (usingEmbedded())
                assertSQLState("08003", e);
            else
            	//We do not make any call on underneath JDBC Connection
            	//object for unwrap and hence never get Connection
            	//closed exception. Instead we got exception because
            	//client driver code is trying to unwrap this.getClass
            	//and it can't do that
                assertSQLState("XJ128", e);
        }
    	assertFalse(aes12.didConnectionClosedEventHappen());
        if (usingEmbedded())
        	assertTrue(aes12.didConnectionErrorEventHappen());
        else
        	//We do not make any call on underneath JDBC Connection
        	//object for isWrapperFor and hence never get Connection
        	//Error event
        	assertFalse(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        try {
            conn.isValid(5);
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }
        if (usingEmbedded())
        	assertTrue(aes12.didConnectionClosedEventHappen());
        else
        	assertFalse(aes12.didConnectionClosedEventHappen());
    	//As per the JDBC definition, an exception and hence an event is raised
    	//for isValid only if the param value is illegal
    	assertFalse(aes12.didConnectionErrorEventHappen());
        aes12.resetState();        	
        if (usingEmbedded())
        {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        }else
        {
        	startNetworkServer();
        }

        // Get a new connection to the database
        conn = getConnection();
        conn.close();
    }
    /**
     * Stop the network server
     */
    private void stopNetworkServer() {
        try {
            NetworkServerControl networkServer = new NetworkServerControl();
            networkServer.shutdown();
        } catch(Exception e) {
            System.out.println("INFO: Network server shutdown returned: " + e);
        }
    }

    /**
     * Start the network server
     */
    private void startNetworkServer() {
        int serverPort;

        // Determines which host and port to run the network server on
        // This is based how it is done in the test testSecMec.java
        String serverName = TestUtil.getHostName();
        if (serverName.equals("localhost")) {
            serverPort = 1527;
        }
        else {
            serverPort = 20000;
        }

        try {
            NetworkServerControl networkServer = 
                     new NetworkServerControl(InetAddress.getByName(serverName), 
                                              serverPort);
            networkServer.start(null);

            // Wait for the network server to start
            boolean started = false;
            int retries = 10;         // Max retries = max seconds to wait
            while (!started && retries > 0) {
                try {
                    // Sleep 1 second and then ping the network server
		      Thread.sleep(1000);
                    networkServer.ping();

                    // If ping does not throw an exception the server has started
                    started = true;
                } catch(Exception e) {
                    System.out.println("INFO: ping returned: " + e);
                    retries--;
	         }
	     }

            // Check if we got a reply on ping
            if (!started) {
                System.out.println("FAIL: Failed to start network server");
            }
        } catch (Exception e) {
            System.out.println("FAIL: startNetworkServer got exception: " + e);
        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(DataSourceTest.class);
    }
}
