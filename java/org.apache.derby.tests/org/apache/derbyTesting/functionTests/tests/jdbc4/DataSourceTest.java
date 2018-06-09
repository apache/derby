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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.CommonDataSource;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XADataSource;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.AssertEventCatcher;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests of the <code>javax.sql.DataSource</code> JDBC40 API.
 */

public class DataSourceTest extends BaseJDBCTestCase {
    
    //Default DataSource that will be used by the tests
    private DataSource ds = null;
    
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
        AssertEventCatcher aes12 = new AssertEventCatcher(12);
        //Get the correct ConnectionPoolDataSource object
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
        Class<?> clazz;
        if (usingEmbedded())
        {
            clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
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
     * <p>
     * Test the new method added by JDBC 4.1.
     * </p>
     */
    public void test_jdbc4_1() throws Exception
    {
        DataSource  ds = JDBCDataSource.getDataSource();
        ConnectionPoolDataSource cpds = J2EEDataSource.getConnectionPoolDataSource();
        XADataSource xads = J2EEDataSource.getXADataSource();

        vetDSjdbc4_1( ds );
        vetDSjdbc4_1( cpds );
        vetDSjdbc4_1( xads );
    }
    private void    vetDSjdbc4_1( CommonDataSource cds ) throws Exception
    {
        println( "Vetting a " + cds.getClass().getName() );

        Wrapper41DataSource wrapper = new Wrapper41DataSource( cds );
        
        try {
            wrapper.getParentLogger();
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }
    }


    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        // Use explicit ordering of fixtures until fix of DERBY-5988
        BaseTestSuite s = new BaseTestSuite("datasourcetestsuite-embedded");
        s.addTest(new DataSourceTest("testIsNotWrapperForPoolDataSource"));
        s.addTest(new DataSourceTest("testIsNotWrapperForXADataSource"));
        s.addTest(new DataSourceTest("testUnwrapConnectionPoolDataSource"));
        s.addTest(new DataSourceTest("testIsWrapperForDataSource"));
        s.addTest(new DataSourceTest("testIsNotWrapperForResultSet"));
        s.addTest(new DataSourceTest("testUnwrapXADataSource"));
        s.addTest(new DataSourceTest("testConnectionErrorEvent"));
        s.addTest(new DataSourceTest("testUnwrapResultSet"));
        s.addTest(new DataSourceTest("testUnwrapDataSource"));
        s.addTest(new DataSourceTest("test_jdbc4_1"));
        BaseTestSuite ns = new BaseTestSuite("datasourcetestsuite-net");
        ns.addTest(new DataSourceTest("testIsNotWrapperForPoolDataSource"));
        ns.addTest(new DataSourceTest("testIsNotWrapperForXADataSource"));
        ns.addTest(new DataSourceTest("testUnwrapConnectionPoolDataSource"));
        ns.addTest(new DataSourceTest("testIsWrapperForDataSource"));
        ns.addTest(new DataSourceTest("testIsNotWrapperForResultSet"));
        ns.addTest(new DataSourceTest("testUnwrapXADataSource"));
        ns.addTest(new DataSourceTest("testConnectionErrorEvent"));
        ns.addTest(new DataSourceTest("testUnwrapResultSet"));
        ns.addTest(new DataSourceTest("testUnwrapDataSource"));
        ns.addTest(new DataSourceTest("test_jdbc4_1"));

        BaseTestSuite totalsuite = new BaseTestSuite("datasourcetest");
        totalsuite.addTest(new CleanDatabaseTestSetup(s));
        totalsuite.addTest(TestConfiguration.clientServerDecorator(
                               new CleanDatabaseTestSetup(ns)));
        return totalsuite;
    }
}
