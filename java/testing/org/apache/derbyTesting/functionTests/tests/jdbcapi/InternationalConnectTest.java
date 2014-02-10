/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.InternationalConnectTest
 
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class InternationalConnectTest extends BaseJDBCTestCase {

    /**
     * Test connecting with multibyte characters in:
     * - Database name 
     * - User
     * - Password
     * 
     * Currently just throws an exception for client.
     * Works ok for embedded.
     * 
     * This test tests DriverManager, XADataSource and ConnectionPoolDataSource
     * and is not run with J2ME.  Simple DataSource is tested with 
     * InternationalConnectSimpleDSTest
     * 
     */
   
    /* Keep track of the databases created in the fixtures to cleanup in tearDown() */
    private ArrayList<String> databasesForCleanup;
    
    /**
     * @param name
     */
    public InternationalConnectTest(String name) {
        super(name);
        
        databasesForCleanup = new ArrayList<String>();
    }

    //DERBY-4805(Increase the length of the RDBNAM field in the DRDA 
    // implementation)
    //Fixing the jira above now prevents us from getting limited to 255 bytes
    // in network server case for RDBNAM. The new limit now is 1024 bytes.
    //Try 2 test cases for database name. 
    // One right at the upper boundary of 1024 byte length
    // and second with going little over 1024 byte length.
    //Note that the test below is written for in-memory db rather than
    // traditional on disk dbs. This is because depending on the file
    // system and operating systems, there are different limits on
    // how long a file name can be. In order to avoid having to 
    // worry about various OSes, it is more portable to do the testing
    // with in-memory db. 
    public void testBoundaries() throws SQLException, UnsupportedEncodingException {
        if (usingEmbedded()) return; /* This test is only for Client/Server */

        //To get around the file name length limit on various operating 
        // systems, using in memory db to try the long RDBNAM of 1024
        // bytes
        //Following url works fine because the length of string
        // memory...;true is 1024 bytes long
        String dbUrl1024bytes = "memory:/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;create=true";
        //Following url fails because the length of string
        // memory...;true is 1025 bytes long, 1 byte longer than max length
        String dbUrl1025bytes = "memory:/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/dir1234567890/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;create=true";

        /*
         *Prior to DERBY-4805 fix, maximum length in bytes was 255. With
         * the fix, the new maximum length is 1024 bytes
         *Try 2 test cases. One right at the upper boundary of 1024 byte length
         * and second with 1025 byte length.
         */

        /* This time it should work as we're right at the limit */
        String url = TestConfiguration
                .getCurrent().getJDBCUrl(dbUrl1024bytes);
        loadDriver(url);
        Connection conn = DriverManager.getConnection(url);
        conn.close();

        //Add test case 2
        //We will try going over 1024 byte length for database name and it 
        // will fail as expected
        url = TestConfiguration
                .getCurrent().getJDBCUrl(dbUrl1025bytes);

        try {
                conn = DriverManager.getConnection(url);
                assertTrue("Used more characters than possible in database name",
                        false);
        } catch (SQLException e) {
            	assertSQLState("08001", e); /* Check if it failed */
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
                Class.forName(driverClass).newInstance();
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
            }
        }
    }
    
    /**
     * Test Chinese character in database name, user and password, using 
     * DriverManager methods.
     * 
     * @throws SQLException
     */
    public void testDriverManagerConnect() throws SQLException {        
        //get a connection to load the driver
        getConnection();
        Connection conn = null;
        String url = null;

        //Test Chinese database name
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;create=true");
        
        conn = DriverManager.getConnection(url);
        conn.close();           

        // Test Chinese user name
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;user=\u4e10");
        conn = DriverManager.getConnection(url);
        conn.close();

        // Test Chinese user name in parameter to getConnection
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10");
        conn = DriverManager.getConnection(url,"\u4e10","pass");
        conn.close();

        // Test Chinese password in url
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;user=user;password=\u4e10");
        conn = DriverManager.getConnection(url);
        conn.close();

        // Test Chinese password in parameter to getConnection()
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10");
        conn = DriverManager.getConnection(url,"\u4e10","\u4e10");
        conn.close();
        
        /* Add the created database for cleanup by tearDown() */
        databasesForCleanup.add("\u4e10");
    }
    
    
    /**
     * Test XA Connection for chinese database name, user and password.
     * @throws SQLException
     */
    public void testXADSConnect() throws SQLException {        
        // Test chinese database name.
        XADataSource ds = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");        

        XAConnection xaconn = ds.getXAConnection();
        Connection conn = xaconn.getConnection();
        conn.close();
        xaconn.close();
  
        // Chinese user
        J2EEDataSource.setBeanProperty(ds, "user", "\u4e10");
        xaconn = ds.getXAConnection();
        conn = xaconn.getConnection();
        conn.close();
        xaconn.close();

        // Chinese password
        J2EEDataSource.setBeanProperty(ds, "password", "\u4e10");
        xaconn = ds.getXAConnection();
        conn = xaconn.getConnection();
        conn.close();
        xaconn.close();
        
        /* Add the created database for cleanup by tearDown() */
        databasesForCleanup.add("\u4e10");
    }
    
    
    /**
     * Test pooled connetion for chinese database name, user and password.
     * @throws SQLException
     */
    public void testCPDSConnect() throws SQLException {
        // Test chinese database name.
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");        

        PooledConnection poolConn = ds.getPooledConnection();
        Connection conn = poolConn.getConnection();
        conn.close();
        poolConn.close();
 
        // Chinese user
        J2EEDataSource.setBeanProperty(ds, "user", "\u4e10");
        poolConn = ds.getPooledConnection();
        conn = poolConn.getConnection();
        conn.close();
        poolConn.close();

        // Chinese password
        J2EEDataSource.setBeanProperty(ds, "password", "\u4e10");
        poolConn= ds.getPooledConnection();
        conn = poolConn.getConnection();
        conn.close();
        poolConn.close();
        
        /* Add the created database for cleanup by tearDown() */
        databasesForCleanup.add("\u4e10");
    }

    /**
     * Regression test case for DERBY-4799. Attempting to connect to a
     * database that doesn't exist used to cause a protocol error between
     * the network server and the client. This only happened if the
     * database name was at least 18 characters and the name contained at
     * least one non-ascii character.
     */
    public void testFailureOnNonExistentDatabase() throws SQLException {
        
        String url = TestConfiguration.getCurrent().getJDBCUrl(
                "abcdefghijklmnopq\u00E5");
        try {
            // This call used to fail with a protocol error with the
            // client driver. Check that it fails gracefully now.
            DriverManager.getConnection(url);
            fail(url + " should not exist");
        } catch (SQLException sqle) {
            // Embedded responds with XJ004 - database not found.
            // Client responds with 08004 - connection refused because
            // the database was not found.
            String expected = usingEmbedded() ? "XJ004" : "08004";
            assertSQLState(expected, sqle);
        }
    }

    public void tearDown() throws Exception {
        /* Iterate through the databases for cleanup and delete them */
        for (int i=0; i<databasesForCleanup.size(); i++) {
            String shutdownUrl = TestConfiguration.getCurrent()
                                .getJDBCUrl(databasesForCleanup.get(i) + ";shutdown=true");
            try {
                DriverManager.getConnection(shutdownUrl);
                fail("Database didn't shut down");
            } catch (SQLException se) {
                // ignore shutdown exception
                assertSQLState("08006", se);
            }
            removeDirectory(getSystemProperty("derby.system.home") +  File.separator + 
                    databasesForCleanup.get(i));
        }
        
        /* Clear the array list as new fixtures will add other databases */
        databasesForCleanup = null;

        super.tearDown();
    }
    
    public static Test suite() {        
        return TestConfiguration.defaultSuite(InternationalConnectTest.class);
    }
   
}
