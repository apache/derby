/*
 *
 * Derby - Class BaseJDBCTestCase
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

import java.sql.*;

/**
 * Base class for JDBC JUnit tests.
 * A method for getting a default connection is provided, along with methods
 * for telling if a specific JDBC client is used.
 */
public class BaseJDBCTestCase
    extends BaseTestCase {

    /**
     * Tell if we are allowed to use DriverManager to create database
     * connections.
     */
    private static final boolean HAVE_DRIVER;

    static {
        // See if java.sql.Driver is available. If it is not, we must use
        // DataSource to create connections.
        boolean haveDriver = false;
        try {
            Class.forName("java.sql.Driver");
            haveDriver = true;
        } catch (Exception e) {}
        HAVE_DRIVER = haveDriver;
    }
    
    /**
     * Create a test case with the given name.
     *
     * @param name of the test case.
     */
    public BaseJDBCTestCase(String name) {
        super(name);
    }

    /**
     * Get connection to the default database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     *
     * @return connection to default database.
     */
    public static Connection getConnection()
        throws SQLException {
        Connection con = null;
        JDBCClient client = CONFIG.getJDBCClient();
        if (HAVE_DRIVER) {
            loadJDBCDriver(client.getJDBCDriverName());
            con = DriverManager.getConnection(
                    CONFIG.getJDBCUrl() + ";create=true",
                    CONFIG.getUserName(),
                    CONFIG.getUserPassword());
        } else {
            throw new UnsupportedOperationException(
                    "Creating a connection in a JSR-169 " +
                    "environment is not yet supported. " +
                    "Please implement :)");
        }
        return con;
    }

   /**
    * Tell if the client is embedded.
    *
    * @return <code>true</code> if using the embedded client
    *         <code>false</code> otherwise.
    */
    public static boolean usingEmbedded() {
        return (CONFIG.getJDBCClient() == JDBCClient.EMBEDDED);
    }
   
    /**
    * Tell if the client is DerbyNetClient.
    *
    * @return <code>true</code> if using the DerbyNetClient client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNetClient() {
        return (CONFIG.getJDBCClient() == JDBCClient.DERBYNETCLIENT);
    }
    
    /**
    * Tell if the client is DerbyNet.
    *
    * @return <code>true</code> if using the DerbyNet client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNet() {
        return (CONFIG.getJDBCClient() == JDBCClient.DERBYNET);
    }

    /**
     * Load the specified JDBC driver
     *
     * @param driverClass name of the JDBC driver class.
     * @throws SQLException if loading the driver fails.
     */
    private static void loadJDBCDriver(String driverClass) 
        throws SQLException {
        try {
            Class.forName(driverClass).newInstance();
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException("Failed to load JDBC driver '" + 
                                    driverClass + "': " + cnfe.getMessage());
        } catch (IllegalAccessException iae) {
            throw new SQLException("Failed to load JDBC driver '" +
                                    driverClass + "': " + iae.getMessage());
        } catch (InstantiationException ie) {
            throw new SQLException("Failed to load JDBC driver '" +
                                    driverClass + "': " + ie.getMessage());
        }
    }

} // Enc class BaseJDBCTestCase
