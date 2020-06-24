/*
 *
 * Derby - Class org.apache.derbyTesting.junit.DriverManagerConnector
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
package org.apache.derbyTesting.junit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Connection factory using DriverManager.
 *
 */
public class DriverManagerConnector implements Connector {

    private TestConfiguration config;

    public DriverManagerConnector() {
    }

    public void setConfiguration(TestConfiguration config) {
        this.config = config;
    }

    public Connection openConnection() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        return openConnection(config.getDefaultDatabaseName(), config.getUserName(), config.getUserPassword());
    }

    public Connection openConnection(String databaseName) throws SQLException {
        return openConnection(databaseName, config.getUserName(), config.getUserPassword());
    }

    public Connection openConnection(String user, String password) throws SQLException {
        return openConnection(config.getDefaultDatabaseName(), user, password);
    }

    /**
     * Open a connection using the DriverManager.
     * <BR>
     * The JDBC driver is only loaded if DriverManager.getDriver()
     * for the JDBC URL throws an exception indicating no driver is loaded.
     * <BR>
     * If the connection request fails with SQLState XJ004
     * (database not found) then the connection is retried
     * with attributes create=true.
     */
    public Connection openConnection(String databaseName, String user, String password)
            throws SQLException
    {
        return openConnection( databaseName, user, password, (Properties)  null );
    }
    
    /**
     * Open a connection using the DriverManager.
     * <BR>
     * The JDBC driver is only loaded if DriverManager.getDriver()
     * for the JDBC URL throws an exception indicating no driver is loaded.
     * <BR>
     * If the connection request fails with SQLState XJ004
     * (database not found) then the connection is retried
     * with attributes create=true.
     */
    public  Connection openConnection
        (String databaseName, String user, String password, Properties connectionProperties)
         throws SQLException
    {
        String url = config.getJDBCUrl(databaseName);
//IC see: https://issues.apache.org/jira/browse/DERBY-2087

        try {
            DriverManager.getDriver(url);
        } catch (SQLException e) {
            loadJDBCDriver();
        }

        Properties connectionAttributes =
                new Properties(config.getConnectionAttributes());
        if ( user != null ) { connectionAttributes.setProperty("user", user); }
        if ( password  != null ) { connectionAttributes.setProperty("password", password); }

        if ( connectionProperties != null ) { connectionAttributes.putAll( connectionProperties ); }

        try {
            return DriverManager.getConnection(url, connectionAttributes);
        } catch (SQLException e) {

            // Uncomment this for more information
            // printFullException( e, 0 );

            // Expected state for database not found.
            // For the client the generic 08004 is returned,
            // will just retry on that.
            String expectedState = 
                config.getJDBCClient().isEmbedded() ? "XJ004" : "08004";

            // If there is a database not found exception
            // then retry the connection request with
            // a create attribute.
            if (!expectedState.equals(e.getSQLState()))
                throw e;
            
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
            Properties attributes = new Properties(connectionAttributes);
            attributes.setProperty("create", "true");

            return DriverManager.getConnection(url, attributes);
        }
    }

    private static void printFullException( Throwable t, int indentLevel )
    {
        if ( t == null ) { return; }
//IC see: https://issues.apache.org/jira/browse/DERBY-6094

        String              tab = "    ";
        StringBuilder   buffer = new StringBuilder();

        for ( int i = 0; i < indentLevel; i++ ) { buffer.append( tab ); }
        buffer.append( "Message:  " + t.getMessage() );

        SQLException    nextSQLException = null;
        
        if ( t instanceof SQLException )
        {
            SQLException    se = (SQLException) t;

            buffer.append( se.getClass().getName() + " : SQLState = " + se.getSQLState() );

            nextSQLException = se.getNextException();
        }

        System.out.println( buffer.toString() );

        printFullException( nextSQLException, indentLevel + 1 );
        printFullException( t.getCause(), indentLevel + 1 );
    }

    /**
     * Shutdown the database using the attributes shutdown=true
     * with the user and password defined by the configuration.
     */
    public void shutDatabase() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        Properties p = new Properties();
        p.setProperty("shutdown", "true");
        getConnectionByAttributes(config.getJDBCUrl(), p);
        config.waitForShutdownComplete(getDatabaseName());
    }

    /**
     * Shutdown the engine using the attributes shutdown=true
     * and no database name with the user and password defined
     * by the configuration.
     * Always shutsdown using the embedded URL thus this
     * method will not work in a remote testing environment.
     * @param deregisterDriver
     * @throws java.sql.SQLException
     */
    public void shutEngine(boolean deregisterDriver) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        Properties p = new Properties();
        p.setProperty("shutdown", "true");
        
        if (!deregisterDriver) {
            p.setProperty("deregister", "false");
        }

        getConnectionByAttributes("jdbc:derby:", p);
    }
    
    public void setLoginTimeout( int seconds ) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6094
        DriverManager.setLoginTimeout( seconds );
    }
    
    public int getLoginTimeout() throws SQLException
    {
        return DriverManager.getLoginTimeout();
    }
    
    /**
     * Open a connection using JDBC attributes with a JDBC URL.
     * The attributes user and password are set from the configuration
     * and then the passed in attribute is set.
     */
    private Connection getConnectionByAttributes(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            String url,
            Properties p)
        throws SQLException
    {
        Properties attributes = new Properties();

        attributes.setProperty("user", config.getUserName());
        attributes.setProperty("password", config.getUserPassword());

        for (Enumeration e = p.keys(); e.hasMoreElements(); ) {
            String key = (String)e.nextElement();
            attributes.setProperty(key, p.getProperty(key));
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-4741
        try {
            DriverManager.getDriver(url);
        } catch (SQLException e) {
            loadJDBCDriver();
        }

        return DriverManager.getConnection(url, attributes);
    }
    
    public String getDatabaseName(){
        // always use the default database name
        // if this connector is used with other databases, we
        // might need another method that takes the databasename
        String databaseName = config.getDefaultDatabaseName();
        return databaseName;
    }

    /**
     * Load the JDBC driver defined by the JDBCClient for
     * the configuration.
     *
     * @throws SQLException if loading the driver fails.
     */
    private void loadJDBCDriver() throws SQLException {
        String driverClass = config.getJDBCClient().getJDBCDriverName();
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            Class<?> clazz = Class.forName(driverClass);
            clazz.getConstructor().newInstance();
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + cnfe.getMessage());
        } catch (IllegalAccessException iae) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + iae.getMessage());
        } catch (InstantiationException ie) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + ie.getMessage());
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        } catch (NoSuchMethodException ie) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + ie.getMessage());
        } catch (java.lang.reflect.InvocationTargetException ie) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + ie.getMessage());
        }
    }
}
