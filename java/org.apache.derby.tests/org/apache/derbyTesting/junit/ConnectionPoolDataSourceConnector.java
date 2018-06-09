/*
 *
 * Derby - Class org.apache.derbyTesting.junit.ConnectionPoolDataSourceConnector
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.ConnectionPoolDataSource;

import junit.framework.AssertionFailedError;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Connection factory using javax.sql.ConnectionPoolDataSource.
 * <p>
 * Connections are obtained by calling
 * <code>getPooledConnection.getConnection()</code>, and statement pooling is
 * enabled.
 */
public class ConnectionPoolDataSourceConnector implements Connector {
    
    private TestConfiguration config;
    /**
     * DataSource that maps to the database for the
     * configuration. The no-arg getPooledConnection() method
     * maps to the default user and password for the
     * configuration.
     */
    private ConnectionPoolDataSource ds;

    public void setConfiguration(TestConfiguration config) {
        
        this.config = config;
        ds = J2EEDataSource.getConnectionPoolDataSource(config, (HashMap) null);

        // Enable statement pooling by default.
        enableStatementPooling(ds);

        // NOTE: Any other setup of the data source that gets added here,
        // probably also needs to be added to singleUseDS(). Otherwise, that
        // setup won't be performed on data sources that are used to create
        // or shut down a database.

    }

    /**
     * Enable statement pooling on the specified data source.
     */
    private static void enableStatementPooling(ConnectionPoolDataSource ds) {
        // Note that this does not automatically test the pooling itself, but it
        // should test basic JDBC operations on the logical wrapper classes.
        try {
            J2EEDataSource.setBeanProperty(ds, "maxStatements", 2);
        } catch (AssertionFailedError afe) {
            // Ignore this, it will fail later if it is an actual error.
            // An assertion error will be thrown every time until statement
            // pooling (or merely the property maxStatement) is implemented in
            // the embedded ConnectionPoolDataSource class.
        }
    }

    public Connection openConnection() throws SQLException {
        try {
            return ds.getPooledConnection().getConnection();
        } catch (SQLException e) {
            // Expected state for database not found.
            // For the client the generic 08004 is returned,
            // will just retry on that.
            String expectedState = 
                config.getJDBCClient().isEmbedded() ? "XJ004" : "08004";

            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDtabase property set.
            if (!expectedState.equals(e.getSQLState()))
                throw e;
            return singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) ).
                   getPooledConnection().getConnection(); 
       }
    }

    public Connection openConnection(String databaseName) throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getPooledConnection().getConnection();
        } catch (SQLException e) {
            // Expected state for database not found.
            // For the client the generic 08004 is returned,
            // will just retry on that.
            String expectedState = 
                config.getJDBCClient().isEmbedded() ? "XJ004" : "08004";

            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDtabase property set.
            if (!expectedState.equals(e.getSQLState()))
                throw e;
            ConnectionPoolDataSource tmpDs =
                    singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getPooledConnection().getConnection();
       }
    }

    public Connection openConnection(String user, String password)
            throws SQLException {
        try {
            return ds.getPooledConnection(user, password).getConnection();
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            return singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) ).
                   getPooledConnection(user, password).getConnection(); 
       }
    }

    public Connection openConnection(String databaseName,
                                     String user,
                                     String password)
            throws SQLException
    {
        return openConnection( databaseName, user, password, null );
    }
    
    public  Connection openConnection
        (String databaseName, String user, String password, Properties connectionProperties)
         throws SQLException
    {
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getPooledConnection(user, password).getConnection();
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            HashMap<String, Object> hm =
                    DataSourceConnector.makeCreateDBAttributes( config );
            DataSourceConnector.copyProperties(connectionProperties, hm);
            ConnectionPoolDataSource tmpDs = singleUseDS( hm );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getPooledConnection(user, password).getConnection(); 
       }
    }

    public void shutDatabase() throws SQLException {
        singleUseDS( DataSourceConnector.makeShutdownDBAttributes( config ) ).
                getPooledConnection().getConnection();
        config.waitForShutdownComplete(getDatabaseName());
    }

    public void shutEngine(boolean deregisterDriver) throws SQLException {
        if (SanityManager.DEBUG) {
             // "false" only used with driver manager
            SanityManager.ASSERT(deregisterDriver);
        }

        ConnectionPoolDataSource tmpDs =
                singleUseDS( DataSourceConnector.makeShutdownDBAttributes( config ) );
        JDBCDataSource.setBeanProperty(tmpDs, "databaseName", "");
        tmpDs.getPooledConnection();
    }
    
    public void setLoginTimeout( int seconds ) throws SQLException
    {
        ds.setLoginTimeout( seconds );
    }
    
    public int getLoginTimeout() throws SQLException
    {
        return ds.getLoginTimeout();
    }
    
    public String getDatabaseName() {
        String databaseName=null;
        try {
            // get the physical database name
            databaseName = (String) JDBCDataSource.getBeanProperty(ds, "databaseName");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return databaseName;
    }
    
    /**
     * Get a connection from a single use ConnectionPoolDataSource configured
     * from the configuration but with the passed in property set.
     */
    private ConnectionPoolDataSource singleUseDS( HashMap hm )
       throws SQLException
    {
        ConnectionPoolDataSource sds =
                J2EEDataSource.getConnectionPoolDataSource(config, hm);

        // Enable statement pooling by default for single-use data sources
        // too, just like it's enabled for the default data source in
        // setConfiguration().
        enableStatementPooling(sds);

        return sds;
    }

}
