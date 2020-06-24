/*
 *
 * Derby - Class org.apache.derbyTesting.junit.DataSourceConnector
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import org.apache.derby.shared.common.sanity.SanityManager;


/**
 * Connection factory using javax.sql.DataSource.
 * Should work for an Derby data source, including JSR169 support.
 *
 */
public class DataSourceConnector implements Connector {
    
    private TestConfiguration config;
    /**
     * DataSource that maps to the database for the
     * configuration. The no-arg getConnection() method
     * maps to the default user and password for the
     * configuration.
     */
    private DataSource ds;

    public void setConfiguration(TestConfiguration config) {
        
        this.config = config;
        ds = JDBCDataSource.getDataSource(config, (HashMap) null);
    }

    public Connection openConnection() throws SQLException {
        try {
            return ds.getConnection();
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
            return singleUseDS( makeCreateDBAttributes( config ) ).getConnection(); 
       }
    }

    public Connection openConnection(String databaseName) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getConnection();
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
            DataSource tmpDs = singleUseDS( makeCreateDBAttributes( config ) );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getConnection();
       }
    }

    public Connection openConnection(String user, String password)
            throws SQLException {
        try {
            return ds.getConnection(user, password);
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            return singleUseDS( makeCreateDBAttributes( config ) ).getConnection
                (user, password); 
       }
    }

    public Connection openConnection(String databaseName, String user, String password)
            throws SQLException
    {
        return openConnection( databaseName, user, password, null );
    }
    
    public  Connection openConnection
        (String databaseName, String user, String password, Properties connectionProperties)
         throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getConnection(user, password);
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            HashMap<String, Object> hm = makeCreateDBAttributes( config );
            copyProperties(connectionProperties, hm);
            DataSource tmpDs = singleUseDS( hm );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getConnection(user, password); 
       }
    }
    

    public void shutDatabase() throws SQLException {
        singleUseDS( makeShutdownDBAttributes( config ) ).getConnection();
        config.waitForShutdownComplete(getDatabaseName());
    }

    public void shutEngine(boolean deregisterDriver) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (SanityManager.DEBUG) {
             // "false" only used with driver manager
            SanityManager.ASSERT(deregisterDriver);
        }

        DataSource tmpDs = singleUseDS( makeShutdownDBAttributes( config ) );
        JDBCDataSource.setBeanProperty(tmpDs, "databaseName", "");
        tmpDs.getConnection();
    }
    
    public void setLoginTimeout( int seconds ) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6094
        ds.setLoginTimeout( seconds );
    }
    
    public int getLoginTimeout() throws SQLException
    {
        return ds.getLoginTimeout();
    }
    
    /**
     * Get a connection from a single use DataSource configured
     * from the configuration but with the passed in property set.
     */
    private DataSource singleUseDS( HashMap hm )
       throws SQLException {
        DataSource sds = JDBCDataSource.getDataSource(config, hm);
        return sds;
    }

    static HashMap<String, Object> makeCreateDBAttributes( TestConfiguration configuration )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<String, Object> hm = JDBCDataSource.getDataSourceProperties( configuration );
        hm.put( "createDatabase", "create" );

        return hm;
    }

    static HashMap<String, Object> makeShutdownDBAttributes( TestConfiguration configuration )
    {
        HashMap<String, Object> hm = JDBCDataSource.getDataSourceProperties( configuration );
        hm.put( "shutdownDatabase", "shutdown" );

        return hm;
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
     * Copy attributes from a {@code Properties} object to a {@code Map}.
     */
    static void copyProperties(Properties src, Map<String, Object> dest) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        if (src != null) {
            for (String key : src.stringPropertyNames()) {
                dest.put(key, src.getProperty(key));
            }
        }
    }

}
