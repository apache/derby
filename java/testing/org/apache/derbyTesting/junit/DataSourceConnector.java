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

import javax.sql.DataSource;

import junit.framework.Assert;

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
            return singleUseDS("createDatabase", "create").getConnection(); 
       }
    }

    public Connection openConnection(String databaseName) throws SQLException {
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
            DataSource tmpDs = singleUseDS("createDatabase", "create");
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
            return singleUseDS(
                    "createDatabase", "create").getConnection(user, password); 
       }
    }

    public Connection openConnection(String databaseName, String user, String password)
            throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getConnection(user, password);
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            DataSource tmpDs = singleUseDS("createDatabase", "create");
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getConnection(user, password); 
       }
    }

    public void shutDatabase() throws SQLException {
        singleUseDS("shutdownDatabase", "shutdown").getConnection();     
    }

    public void shutEngine() throws SQLException {
        DataSource tmpDs = singleUseDS("shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(tmpDs, "databaseName", "");
        tmpDs.getConnection();
    }
    
    /**
     * Get a connection from a single use DataSource configured
     * from the configuration but with the passed in property set.
     */
    private DataSource singleUseDS(String property, String value)
       throws SQLException {
        HashMap hm = JDBCDataSource.getDataSourceProperties(config);
        hm.put(property, value);
        DataSource sds = JDBCDataSource.getDataSource(config, hm);
        return sds;
    }

}
