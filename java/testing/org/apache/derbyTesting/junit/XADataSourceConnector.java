/*
 *
 * Derby - Class org.apache.derbyTesting.junit.XADataSourceConnector
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
import java.util.Properties;

import javax.sql.XADataSource;

import junit.framework.Assert;

/**
 * Connection factory using javax.sql.XADataSource.
 * Returns a connection in local mode obtained from
 * getXAConnection().getConnection().
 *
 */
public class XADataSourceConnector implements Connector {
    
    private TestConfiguration config;
    /**
     * DataSource that maps to the database for the
     * configuration. The no-arg getXAConnection() method
     * maps to the default user and password for the
     * configuration.
     */
    private XADataSource ds;

    public void setConfiguration(TestConfiguration config) {
        
        this.config = config;
        ds = J2EEDataSource.getXADataSource(config, (HashMap) null);
    }

    public Connection openConnection() throws SQLException {
        try {
            return ds.getXAConnection().getConnection();
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
                   getXAConnection().getConnection(); 
       }
    }

    public Connection openConnection(String databaseName) throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getXAConnection().getConnection();
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
            XADataSource tmpDs = singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getXAConnection().getConnection();
       }
    }

    public Connection openConnection(String user, String password)
            throws SQLException {
        try {
            return ds.getXAConnection(user, password).getConnection();
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            return singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) ).
                   getXAConnection(user, password).getConnection(); 
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
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getXAConnection(user, password).getConnection();
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            HashMap hm = DataSourceConnector.makeCreateDBAttributes( config );
            if ( connectionProperties != null ) { hm.putAll( connectionProperties ); }
            XADataSource tmpDs = singleUseDS( hm );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getXAConnection(user, password).getConnection(); 
       }
    }

    public void shutDatabase() throws SQLException {
        singleUseDS( DataSourceConnector.makeShutdownDBAttributes( config ) )
            .getXAConnection().getConnection();     
    }

    public void shutEngine() throws SQLException {
        Assert.fail("shutdown engine not implemened");
    }
    
    /**
     * Get a connection from a single use XADataSource configured
     * from the configuration but with the passed in property set.
     */
    private XADataSource singleUseDS( HashMap hm )
       throws SQLException {
        XADataSource sds = J2EEDataSource.getXADataSource(config, hm);
        return sds;
    }

}
