/*
 *
 * Derby - Class org.apache.derbyTesting.junit.J2EEDataSource
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

import java.util.HashMap;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

/**
 * Utility methods related to J2EE JDBC DataSource objects.
 * Separated out from JDBCDataSource to ensure that no
 * ClassNotFoundExceptions are thrown with JSR169.
 *
 */
public class J2EEDataSource {
    
    /**
     * Return a new DataSource corresponding to the current
     * configuration. The getPooledConnection() method is configured
     * to use the user name and password from the configuration.
     */
    public static javax.sql.ConnectionPoolDataSource getConnectionPoolDataSource()
    {
        return getConnectionPoolDataSource(TestConfiguration.getCurrent(), (HashMap) null);
    }
    /**
     * Create a new DataSource object setup from the passed in TestConfiguration.
     * The getPooledConnection() method is configured
     * to use the user name and password from the configuration.
     */
    static ConnectionPoolDataSource getConnectionPoolDataSource(
            TestConfiguration config,
            HashMap beanProperties)
    {
        if (beanProperties == null)
             beanProperties = JDBCDataSource.getDataSourceProperties(config);
        
        String dataSourceClass = config.getJDBCClient().getConnectionPoolDataSourceClassName();
        
        return (ConnectionPoolDataSource) JDBCDataSource.getDataSourceObject(
                dataSourceClass, beanProperties);
    }
    
    /**
     * Return a new XA DataSource corresponding to the current
     * configuration. The getXAConnection() method is configured
     * to use the user name and password from the configuration.
     */
    public static XADataSource getXADataSource()
    {
        return getXADataSource(TestConfiguration.getCurrent(), (HashMap) null);
    }
    
    
    /**
     * Set a bean property for a data source. This code can be used
     * on any data source type.
     * @param ds DataSource to have property set
     * @param property name of property.
     * @param value Value, type is derived from value's class.
     */
    public static void setBeanProperty(Object ds, String property, Object value) {
       // reuse code from JDBCDataSource
        JDBCDataSource.setBeanProperty(ds, property, value);
    }
    
    /**
     * Create a new DataSource object setup from the passed in TestConfiguration.
     * The getXAConnection() method is configured
     * to use the user name and password from the configuration.
     */
    static XADataSource getXADataSource(TestConfiguration config,
            HashMap beanProperties)
    {
        if (beanProperties == null)
             beanProperties = JDBCDataSource.getDataSourceProperties(config);
        
        String dataSourceClass = config.getJDBCClient().getXADataSourceClassName();
        
        return (XADataSource) JDBCDataSource.getDataSourceObject(
                dataSourceClass, beanProperties);
    }
}
