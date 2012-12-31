/*
 *
 * Derby - Class org.apache.derbyTesting.junit.JDBCDataSource
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

import java.lang.reflect.Method;
import java.security.AccessController;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import javax.sql.DataSource;

import junit.framework.Assert;

/**
 * Utility methods related to JDBC DataSource objects.
 * J2EEDataSource exists to return XA and connection pooling data sources.
 * 
 * @see J2EEDataSource
 */
public class JDBCDataSource {
    
    /**
     * Return a new DataSource corresponding to the current
     * configuration. The getConnection() method will return
     * a connection identical to TestConfiguration.openDefaultConnection().
     */
    public static javax.sql.DataSource getDataSource()
    {
        return getDataSource(TestConfiguration.getCurrent(), (HashMap) null);
    }
    
    /**
     * Return a new DataSource corresponding to the current
     * configuration except that the database name is different.
     */
    public static javax.sql.DataSource getDataSource(String dbName)
    {
        // default DataSource
        javax.sql.DataSource ds = getDataSource();
        
        // Override the database name
        setBeanProperty(ds, "databaseName", dbName);
        
        return ds;
    }
    
    /**
     * Return a DataSource corresponding to one
     * of the logical databases in the current configuration.
     */
    public static javax.sql.DataSource
         getDataSourceLogical(String logicalDatabasename)
    {
        // default DataSource
        javax.sql.DataSource ds = getDataSource();
        
        TestConfiguration current = TestConfiguration.getCurrent();
        String physicalName =
            current.getPhysicalDatabaseName(logicalDatabasename);
        
        // Override the database name
        setBeanProperty(ds, "databaseName", physicalName);
        
        return ds;
    }
    
    /**
     * Create a new DataSource object setup from the passed in TestConfiguration.
     * The getConnection() method will return a connection identical to
     * TestConfiguration.openDefaultConnection().
     */
    static javax.sql.DataSource getDataSource(TestConfiguration config,
            HashMap beanProperties)
    {
        return (javax.sql.DataSource) getDataSource(config,
            beanProperties, config.getJDBCClient().getDataSourceClassName());
    }

    /**
     * Create a new DataSource object setup from the passed in
     * TestConfiguration using the received properties and data
     * source class name.
     */
    static Object getDataSource(TestConfiguration config,
        HashMap beanProperties, String dsClassName)
    {
        if (beanProperties == null)
             beanProperties = getDataSourceProperties(config);
        
        return (javax.sql.DataSource) getDataSourceObject(dsClassName,
            beanProperties);
    }
    
    /**
     * Create a HashMap with the set of Derby DataSource
     * Java bean properties corresponding to the configuration.
     */
    static HashMap getDataSourceProperties(TestConfiguration config) 
    {
        HashMap beanProperties = new HashMap();
        
        if (!config.getJDBCClient().isEmbedded()) {
            beanProperties.put("serverName", config.getHostName());
            beanProperties.put("portNumber", new Integer(config.getPort()));
        }
        
        beanProperties.put("databaseName", config.getDefaultDatabaseName());
        beanProperties.put("user", config.getUserName());
        beanProperties.put("password", config.getUserPassword());

        String attributes = config.getConnectionAttributesString();
        if (attributes != null) {
            beanProperties.put("connectionAttributes", attributes);
        }

        return beanProperties;
    }
    
    /**
     * Return a DataSource object of the passed in type
     * configured with the passed in Java bean properties.
     * This will actually work with any object that has Java bean
     * setter methods.
     * <BR>
     * If a thread context class loader exists then it is used
     * to try and load the class.
     */
    static Object getDataSourceObject(String classname, HashMap beanProperties)
    {
        ClassLoader contextLoader =
            (ClassLoader) AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            
            public Object run()  { 
                return Thread.currentThread().getContextClassLoader();
            }
        });
    
        try {
            Object ds = null;
            if (contextLoader != null)
            {
                try {
                    ds = Class.forName(classname, true, contextLoader).newInstance();
                } catch (Exception e) {
                    // context loader may not be correctly hooked up
                    // with parent, try without it.
                }
            }
            
            if (ds == null)
                ds = Class.forName(classname).newInstance();
            
            for (Iterator i = beanProperties.keySet().iterator();
                i.hasNext(); )
            {
                String property = (String) i.next();
                Object value = beanProperties.get(property);
                
                setBeanProperty(ds, property, value);
            }
            return ds;
        } catch (Exception e) {
            BaseTestCase.fail("unexpected error", e);
            return null;
        }
    }
    
    /**
     * Set a bean property for a data source. This code can be used
     * on any data source type.
     * @param ds DataSource to have property set
     * @param property name of property.
     * @param value Value, type is derived from value's class.
     */
    public static void setBeanProperty(Object ds, String property, Object value)
    {
        String setterName = getSetterName(property);
        
        // Base the type of the setter method from the value's class.
        Class clazz = value.getClass();      
        if (Integer.class.equals(clazz))
            clazz = Integer.TYPE;
        else if (Boolean.class.equals(clazz))
            clazz = Boolean.TYPE;
        else if (Short.class.equals(clazz))
            clazz = Short.TYPE;

        try {
            Method setter = ds.getClass().getMethod(setterName,
                    new Class[] {clazz});
            setter.invoke(ds, new Object[] {value});
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }   
    }
    
    /**
     * Get a bean property for a data source. This code can be used
     * on any data source type.
     * @param ds DataSource to fetch property from
     * @param property name of property.
     */
    public static Object getBeanProperty(Object ds, String property)
        throws Exception
    {
        String getterName = getGetterName(property);

        Method getter = ds.getClass().getMethod(getterName,
                    new Class[0]);
        return getter.invoke(ds, new Object[0]);
    }
    
    /**
     * Clear a String Java bean property by setting it to null.
     * @param ds DataSource to have property cleared
     * @param property name of property.
     */
    public static void clearStringBeanProperty(Object ds, String property)
    {
        String setterName = getSetterName(property);
        try {
            Method setter = ds.getClass().getMethod(setterName,
                    new Class[] {String.class});
            setter.invoke(ds, new Object[] {null});
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }   
    }
    
    private static String getSetterName(String attribute) {
        return "set" + Character.toUpperCase(attribute.charAt(0))
                + attribute.substring(1);
    }
    private static String getGetterName(String attribute) {
        return "get" + Character.toUpperCase(attribute.charAt(0))
                + attribute.substring(1);
    }
    
    /**
     * Shutdown the database described by this data source.
     * The shutdownDatabase property is cleared by this method.
     */
    public static void shutdownDatabase(javax.sql.DataSource ds)
    {
        setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection();
            Assert.fail("Database failed to shut down");
        } catch (SQLException e) {
             BaseJDBCTestCase.assertSQLState("Database shutdown", "08006", e);
        } finally {
            clearStringBeanProperty(ds, "shutdownDatabase");
        }
    }

    /**
     * Shutdown the engine described by this data source.
     * The shutdownDatabase property is cleared by this method.
     */
    public static void shutEngine(javax.sql.DataSource ds) throws SQLException {
        setBeanProperty(ds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(ds, "databaseName", "");
        try {
            ds.getConnection();
            Assert.fail("Engine failed to shut down");
        } catch (SQLException e) {
             BaseJDBCTestCase.assertSQLState("Engine shutdown", "XJ015", e);
        } finally {
            clearStringBeanProperty(ds, "shutdownDatabase");
        }
    }

}
