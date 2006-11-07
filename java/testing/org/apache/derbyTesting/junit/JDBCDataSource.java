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
import java.util.HashMap;
import java.util.Iterator;

import junit.framework.Assert;

/**
 * Utility methods related to JDBC DataSource objects.
 *
 */
public class JDBCDataSource {
    
    /**
     * Return a DataSource corresponding to the current
     * configuration. The getConnection() method will return
     * a connection identical to TestConfiguration.openDefaultConnection().
     */
    public static javax.sql.DataSource getDataSource()
    {
        return getDataSource(TestConfiguration.getCurrent(), (HashMap) null);
    }
    
    /**
     * Create a new DataSource object setup from the passed in TestConfiguration.
     * The getConnection() method will return a connection identical to
     * TestConfiguration.openDefaultConnection().
     */
    static javax.sql.DataSource getDataSource(TestConfiguration config,
            HashMap beanProperties)
    {
        if (beanProperties == null)
             beanProperties = getDataSourceProperties(config);
        
        String dataSourceClass = config.getJDBCClient().getDataSourceClassName();
        
        return (javax.sql.DataSource) getDataSourceObject(dataSourceClass,
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
        
        beanProperties.put("databaseName", config.getDatabaseName());
        beanProperties.put("user", config.getUserName());
        beanProperties.put("password", config.getUserPassword());

        
        return beanProperties;
    }
    
    /**
     * Return a DataSource object of the passsed in type
     * configured with the passed in Java bean properties.
     * This will actually work with an object that has Java bean
     * setter methods.
     */
    static Object getDataSourceObject(String classname, HashMap beanProperties)
    {

        Object ds;
        try {
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
            Assert.fail(e.getMessage());
            return null;
        }
    }
    
    static void setBeanProperty(Object ds, String property, Object value)
    {
        String setterName = getSetterName(property);
        
        // Base the type of the setter method from the value's class.
        Class clazz = value.getClass();      
        if (Integer.class.equals(clazz))
            clazz = Integer.TYPE;
        else if (Boolean.class.equals(clazz))
            clazz = Boolean.TYPE;

        try {
            Method setter = ds.getClass().getMethod(setterName,
                    new Class[] {clazz});
            setter.invoke(ds, new Object[] {value});
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }   
    }
    
    private static String getSetterName(String attribute) {
        return "set" + Character.toUpperCase(attribute.charAt(0))
                + attribute.substring(1);
    }
}
