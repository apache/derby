/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.MBeanTest

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

package org.apache.derbyTesting.functionTests.tests.management;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Class that provided utility methods for the
 * testing of Derby's MBeans.
 */
abstract class MBeanTest extends BaseTestCase {
    
    public MBeanTest(String name) {
        super(name);
    }
    
  
    /**
     * Setup code to be run before each test fixture. This method will make
     * sure that JMX Management is enabled in Derby, so that the test fixtures
     * can access Derby's MBeans without problems.
     * 
     * @throws java.lang.Exception if an unexpected Exception occurs
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        enableManagement();
    }
    
    /**
     * Creates a URL for connecting to the platform MBean server on the host
     * specified by the network server hostname of this test configuration.
     * The JMX port number used is also retreived from the test configuration.
     * @return a service URL for connecting to the platform MBean server
     * @throws MalformedURLException if the URL is malformed
     */
    private JMXServiceURL getJmxUrl() throws MalformedURLException {
        
        // NOTE: This hostname is only valid in a client/server configuration
        String hostname = TestConfiguration.getCurrent().getHostName();
        //String hostname = TestConfiguration.DEFAULT_HOSTNAME; // for embedded?
        int jmxPort = TestConfiguration.getCurrent().getJmxPort();
                
        /* "jmxrmi" is the name of the RMI server connector of the platform
         * MBean server, which is used by Derby */
        JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://" 
                    + hostname
                    + ":" + jmxPort + "/jmxrmi");
        
        return url;
    }
    
    /**
     * Creates a client connector for JMX and uses this to obtain a connection
     * to an MBean server. This method assumes that JMX security has been
     * disabled, meaning that authentication credentials and SSL configuration 
     * details are not supplied to the MBeanServer.
     * 
     * @return a plain connection to an MBean server
     * @throws MalformedURLException if the JMX Service URL used is invalid
     * @throws IOException if connecting to the MBean server fails
     */
    protected MBeanServerConnection getMBeanServerConnection() 
            throws MalformedURLException, IOException {
                
        // assumes that JMX authentication and SSL is not required (hence null)
        JMXConnector jmxc = JMXConnectorFactory.connect(getJmxUrl(), null);
        return jmxc.getMBeanServerConnection();
    }
    
    /**
     * Enables Derby's MBeans in the MBeanServer by accessing Derby's 
     * ManagementMBean. If Derby JMX management has already been enabled, no 
     * changes will be made. The test fixtures in this class require that
     * JMX Management is enabled in Derby, hence this method.
     * 
     * @throws Exception JMX-related exceptions if an unexpected error occurs.
     */
    protected void enableManagement() throws Exception {
        // prepare the Management mbean. Use the same ObjectName that Derby uses
        // by default, to avoid creating multiple instances of the same bean
        ObjectName mgmtObjName 
                = new ObjectName("org.apache.derby", "type", "Management");
        // create/register the MBean. If the same MBean has already been
        // registered with the MBeanServer, that MBean will be referenced.
        //ObjectInstance mgmtObj = 
        MBeanServerConnection serverConn = getMBeanServerConnection();
        
        try {
            serverConn.createMBean("org.apache.derby.mbeans.Management", 
                    mgmtObjName);
        } catch (InstanceAlreadyExistsException e) {
            // Derby's ManagementMBean has already been created
        }
        // check the status of the management service
        Boolean active = (Boolean) 
                serverConn.getAttribute(mgmtObjName, "ManagementActive");

        if (!active.booleanValue()) {
            // JMX management is not active, so activate it by invoking the
            // startManagement operation.
            serverConn.invoke(
                    mgmtObjName, 
                    "startManagement", 
                    new Object[0], new String[0]); // no arguments
            active = (Boolean) 
                    serverConn.getAttribute(mgmtObjName, "ManagementActive");
        }
        
        assertTrue("Failed to activate Derby's JMX management", active);
    }
    
    /**
     * Gets the value of a given attribute that is exposed by the MBean 
     * represented by the given object name.
     * @param objName the object name defining a specific MBean instance
     * @param name the name of the attribute
     * @return the value of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    protected Object getAttribute(ObjectName objName, String name) 
            throws Exception {
        
        return getMBeanServerConnection().getAttribute(objName, name);
    }
    
    /**
     * Checks the readability and type of an attribute value that is supposed 
     * to be a boolean.
     * @param objName the object name representing the MBean instance from which
     *        the attribute value will be retreived
     * @param name the name of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    protected void checkBooleanAttributeValue(ObjectName objName, String name) 
            throws Exception {
        
        Object value = getAttribute(objName, name);
        boolean unboxedValue = ((Boolean)value).booleanValue();
        println(name + " = " + unboxedValue); // for debugging
    }
    
    /**
     * Checks the readability and type of an attribute value that is supposed 
     * to be an int.
     * @param objName the object name representing the MBean instance from which
     *        the attribute value will be retreived
     * @param name the name of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    protected void checkIntAttributeValue(ObjectName objName, String name) 
            throws Exception {
        
        Object value = getAttribute(objName, name);
        int unboxedValue = ((Integer)value).intValue();
        println(name + " = " + unboxedValue); // for debugging
    }
    
    /**
     * Checks the readability and type of an attribute value that is supposed 
     * to be a String.
     * @param objName the object name representing the MBean instance from which
     *        the attribute value will be retreived
     * @param name the name of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    protected void checkStringAttributeValue(ObjectName objName, String name) 
            throws Exception {
        
        String value = (String)getAttribute(objName, name);
        println(name + " = " + value); // for debugging
    }
}
