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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Class that provided utility methods for the
 * testing of Derby's MBeans.
 */
abstract class MBeanTest extends BaseTestCase {
    
    public MBeanTest(String name) {
        super(name);
    }
    
    protected static Test suite(Class testClass, String suiteName) {
        
        // TODO -
        // Check for J2SE 5.0 or better? Or java.lang.management.ManagementFactory?
        // Older VMs will get UnsupportedClassVersionError anyway...
        
        // Create a suite of all "test..." methods in the class.
        TestSuite suite = new TestSuite(testClass,  suiteName);

        /* Connecting to an MBean server using a URL requires setting up remote
         * JMX in the JVM to which we want to connect. This is usually done by
         * setting a few system properties at JVM startup.
         * A quick solution is to set up a new network server JVM with
         * the required jmx properties.
         * A future improvement could be to fork a new JVM for embedded (?).
         *
         * This requires that the default security policy of the network server
         * includes the permissions required to perform the actions of these 
         * tests. Otherwise, we'd probably have to supply a custom policy file
         * and specify this using additional command line properties at server 
         * startup.
         */
        NetworkServerTestSetup networkServerTestSetup = 
                new NetworkServerTestSetup (
                        suite, // run all tests in this class in the same setup
                        getCommandLineProperties(), // need to set up JMX in JVM
                        new String[0], // no server arguments needed
                        true,   // wait for the server to start properly
                        new InputStream[1] // no need to check server output
                );

        /* Since the server will be started in a new process we need "execute" 
         * FilePermission on all files (at least Java executables)...
         * Will run without SecurityManager for now, but could probably add a 
         * JMX specific policy file later. Or use the property trick reported
         * on derby-dev 2008-02-26 and add the permission to the generic 
         * policy.
         */
        Test testSetup = 
                SecurityManagerSetup.noSecurityManager(networkServerTestSetup);
        // this decorator makes sure the suite is empty if this configration
        // does not support the network server:
        return TestConfiguration.defaultServerDecorator(testSetup);
    }
    
    // ---------- UTILITY METHODS ------------
    
    /**
     * Returns a set of startup properties suitable for VersionMBeanTest.
     * These properties are used to configure JMX in a different JVM.
     * Will set up remote JMX using the port 9999 (TODO: make this 
     * configurable), and with JMX security (authentication & SSL) disabled.
     * 
     * @return a set of Java system properties to be set on the command line
     *         when starting a new JVM in order to enable remote JMX.
     */
    private static String[] getCommandLineProperties()
    {
        ArrayList<String> list = new ArrayList<String>();
        list.add("com.sun.management.jmxremote.port=" 
                + TestConfiguration.getCurrent().getJmxPort());
        list.add("com.sun.management.jmxremote.authenticate=false");
        list.add("com.sun.management.jmxremote.ssl=false");
        String[] result = new String[list.size()];
        list.toArray(result);
        return result;
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
        
        ObjectName mgmtObjName = getApplicationManagementMBean();
        
        MBeanServerConnection serverConn = getMBeanServerConnection();

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
     * Get all MBeans registered in Derby's domain.
     * @return Set of ObjectNames for all of Derby's registered MBeans.
     * @throws Exception
     */
    protected Set<ObjectName> getDerbyDomainMBeans() throws Exception
    {
        ObjectName derbyDomain = new ObjectName("org.apache.derby:*");
        return  (Set<ObjectName>)
            getMBeanServerConnection().queryNames(derbyDomain, null);
    }
    
    /**
     * Get the ObjectName for the application
     * created ManagementMBean. The MBean will be
     * created if it is not already registered.
     * @return
     * @throws Exception
     */
    protected ObjectName getApplicationManagementMBean() throws Exception
    {
        // prepare the Management mbean. Use the same ObjectName that Derby uses
        // by default, to avoid creating multiple instances of the same bean
        ObjectName mgmtObjName 
                = new ObjectName("org.apache.derby", "type", "Management");
        // create/register the MBean. If the same MBean has already been
        // registered with the MBeanServer, that MBean will be referenced.
        //ObjectInstance mgmtObj = 
        MBeanServerConnection serverConn = getMBeanServerConnection();
        
        if (!serverConn.isRegistered(mgmtObjName))
        {
        
            serverConn.createMBean(
                    "org.apache.derby.mbeans.Management", 
                    mgmtObjName);
        }
        
        return mgmtObjName;
    }
    
    /**
     * Invoke an operation with no arguments.
     * @param objName MBean to operate on
     * @param name Operation name.
     */
    protected void invokeOperation(ObjectName objName, String name)
        throws Exception
    {
        getMBeanServerConnection().invoke(
                objName, 
                name, 
                new Object[0], new String[0]); // no arguments
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
    
    protected void assertBooleanAttribute(boolean expected,
            ObjectName objName, String name) throws Exception
    {
        Boolean bool = (Boolean) getAttribute(objName, name);
        assertNotNull(bool);
        assertEquals(expected, bool.booleanValue());
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
