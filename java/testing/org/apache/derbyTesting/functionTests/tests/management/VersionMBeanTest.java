/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.VersionMBeanTest

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
import java.util.Hashtable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
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
 * <p>
 * This JUnit test class is for testing the VersionMBean that is available in
 * Derby. Running these tests requires a JVM supporting J2SE 5.0 or better, due 
 * to the implementation's dependency of the platform management agent.</p>
 * <p>
 * This class currently tests the following:</p>
 * <ul>
 *   <li>That the attributes we expect to be available exist</li>
 *   <li>That these attributes are readable</li>
 *   <li>That these attributes have the correct type</li>
 * <p>
 * The test fixtures will fail if an exception occurs (will be reported as an 
 * error in JUnit).</p>
 */
public class VersionMBeanTest extends BaseTestCase {
    
    public VersionMBeanTest(String name) {
        super(name);
    }
    
    public static Test suite() {
                
        // TODO -
        // Check for J2SE 5.0 or better? Or java.lang.management.ManagementFactory?
        // Older VMs will get UnsupportedClassVersionError anyway...
        
        // Create a suite of all "test..." methods in this class.
        TestSuite suite = new TestSuite(VersionMBeanTest.class, 
                                        "VersionMBeanTest:client");

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
    
    /**
     * Setup code to be run before each test fixture. This method will make
     * sure that JMX Management is enabled in Derby, so that the test fixtures
     * can access the VersionMBean without problems.
     * 
     * @throws java.lang.Exception if an unexpected Exception occurs
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        enableManagement();
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
    private MBeanServerConnection getMBeanServerConnection() 
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
    private void enableManagement() throws Exception {
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
     * <p>
     * Creates an object name instance for the MBean whose object name's textual
     * representation is:</p>
     * <pre>
     *     org.apache.derby:type=Version,jar=derby.jar
     * </pre>
     * @return the object name representing the VersionMBean for the derby 
     *         engine
     * @throws MalformedObjectNameException if the object name is not valid
     */
    private ObjectName getDerbyJarObjectName() 
            throws MalformedObjectNameException {
        
        // get a reference to the VersionMBean instance for derby.jar
        Hashtable<String, String> keyProps = new Hashtable<String, String>();
        keyProps.put("type", "Version");
        keyProps.put("jar", "derby.jar");
        return new ObjectName("org.apache.derby", keyProps);
    }
    
    /**
     * Gets the value of a given attribute that is exposed by the MBean 
     * represented by the given object name.
     * @param objName the object name defining a specific MBean instance
     * @param name the name of the attribute
     * @return the value of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    private Object getAttribute(ObjectName objName, String name) 
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
    private void checkBooleanAttributeValue(ObjectName objName, String name) 
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
    private void checkIntAttributeValue(ObjectName objName, String name) 
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
    private void checkStringAttributeValue(ObjectName objName, String name) 
            throws Exception {
        
        String value = (String)getAttribute(objName, name);
        println(name + " = " + value); // for debugging
    }
    
    //
    // ---------- TEST FIXTURES ------------
    //
    // This MBean currently has only read-only attributes. Test that it is
    // possible to read all attribute values that we expect to be there.
    // There are no operations to test.
    
    
    public void testDerbyJarAttributeAlpha() throws Exception {
        checkBooleanAttributeValue(getDerbyJarObjectName(), "Alpha");
    }
    
    public void testDerbyJarAttributeBeta() throws Exception {
        checkBooleanAttributeValue(getDerbyJarObjectName(), "Beta");
    }
    
    public void testDerbyJarAttributeBuildNumber() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "BuildNumber");
    }
    
    // Will fail until the MBean is updated (MaintVersion -> MaintenanceVersion)
    public void testDerbyJarAttributeMaintenanceVersion() throws Exception {
        checkIntAttributeValue(getDerbyJarObjectName(), "MaintenanceVersion");
    }
    
    public void testDerbyJarAttributeMajorVersion() throws Exception {
        checkIntAttributeValue(getDerbyJarObjectName(), "MajorVersion");
    }
    
    public void testDerbyJarAttributeMinorVersion() throws Exception {
        checkIntAttributeValue(getDerbyJarObjectName(), "MinorVersion");
    }
    
    public void testDerbyJarAttributeProductName() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "ProductName");
    }
    
    public void testDerbyJarAttributeProductTechnologyName() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), 
                                   "ProductTechnologyName");
    }
    
    public void testDerbyJarAttributeProductVendorName() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "ProductVendorName");
    }
    
    public void testDerbyJarAttributeVersionString() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "VersionString");
    }

}
