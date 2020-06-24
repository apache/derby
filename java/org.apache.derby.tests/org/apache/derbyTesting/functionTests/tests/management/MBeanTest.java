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
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServerConnection;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Class that provided utility methods for the
 * testing of Derby's MBeans. Requires J2SE 5.0 or higher (platform management).
 * 
 * Subclasses may require JDBC access for verifying values returned by the
 * MBeans, which is why this class extends BaseJDBCTestCase instead of 
 * BaseTestCase.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
abstract class MBeanTest extends BaseJDBCTestCase {
     
    /**
     * JMX connection to use throughout the instance.
     */
    private MBeanServerConnection jmxConnection;
    
    public MBeanTest(String name) {
        super(name);
    }
    
    protected static Test suite(Class<? extends MBeanTest> testClass, String suiteName) {
                
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite outerSuite = new BaseTestSuite(suiteName);
        
        Test platform = new BaseTestSuite(testClass,  suiteName + ":platform");
        
        // Start the network server to ensure Derby is running and
        // all the MBeans are running.
        platform = TestConfiguration.clientServerDecorator(platform);
        platform = JMXConnectionDecorator.platformMBeanServer(platform);
                
        // Set of tests that run within the same virtual machine using
        // the platform MBeanServer directly.
        outerSuite.addTest(platform);
        
        // Create a suite of all "test..." methods in the class.
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        Test suite = new BaseTestSuite(testClass,  suiteName + ":client");
        
        // Set up to get JMX connections using remote JMX
        suite = JMXConnectionDecorator.remoteNoSecurity(suite);

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1387
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
        NetworkServerTestSetup networkServerTestSetup = 
                new NetworkServerTestSetup (
                        suite, // run all tests in this class in the same setup
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
                        getCommandLineProperties(false), // need to set up JMX in JVM
                        new String[0], // no server arguments needed
//IC see: https://issues.apache.org/jira/browse/DERBY-3504
                        true   // wait for the server to start properly
                );

        /* Since the server will be started in a new process we need "execute" 
         * FilePermission on all files (at least Java executables)...
         * Will run without SecurityManager for now, but could probably add a 
         * JMX specific policy file later. Or use the property trick reported
         * on derby-dev 2008-02-26 and add the permission to the generic 
         * policy.
         * Note that the remote server will be running with a security
         * manager (by default) if testing with jars.
         */
        Test testSetup = 
                SecurityManagerSetup.noSecurityManager(networkServerTestSetup);
        // this decorator makes sure the suite is empty if this configration
        // does not support the network server:
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        outerSuite.addTest(TestConfiguration.defaultServerDecorator(testSetup));
       
        return outerSuite;
    }
    
    // ---------- UTILITY METHODS ------------
    
    /**
     * Returns a set of startup properties suitable for VersionMBeanTest.
     * These properties are used to configure JMX in a different JVM.
     * Will set up remote JMX using the port defined by the current test 
     * configuration, and with JMX security (authentication and SSL) disabled.
     * 
     * @return a set of Java system properties to be set on the command line
     *         when starting a new JVM in order to enable remote JMX.
     */
    protected static String[] getCommandLineProperties(boolean authentication)
    {
        ArrayList<String> list = new ArrayList<String>();
        list.add("com.sun.management.jmxremote.port=" 
                + TestConfiguration.getCurrent().getJmxPort());
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
        list.add("com.sun.management.jmxremote.authenticate=" +
                Boolean.toString(authentication));
        list.add("com.sun.management.jmxremote.ssl=false");
        
        if (authentication) {
            list.add("com.sun.management.jmxremote.password.file=" +
                    "extin/jmx.password");
            list.add("com.sun.management.jmxremote.access.file=" +
                    "extin/jmx.access");
        }
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
    
    @Override
    protected void tearDown() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-3424
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        super.tearDown();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        if (jmxConnection != null) {
           JMXConnectionGetter.mbeanServerConnector.get().close(jmxConnection);
           jmxConnection = null;
        }
    }
    
    /**
     * Obtains a connection to an MBean server with
     * no user name or password,
     * opens a single connection for the lifetime
     * of the fixture
     * 
     * @return a plain connection to an MBean server
     */
    protected MBeanServerConnection getMBeanServerConnection() 
            throws Exception {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3424
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        if (jmxConnection == null)
            jmxConnection = 
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
            JMXConnectionGetter.mbeanServerConnector.get()
                .getMBeanServerConnection(null, null);
        return jmxConnection;
    }
    
    /**
     * Obtains a connection to an MBean server with
     * a user name or password. A new connection is
     * returned for each call.
     */
    protected MBeanServerConnection getMBeanServerConnection(
            String user, String password) throws Exception {

        return JMXConnectionGetter.mbeanServerConnector.get()
                    .getMBeanServerConnection(user, password);
    }
    
    /**
     * Is the JMX connecting using platform JMX.
     * @return True jmx connections via the platform server, false remote connections. 
     */
    protected boolean isPlatformJMXClient() {
        return JMXConnectionGetter.mbeanServerConnector.get()
            instanceof PlatformConnectionGetter;
    }
    
    /**
     * Enables Derby's MBeans in the MBeanServer by accessing Derby's 
     * ManagementMBean. If Derby JMX management has already been enabled, no 
     * changes will be made. The test fixtures in some subclasses require that
     * JMX Management is enabled in Derby, hence this method.
     * 
     * @throws Exception JMX-related exceptions if an unexpected error occurs.
     */
    protected void enableManagement() throws Exception {
        
        ObjectName mgmtObjName = getApplicationManagementMBean();
        
        // check the status of the management service
        Boolean active = (Boolean) 
                getAttribute(mgmtObjName, "ManagementActive");
//IC see: https://issues.apache.org/jira/browse/DERBY-3506

        if (!active.booleanValue()) {
            // JMX management is not active, so activate it by invoking the
            // startManagement operation.
            invokeOperation(mgmtObjName, "startManagement");
//IC see: https://issues.apache.org/jira/browse/DERBY-3506

            active = (Boolean) 
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
                    getAttribute(mgmtObjName, "ManagementActive");
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
        final ObjectName derbyDomain = new ObjectName("org.apache.derby:*");
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        return queryMBeans(derbyDomain);
    }

    /**
     * Get all MBeans that match the specified name.
     * @param name the name pattern to look for
     * @return a set of names that match
     */
    Set<ObjectName> queryMBeans(final ObjectName name) throws Exception {
        final MBeanServerConnection serverConn = getMBeanServerConnection(); 
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return AccessController.doPrivileged(
            new PrivilegedExceptionAction<Set<ObjectName>>() {
                public Set<ObjectName> run() throws IOException {
                    return serverConn.queryNames(name, null);
               }   
            }
        );   
    }
    
    /**
     * Get the ObjectName for the application
     * created ManagementMBean. The MBean will be
     * created if it is not already registered.
     * @throws Exception
     */
    protected ObjectName getApplicationManagementMBean() throws Exception
    {
        /* prepare the Management mbean, which is (so far) the only MBean that
         * can be created/registered from a JMX client, and without knowing the
         * system identifier */
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
        final ObjectName mgmtObjName 
                = new ObjectName("org.apache.derby", "type", "Management");
        // create/register the MBean. If the same MBean has already been
        // registered with the MBeanServer, that MBean will be referenced.
        //ObjectInstance mgmtObj = 
        final MBeanServerConnection serverConn = getMBeanServerConnection();
        
        if (!serverConn.isRegistered(mgmtObjName))
        {       
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>() {
                    public Object run() throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, ReflectionException, MBeanException, IOException {
                        serverConn.createMBean(
                                "org.apache.derby.mbeans.Management", 
                                mgmtObjName);
                        return null;
                   }   
                }
            );
        }
        
        return mgmtObjName;
    }
    
    /**
     * Get the ObjectName for an MBean registered by Derby for a set of
     * key properties. The ObjectName has the domain org.apache.derby, and
     * the key property <code>system</code> will be set to the system identifier
     * for the Derby system under test (if Derby is running).
     * @param keyProperties Set of key properties, may be modified by this call.
     * @return ObjectName to access MBean.
     */
    protected ObjectName getDerbyMBeanName(Hashtable<String,String> keyProperties)
        throws Exception
    {
        String systemIdentifier = (String)
                  getAttribute(getApplicationManagementMBean(), "SystemIdentifier");
        if (systemIdentifier != null)
            keyProperties.put("system", systemIdentifier);
        return new ObjectName("org.apache.derby", keyProperties);
    }
    
    /**
     * Invokes an operation with no arguments.
     * @param objName MBean to operate on
     * @param name Operation name.
     * @return the value returned by the operation being invoked, or 
     *         <code>null</code> if there is no return value.
     */
    protected Object invokeOperation(ObjectName objName, String name)
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
            throws Exception
    {
        return invokeOperation(objName, name, new Object[0], new String[0]);
    }
    
    /**
     * Invokes an operation with arguments.
     * 
     * @param objName MBean to operate on
     * @param name Operation name.
     * @param params An array containing the parameters to be set when the 
     *        operation is invoked.
     * @param sign An array containing the signature of the operation, i.e.
     *        the types of the parameters.
     * @return the value returned by the operation being invoked, or 
     *         <code>null</code> if there is no return value.
     */
    protected Object invokeOperation(final ObjectName objName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
                                     final String name, 
                                     final Object[] params, 
                                     final String[] sign)
            throws Exception
    {
        final MBeanServerConnection jmxConn = getMBeanServerConnection();
        
        return AccessController.doPrivileged(
            new PrivilegedExceptionAction<Object>() {
                public Object run() throws InstanceNotFoundException, MBeanException, ReflectionException, IOException  {
                    return jmxConn.invoke(objName, name,
                            params, sign);
                }
            }
        );
    }

    /**
     * Set the value of an attribute via JMX.
     *
     * @param objName the name of the object that has the attribute
     * @param name the name of the attribute
     * @param value the new value of the attribute
     * @throws Exception if an error occurs when changing the attribute
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
    void setAttribute(final ObjectName objName, String name, Object value)
            throws Exception {
        final MBeanServerConnection jmxConn = getMBeanServerConnection();
        final Attribute attribute = new Attribute(name, value);
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                jmxConn.setAttribute(objName, attribute);
                return null;
            }
        });
    }
    
    /**
     * Gets the value of a given attribute that is exposed by the MBean 
     * represented by the given object name.
     * @param objName the object name defining a specific MBean instance
     * @param name the name of the attribute
     * @return the value of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    protected Object getAttribute(final ObjectName objName, final String name) 
            throws Exception {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
        final MBeanServerConnection jmxConn = getMBeanServerConnection();
        
        return AccessController.doPrivileged(
            new PrivilegedExceptionAction<Object>() {
                public Object run() throws AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException {
                    return jmxConn.getAttribute(objName, name);
                }
            }
        );
    }
    
    protected void assertBooleanAttribute(boolean expected,
//IC see: https://issues.apache.org/jira/browse/DERBY-1387
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
            ObjectName objName, String name) throws Exception
    {
        Boolean bool = (Boolean) getAttribute(objName, name);
        assertNotNull(bool);
        assertEquals(expected, bool.booleanValue());
    }
    
    protected void assertIntAttribute(int expected,
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
            ObjectName objName, String name) throws Exception
    {
        Integer integer = (Integer) getAttribute(objName, name);
        assertNotNull(integer);
        assertEquals(expected, integer.intValue());
    }
    
    protected void assertLongAttribute(int expected,
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
            ObjectName objName, String name) throws Exception
    {
        Long longNumber = (Long) getAttribute(objName, name);
        assertNotNull(longNumber);
        assertEquals(expected, longNumber.longValue());
    }
    
    protected void assertStringAttribute(String expected,
            ObjectName objName, String name) throws Exception
    {
        String str = (String) getAttribute(objName, name);
        assertNotNull(str);
        assertEquals(expected, str);
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
     * to be a long.
     * @param objName the object name representing the MBean instance from which
     *        the attribute value will be retreived
     * @param name the name of the attribute
     * @throws java.lang.Exception if an unexpected error occurs
     */
    protected void checkLongAttributeValue(ObjectName objName, String name) 
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
            throws Exception {
        
        Object value = getAttribute(objName, name);
        long unboxedValue = ((Long)value).longValue();
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
