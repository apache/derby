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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import junit.framework.Test;
import junit.framework.TestSuite;

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
public class VersionMBeanTest extends MBeanTest {
    
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
