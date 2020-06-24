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

import java.util.Hashtable;
import javax.management.ObjectName;
import junit.framework.Test;

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
        
        /* The current test fixtures of this class assume that both instances
         * of the version MBean are accessible, i.e. both for derby.jar and
         * derbynet.jar. This means that it is assumed that the Network Server
         * is running and that JMX is enabled. This is being handled by the
         * super class. */
//IC see: https://issues.apache.org/jira/browse/DERBY-1387
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
        return MBeanTest.suite(VersionMBeanTest.class, 
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
                                        "VersionMBeanTest");
    }
    
    /**
     * <p>
     * Creates an object name instance for the Derby MBean whose object name's 
     * textual representation includes the following key properties:</p>
     * <ul>
     *   <li>type=Version</li>
     *   <li>jar=derby.jar</li>
     * </ul>
     * <p>
     * The object name may also include other key properties such as a system
     * identifier (DERBY-3466).</p>
     * @return the object name representing the VersionMBean for the derby 
     *         engine in this Derby system.
     * @throws MalformedObjectNameException if the object name is not valid
     */
    private ObjectName getDerbyJarObjectName() 
            throws Exception {
        
        // get a reference to the VersionMBean instance for derby.jar
        Hashtable<String, String> keyProps = new Hashtable<String, String>();
        keyProps.put("type", "Version");
        keyProps.put("jar", "derby.jar");
        return getDerbyMBeanName(keyProps);
    }
    
    /**
     * <p>
     * Creates an object name instance for the Derby MBean whose object name's 
     * textual representation includes the following key properties:</p>
     * <ul>
     *   <li>type=Version</li>
     *   <li>jar=derbynet.jar</li>
     * </ul>
     * <p>
     * The object name may also include other key properties such as a system
     * identifier (DERBY-3466).</p>
     * @return the object name representing the VersionMBean for the Network 
     *         Server running the Derby system represented by the system
     *         identifier obtained from Derby's management service.
     * @throws MalformedObjectNameException if the object name is not valid
     */
    private ObjectName getDerbynetJarObjectName() 
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
            throws Exception {
        
        // get a reference to the VersionMBean instance for derbynet.jar
        Hashtable<String, String> keyProps = new Hashtable<String, String>();
        keyProps.put("type", "Version");
        keyProps.put("jar", "derbynet.jar");
        return getDerbyMBeanName(keyProps);
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
    
    public void testDerbynetJarAttributeAlpha() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        checkBooleanAttributeValue(getDerbynetJarObjectName(), "Alpha");
    }
    
    public void testDerbyJarAttributeBeta() throws Exception {
        checkBooleanAttributeValue(getDerbyJarObjectName(), "Beta");
    }
    
    public void testDerbynetJarAttributeBeta() throws Exception {
        checkBooleanAttributeValue(getDerbynetJarObjectName(), "Beta");
    }
    
    public void testDerbyJarAttributeBuildNumber() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "BuildNumber");
    }
    
    public void testDerbynetJarAttributeBuildNumber() throws Exception {
        checkStringAttributeValue(getDerbynetJarObjectName(), "BuildNumber");
    }
    
    public void testDerbyJarAttributeMaintenanceVersion() throws Exception {
        checkIntAttributeValue(getDerbyJarObjectName(), "MaintenanceVersion");
    }
    
    public void testDerbynetJarAttributeMaintenanceVersion() throws Exception {
        checkIntAttributeValue(getDerbynetJarObjectName(), "MaintenanceVersion");
    }
    
    public void testDerbyJarAttributeMajorVersion() throws Exception {
        checkIntAttributeValue(getDerbyJarObjectName(), "MajorVersion");
    }
    
    public void testDerbynetJarAttributeMajorVersion() throws Exception {
        checkIntAttributeValue(getDerbynetJarObjectName(), "MajorVersion");
    }
    
    public void testDerbyJarAttributeMinorVersion() throws Exception {
        checkIntAttributeValue(getDerbyJarObjectName(), "MinorVersion");
    }
    
    public void testDerbynetJarAttributeMinorVersion() throws Exception {
        checkIntAttributeValue(getDerbynetJarObjectName(), "MinorVersion");
    }
    
    public void testDerbyJarAttributeProductName() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "ProductName");
    }
    
    public void testDerbynetJarAttributeProductName() throws Exception {
        checkStringAttributeValue(getDerbynetJarObjectName(), "ProductName");
    }
    
    public void testDerbyJarAttributeProductTechnologyName() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), 
                                   "ProductTechnologyName");
    }
    
    public void testDerbynetJarAttributeProductTechnologyName() throws Exception {
        checkStringAttributeValue(getDerbynetJarObjectName(), 
                                   "ProductTechnologyName");
    }
    
    public void testDerbyJarAttributeProductVendorName() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "ProductVendorName");
    }
    
    public void testDerbynetJarAttributeProductVendorName() throws Exception {
        checkStringAttributeValue(getDerbynetJarObjectName(), "ProductVendorName");
    }
    
    public void testDerbyJarAttributeVersionString() throws Exception {
        checkStringAttributeValue(getDerbyJarObjectName(), "VersionString");
    }
    
    public void testDerbynetJarAttributeVersionString() throws Exception {
        checkStringAttributeValue(getDerbynetJarObjectName(), "VersionString");
    }
    
}
