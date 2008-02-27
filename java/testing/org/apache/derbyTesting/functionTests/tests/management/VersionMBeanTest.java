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
        
        return MBeanTest.suite(VersionMBeanTest.class, 
                                        "VersionMBeanTest");
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
