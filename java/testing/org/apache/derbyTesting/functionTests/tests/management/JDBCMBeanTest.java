/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.JDBCMBeanTest

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

import java.io.BufferedReader;
import java.sql.DatabaseMetaData;
import java.util.Hashtable;
import javax.management.ObjectName;
import junit.framework.Test;


/**
 * <p>
 * This JUnit test class is for testing the JDBCMBean that is available in
 * Derby. Running these tests requires a JVM supporting J2SE 5.0 or better, due 
 * to the implementation's dependency of the platform management agent.</p>
 * <p>
 * This class currently tests the following:</p>
 * <ul>
 *   <li>That the attributes we expect to be available exist</li>
 *   <li>That these attributes are readable</li>
 *   <li>That these attributes have the correct type</li>
 *   <li>That these attributes have the correct value</li>
 *   <li>That the MBean operations we expect to see are invokeable, and that
 *       their return values are as expected.</li>
 * <p>
 * The test fixtures will fail if an exception occurs (will be reported as an 
 * error in JUnit).</p>
 */
public class JDBCMBeanTest extends MBeanTest {
    
    public JDBCMBeanTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        return MBeanTest.suite(JDBCMBeanTest.class, 
                                        "JDBCMBeanTest");
    }
    
    /**
     * <p>
     * Creates an object name instance for the MBean whose object name's textual
     * representation contains:</p>
     * <ul>
     *   <li>type=JDBC</li>
     * </ul>
     * @return the object name representing Derby's JDBCMBean
     * @throws MalformedObjectNameException if the object name is not valid
     */
    private ObjectName getJdbcMBeanObjectName() 
            throws Exception {
        
        // get a reference to the JDBCMBean instance
        Hashtable<String, String> keyProps = new Hashtable<String, String>();
        keyProps.put("type", "JDBC");
        return getDerbyMBeanName(keyProps);
    }
    
    //
    // ---------- TEST FIXTURES ------------
    //
    // This MBean currently has only read-only attributes, which will be tested.
    // Expected operations will be invoked.
    
    
    public void testAttributeCompliantDriver() throws Exception {
        // we expect Derby's driver always to be JDBC compliant
        assertBooleanAttribute(true, getJdbcMBeanObjectName(), "CompliantDriver");
    }
    
    public void testAttributeDriverLevel() throws Exception {
        String expected = "[Unable to get driver level from sysinfo]";
        // Get the expected value from sysinfo
        BufferedReader sysinfoOutput = getSysinfoFromServer();
        String line = null;
        while ((line = sysinfoOutput.readLine()) != null) {
            /* Looking for:
             *--------- Derby Information --------
             *JRE - JDBC: J2SE 5.0 - JDBC 3.0
             *            ^^^^^^^^^^^^^^^^^^^
             * (actual JRE/JDBC values may vary)*/
            if (line.matches("^JRE - JDBC: .*")) {
                expected = line.substring(line.indexOf(": ") + 2);
            }
        }
        
        // test the attribute value against the expected value
        assertStringAttribute(expected,getJdbcMBeanObjectName(), "DriverLevel");
    }
    
    public void testAttributeMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getConnection().getMetaData();
        int expected = dbmd.getDriverMajorVersion();
        assertIntAttribute(expected, getJdbcMBeanObjectName(), "MajorVersion");
    }
    
    public void testAttributeMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getConnection().getMetaData();
        int expected = dbmd.getDriverMinorVersion();
        assertIntAttribute(expected, getJdbcMBeanObjectName(), "MinorVersion");
    }

    public void testOperationAcceptsURL() throws Exception {
        String opName = "acceptsURL";
        ObjectName objName = getJdbcMBeanObjectName();
        Object[] params = new Object[1];
        String[] signature = { "java.lang.String" };
        Boolean accepted;
        
        // first, test that a simple valid embedded driver URL is accepted
        params[0] = "jdbc:derby:testDatabase";
        accepted = (Boolean)invokeOperation(objName, opName, params, signature);
        assertTrue("URL: " + params[0], accepted);
                
        // then, test that a valid embedded URL with a number of attributes is
        // accepted
        params[0] = "jdbc:derby:testDB;create=true;user=tester;password=mypass";
        accepted = (Boolean)invokeOperation(objName, opName, params, signature);
        assertTrue("URL: " + params[0], accepted);
        
        // then, check that an invalid URL is not accepted
        params[0] = "jdbc:invalidProtocol:newDatabase";
        accepted = (Boolean)invokeOperation(objName, opName, params, signature);
        assertFalse("URL: " + params[0], accepted);
    }

}
