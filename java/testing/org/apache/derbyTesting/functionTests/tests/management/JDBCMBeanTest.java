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
import java.sql.Driver;
import java.util.Hashtable;
import javax.management.ObjectName;
import junit.framework.Test;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derbyTesting.junit.Utilities;


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
        // get JDBC version from DatabaseMetaData for comparison
        DatabaseMetaData dmd = getConnection().getMetaData();
        String JDBCVersion = "" + dmd.getJDBCMajorVersion() + 
            dmd.getJDBCMajorVersion() + "." +
            dmd.getJDBCMinorVersion();
        println("DatabaseMetaDataJDBCLevel = " + JDBCVersion);
        ObjectName driverLevel = getJdbcMBeanObjectName();
        String driverLevelString = driverLevel.toString();
        println("MBean driverLevel  = " + driverLevelString);
       
        assert(driverLevelString.indexOf('?') == -1);
        assert(driverLevelString.matches("^JRE - JDBC: " + JDBCVersion + ".*"));

    }
    
    /**
     * <p>
     * Tests the MajorVersion attribute of the JDBCMBean. Will test that there
     * exists an attribute with that name that we are able to read, that it 
     * returns the correct type, and that the return value is as expected.</p>
     * <p>
     * The expected value is retreived from the embedded driver that is directly
     * accessible to this JVM, making the assumption that this driver's version
     * information is the same as the version information of the embedded driver
     * used in the JVM being instrumented using JMX (this may or may not be the
     * same JVM).</p>
     * 
     * @throws java.lang.Exception if an error occurs, or if the test fails.
     */
    public void testAttributeMajorVersion() throws Exception {
        /* since the JDBCMBean instruments the embedded driver (InternalDriver),
         * we need to get expected values from the embedded driver even if
         * this test configuration is client/server.
         * Assuming that the embedded driver is available in the classpath.
         */
        Driver d = new org.apache.derby.jdbc.EmbeddedDriver();
        int expected = d.getMajorVersion();
        assertIntAttribute(expected, getJdbcMBeanObjectName(), "MajorVersion");
    }
    
    /**
     * <p>
     * Tests the MinorVersion attribute of the JDBCMBean. Will test that there
     * exists an attribute with that name that we are able to read, that it 
     * returns the correct type, and that the return value is as expected.</p>
     * <p>
     * The expected value is retreived from the embedded driver that is directly
     * accessible to this JVM, making the assumption that this driver's version
     * information is the same as the version information of the embedded driver
     * used in the JVM being instrumented using JMX (this may or may not be the
     * same JVM).</p>
     * 
     * @throws java.lang.Exception if an error occurs, or if the test fails.
     */
    public void testAttributeMinorVersion() throws Exception {
        /* since the JDBCMBean instruments the embedded driver (InternalDriver),
         * we need to get expected values from the embedded driver even if
         * this test configuration is client/server.
         * Assuming that DriverManager is available in the classpath.
         */
        Driver d = new org.apache.derby.jdbc.EmbeddedDriver();
        int expected = d.getMinorVersion();
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
