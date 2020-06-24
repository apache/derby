/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.GetCurrentPropertiesTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.File;
import java.util.Enumeration;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This tests getCurrentProperties
 * 
 */
public class GetCurrentPropertiesTest extends BaseJDBCTestCase {
    // create own policy file
    private static final String POLICY_FILE_NAME =
        "org/apache/derbyTesting/functionTests/tests/derbynet/GetCurrentPropertiesTest.policy";
//IC see: https://issues.apache.org/jira/browse/DERBY-6162

    public GetCurrentPropertiesTest(String name) {
        super(name);
    }

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        Test test = new BaseTestSuite(GetCurrentPropertiesTest.class);
        test = TestConfiguration.clientServerDecorator(test);

        // Install a security manager using the special policy file.
        // Grant ALL FILES execute, and getPolicy permissions,
        // as well as write for the trace files.
        test = new SecurityManagerSetup(test, POLICY_FILE_NAME);
//IC see: https://issues.apache.org/jira/browse/DERBY-6162

        // return suite; to ensure that nothing interferes with setting of
        // properties, wrap in singleUseDatabaseDecorator 
        return TestConfiguration.singleUseDatabaseDecorator(test);
    }

    /**
     * Testing the properties before connecting to a database
     * 
     * @throws Exception
     */
    public void test_01_propertiesBeforeConnection() throws Exception {
        Properties p = null;
        String  userDir = getSystemProperty( "user.dir" );
        String traceDir = userDir + File.separator + "system";
        Properties expectedValues = new Properties();
        expectedValues.setProperty("derby.drda.traceDirectory",traceDir);
        expectedValues.setProperty("derby.drda.maxThreads","0");
        expectedValues.setProperty("derby.drda.sslMode","off");
        expectedValues.setProperty("derby.drda.keepAlive","true");
        expectedValues.setProperty("derby.drda.minThreads","0");
        expectedValues.setProperty("derby.drda.portNumber",TestConfiguration.getCurrent().getPort()+"");
        expectedValues.setProperty("derby.drda.logConnections","false");
        expectedValues.setProperty("derby.drda.timeSlice","0");
        expectedValues.setProperty("derby.drda.startNetworkServer","false");
        expectedValues.setProperty("derby.drda.host","127.0.0.1");
        expectedValues.setProperty("derby.drda.traceAll","false");
        p = NetworkServerTestSetup.getNetworkServerControl().getCurrentProperties();

        Enumeration expectedProps = expectedValues.propertyNames();
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
        while (expectedProps.hasMoreElements()) {
            String propName = (String)expectedProps.nextElement();
            String propVal = (String)p.get(propName);
            //for debug
            println(expectedValues.getProperty(propName));
            println(propVal);
            assertEquals(expectedValues.getProperty(propName), propVal);

        }
    }
    /**
     * Testing the properties after connecting to a database
     * 
     * @throws Exception
     */
    public void test_02_propertiesAfterConnection() throws Exception {
        Properties p = null;
        String  userDir = getSystemProperty( "user.dir" );
        String traceDir = userDir + File.separator + "system";
        Properties expectedValues = new Properties();
        expectedValues.setProperty("derby.drda.traceDirectory",traceDir);
        expectedValues.setProperty("derby.drda.maxThreads","0");
        expectedValues.setProperty("derby.drda.sslMode","off");
        expectedValues.setProperty("derby.drda.trace.4","true");
        expectedValues.setProperty("derby.drda.keepAlive","true");
        expectedValues.setProperty("derby.drda.minThreads","0");
        expectedValues.setProperty("derby.drda.portNumber",TestConfiguration.getCurrent().getPort()+"");
        expectedValues.setProperty("derby.drda.logConnections","true");
        expectedValues.setProperty("derby.drda.timeSlice","0");
        expectedValues.setProperty("derby.drda.startNetworkServer","false");
        expectedValues.setProperty("derby.drda.host","127.0.0.1");
        expectedValues.setProperty("derby.drda.traceAll","false");  
        getConnection().setAutoCommit(false);
        NetworkServerControl nsctrl = NetworkServerTestSetup.getNetworkServerControl();
        nsctrl.trace(4,true);
        nsctrl.logConnections(true);
        p = NetworkServerTestSetup.getNetworkServerControl().getCurrentProperties();
        Enumeration expectedProps = expectedValues.propertyNames();
        while (expectedProps.hasMoreElements()) {
            String propName = (String) expectedProps.nextElement();
            String propVal = (String)p.get(propName);
            //for debug
            println(expectedValues.getProperty(propName));
            println(propVal);
            assertEquals(expectedValues.getProperty(propName), propVal);

        }
    } 
    /**
     * Testing the properties after setting the trace dir and tracing on
     * 
     * @throws Exception
     */
    public void test_03_propertiesTraceOn() throws Exception {
        Properties p = null;

        NetworkServerControl nsctrl = NetworkServerTestSetup.getNetworkServerControl();
        nsctrl.trace(true);
        String derbySystemHome = getSystemProperty("derby.system.home");
        nsctrl.setTraceDirectory(derbySystemHome);
        Properties expectedValues = new Properties();
        expectedValues.setProperty("derby.drda.traceDirectory",derbySystemHome);
        expectedValues.setProperty("derby.drda.maxThreads","0");
        expectedValues.setProperty("derby.drda.sslMode","off");
        expectedValues.setProperty("derby.drda.keepAlive","true");
        expectedValues.setProperty("derby.drda.minThreads","0");
        expectedValues.setProperty("derby.drda.portNumber",TestConfiguration.getCurrent().getPort()+"");
        expectedValues.setProperty("derby.drda.logConnections","true");
        expectedValues.setProperty("derby.drda.timeSlice","0");
        expectedValues.setProperty("derby.drda.startNetworkServer","false");
        expectedValues.setProperty("derby.drda.host","127.0.0.1");
        expectedValues.setProperty("derby.drda.traceAll","true");
        p = NetworkServerTestSetup.getNetworkServerControl().getCurrentProperties();
        Enumeration expectedProps = expectedValues.propertyNames();
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
        while (expectedProps.hasMoreElements()) {
            String propName = (String) expectedProps.nextElement();
            String propVal = (String)p.get(propName);
            //for debug
            println(expectedValues.getProperty(propName));
            println(propVal);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            assertEquals
              (
               "Unexpected value for property " + propName,
               expectedValues.getProperty(propName),
               propVal
               );
        }
    }
}
