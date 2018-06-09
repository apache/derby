/*

   Derby - Class 
   org.apache.derbyTesting.functionTests.tests.derbynet.SysinfoTest

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

import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
	This tests the sysinfo command
 */

public class SysinfoTest extends BaseJDBCTestCase {

    private static final String POLICY_FILE_NAME=
        "org/apache/derbyTesting/functionTests/tests/derbynet/SysinfoTest.policy";
    private ArrayList<String> OUTPUT;

    /**
     * Set to true before adding a test to the suite to add some extra properties.
     */
    private static boolean useProperties = false;


    /**
     * Default constructor
     * @param name
     */
    public SysinfoTest(String name) {
        super(name);
        /**
         * Output from sysinfo without the extra properties. 
         */
        ArrayList<String> OUTPUT1 = new ArrayList<String>();
        OUTPUT1.add("--------- Derby Network Server Information --------");
        OUTPUT1.add("derby.drda.maxThreads=0");
        OUTPUT1.add("derby.drda.sslMode=off"); 
        OUTPUT1.add("derby.drda.keepAlive=true"); 
        OUTPUT1.add("derby.drda.minThreads=0");
        OUTPUT1.add("derby.drda.portNumber="+TestConfiguration.getCurrent().getPort());
        OUTPUT1.add("derby.drda.logConnections=false");
        OUTPUT1.add("derby.drda.timeSlice=0"); 
        OUTPUT1.add("derby.drda.startNetworkServer=false"); 
        OUTPUT1.add("derby.drda.traceAll=false");
        OUTPUT1.add("--------- Derby Information --------"); 
        OUTPUT1.add("------------------------------------------------------"); 
        OUTPUT1.add("----------------- Locale Information -----------------" ); 
        OUTPUT1.add("------------------------------------------------------");

        /**
         * Output by sysinfo with the extra properties.
         */
        ArrayList<String> OUTPUT2 = new ArrayList<String>(OUTPUT1);
        OUTPUT2.add("--------- Derby Network Server Information --------"); 
        OUTPUT2.add("derby.drda.securityMechanism=USER_ONLY_SECURITY"); 

        if (useProperties)
            OUTPUT = OUTPUT2;
        else 
            OUTPUT = OUTPUT1;
    }

    /**
     * Creates a suite with two testcases, with and without some extra 
     * system properties.
     * 
     * @return an empty suite if derbynet.jar is not available, and
     *      if the JVM only supports JSR169, otherwise, return a suite with
     *      6 tests, 3 with properties set, 3 without.
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("SysinfoTest");

        // we need to be able to run the server
        if (!Derby.hasServer()) return suite;
        // don't run with JSR169 for this is a network server test
        if (JDBC.vmSupportsJSR169()) return suite;

        useProperties = false;
        // a call to sysinfo will eventually attempt to load resource 
        // org.apache.derby.info.DBMS.properties.
        // If we're using classes, we don't have read permission for the dir.
        // So, figure out the path & pass the property on so the reference
        // in the policy file can be resolved.
        // Note: can't use $derbyTesting.codeclasses as on windows it has
        // the slashes adjusted.
        if (!TestConfiguration.loadingFromJars()) {
            Properties propstmp = new Properties();
            propstmp.put("sysinfotest.classesdir", findClassDir());
            suite.addTest(new SystemPropertyTestSetup(decorateTest(), propstmp));
        }
        else
            suite.addTest(decorateTest());

        useProperties = true;
        Properties sysprops = new Properties();
        if (!TestConfiguration.loadingFromJars())
            sysprops.put("sysinfotest.classesdir", findClassDir());
        sysprops.put("derby.infolog.append","true");
        sysprops.put("derby.language.logStatementText","true");
        //#drda property ,test for it in sysinfo output
        sysprops.put("derby.drda.securityMechanism","USER_ONLY_SECURITY");
        Test test = new SystemPropertyTestSetup(decorateTest(), sysprops);
        Properties prop = new Properties();
        prop.put("derby.locks.waitTimeout","120");
        test = new DatabasePropertyTestSetup(test, prop);
        //suite.addTest(new SystemPropertyTestSetup(decorateTest(), props));
        suite.addTest(test);

        return suite;
    }

    /**
     * Decorate a test with SecurityManagerSetup, clientServersuite, and
     * SupportFilesSetup.
     * 
     * @return the decorated test
     */
    private static Test decorateTest() {
        Test test = TestConfiguration.clientServerSuite(SysinfoTest.class);

        // Install a security manager using the initial policy file.
        return TestConfiguration.singleUseDatabaseDecorator(
                new SecurityManagerSetup(test, POLICY_FILE_NAME));
    }

    /**
     * Test sysinfo
     * 
     * @throws Exception
     */	
    public void testSysinfo() throws Exception {
        String[] SysInfoCmd = 
            new String[] {"org.apache.derby.drda.NetworkServerControl", "sysinfo",
            "-p", String.valueOf(TestConfiguration.getCurrent().getPort())};

        Process p = execJavaCmd(SysInfoCmd);
        String s = readProcessOutput(p);
 
        print("testSysinfo", s);
  
        assertMatchingStringExists(s);
    }

    /**
     * Test sysinfo by calling NetworkServerControl.getSysinfo()
     * 
     * @throws Exception
     */
    public void testSysinfoMethod() throws Exception {	

        String s = NetworkServerTestSetup.
        getNetworkServerControl(TestConfiguration.getCurrent().getPort()).getSysinfo();
        print("testSysinfoMethod", s);
        assertMatchingStringExists(s);
    }		

    /**
     * Test sysinfo w/ foreign (non-English) locale.
     * 
     * @throws Exception
     */
    public void testSysinfoLocale() throws Exception {

        String[] SysInfoLocaleCmd = 
            new String[] {"-Duser.language=de", "-Duser.country=DE", 
                "org.apache.derby.drda.NetworkServerControl", "sysinfo",
                "-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        Process p = execJavaCmd(SysInfoLocaleCmd);
        String s = readProcessOutput(p);
        print("testSysinfoLocale", s);
        assertMatchingStringExists(s);
    }

    /**
     * Prints strings to System.out to make it easier to update the tests
     * when the output changes if derby.tests.debug is true.
     * 
     * @param name just a label to identify the string
     * @param s the string to be printed
     */
    private void print(String name,String s) {
        println("\n\n>>>" + name + ">>>");
        println(s);
        println("<<<" + name + "<<<\n\n");
    }

    public void tearDown() throws Exception {
        super.tearDown();
        OUTPUT = null;
    }

    private static String findClassDir() {
        URL url = SecurityManagerSetup.getURL("org.apache.derbyTesting." +
                    "functionTests.tests.derbynet.SysinfoTest");
        return url.getPath();
    }

    
    /**
     *  Check sysinfo output to make sure that it contains strings
     *  specfied in OUTPUT. This was changed in DERBY-4997 to no
     *  longer use a sed method to strip out the unchecked lines,
     *  but rather to just make sure the ones we want to check are 
     *  there
     *  
     * @param actualOutput Actual sysinfo output 
     */
    private void assertMatchingStringExists(String actualOutput) {
        for (String s : OUTPUT) {
            assertTrue("cannot find " + s + " in actualOutput:" + actualOutput,
                    actualOutput.indexOf(s) >=0);            
        }        
    }
        
 
}
