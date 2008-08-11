/*

Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.NetworkServerControlClientCommandTest

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

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Utilities;

public class NetworkServerControlClientCommandTest extends BaseJDBCTestCase {

    public NetworkServerControlClientCommandTest(String name) {
        super(name);

    }

    /**
     * Test various ping commands from the command line
     * 
     * @throws Exception
     */
    public void testPing() throws Exception {
        String currentHost = TestConfiguration.getCurrent().getHostName();
        String currentPort = Integer.toString(TestConfiguration.getCurrent().getPort());
        
        String[] pingCmd1 = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "ping" };
        assertSuccessfulPing(pingCmd1);
      
        String[] pingCmd2 = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "ping", "-h",
                currentHost, "-p", currentPort};
        assertSuccessfulPing(pingCmd2);
        
        String[] pingCmd3 = new String[] {"org.apache.derby.drda.NetworkServerControl",
        "ping", "-h", currentHost};
        assertSuccessfulPing(pingCmd3);
        
        String[] pingCmd4 = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "ping", "-h",
                "nothere" };
        assertFailedPing(pingCmd4,"Unable to find host");
        String[] pingCmd5= new String[] {"org.apache.derby.drda.NetworkServerControl",
        "ping", "-h", currentHost, "-p", "9393"};
        assertFailedPing(pingCmd5,"Could not connect to Derby Network Server");

    }

    /**
     * Execute ping command and verify that it completes successfully
     * @param pingCmd array of java arguments for ping command
     * @throws InterruptedException
     * @throws IOException
     */
    private void  assertSuccessfulPing(String[] pingCmd) throws InterruptedException, IOException {
        
/*        InputStream is = Utilities.execJavaCmd(pingCmd, 0);
        byte[] b = new byte[80];
        is.read(b, 0, 80);
        String output = new String(b);
        assertTrue(output.startsWith("Connection obtained"));
*/
        assertExecJavaCmdAsExpected(new String[] {"Connection obtained"}, pingCmd, 0);
    }
    /**
     * Execute ping command and verify that it fails with the expected message
     * 
     * @param pingCmd array of java arguments for ping command
     * @param expectedMessage expected error message
     * @throws InterruptedException
     * @throws IOException
     */
    private void assertFailedPing(String[] pingCmd,String expectedMessage) throws InterruptedException, IOException {
        
        /*InputStream is = Utilities.execJavaCmd(pingCmd, 1);
        byte[] b = new byte[80];
        is.read(b, 0, 80);
        String output = new String(b);
        assertTrue(output.startsWith(expectedMessage));*/
        assertExecJavaCmdAsExpected(new String[] {expectedMessage}, pingCmd, 1);
    }
    

    public static Test suite() {

        TestSuite suite = new TestSuite("NetworkServerControlClientCommandTest");
        

        // need network server, english locale so we can compare command output 
        // and we don't run on J2ME because java command is different.
        if (!Derby.hasServer() || !Locale.getDefault().getLanguage().equals("en") ||
                JDBC.vmSupportsJSR169())
            return suite;
        Test test = TestConfiguration
                .clientServerSuite(NetworkServerControlClientCommandTest.class);
        
        // no security manager because we exec a process and don't have permission for that.
        test = SecurityManagerSetup.noSecurityManager(test);
        suite.addTest(test);
        return suite;
    }

}
