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
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.DerbyConstants;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class NetworkServerControlClientCommandTest extends BaseJDBCTestCase {

    public NetworkServerControlClientCommandTest(String name) {
        super(name);
    }
    
    public void testPingWithoutArgs() throws InterruptedException, IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4260
        if (!hasDefaultDerbyPortUsing()) {
            /* If the port isn't the default one, we make sure that the test passes.
             * The -p parameter isn't specified here.
             * Changed to accomodate DERBY-4217
             */
            return;
        }        
        
        String[] pingWithoutArgsCmd = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "ping" };
        
        pingWithoutArgsCmd = decorateCmdWithEnglishLocale(pingWithoutArgsCmd); 
        
        assertSuccessfulPing(pingWithoutArgsCmd);                
    }
    
    private boolean hasDefaultDerbyPortUsing() {
        return TestConfiguration.getCurrent().getPort() ==
                DerbyConstants.DEFAULT_DERBY_PORT;
    }

    /*
     * English locale is neccessary for running on non-English Locale.
     * See #Derby-4260
     */
    private String[] decorateCmdWithEnglishLocale(String[] cmd) {
        String[] newCmd = new String[cmd.length + 1];
        newCmd[0] = "-Dderby.ui.locale=en_US";
        
        System.arraycopy(cmd, 0, newCmd, 1, cmd.length);
        
        return newCmd;
    }
    
    public void testPingWithDefinedHost() throws InterruptedException, IOException {
        if (!hasDefaultDerbyPortUsing()) {
            /* If the port isn't the default one, we make sure that the test passes.
             * The -p parameter isn't specified here.
             * Changed to accomodate DERBY-4217
             */
            return;
        }        
        
        String currentHost = TestConfiguration.getCurrent().getHostName();
        String[] pingWithoutArgsCmd = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "ping", "-h", currentHost};
                
        pingWithoutArgsCmd = decorateCmdWithEnglishLocale(pingWithoutArgsCmd);
                
        assertSuccessfulPing(pingWithoutArgsCmd);
    }
    
    public void testPingWithDefinedHostAndPort() throws InterruptedException, IOException {
        String currentPort = Integer.toString(TestConfiguration.getCurrent().getPort());
        String currentHost = TestConfiguration.getCurrent().getHostName();
        String[] pingWithoutArgsCmd = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "ping", "-h",
                currentHost, "-p", currentPort};
        
        pingWithoutArgsCmd = decorateCmdWithEnglishLocale(pingWithoutArgsCmd);
        
        assertSuccessfulPing(pingWithoutArgsCmd);
    }
    
    public void testPingWithWrongHost() throws InterruptedException, IOException {
        String[] pingWithoutArgsCmd = new String[] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5942
                "org.apache.derby.drda.NetworkServerControl",
                "ping", "-h", "nothere.invalid"};
                
        pingWithoutArgsCmd = decorateCmdWithEnglishLocale(pingWithoutArgsCmd);
                
        assertFailedPing(pingWithoutArgsCmd, "Unable to find host");
    }
    
    public void testPingWithBogusPort() throws InterruptedException, IOException {
        String currentHost = TestConfiguration.getCurrent().getHostName();
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
        String bogusPort = Integer.toString(
                TestConfiguration.getCurrent().getBogusPort());
        String[] pingWithoutArgsCmd = new String[] {
                "org.apache.derby.drda.NetworkServerControl",
                "ping", "-h", currentHost, "-p", bogusPort};
                
        pingWithoutArgsCmd = decorateCmdWithEnglishLocale(pingWithoutArgsCmd);
                
        assertFailedPing(pingWithoutArgsCmd, "Could not connect to Derby Network Server");
    }
    
    /**
     * Execute ping command and verify that it completes successfully
     * @param pingCmd array of java arguments for ping command
     * @throws InterruptedException
     * @throws IOException
     */
    private void  assertSuccessfulPing(String[] pingCmd) throws InterruptedException, IOException {
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
        assertExecJavaCmdAsExpected(new String[] {expectedMessage}, pingCmd, 1);
    }
    

    public static Test suite() {

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("NetworkServerControlClientCommandTest");

        // need network server so we can compare command output 
        // and we don't run on J2ME because java command is different.
//IC see: https://issues.apache.org/jira/browse/DERBY-4260
        if (!Derby.hasServer() ||
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
