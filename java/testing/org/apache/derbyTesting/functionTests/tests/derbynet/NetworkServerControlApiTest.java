/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.NetworkServerControlApiTest

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

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.tests.lang.SecurityPolicyReloadingTest;
import org.apache.derbyTesting.functionTests.tests.lang.SimpleTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.File;
import java.security.AccessController;
import java.security.Policy;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;

import junit.framework.Test;
import junit.framework.TestSuite;

public class NetworkServerControlApiTest extends BaseJDBCTestCase {

    private static String POLICY_FILE_NAME="functionTests/tests/derbynet/NetworkServerControlApiTest.policy";
    private static String TARGET_POLICY_FILE_NAME="server.policy";
    
    public NetworkServerControlApiTest(String name) {
        super(name);
       
    }

    /** Test NetworkServerControl API.
     *  Right now it tests only the trace command for DERBY-3110.
     *  TODO: Add tests for other API calls.
     */
    
    
     /** 
     * @throws Exception
     */
    public void testTraceCommands() throws Exception
    {
        NetworkServerControl nsctrl = new NetworkServerControl();
        String derbySystemHome = getSystemProperty("derby.system.home");
        nsctrl.setTraceDirectory(derbySystemHome);
       
        nsctrl.trace(true);
        nsctrl.ping();
        assertTrue(fileExists(derbySystemHome+"/Server3.trace"));
        nsctrl.trace(false);
        
        // now try on a directory where we don't have permission
        // this won't actually cause a failure until we turn on tracing.
        // assume we don't have permission to write to root.
        nsctrl.setTraceDirectory("/");
        
        // attempt to turn on tracing to location where we don't have permisson
        try {
            nsctrl.trace(true);
            fail("Should have gotten an exception turning on tracing");
        } catch (Exception e) {
            // expected exception
        }
        // make sure we can still ping
        nsctrl.ping();
    
                        
    }

    private boolean fileExists(String filename) {
        final File file = new File(filename);
        try {
            return ((Boolean)AccessController.doPrivileged(
                new PrivilegedExceptionAction() {
                    public Object run() throws SecurityException {
                        return new Boolean(file.exists());
                    }
                })).booleanValue();
        } catch (PrivilegedActionException pae) {
            throw (SecurityException)pae.getException();
        }
        
    }
    
    /**
     * Construct the name of the server policy file.
     */
    private String makeServerPolicyName()
    {
        try {
            String  userDir = getSystemProperty( "user.dir" );
            String  fileName = userDir + File.separator + SupportFilesSetup.EXTINOUT + File.separator + TARGET_POLICY_FILE_NAME;
            File      file = new File( fileName );
            String  urlString = file.toURL().toExternalForm();

            return urlString;
        }
        catch (Exception e)
        {
            System.out.println( "Unexpected exception caught by makeServerPolicyName(): " + e );

            return null;
        }
    }
    
    
    /**
     * Add decorators to a test run. Context is established in the reverse order
     * that decorators are declared here. That is, decorators compose in reverse
     * order. The order of the setup methods is:
     *
     * <ul>
     * <li>Copy security policy to visible location.</li>
     * <li>Install a security manager.</li>
     * <li>Run the tests.</li>
     * </ul>
     */
    private static Test decorateTest()
    {
        
        NetworkServerControlApiTest nsapitest = new NetworkServerControlApiTest("test");
        
        String serverPolicyName = nsapitest.makeServerPolicyName();
    
        
        Test test = TestConfiguration.clientServerSuite(NetworkServerControlApiTest.class);
        //
        // Install a security manager using the initial policy file.
        //
        
        test = new SecurityManagerSetup( test,serverPolicyName );
        
        
        
        //
        // Copy over the policy file we want to use.
        //
        test = new SupportFilesSetup
            (
             test,
             null,
             new String[] { POLICY_FILE_NAME },
             null,
             new String[] { TARGET_POLICY_FILE_NAME}
             );

        return test;
    }
    
    public static Test suite()
    {
        
        TestSuite suite = new TestSuite("NetworkServerControlApiTest");
        
        // Need derbynet.jar in the classpath!
        if (!Derby.hasServer())
            return suite;
        
        return decorateTest();
    }
}
