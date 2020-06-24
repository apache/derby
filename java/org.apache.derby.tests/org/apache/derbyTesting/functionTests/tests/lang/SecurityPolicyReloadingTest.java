/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SecurityPolicyReloadingTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.security.AccessControlException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DerbyConstants;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the dynamic reloading of the security policy file while the
 * engine is still running.
 */
public class SecurityPolicyReloadingTest extends BaseJDBCTestCase {

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  RELOADABLE_INITIAL_SOURCE_POLICY = "functionTests/tests/lang/SecurityPolicyReloadingTest.initial.policy";
    private static  final   String  RELOADABLE_MODIFIED_SOURCE_POLICY = "functionTests/tests/lang/SecurityPolicyReloadingTest.modified.policy";
    private static  final   String  UNRELOADABLE_SOURCE_POLICY = "functionTests/tests/lang/SecurityPolicyReloadingTest.unreloadable.policy";
    private static  final   String  TARGET_POLICY = "server.policy";

    private static  final   String  NON_DBO_USER = "NON_DBO_USER";
    private static  final   String  PASSWORD_TOKEN = "PASSWORD_TOKEN";
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  SecurityPolicyReloadingTest
        (
         )
    {
        super( "testPolicyReloading" );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("SecurityPolicyReloadingTest");

        // The reloaded policy requires restricted property-reading permissions,
        // which is easy to do if you can subdivide the protection domains by
        // jar file but is not easy to do with all of the testing and server
        // classes jumbled together in the same class tree.
        if ( !TestConfiguration.loadingFromJars() ) { return suite; }
//IC see: https://issues.apache.org/jira/browse/DERBY-2466

        suite.addTest( decorateTest() );

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TEST DECORATION
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Add decorators to a test run. Context is established in the reverse order
     * that decorators are declared here. That is, decorators compose in reverse
     * order. The order of the setup methods is:
     *
     * <ul>
     * <li>Copy security policy to visible location.</li>
     * <li>Setup authorization-enabling properties.</li>
     * <li>Install a security manager.</li>
     * <li>Run the tests.</li>
     * </ul>
     */
    private static Test decorateTest()
    {
        Test test = new SecurityPolicyReloadingTest();
//IC see: https://issues.apache.org/jira/browse/DERBY-6162

        //
        // Install a security manager using the initial policy file.
        //
        test = new SecurityManagerSetup(test, makeServerPolicyName());
        
        //
        // Set up authorization with a DBO and non-DBO user
        //
        test = TestConfiguration.sqlAuthorizationDecorator
            (
             test,
             new String[] { NON_DBO_USER },
             PASSWORD_TOKEN
             );
        
        //
        // Copy over the initial policy file we want to use.
        //
        test = new SupportFilesSetup
            (
             test,
             null,
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
             new String[] { getSourcePolicy() },
             null,
             new String[] { makeTargetPolicyStub() }
             );

        // No need to run with default testing policy file because we install our 
        // own initial policy file.
        test = SecurityManagerSetup.noSecurityManager(test);
        return test;
    }

        
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Verify that policy file reloading is allowed and forbidden as expected.
     */
    public void testPolicyReloading()
        throws Exception
    {
        //getTestConfiguration().setVerbosity( true );

        doPolicyReloadingIsGranted();
        doPolicyReloadingIsNotGranted();
    }
    
    ////////////////////////////////////////////////////
    //
    // getPolicy() PRIVILEGE GRANTED
    //
    ////////////////////////////////////////////////////
    
    /**
     * Verify that the DBA has the power to reload the security policy file and
     * that a non-DBA does not have this power.
     */
    private void doPolicyReloadingIsGranted()
        throws Exception
    {
        dbaTest();
        nonDbaTest();
    }
    
    /**
     * Verify that the DBA has the power to reload the security policy file.
     */
    private void dbaTest()
        throws Exception
    {
        Connection  conn = openUserConnection( DerbyConstants.TEST_DBO );
//IC see: https://issues.apache.org/jira/browse/DERBY-2419

        assertTrue( "Initially, should be able to read property.", canReadProperty() );

        // Now prove that the DBO can reload the policy file.
        changePolicyFile( conn, RELOADABLE_MODIFIED_SOURCE_POLICY, true, null );
        assertFalse( "Policy file changed. Should not be able to read the property.", canReadProperty() );

        // Return to initial policy file.
        changePolicyFile( conn, RELOADABLE_INITIAL_SOURCE_POLICY, true, null );
        assertTrue( "Reverted to initial policy. Should be able to read the property again.", canReadProperty() );

        conn.close();
    }
    
    /**
     * Verify that the non-DBA does not have the power to reload the security policy file.
     */
    private void nonDbaTest()
        throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        String          reservedToDBO = "42504";
        Connection  conn = openUserConnection( NON_DBO_USER );

        assertTrue( "Initially, should be able to read property.", canReadProperty() );

        // Now prove that the non-DBO can't reload the policy file.
        changePolicyFile( conn, RELOADABLE_MODIFIED_SOURCE_POLICY, false, reservedToDBO );
        assertTrue( "Policy file not changed. Should still be able to read the property.", canReadProperty() );

        // Return to initial policy file.
        changePolicyFile( conn, RELOADABLE_INITIAL_SOURCE_POLICY, false, reservedToDBO );
        assertTrue( "Reverted to initial policy. Should still be able to read the property again.", canReadProperty() );

        conn.close();
    }
    
    /////////////////////////////////////////////
    //
    // getPolicy() IS NOT GRANTED
    //
    /////////////////////////////////////////////
    
    /**
     * Verify that even the DBA can't reload the policy file if getPolicy() has
     * not been granted.
     */
    private void doPolicyReloadingIsNotGranted()
        throws Exception
    {
        String          insufficientPrivilege = "XK000";
        Connection  conn = openUserConnection( DerbyConstants.TEST_DBO );
//IC see: https://issues.apache.org/jira/browse/DERBY-2419

        // First change to a policy which does not permit policy reloading
        changePolicyFile( conn, UNRELOADABLE_SOURCE_POLICY, true, null );

        // Verify that we get an exception when we try to reload the policy file.
        changePolicyFile( conn, RELOADABLE_INITIAL_SOURCE_POLICY, false, insufficientPrivilege );

        conn.close();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Object OVERLOADS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        StringBuffer    buffer = new StringBuffer();

        buffer.append( "SecurityPolicyReloadingTest( " );
        buffer.append( " )" );

        return buffer.toString();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////
     
    /**
     * Return true if we have sufficient privilege to read a special property.
     */
    private boolean canReadProperty()
        throws Exception
    {
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            getSystemProperty("SecurityPolicyReloadingTest.property");
            return true;
        }
        catch (AccessControlException ace) { return false; }
    }

    /**
     * Try to change the policy file.
     */
    private void changePolicyFile( Connection conn, String newPolicyFileName, boolean shouldSucceed, String expectedSQLState )
        throws Exception
    {
        boolean     reloaded = true;
        
        writePolicyFile( newPolicyFileName );

        CallableStatement   cs = conn.prepareCall( "call SYSCS_UTIL.SYSCS_RELOAD_SECURITY_POLICY()" );

        try {
            cs.execute();
        }
        catch (SQLException se)
        {
            reloaded = false;

            assertSQLState( expectedSQLState, se );
        }
    
        assertEquals( shouldSucceed, reloaded );
    }

    /**
     * Write a new policy file.
     */
    private void writePolicyFile( String newPolicyFileName )
        throws Exception
    {
        SupportFilesSetup.privCopyFiles
             (
              SupportFilesSetup.EXTINOUT,
              new String[] { newPolicyFileName },
              new String[] { makeTargetPolicyStub() }
             );
   }

    /**
     * Construct the name of the server policy file.
     */
    private static String makeServerPolicyName()
    {
        try {
            String  userDir = getSystemProperty( "user.dir" );
            String  fileName = userDir + File.separator + SupportFilesSetup.EXTINOUT + File.separator + makeTargetPolicyStub();
            File      file = new File( fileName );
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
            return file.toURI().toURL().toExternalForm();
        }
        catch (Exception e)
        {
            System.out.println( "Unexpected exception caught by makeServerPolicyName(): " + e );

            return null;
        }
    }

    /**
     * Get the stub name (no directory spec) for the server policy file we create.
     */
    private static String makeTargetPolicyStub()
    {
        return TARGET_POLICY;
   }

    /**
     * Get the source file which has the correct permissions.
     */
    private static String getSourcePolicy()
    {
        return RELOADABLE_INITIAL_SOURCE_POLICY;
    }
    
}
