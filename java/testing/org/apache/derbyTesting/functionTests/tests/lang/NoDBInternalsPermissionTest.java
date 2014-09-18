/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NoDBInternalsPermissionTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.security.AccessControlException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import org.apache.derby.iapi.services.context.ContextService;

/**
 * <p>
 * Test backup and restore of databases with Lucene indexes.
 * </p>
 */
public class NoDBInternalsPermissionTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      POLICY_FILE = "org/apache/derbyTesting/functionTests/tests/lang/no_derby_internals.policy";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public NoDBInternalsPermissionTest( String name )
    {
        super( name );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite( NoDBInternalsPermissionTest.class );

        Test        secureTest = new SecurityManagerSetup( suite, POLICY_FILE );

        return secureTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Verify that user code can't call static entry points in ContextService.
     * </p>
     */
    public  void    test_001_ContextService()
        throws Exception
    {
        try {
            ContextService.stop();
            fail( "Should have raised an AccessControlException" );
        }
        catch (AccessControlException e) { println( "Caught an AccessControlException" ); }
        try {
            ContextService.getFactory();
            fail( "Should have raised an AccessControlException" );
        }
        catch (AccessControlException e) { println( "Caught an AccessControlException" ); }
        try {
            ContextService.getContext( "some context id"  );
            fail( "Should have raised an AccessControlException" );
        }
        catch (AccessControlException e) { println( "Caught an AccessControlException" ); }
        try {
            ContextService.getContextOrNull( "some context id" );
            fail( "Should have raised an AccessControlException" );
        }
        catch (AccessControlException e) { println( "Caught an AccessControlException" ); }
    }

}
