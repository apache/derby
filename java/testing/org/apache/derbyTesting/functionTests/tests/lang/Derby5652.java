/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.Derby5652
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

import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class Derby5652 extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  PROVIDER_PROPERTY = "derby.authentication.provider";

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

    public Derby5652( String name ) { super(name); }

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
        TestSuite suite = new TestSuite( "Derby5652" );

        Test    test = new Derby5652( "basicTest" );

        // turn off security manager so that we can change system properties
        test = SecurityManagerSetup.noSecurityManager( test );

        suite.addTest( test );

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Trailing garbage after the credentials db name should produce a useful
     * error message instead of an assertion failure.
     * </p>
     */
    public  void    basicTest()  throws  Exception
    {
        // run the test in another process because it creates a zombie engine
        // which can't be killed. see derby-5757.
        assertLaunchedJUnitTestMethod( getClass().getName() + ".badProperty", null );
    }
    
    /**
     * <p>
     * Trailing garbage after the credentials db name should produce a useful
     * error message.
     * </p>
     */
    public  void    badProperty()  throws  Exception
    {
        // bring down the engine in order to have a clean environment
        getTestConfiguration().shutdownEngine();

        // configure an illegal credentials db name--this one has an illegal trailing colon
        setSystemProperty( PROVIDER_PROPERTY, "NATIVE:db:" );

        // verify that you can't connect with this provider setting
        try {
            openUserConnection( "fooUser" );
        }
        catch (SQLException se)
        {
            // look for a login failure message. the detailed error is printed to
            // derby.log and not percolated out of the Monitor.
            assertSQLState( "08004", se );
        }
    }
    
}
