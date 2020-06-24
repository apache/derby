/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.Driver40UnbootedTest

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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SpawnedProcess;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Test that getParentLogger() returns the correct kind of exception when
 * the engine is not booted.
 */

public class Driver40UnbootedTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  SUCCESS = "Success";

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

    public Driver40UnbootedTest(String name) { super( name ); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite()
    {
        if (JDBC.vmSupportsJSR169())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite(
                "DriverTest tests java.sql.Driver, not supported with JSR169");
        }
        
        Test test = TestConfiguration.embeddedSuite(Driver40UnbootedTest.class);

        return SecurityManagerSetup.noSecurityManager( test );
    }
   
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ENTRY POINT
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This entry point is used to run a separate java process in order to verify
     * that the correct exception is being raised by getParentLogger() when the
     * engine hasn't been booted yet.
     * </p>
     */
    public  static  void    main( String[] args )  throws Exception
    {
        Driver  embeddedDriver = DriverManager.getDriver( "jdbc:derby:" );
        Wrapper41Driver embeddedWrapper = new Wrapper41Driver( embeddedDriver );

        String  statusMessage = SUCCESS;
        
        try {
            embeddedWrapper.getParentLogger();
            statusMessage = "getParentLogger() unexpectedly succeeded";
        }
        catch (Exception se)
        {
            if ( !( se instanceof SQLFeatureNotSupportedException ) )
            {
                statusMessage = "Exception was not a SQLFeatureNotSupportedException. It was a " + se.getClass().getName();
            }
        }

        System.out.print( statusMessage );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * <p>
     * Test that getParentLogger() raises the right exception even if the engine
     * isn't booted.
     * </p>
     */
    public void test_notBooted() throws Exception
    {
        if ( !getTestConfiguration().loadingFromJars() ) { return ; }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        String[] command = {
            "-Demma.verbosity.level=silent",
            getClass().getName()
        };

        Process process = execJavaCmd(command);
        
        SpawnedProcess spawned = new SpawnedProcess( process, "UnbootedTest" );
        
        // Ensure it completes without failures.
        assertEquals(0, spawned.complete());
//IC see: https://issues.apache.org/jira/browse/DERBY-5617

        assertEquals( SUCCESS, spawned.getFullServerOutput() );
    }

}
