/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.Driver40Test

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
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Tests of <code>javax.sql.Driver</code> for JDBC40 and up.
 */

public class Driver40Test extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public Driver40Test(String name) { super( name ); }

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
            return new TestSuite(
                "DriverTest40 tests java.sql.Driver, not supported with JSR169");
        }
        return TestConfiguration.defaultSuite(Driver40Test.class);
    }
   
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * <p>
     * Test new method added by JDBC 4.1.
     * </p>
     */
    public void test_jdbc4_1() throws Exception
    {
        Driver  driver = DriverManager.getDriver( getTestConfiguration().getJDBCClient().getUrlBase() );
        println( "Testing a " + driver.getClass().getName() );

        Wrapper41Driver wrapper = new Wrapper41Driver( driver );

        try {
            wrapper.getParentLogger();
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }
    }

}
