/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UDTTest

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * <p>
 * Test user defined types. See DERBY-651.
 * </p>
 */
public class UDTTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static final String OBJECT_EXISTS = "X0Y68";
    public static final String NONEXISTENT_OBJECT = "42Y55";
    public static final String SYNTAX_ERROR = "42X01";

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

    public UDTTest(String name)
    {
        super(name);
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(UDTTest.class);
        Test        result = new CleanDatabaseTestSetup( suite );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Basic syntax.
     * </p>
     */
    public void test_01_basicSyntax() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "create schema adt_schema\n" );

        // create some types
        makeGoodTypes( conn );

        // duplicate type names should raise errors
        expectExecutionError( conn, OBJECT_EXISTS, "create type fooType external name 'mypackage.foo' language java\n" );
        expectExecutionError( conn, OBJECT_EXISTS, "create type adt_schema.fooType external name 'mypackage.foo' language java\n" );
        expectExecutionError( conn, OBJECT_EXISTS, "create type \"smallint\" external name 'mypackage.foo' language java\n" );
        expectExecutionError( conn, OBJECT_EXISTS, "create type \"SMALLINT\" external name 'mypackage.foo' language java\n" );

        // only RESTRICTed drops allowed now
        expectCompilationError( SYNTAX_ERROR, "drop type fooType\n" );

        // drop some types
        goodStatement( conn, "drop type fooType restrict\n" );
        goodStatement( conn, "drop type adt_schema.fooType restrict\n" );
        goodStatement( conn, "drop type \"smallint\" restrict\n" );
        goodStatement( conn, "drop type \"SMALLINT\" restrict\n" );

        // can't drop a non-existent type
        expectCompilationError( NONEXISTENT_OBJECT, "drop type fooType restrict\n" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop type adt_schema.fooType restrict\n" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop type \"smallint\" restrict\n" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop type \"SMALLINT\" restrict\n" );

        // re-create the types
        makeGoodTypes( conn );
    }
    private void makeGoodTypes( Connection conn ) throws Exception
    {
        goodStatement( conn, "create type fooType external name 'mypackage.foo' language java\n" );
        goodStatement( conn, "create type adt_schema.fooType external name 'mypackage.foo' language java\n" );
        goodStatement( conn, "create type \"smallint\" external name 'mypackage.foo' language java\n" );
        goodStatement( conn, "create type \"SMALLINT\" external name 'mypackage.foo' language java\n" );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}
