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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

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

    /**
     * <p>
     * Basic column, return value, and parameter support.
     * </p>
     */
    public void test_02_basicColumnRetvalParam() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement( conn, "create table orders( orderID int generated always as identity, customerID int, totalPrice price )\n" );
        goodStatement
            ( conn,
              "create function makePrice( currencyCode char( 3 ), amount decimal( 31, 5 ), timeInstant Timestamp )\n" +
              "returns Price language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice'\n");
        goodStatement
            ( conn,
              "create function getCurrencyCode( price Price ) returns char( 3 ) language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.getCurrencyCode'\n" );
        goodStatement
            ( conn,
              "create function getAmount( price Price ) returns decimal( 31, 5 ) language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.getAmount'\n" );
        goodStatement
            ( conn,
              "create function getTimeInstant( price Price ) returns timestamp language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.getTimeInstant'\n" );
        goodStatement
            ( conn,
              "create procedure savePrice( in a Price ) language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.savePrice'\n" );
        goodStatement
            ( conn,
              "create function getSavedPrice() returns Price language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.getSavedPrice'\n" );
        goodStatement
            ( conn,
              "insert into orders( customerID, totalPrice ) values\n" +
              "( 12345, makePrice( 'USD', cast( 9.99 as decimal( 31, 5 ) ), timestamp('2009-10-16 14:24:43') ) )\n" );

        assertResults
            (
             conn,
             "select getCurrencyCode( totalPrice ), getAmount( totalPrice ), getTimeInstant( totalPrice ) from orders",
             new String[][]
             {
                 { "USD" ,         "9.99000" ,        "2009-10-16 14:24:43.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select totalPrice from orders",
             new String[][]
             {
                 { "Price( USD, 9.99000, 2009-10-16 14:24:43.0 )" },
             },
             false
             );

        goodStatement
            ( conn,
              "call savePrice\n" +
              "( makePrice( 'EUR', cast( 1.23 as decimal( 31, 5 ) ), timestamp('2008-10-16 14:24:43') ) )\n" );
        assertResults
            (
             conn,
             "values( getSavedPrice() )",
             new String[][]
             {
                 { "Price( EUR, 1.23000, 2008-10-16 14:24:43.0 )" },
             },
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}
