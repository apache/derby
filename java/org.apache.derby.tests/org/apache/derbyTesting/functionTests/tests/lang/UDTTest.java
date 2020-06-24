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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import junit.framework.Test;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

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
    public static final String VIEW_DEPENDS_ON_TYPE = "X0Y23";
    public static final String TRIGGER_DEPENDS_ON_TYPE = "X0Y24";

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
        return TestConfiguration.defaultSuite(UDTTest.class);
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
        //
        // DECIMAL datatype used here and the JSR169 support for it is less complete.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-4454
        if ( JDBC.vmSupportsJSR169() ) { return; }
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
             "select getCurrencyCode( totalPrice ), getTimeInstant( totalPrice ) from orders",
             new String[][]
             {
                 { "USD" ,         "2009-10-16 14:24:43.0" },
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

    /**
     * <p>
     * Adding and dropping udt columns.
     * </p>
     */
    public void test_03_addDropColumn() throws Exception
    {
        Connection conn = getConnection();
        String tableName1 = "UDTCOLUMNS";
        String tableName2 = "UDTCOLUMNS2";

        goodStatement
            ( conn,
              "create type price_03 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );

        // even though there are 2 price_03 columns, we only create 1 dependency
        goodStatement
            ( conn,
              "create table " + tableName1 + "\n" +
              "(\n" +
              "    a int, b int,\n" +
              "    price1 price_03,\n" +
              "    price2 price_03\n" +
              ")\n"
              );
        assertEquals( 1, countTableDependencies( conn, tableName1 ) );

        // verify that we can't drop the type while the table depends on it
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03 restrict\n" );

        // add another price_03 column. should not add another dependency
        goodStatement
            ( conn,
              "alter table udtColumns add column price3 price_03\n" );
        assertEquals( 1, countTableDependencies( conn, tableName1 ) );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03 restrict\n" );

        // drop one of the price_03 column. there should still be a dependency
        goodStatement
            ( conn,
              "alter table udtColumns drop column price3\n" );
        assertEquals( 1, countTableDependencies( conn, tableName1 ) );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03 restrict\n" );

        // drop another column. same story.
        goodStatement
            ( conn,
              "alter table udtColumns drop column price2\n" );
        assertEquals( 1, countTableDependencies( conn, tableName1 ) );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03 restrict\n" );

        // drop the last udt column. dependency should disappear
        goodStatement
            ( conn,
              "alter table udtColumns drop column price1\n" );
        assertEquals( 0, countTableDependencies( conn, tableName1 ) );
        goodStatement
            ( conn,
              "drop type Price_03 restrict\n" );

        // similar experiments with more types
        goodStatement
            ( conn,
              "create type price_03_a external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create type price_03_b external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create type price_03_c external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );

        goodStatement
            ( conn,
              "create table udtColumns2\n" +
              "(\n" +
              "    a int, b int,\n" +
              "    price1 price_03_a,\n" +
              "    price2 price_03_b\n" +
              ")\n"
              );
        assertEquals( 2, countTableDependencies( conn, tableName2 ) );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_a restrict\n" );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_b restrict\n" );
        
        goodStatement
            ( conn,
              "alter table udtColumns2 add column price3 price_03_c\n" );
        assertEquals( 3, countTableDependencies( conn, tableName2 ) );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_a restrict\n" );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_b restrict\n" );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_c restrict\n" );

        goodStatement
            ( conn,
              "alter table udtColumns2 drop column b\n" );
        assertEquals( 3, countTableDependencies( conn, tableName2 ) );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_a restrict\n" );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_b restrict\n" );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type Price_03_c restrict\n" );

        goodStatement
            ( conn,
              "alter table udtColumns2 drop column price3\n" );
        assertEquals( 2, countTableDependencies( conn, tableName2 ) );
        goodStatement
            ( conn,
              "drop type Price_03_c restrict\n" );

        goodStatement
            ( conn,
              "alter table udtColumns2 drop column price2\n" );
        assertEquals( 1, countTableDependencies( conn, tableName2 ) );
        goodStatement
            ( conn,
              "drop type Price_03_b restrict\n" );

        goodStatement
            ( conn,
              "alter table udtColumns2 drop column price1\n" );
        assertEquals( 0, countTableDependencies( conn, tableName2 ) );
        goodStatement
            ( conn,
              "drop type Price_03_a restrict\n" );
    }

    /**
     * <p>
     * Dropping a whole table which has udt columns.
     * </p>
     */
    public void test_04_dropTable() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            ( conn,
              "create type price_orphan external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create type price_orphan2 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create type price_orphan3 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create type price_orphan4 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create table t_orphan( a price_orphan )\n" );
        goodStatement
            ( conn,
              "create table t_orphan2( a price_orphan2, b int, c price_orphan2 )\n" );
        goodStatement
            ( conn,
              "create table t_orphan3( a price_orphan3, b int, c price_orphan4 )\n" );
        
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type price_orphan restrict\n" );
        goodStatement
            ( conn,
              "drop table t_orphan\n" );
        goodStatement
            ( conn,
              "drop type price_orphan restrict\n" );
        
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type price_orphan2 restrict\n" );
        goodStatement
            ( conn,
              "drop table t_orphan2\n" );
        goodStatement
            ( conn,
              "drop type price_orphan2 restrict\n" );
        
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type price_orphan3 restrict\n" );
        expectExecutionError( conn, TABLE_DEPENDS_ON_TYPE, "drop type price_orphan4 restrict\n" );
        goodStatement
            ( conn,
              "drop table t_orphan3\n" );
        goodStatement
            ( conn,
              "drop type price_orphan3 restrict\n" );
        goodStatement
            ( conn,
              "drop type price_orphan4 restrict\n" );
    }
    
    /**
     * <p>
     * Dependencies of views on UDTs.
     * </p>
     */
    public void test_05_viewDependencies() throws Exception
    {
        Connection conn = getConnection();

        String createTypeStatement;
        String dropTypeStatement;
        String createObjectStatement;
        String dropObjectStatement;
        String badDropSQLState;
        
        // view with UDT in select list
        createTypeStatement = "create type price_05_a external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_05_a restrict\n";
        createObjectStatement = 
             "create view udtView( a, b, c ) as\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4469
             "select tabletype, cast (null as price_05_a), cast( null as price_05_a)\n" +
            "from sys.systables\n";
        dropObjectStatement = "drop view udtView\n";
        badDropSQLState = VIEW_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

        // view with UDT in where clause
        createTypeStatement = "create type price_05_b external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_05_b restrict\n";
        createObjectStatement = 
             "create view udtView_b( a ) as\n" +
            "select tabletype from sys.systables where ( cast (null as price_05_b) ) is not null\n";
        dropObjectStatement = "drop view udtView_b\n";
        badDropSQLState = VIEW_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

    }

    /**
     * <p>
     * Casting to UDTs.
     * </p>
     */
    public void test_06_casts() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4469

        // cast a NULL as a UDT
        goodStatement
            ( conn,
              "create type price_06_b external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        assertResults
            (
             conn,
             "values ( cast ( null as price_06_b ) )\n",
             new String[][]
             {
                 { null },
             },
             false
             );

        // casting an untyped parameter to a UDT
        PreparedStatement ps = chattyPrepare
            ( conn, "values ( cast ( ? as price_06_b ) )" );
        ps.setObject( 1, Price.makePrice() );
        ResultSet rs = ps.executeQuery();
        rs.next();
        Price result = (Price) rs.getObject( 1 );
        rs.close();
        ps.close();
        assertTrue( Price.makePrice().equals( result ) );
    }

    /**
     * <p>
     * Dependencies of routines on UDTs.
     * </p>
     */
    public void test_07_routineDependencies() throws Exception
    {
        Connection conn = getConnection();

        String createTypeStatement;
        String dropTypeStatement;
        String createObjectStatement;
        String dropObjectStatement;
        String badDropSQLState;
        
        // function that returns a udt
        createTypeStatement = "create type price_07_a external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_07_a restrict\n";
        createObjectStatement = 
            "create function makePrice_07_a( )\n" +
            "returns price_07_a\n" +
            "language java\n" +
            "parameter style java\n" +
            "no sql\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice'\n";
        dropObjectStatement = "drop function makePrice_07_a\n";
        badDropSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

        // function with a udt arg
        createTypeStatement = "create type price_07_b external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_07_b restrict\n";
        createObjectStatement = 
            "create function getCurrencyCode_07_b(  priceArg1 price_07_b  )\n" +
            "returns char( 3 )\n" +
            "language java\n" +
            "parameter style java\n" +
            "no sql\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.getCurrencyCode'\n";
        dropObjectStatement = "drop function getCurrencyCode_07_b\n";
        badDropSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

        // procedure with a udt arg
        createTypeStatement = "create type price_07_c external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_07_c restrict\n";
        createObjectStatement = 
            "create procedure oneArgPriceProc_07( price1 price_07_c )\n" +
            "language java\n" +
            "parameter style java\n" +
            "no sql\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.oneArgPriceProc_07'\n";
        dropObjectStatement = "drop procedure oneArgPriceProc_07\n";
        badDropSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

        // procedure with two udt args
        createTypeStatement = "create type price_07_d external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_07_d restrict\n";
        createObjectStatement = 
            "create procedure twoArgPriceProc_07( price1 price_07_d, price2 price_07_d )\n" +
            "language java\n" +
            "parameter style java\n" +
            "no sql\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.twoArgPriceProc_07'\n";
        dropObjectStatement = "drop procedure twoArgPriceProc_07\n";
        badDropSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

        // table function with a udt column
        createTypeStatement = "create type hashmap_07 external name 'java.util.HashMap' language java\n";
        dropTypeStatement = "drop type hashmap_07 restrict\n";
        createObjectStatement = 
            "create function hashmapReader_07() returns table\n" +
            "(\n" +
            "    a int,\n" +
            "    b hashmap_07\n" +
            ")\n" +
            "language java parameter style derby_jdbc_result_set\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.hashmapReader'\n";
        dropObjectStatement = "drop function hashmapReader_07\n";
        badDropSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

        // table function with a udt column and udt arg
        createTypeStatement = "create type hashmap_07_a external name 'java.util.HashMap' language java\n";
        dropTypeStatement = "drop type hashmap_07_a restrict\n";
        createObjectStatement = 
            "create function hashmapReader_07_a( a hashmap_07_a ) returns table\n" +
            "(\n" +
            "    a int,\n" +
            "    b hashmap_07_a\n" +
            ")\n" +
            "language java parameter style derby_jdbc_result_set\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.hashmapReader'\n";
        dropObjectStatement = "drop function hashmapReader_07_a\n";
        badDropSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

    }

    /**
     * <p>
     * Dependencies of triggers on UDTs.
     * </p>
     */
    public void test_08_triggerDependencies() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "create table t_08_a( a int )" );
        goodStatement( conn, "create table t_08_b( a int )" );

        String createTypeStatement;
        String dropTypeStatement;
        String createObjectStatement;
        String dropObjectStatement;
        String badDropSQLState;
        
        // trigger that mentions a udt
        createTypeStatement = "create type price_08_a external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n";
        dropTypeStatement = "drop type price_08_a restrict\n";
        createObjectStatement = 
            "create trigger trig_08_a after insert on t_08_a\n" +
            "  insert into t_08_b( a ) select ( a ) from t_08_a where ( cast( null as price_08_a ) ) is not null\n";
        dropObjectStatement = "drop trigger trig_08_a";
        badDropSQLState = TRIGGER_DEPENDS_ON_TYPE;
        verifyDropRestrictions
            (
             conn,
             createTypeStatement,
             dropTypeStatement,
             createObjectStatement,
             dropObjectStatement,
             badDropSQLState
             );

    }
    
    /**
     * <p>
     * Check result set metadata for UDT columns.
     * </p>
     */
    public void test_09_resultSetMetaData() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "create type price_09_a external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement( conn, "create table t_09_a( a price_09_a )\n" );

        // ANSI UDT
        checkRSMD
            (
             conn,
             "select a from t_09_a\n",
             "org.apache.derbyTesting.functionTests.tests.lang.Price",
             15,
             java.sql.Types.JAVA_OBJECT,
             "\"APP\".\"PRICE_09_A\"",
             0,
             0
             );

        // old-style objects in Derby system tables do not have
        // schema-qualified type names
        checkRSMD
            (
             conn,
             "select aliasinfo from sys.sysaliases\n",
             "org.apache.derby.catalog.AliasInfo",
             15,
             java.sql.Types.JAVA_OBJECT,
             "org.apache.derby.catalog.AliasInfo",
             0,
             0
             );
    }

    /**
     * <p>
     * Check parameter metadata for UDT parameters.
     * </p>
     */
    public void test_10_parameterMetaData() throws Exception
    {
        //
        // Parameter meta data is not available on JSR-169 platforms,
        // so skip this test in those environments.
        //
        if ( JDBC.vmSupportsJSR169() ) { return; }
        
        Connection conn = getConnection();

        goodStatement( conn, "create type price_10_a external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement( conn, "create table t_10_a( a price_10_a )\n" );

        // ANSI UDT
        checkPMD
            (
             conn,
             "insert into t_10_a( a ) values ( ? )\n",
             "org.apache.derbyTesting.functionTests.tests.lang.Price",
             java.sql.Types.JAVA_OBJECT,
             "\"APP\".\"PRICE_10_A\"",
             0,
             0
             );

        //
        // I don't know of any way to create a statement with a parameter
        // whose type is an old-style object from Derby's system tables.
        // If you figure out how to trick Derby into letting you do that,
        // this would be a good place to assert the shape of the parameter
        // meta data for that statement.
        //
    }

    /**
     * <p>
     * Verify that table functions can have UDT columns.
     * </p>
     */
    public void test_11_tableFunctionColumns() throws Exception
    {
        //
        // This test uses DriverManager.
        //
        if ( JDBC.vmSupportsJSR169() ) { return; }
//IC see: https://issues.apache.org/jira/browse/DERBY-4545

        Connection conn = getConnection();

        goodStatement( conn, "create type hashmap_11 external name 'java.util.HashMap' language java\n" );
        goodStatement
            (
             conn,
             "create function makeHashMap_11() returns hashmap_11\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             conn,
             "create function putValue_11( map hashmap_11, k varchar( 100 ), v varchar( 100 ) ) returns hashmap_11\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.putValue'\n"
             );
        goodStatement( conn, "create table t_11( intCol int, varcharCol varchar( 10 ), hashmapCol hashmap_11 )\n" );
        goodStatement
            (
             conn,
             "create function hashmapReader() returns table\n" +
             "(\n" +
             "    a int,\n" +
             "    b hashmap_11\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.hashmapReader'\n"
             );
        goodStatement( conn, "insert into t_11( intCol, varcharCol, hashmapCol ) values ( 2, 'def', putValue_11( makeHashMap_11(), 'kangaroo', 'foo' ) )\n" );

        assertResults
            (
             conn,
             "select * from table( hashmapReader() ) s",
             new String[][]
             {
                 { "2" ,         "{kangaroo=foo}" },
             },
             true
             );
    }

    /**
     * <p>
     * Verify that you can store large objects in UDT columns.
     * </p>
     */
    public void test_12_largeUDTs() throws Exception
    {
        Connection conn = getConnection();

        //
        // Store and retrieve a UDT which is more than 90K bytes long
        //
        goodStatement( conn, "create type IntArray external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray' language java\n" );
        goodStatement
            ( conn,
              "create function makeIntArray( arrayLength int ) returns IntArray\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray.makeIntArray'\n" );
        goodStatement
            ( conn,
              "create function setCell( array IntArray, cellNumber int, cellValue int ) returns IntArray\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray.setCell'\n" );
        goodStatement
            ( conn,
              "create function getCell( array IntArray, cellNumber int ) returns int\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray.getCell'\n" );
        goodStatement( conn, "create table t_12( id int generated always as identity, data IntArray )\n" );
        goodStatement( conn, "insert into t_12( data ) values ( setCell( makeIntArray( 3 ), 1, 5 ) )\n" );
        goodStatement( conn, "insert into t_12( data ) values ( setCell( makeIntArray( 100000 ), 90000, 3 ) )\n" );

        assertResults
            (
             conn,
             "select getCell( data, 1 ), getCell( data, 2 ) from t_12 where id = 1",
             new String[][]
             {
                 { "5" ,         "0" },
             },
             true
             );

        assertResults
            (
             conn,
             "select getCell( data, 1 ), getCell( data, 90000 ) from t_12 where id = 2",
             new String[][]
             {
                 { "0" ,         "3" },
             },
             true
             );
        
        //
        // Store and retrieve a UDT which is more than 1000K bytes long
        //
        goodStatement( conn, "create type FakeByteArray external name 'org.apache.derbyTesting.functionTests.tests.lang.FakeByteArray' language java\n" );
        goodStatement
            ( conn,
              "create function makeFakeByteArray( l int, f int ) returns FakeByteArray\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.FakeByteArray.makeFakeByteArray'\n" );
        goodStatement
            ( conn,
              "create function toString( arg FakeByteArray ) returns varchar( 30 )\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.FakeByteArray.toString'\n" );
        goodStatement( conn, "create table t_12_a( id int generated always as identity, data FakeByteArray )\n" );
        goodStatement( conn, "insert into t_12_a( data ) values ( makeFakeByteArray( 3, 33 ) )\n" );
        goodStatement( conn, "insert into t_12_a( data ) values ( makeFakeByteArray( 1000000, 44 ) )\n" );

        assertResults
            (
             conn,
             "select id, toString( data ) from t_12_a order by id",
             new String[][]
             {
                 { "1" ,         "[ 3, 33 ]" },
                 { "2" ,         "[ 1000000, 44 ]" },
             },
             true
             );
    }
    
    /**
     * <p>
     * Verify that implementing the SQLData interface does not make an object storeable.
     * </p>
     */
    public void test_13_sqlData() throws Exception
    {
        //
        // SQLData not defined in JSR 169.
        //
        if ( JDBC.vmSupportsJSR169() ) { return; }
//IC see: https://issues.apache.org/jira/browse/DERBY-4545

        Connection conn = getConnection();

        goodStatement( conn, "create type SampleSQLData external name 'org.apache.derbyTesting.functionTests.tests.lang.SampleSQLData' language java\n" );
        goodStatement
            ( conn,
              "create function makeSampleSQLData( l int ) returns SampleSQLData\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.SampleSQLData.makeSampleSQLData'\n" );
        goodStatement( conn, "create table t_13_a( id int generated always as identity, data SampleSQLData )\n" );

        expectExecutionError( conn, JAVA_EXCEPTION, "insert into t_13_a( data ) values ( makeSampleSQLData( 3 ) )\n" );
    }
    
    /**
     * <p>
     * Verify that you can't bind UDTs to the classes which back the system types.
     * </p>
     */
    public void test_14_systemClasses() throws Exception
    {
        Connection conn = getConnection();

        //
        // Before checking types, make sure that all types we understand are accounted for.
        // If a new system type is added, then we need to add it to the following block
        // of compilation errors.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        assertEquals( 21, vetDatatypeCount( conn ) );
        
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'byte[]' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.lang.Boolean' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.lang.Integer' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.lang.Long' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.lang.Float' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.lang.Double' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.lang.String' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.math.BigDecimal' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.sql.Blob' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.sql.Clob' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.sql.Date' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.sql.Ref' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.sql.Time' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'java.sql.Timestamp' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'org.apache.derby.iapi.types.XML' language java\n" );
        expectCompilationError( ILLEGAL_UDT_CLASS, "create type java_string external name 'org.apache.derby.Foo' language java\n" );
    }
    private int vetDatatypeCount( Connection conn ) throws Exception
    {
        ResultSet rs = conn.getMetaData().getTypeInfo();
        int expectedTypeCount = 0;
        while ( rs.next() ) { expectedTypeCount++; }
        rs.close();

        expectedTypeCount--; // eliminate JAVA_OBJECT

        int actualTypeCount = org.apache.derby.iapi.types.TypeId.getAllBuiltinTypeIds().length;
        actualTypeCount--;  // eliminate TINYINT
        actualTypeCount--;  // eliminate REF
        actualTypeCount++;  // add FLOAT (synonym of REAL)

        //
        // Make sure that all types have been added to TypeId.getAllBuiltinTypeIds().
        //
        assertEquals( expectedTypeCount, actualTypeCount );

        return actualTypeCount;
    }
    

    /**
     * <p>
     * Verify that UDTs have no ordering.
     * </p>
     */
    public void test_15_ordering() throws Exception
    {
        Connection conn = getConnection();

        // Create a Comparable type. We can't take advantage of that interface yet.
        goodStatement( conn, "create type IntArray_15 external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray' language java\n" );
        goodStatement
            ( conn,
              "create function makeIntArray_15( arrayLength int ) returns IntArray_15\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray.makeIntArray'\n" );
        goodStatement( conn, "create table t_15( a IntArray_15 )\n" );
        goodStatement( conn, "insert into t_15( a ) values ( makeIntArray_15( 3 ) )\n" );
        goodStatement( conn, "insert into t_15( a ) values ( makeIntArray_15( 4 ) )\n" );

        expectCompilationError( FORBIDDEN_ORDERING_OPERATION, "create index t_15_idx on t_15( a )\n" );
        expectCompilationError( FORBIDDEN_ORDERING_OPERATION, "select * from t_15 order by a\n" );
        expectCompilationError( FORBIDDEN_ORDERING_OPERATION, "select * from t_15 group by a\n" );
        expectCompilationError( FORBIDDEN_ORDERING_OPERATION, "select distinct a from t_15\n" );
        expectCompilationError( ILLEGAL_AGG, "select max( a ) from t_15\n" );
        expectCompilationError( ILLEGAL_AGG, "select min( a ) from t_15\n" );
        expectCompilationError( ILLEGAL_AGG, "select avg( a ) from t_15\n" );
        expectCompilationError( FORBIDDEN_ORDERING_OPERATION, "select * from t_15 union select * from t_15\n" );
//IC see: https://issues.apache.org/jira/browse/DERBY-4545
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 where a = makeIntArray_15( 3 )\n" );
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 where a between makeIntArray_15( 2 ) and makeIntArray_15( 4 )\n" );
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 l, t_15 r where l.a = r.a\n" );
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 l, t_15 r where l.a < r.a\n" );
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 l, t_15 r where l.a > r.a\n" );
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 l, t_15 r where l.a <= r.a\n" );
        expectCompilationError( ILLEGAL_COMPARISON, "select * from t_15 l, t_15 r where l.a >= r.a\n" );
        expectCompilationError( FORBIDDEN_ORDERING_OPERATION, "select count( distinct a ) from t_15\n" );

        // but these don't involve any comparisons
        goodStatement( conn, "select count(*) from t_15\n" );
        goodStatement( conn, "select all * from t_15\n" );
    }

    /**
     * <p>
     * Verify implicit and explicit casts.
     * </p>
     */
    public void test_16_casts() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4469

        goodStatement( conn, "create type javaSerializable external name 'java.io.Serializable' language java\n" );
        goodStatement( conn, "create type javaNumber external name 'java.lang.Number' language java\n" );
        goodStatement( conn, "create type javaDate external name 'java.util.Date' language java\n" );
        goodStatement
            ( conn,
              "create function makeNumber( arg int ) returns javaNumber\n" +
              "language java parameter style java no sql external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeNumber'\n" );
        goodStatement( conn, "create table t_16( a int generated always as identity, b javaNumber )\n" );
        goodStatement( conn, "create table t_16_1( a int generated always as identity, b javaDate )\n" );
        goodStatement( conn, "create table t_16_2( a int generated always as identity, b javaSerializable )\n" );
        goodStatement( conn, "insert into t_16( b ) values ( makeNumber( 1 ) )\n" );
        goodStatement( conn, "insert into t_16( b ) select b from t_16\n" );
        
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16( b ) values ( 1 )\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16( b ) values ( 1.0 )\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16( b ) values ( '1' )\n" );
        
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_1( b ) values ( date('1994-02-23') )\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_1( b ) values ( time('15:09:02') )\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_1( b ) values ( timestamp('1960-01-01 23:03:20') )\n" );

        // subtypes not recognized yet
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_2( b ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_2( b ) values( cast (null as javaNumber) )\n" );

        // casts to other udts not allowed
        expectCompilationError( BAD_CAST, "select cast (b as javaDate) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as javaSerializable) from t_16\n" );

        //
        // If this fails, it means that we need to add another system type to the
        // cast checks below.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        assertEquals( 21, vetDatatypeCount( conn ) );
        
        // casts to system types not allowed
        expectCompilationError( BAD_CAST, "select cast (b as boolean) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as bigint) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as blob) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as char( 1 ) ) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as char( 1 ) for bit data) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as clob) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as date) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as decimal) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as double) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as float) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as int) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as long varchar) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as long varchar for bit data) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as numeric) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as real) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as smallint) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as time) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as timestamp) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as varchar(10)) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as varchar(10) for bit data) from t_16\n" );
        expectCompilationError( BAD_CAST, "select cast (b as xml) from t_16\n" );


        //
        // If this fails, it means that we need to add another system type to the
        // t_16_all_types table and add a corresponding cast check below.
        //
        assertEquals( 21, vetDatatypeCount( conn ) );
        
        goodStatement
            (
             conn,
             "create table t_16_all_types\n" +
             "(\n" +
             "    a01 bigint,\n" +
             "    a02 blob,\n" +
             "    a03 char( 1 ),\n" +
             "    a04 char( 1 ) for bit data ,\n" +
             "    a05 clob,\n" +
             "    a06 date,\n" +
             "    a07 decimal,\n" +
             "    a08 double,\n" +
             "    a09 float,\n" +
             "    a10 int,\n" +
             "    a11 long varchar,\n" +
             "    a12 long varchar for bit data,\n" +
             "    a13 numeric,\n" +
             "    a14 real,\n" +
             "    a15 smallint,\n" +
             "    a16 time,\n" +
             "    a17 timestamp,\n" +
             "    a18 varchar(10),\n" +
             "    a19 varchar(10) for bit data,\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
             "    a20 xml,\n" +
             "    a21 boolean\n" +
             ")"
             );

        expectCompilationError( BAD_CAST, "select cast( a01 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a02 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a03 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a04 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a05 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a06 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a07 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a08 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a09 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a10 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a11 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a12 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a13 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a14 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a15 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a16 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a17 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a18 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a19 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a20 as javaSerializable ) from t_16_all_types\n" );
        expectCompilationError( BAD_CAST, "select cast( a21 as javaSerializable ) from t_16_all_types\n" );

//IC see: https://issues.apache.org/jira/browse/DERBY-4730

        //
        // If this fails, it means that we need to add another system type to the
        // implicit casts which follow.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        assertEquals( 21, vetDatatypeCount( conn ) );
        
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a01 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a02 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a03 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a04 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a05 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a06 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a07 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a08 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a09 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a10 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a11 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a12 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a13 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a14 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a15 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a16 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a17 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a18 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a19 ) select b from t_16\n" );
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a20 ) select b from t_16\n" );
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        expectCompilationError( ILLEGAL_STORAGE, "insert into t_16_all_types( a21 ) select b from t_16\n" );
        
        // test cast from the half-supported boolean type
        expectCompilationError( BAD_CAST, "select cast (isindex as javaNumber) from sys.sysconglomerates\n" );

        // good cast to self
        assertResults
            (
             conn,
             "select cast (b as javaNumber) from t_16",
             new String[][]
             {
                 { "1" },
                 { "1" },
             },
             false
             );
    }

    /**
     * <p>
     * Verify that you can use UDTs as output parameters in database procedures.
     * </p>
     */
    public void test_17_outputParameters() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4499

        goodStatement( conn, "create type intArray_17 external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray' language java\n" );
        goodStatement
            ( conn,
              "create procedure changeIntArray_17\n" +
              "( in newSize int, inout oldIntArray intArray_17 )\n" +
              "language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.changeIntArray'\n" );
        
        CallableStatement cs = chattyPrepareCall( conn, "call changeIntArray_17( ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.JAVA_OBJECT );
        cs.setInt( 1, 2 );
        cs.setObject( 2,  new IntArray( new int[ 5 ] ) );
        cs.execute();
        Object obj = cs.getObject( 2 );
        cs.close();

        assertEquals( "[ 0, 0 ]", obj.toString() );
    }

    /**
     * Verify that you can cast a value to an UDT in a generation clause or
     * a CHECK constraint. Regression test case for DERBY-6421.
     */
    public void test_18_derby6421() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create type d6421_type external name 'java.util.ArrayList' "
                + "language java");
        s.execute("create table d6421_table "
                + "(x generated always as (cast(null as d6421_type)), "
                + "check (cast(null as d6421_type) is null))");

        // This insert used to cause assert failure (in sane builds) or
        // NullPointerException (in insane builds).
        s.execute("insert into d6421_table values default");
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PROCEDURES AND FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static void oneArgPriceProc( Price price1 ) {}
    public static void twoArgPriceProc( Price price1, Price price2 ) {}

    public static void changeIntArray( int newSize, IntArray[] array )
    {
        IntArray newArray = new IntArray( new int[ newSize ] );
//IC see: https://issues.apache.org/jira/browse/DERBY-4499

        array[ 0 ] = newArray;
    }

    public static HashMap makeHashMap() { return new HashMap(); }

    public static HashMap makeHashMap( String key, Integer value )
    {
        HashMap<String,Integer> map = new HashMap<String,Integer>();
        map.put( key, value );
        return map;
    }

    public static HashMap putValue(
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            HashMap<String, String> map, String key, String value)
    {
        map.put( key, value );
//IC see: https://issues.apache.org/jira/browse/DERBY-4484

        return map;
    }

    public static ResultSet hashmapReader() throws Exception
    {
        Connection conn = DriverManager.getConnection( "jdbc:default:connection" );

        PreparedStatement ps = conn.prepareStatement( "select intCol, hashmapCol from t_11" );

        return ps.executeQuery();
    }

    public static Number makeNumber( int arg ) { return arg; }

    public  static  Integer getIntValue( HashMap<String,Integer> map, String key )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
        return map.get( key );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Verify that a type can't be dropped if it is used by a schema object.
     */
    private void verifyDropRestrictions
        (
         Connection conn,
         String createTypeStatement,
         String dropTypeStatement,
         String createObjectStatement,
         String dropObjectStatement,
         String badDropSQLState
         )
        throws Exception
    {
        goodStatement( conn, createTypeStatement );
        goodStatement( conn, createObjectStatement );
        expectExecutionError( conn, badDropSQLState, dropTypeStatement );
        goodStatement( conn, dropObjectStatement );
        goodStatement( conn, dropTypeStatement );
    }

    /** Get the number of dependencies that a table has */
    private int countTableDependencies( Connection conn, String tableName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare
            ( conn, "select count(*) from sys.sysdepends d, sys.systables t where d.dependentid = t.tableid and t.tablename = ?" );
        ps.setString( 1, tableName );

        return getScalarInteger( ps );
    }

    /** Get a scalar integer result from a query */
    private int getScalarInteger( PreparedStatement ps ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        rs.next();
        int retval = rs.getInt( 1 );

        rs.close();
        ps.close();

        return retval;
    }

    /**
     * Check the ResultSetMetaData for a query whose first column is a UDT.
     */
    private void checkRSMD
        (
         Connection conn,
         String query,
         String expectedClassName,
         int expectedDisplaySize,
         int expectedJDBCType,
         String expectedSQLTypeName,
         int expectedPrecision,
         int expectedScale
         ) throws Exception
    {
        PreparedStatement ps = conn.prepareStatement( query );
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals( rsmd.getColumnClassName( 1 ), expectedClassName );
        assertEquals( rsmd.getColumnDisplaySize( 1 ), expectedDisplaySize );
        assertEquals( rsmd.getColumnType( 1 ), expectedJDBCType );
        assertEquals( rsmd.getColumnTypeName( 1 ), expectedSQLTypeName );
        assertEquals( rsmd.getPrecision( 1 ), expectedPrecision );
        assertEquals( rsmd.getScale( 1 ), expectedScale );

        rs.close();
        ps.close();
    }
    
    /**
     * Check the ParameterMetaData for a statement whose first parameter is a UDT.
     */
    private void checkPMD
        (
         Connection conn,
         String query,
         String expectedClassName,
         int expectedJDBCType,
         String expectedSQLTypeName,
         int expectedPrecision,
         int expectedScale
         ) throws Exception
    {
        PreparedStatement ps = conn.prepareStatement( query );
        ParameterMetaData pmd = ps.getParameterMetaData();

        assertEquals( pmd.getParameterClassName( 1 ), expectedClassName );
        assertEquals( pmd.getParameterType( 1 ), expectedJDBCType );
        assertEquals( pmd.getParameterTypeName( 1 ), expectedSQLTypeName );
        assertEquals( pmd.getPrecision( 1 ), expectedPrecision );
        assertEquals( pmd.getScale( 1 ), expectedScale );

        ps.close();
    }

}
