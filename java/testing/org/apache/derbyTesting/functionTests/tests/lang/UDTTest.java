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
    public static final String NONEXISTENT_OBJECT = "42Y55";
    public static final String SYNTAX_ERROR = "42X01";
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
        //
        // DECIMAL datatype used here and the JSR169 support for it is less complete.
        //
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
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PROCEDURES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static void oneArgPriceProc( Price price1 ) {}
    public static void twoArgPriceProc( Price price1, Price price2 ) {}

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

}
