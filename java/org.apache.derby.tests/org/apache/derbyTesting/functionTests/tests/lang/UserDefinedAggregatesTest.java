/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UserDefinedAggregatesTest

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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import junit.framework.Test;
import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test user defined aggregates. See DERBY-672.
 * </p>
 */
public class UserDefinedAggregatesTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static final String OBJECT_EXISTS = "X0Y68";
    public static final String ILLEGAL_AGGREGATE = "42ZC3";
    public static final String NAME_COLLISION = "X0Y87";
    public static final String MISSING_FUNCTION = "42Y03";
    public static final String MISSING_SCHEMA = "42Y07";
    public static final String BAD_AGGREGATE_USAGE = "42903";
    public static final String BAD_AGG_PLACEMENT = "42Y35";
    public static final String INPUT_MISMATCH = "42Y22";
    public static final String BAD_GEN_COL = "42XA1";
    public static final String INPUT_OUTSIDE_BOUNDS = "42ZC6";
    public static final String RETURN_OUTSIDE_BOUNDS = "42ZC7";
    public static final String XML_TYPE = "42ZB3";
    public static final String INT_TRUNCATION = "22003";
    public static final String CAST_FAILURE = "22018";
    public static final String AGG_IN_GROUP_BY = "42Y26";
    public static final String NESTED_AGGS = "42Y33";
    public static final String UNTYPED_NULL = "42Y83";
    public static final String MISSING_CLASS = "42ZC8";
    public static final String AGG_IN_ON_CLAUSE = "42Z07";
    public static final String BAD_CONSTRAINT = "42Y01";
    public static final String DEPENDENCY_VIOLATION = "X0Y30";

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

    public UserDefinedAggregatesTest(String name)
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
        BaseTestSuite suite = new BaseTestSuite("UserDefinedAggregatesTest");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest( TestConfiguration.defaultSuite(UserDefinedAggregatesTest.class) );
        suite.addTest( collatedSuite( "en" ) );

        return suite;
    }

    /**
     * Return a suite that uses a single use database with
     * a primary fixture from this test plus potentially other
     * fixtures.
     * @param locale Locale to use for the database
     * @return suite of tests to run for the given locale
     */
    private static Test collatedSuite(String locale)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("UserDefinedAggregatesTest:territory=" + locale);

        suite.addTest( TestConfiguration.defaultSuite(UserDefinedAggregatesTest.class) );

        return Decorator.territoryCollatedDatabase( suite, locale );
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

        goodStatement( conn, "create schema uda_schema\n" );
        goodStatement( conn, "create schema uda_schema2\n" );
        goodStatement( conn, "create schema uda_schema3\n" );

        // some good aggregate creations
        goodStatement( conn, "create derby aggregate mode_01 for int external name 'foo.bar.Wibble'" );
        goodStatement( conn, "create derby aggregate uda_schema.mode_012 for int external name 'foo.bar.Wibble'" );

        // can't create an aggregate with an existing name
        expectExecutionError
            ( conn, OBJECT_EXISTS, "create derby aggregate mode_01 for int external name 'foo.bar.Wibble'" );
        expectExecutionError
            ( conn, OBJECT_EXISTS, "create derby aggregate uda_schema.mode_012 for int external name 'foo.bar.Wibble'" );
        
        // only RESTRICTed drops allowed now
        expectCompilationError( SYNTAX_ERROR, "drop derby aggregate mode_01" );

        // successfully drop an aggregate
        goodStatement( conn, "drop derby aggregate mode_01 restrict" );

        // can't create an aggregate with the same name as a 1-arg function
        // but no collision with 2-arg function names
        goodStatement
            ( conn, "create function uda_schema3.agg_collide( a int ) returns int language\n" +
              "java parameter style java external name 'Foo.f'" );
        goodStatement
            ( conn, "create function uda_schema3.agg_nocollide( a int, b int ) returns int language java\n" +
              "parameter style java external name 'Foo.f'" );
        expectExecutionError
            ( conn, NAME_COLLISION, "create derby aggregate uda_schema3.agg_collide for int external name 'foo.bar.Wibble'" );
        goodStatement
            ( conn, "create derby aggregate uda_schema3.agg_nocollide for int external name 'foo.bar.Wibble'" );

        goodStatement
            ( conn, "create function agg_collide( a int ) returns int language java parameter style java external name 'Foo.f'" );
        goodStatement
            ( conn, "create function agg_nocollide( a int, b int ) returns int language java parameter style java external name 'Foo.f'" );
        expectExecutionError( conn, NAME_COLLISION, "create derby aggregate agg_collide for int external name 'foo.bar.Wibble'" );
        goodStatement( conn, "create derby aggregate agg_nocollide for int external name 'foo.bar.Wibble'" );

        // can't create a 1-arg function with same name as an aggregate
        goodStatement
            ( conn, "create derby aggregate func_collide for int external name 'foo.bar.Wibble'" );
        goodStatement
            ( conn, "create derby aggregate func_nocollide for int external name 'foo.bar.Wibble'" );
        expectExecutionError
            ( conn, NAME_COLLISION,
              "create function func_collide( a int ) returns int language java parameter style java external name 'Foo.f'" );
        goodStatement
            ( conn, "create function func_nocollide( a int, b int ) returns int language java parameter style java external name 'Foo.f'" );
        
        goodStatement
            ( conn, "create derby aggregate uda_schema3.func_collide for int external name 'foo.bar.Wibble'" );
        goodStatement
            ( conn, "create derby aggregate uda_schema3.func_nocollide for int external name 'foo.bar.Wibble'" );
        expectExecutionError
            ( conn, NAME_COLLISION,
              "create function uda_schema3.func_collide( a int ) returns int language java parameter style java external name 'Foo.f'" );
        goodStatement
            ( conn, "create function uda_schema3.func_nocollide( a int, b int ) returns int language\n" +
              "java parameter style java external name 'Foo.f'" );
        
        // can't drop a schema which still has an aggregate in it
        expectExecutionError( conn, NON_EMPTY_SCHEMA, "drop schema uda_schema restrict" );

        // drop the aggregate, then drop the schema
        goodStatement( conn, "drop derby aggregate uda_schema.mode_012 restrict" );
        goodStatement( conn, "drop schema uda_schema restrict" );

        // can't drop a non-existent aggregate
        expectCompilationError( NONEXISTENT_OBJECT, "drop derby aggregate mode_01 restrict" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop derby aggregate mode_011 restrict" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop derby aggregate uda_schema2.mode_01 restrict" );
    }

    /**
     * <p>
     * Don't allow aggregates to have the names of builtin functions with 1 argument.
     * See also DERBY-5901.
     * </p>
     */
    public void test_02_builtinConflicts() throws Exception
    {
        Connection conn = getConnection();

        // 1 argument bad
        badAggregate( conn, ILLEGAL_AGGREGATE, "abs" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "absval" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "acos" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "asin" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "atan" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "ceil" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "ceiling" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "cos" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "cosh" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "cot" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "date" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "day" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "degrees" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "exp" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "floor" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "lcase" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "length" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "ln" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "log" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "log10" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "month" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "radians" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "rand" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "sign" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "sin" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "sinh" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "sqrt" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "tan" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "tanh" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "time" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "timestamp" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "ucase" );

        // no conflict with 2 argument builtin functions
        goodStatement( conn, "create derby aggregate locate for int external name 'foo.bar.Wibble'" );
        goodStatement( conn, "drop derby aggregate locate restrict" );
    }
    private void    badAggregate( Connection conn, String expectedSQLState, String name ) throws Exception
    {
        String  ddl = "create derby aggregate " + name + " for int external name 'foo.bar.Wibble'";
        
        expectCompilationError( expectedSQLState, ddl );
    }

    /**
     * <p>
     * Various 1-arg operators and SQL aggregates should not be legal aggregate names because they
     * are supposed to be reserved keywords.
     * See also DERBY-5901.
     * </p>
     */
    public void test_03_keywordConflicts() throws Exception
    {
        Connection conn = getConnection();

        // 1-arg functions which are reserved keywords
        badAggregate( conn, SYNTAX_ERROR, "bigint" );
        badAggregate( conn, SYNTAX_ERROR, "char" );
        badAggregate( conn, SYNTAX_ERROR, "double" );
        badAggregate( conn, SYNTAX_ERROR, "hour" );
        badAggregate( conn, SYNTAX_ERROR, "integer" );
        badAggregate( conn, SYNTAX_ERROR, "ltrim" );
        badAggregate( conn, SYNTAX_ERROR, "lower" );
        badAggregate( conn, SYNTAX_ERROR, "minute" );
        badAggregate( conn, SYNTAX_ERROR, "rtrim" );
        badAggregate( conn, SYNTAX_ERROR, "second" );
        badAggregate( conn, SYNTAX_ERROR, "smallint" );
        badAggregate( conn, SYNTAX_ERROR, "trim" );
        badAggregate( conn, SYNTAX_ERROR, "upper" );
        badAggregate( conn, SYNTAX_ERROR, "varchar" );
        badAggregate( conn, SYNTAX_ERROR, "year" );

        // SQL aggregates which are reserved keywords
        badAggregate( conn, SYNTAX_ERROR, "any" );
        badAggregate( conn, SYNTAX_ERROR, "avg" );        
        badAggregate( conn, SYNTAX_ERROR, "max" );        
        badAggregate( conn, SYNTAX_ERROR, "min" );        
        badAggregate( conn, SYNTAX_ERROR, "some" );
        badAggregate( conn, SYNTAX_ERROR, "sum" );        
    }

    /**
     * <p>
     * Various aggregates defined by the SQL Standard do not appear in the Derby
     * grammar as reserved keywords. They are, nonetheless, illegal as the names
     * of user-defined aggregates.
     * See also DERBY-5901.
     * </p>
     */
    public void test_04_nonReservedAggregateConflicts() throws Exception
    {
        Connection conn = getConnection();

        badAggregate( conn, ILLEGAL_AGGREGATE, "collect" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "count" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "every" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "fusion" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "intersection" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "stddev_pop" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "stddev_samp" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "var_pop" );
        badAggregate( conn, ILLEGAL_AGGREGATE, "var_samp" );
    }

    /**
     * <p>
     * Basic test for aggregates in the select list.
     * </p>
     */
    public void test_05_basicSelectList() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "create schema agg_schema\n" );
        goodStatement
            ( conn, "create derby aggregate mode_05 for int\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'" );
        goodStatement
            ( conn, "create derby aggregate agg_schema.mode_052 for int\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'" );
        goodStatement( conn, "create table mode_05_inputs( a int, b int )" );
        goodStatement( conn, "insert into mode_05_inputs( a, b ) values ( 1, 1 ), ( 1, 2 ), ( 1, 2 ), ( 1, 2 ), ( 2, 3 ), ( 2, 3 ), ( 2, 4 )" );

        // scalar aggregate
        assertResults
            (
             conn,
             "select mode_05( b ) from mode_05_inputs",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select app.mode_05( b ) from mode_05_inputs",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select agg_schema.mode_052( b ) from mode_05_inputs",
             new String[][]
             {
                 { "2" },
             },
             false
             );

        // grouped aggregate
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        
        // distinct scalar aggregate
        assertResults
            (
             conn,
             "select mode_05( distinct b ) from mode_05_inputs",
             new String[][]
             {
                 { "4" },
             },
             false
             );
        assertResults
            (
             conn,
             "select agg_schema.mode_052( distinct b ) from mode_05_inputs",
             new String[][]
             {
                 { "4" },
             },
             false
             );

        // distinct grouped aggregate
        assertResults
            (
             conn,
             "select a, mode_05( distinct b ) from mode_05_inputs group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "4" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, agg_schema.mode_052( distinct b ) from mode_05_inputs group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "4" },
             },
             false
             );

        // some negative tests for missing aggregates
        expectCompilationError( MISSING_FUNCTION, "select agg_schema.mode_05( b ) from mode_05_inputs" );
        expectCompilationError( OBJECT_DOES_NOT_EXIST, "select agg_schema.mode_05( distinct b ) from mode_05_inputs" );
        expectCompilationError( MISSING_SCHEMA, "select missing_schema.mode_05( b ) from mode_05_inputs" );
        expectCompilationError( MISSING_SCHEMA, "select missing_schema.mode_05( distinct b ) from mode_05_inputs" );

        // some negative tests for aggregates in the WHERE clause
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_05_inputs where mode_05( b ) = 4" );
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_05_inputs where mode_05( distinct b ) = 4" );
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_05_inputs where app.mode_05( b ) = 4" );
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_05_inputs where app.mode_05( distinct b ) = 4" );

        // negative test: can't put an aggregate in an ORDER BY list unless it's in the SELECT list too
        expectCompilationError( BAD_AGG_PLACEMENT, "select * from mode_05_inputs order by mode_05( b )" );

        // various other syntactically correct placements of user-defined aggregates
        assertResults
            (
             conn,
             "select mode_05( b ) from mode_05_inputs order by mode_05( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a order by mode_05( b )",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a order by mode_05( b ) desc",
             new String[][]
             {
                 { "2", "3" },
                 { "1", "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a having mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, count( b ) from mode_05_inputs group by a having mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, sum( b ) from mode_05_inputs group by a having mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "10" },
             },
             false
             );

        assertResults
            (
             conn,
             "select mode_05( b ) from mode_05_inputs order by app.mode_05( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a order by app.mode_05( b )",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a order by app.mode_05( b ) desc",
             new String[][]
             {
                 { "2", "3" },
                 { "1", "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode_05( b ) from mode_05_inputs group by a having app.mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, count( b ) from mode_05_inputs group by a having app.mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, sum( b ) from mode_05_inputs group by a having app.mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "10" },
             },
             false
             );

        assertResults
            (
             conn,
             "select app.mode_05( b ) from mode_05_inputs order by app.mode_05( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a order by app.mode_05( b )",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a order by app.mode_05( b ) desc",
             new String[][]
             {
                 { "2", "3" },
                 { "1", "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a having app.mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );

        assertResults
            (
             conn,
             "select app.mode_05( b ) from mode_05_inputs order by mode_05( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a order by mode_05( b )",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a order by mode_05( b ) desc",
             new String[][]
             {
                 { "2", "3" },
                 { "1", "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode_05( b ) from mode_05_inputs group by a having mode_05( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
    }

    /**
     * <p>
     * Test for parameterized aggregates.
     * </p>
     */
    public void test_06_parameterizedAggregates() throws Exception
    {
        Connection conn = getConnection();

        vetParameterizedAggregate
            (
             conn,
             "intMode",
             "int",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode",
             "( 1, 1 ), ( 1, 2 ), ( 1, 2 ), ( 1, 2 ), ( 2, 3 ), ( 2, 3 ), ( 2, 4 )",
             new String[][]
             {
                 { "2" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             new String[][]
             {
                 { "4" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "4" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "varcharMode",
             "varchar( 5 )",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$StringMode",
             "( 1, 'a' ), ( 1, 'ab' ), ( 1, 'ab' ), ( 1, 'ab' ), ( 2, 'abc' ), ( 2, 'abc' ), ( 2, 'abcd' )",
             new String[][]
             {
                 { "ab" },
             },
             new String[][]
             {
                 { "1", "ab" },
                 { "2", "abc" },
             },
             new String[][]
             {
                 { "abcd" },
             },
             new String[][]
             {
                 { "1", "ab" },
                 { "2", "abcd" },
             }
             );

    }
    private void    vetParameterizedAggregate
        (
         Connection conn,
         String aggName,
         String sqlType,
         String externalName,
         String values,
         String[][] scalarResult,
         String[][] groupedResult,
         String[][] distinctScalarResult,
         String[][] distinctGroupedResult
         )
        throws Exception
    {
        String  tableName = aggName + "_mode_inputs";
        
        goodStatement
            ( conn, "create derby aggregate " + aggName + " for " + sqlType + "\n" +
              "external name '" + externalName + "'" );
        goodStatement( conn, "create table " + tableName + "( a int, b " + sqlType + " )" );
        goodStatement( conn, "insert into " + tableName + "( a, b ) values " + values );

        assertResults
            (
             conn,
             "select " + aggName + "( b ) from " + tableName,
             scalarResult,
             false
             );

        assertResults
            (
             conn,
             "select a, " + aggName + "( b ) from " + tableName + " group by a",
             groupedResult,
             false
             );

        if ( distinctScalarResult != null )
        {
            assertResults
                (
                 conn,
                 "select " + aggName + "( distinct b ) from " + tableName,
                 distinctScalarResult,
                 false
                 );
        }

        if ( distinctGroupedResult != null )
        {
            assertResults
                (
                 conn,
                 "select a, " + aggName + "( distinct b ) from " + tableName + " group by a",
                 distinctGroupedResult,
                 false
                 );
        }
    }

    /**
     * <p>
     * Test restricted drops of aggregates.
     * </p>
     */
    public void test_07_restrictedDrops() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            ( conn,
              "create derby aggregate mode_07 for int external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'" );
        goodStatement
            ( conn,
              "create table mode_inputs_07( a int, b int )" );

        // restricted drop blocked by a view
        goodStatement
            ( conn,
              "create view v_dbo_07( a, modeOfA ) as select a, mode_07( b ) from mode_inputs_07 group by a" );
        expectExecutionError
            ( conn, VIEW_DEPENDENCY, "drop derby aggregate mode_07 restrict" );
        goodStatement
            ( conn,
              "drop view v_dbo_07" );
        
        // restricted drop blocked by a trigger
        goodStatement
            ( conn,
              "create table t_source_07( a int )" );
        goodStatement
            ( conn,
              "create table t_target_07( a int )" );
        goodStatement
            ( conn,
              "create trigger t_insert_trigger_07\n" +
              "after insert on t_source_07\n" +
              "for each row\n" +
              "insert into t_target_07( a ) select mode_07( b ) from mode_inputs_07\n"
              );
        expectExecutionError
            ( conn, FORBIDDEN_DROP_TRIGGER, "drop derby aggregate mode_07 restrict" );
        goodStatement
            ( conn,
              "drop trigger t_insert_trigger_07" );

        // blocking objects dropped. aggregate is now droppable
        goodStatement( conn, "drop derby aggregate mode_07 restrict" );
        
    }
    
    /**
     * <p>
     * Test aggregates on user defined types.
     * </p>
     */
    public void test_08_basicUDTaggregates() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            ( conn,
              "create type FullName external name 'org.apache.derbyTesting.functionTests.tests.lang.FullName' language java" );
        goodStatement
            (
             conn,
             "create function makeFullName( firstName varchar( 32672 ), lastName varchar( 32672 ) )\n" +
             "returns FullName language java parameter style java\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.FullName.makeFullName'"
             );
        goodStatement
            (
             conn,
             "create derby aggregate fullNameMode for FullName\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$FullNameMode'"
             );
        goodStatement
            ( conn,
              "create table fullNameMode_inputs( a int, b FullName )" );
        goodStatement
            (
             conn,
             "insert into fullNameMode_inputs( a, b )\n" +
             "values\n" +
             "( 1, makeFullName( 'one', 'name'  ) ),\n" +
             "( 1, makeFullName( 'two', 'name' ) ),\n" +
             "( 1, makeFullName( 'two', 'name' ) ),\n" +
             "( 1, makeFullName( 'two', 'name' ) ),\n" +
             "( 2, makeFullName( 'three', 'name' ) ),\n" +
             "( 2, makeFullName( 'three', 'name' ) ),\n" +
             "( 2, makeFullName( 'four', 'name' ) )\n"
             );
        
        assertResults
            (
             conn,
             "select fullNameMode( b ) from fullNameMode_inputs",
             new String[][]
             {
                 { "two name" }
             },
             false
             );
        assertResults
            (
             conn,
             "select a, fullNameMode( b ) from fullNameMode_inputs group by a",
             new String[][]
             {
                 { "1", "two name" },
                 { "2", "three name" }
             },
             false
             );
    }

    /**
     * <p>
     * Test aggregates bound to generic classes.
     * </p>
     */
    public void test_09_genericAggregates() throws Exception
    {
        Connection conn = getConnection();

        vetParameterizedAggregate
            (
             conn,
             "intMode_09",
             "int",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode",
             "( 1, 1 ), ( 1, 2 ), ( 1, 2 ), ( 1, 2 ), ( 2, 3 ), ( 2, 3 ), ( 2, 4 )",
             new String[][]
             {
                 { "2" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             new String[][]
             {
                 { "4" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "4" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "varcharMode_09",
             "varchar( 5 )",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode",
             "( 1, 'a' ), ( 1, 'ab' ), ( 1, 'ab' ), ( 1, 'ab' ), ( 2, 'abc' ), ( 2, 'abc' ), ( 2, 'abcd' )",
             new String[][]
             {
                 { "ab" },
             },
             new String[][]
             {
                 { "1", "ab" },
                 { "2", "abc" },
             },
             new String[][]
             {
                 { "abcd" },
             },
             new String[][]
             {
                 { "1", "ab" },
                 { "2", "abcd" },
             }
             );

        goodStatement
            (
             conn,
             "create type FullName_09 external name 'org.apache.derbyTesting.functionTests.tests.lang.FullName' language java"
             );
        goodStatement
            (
             conn,
             "create function makeFullName_09( firstName varchar( 32672 ), lastName varchar( 32672 ) )\n" +
             "returns FullName_09 language java parameter style java\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.FullName.makeFullName'\n"
             );
        vetParameterizedAggregate
            (
             conn,
             "fullNameMode_09",
             "FullName_09",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode",
             "( 1, makeFullName_09( 'one', 'name'  ) ),\n" +
             "( 1, makeFullName_09( 'two', 'name' ) ),\n" +
             "( 1, makeFullName_09( 'two', 'name' ) ),\n" +
             "( 1, makeFullName_09( 'two', 'name' ) ),\n" +
             "( 2, makeFullName_09( 'three', 'name' ) ),\n" +
             "( 2, makeFullName_09( 'three', 'name' ) ),\n" +
             "( 2, makeFullName_09( 'four', 'name' ) )\n",
             new String[][]
             {
                 { "two name" },
             },
             new String[][]
             {
                 { "1", "two name" },
                 { "2", "three name" },
             },
             null,
             null
             );
    }

    /**
     * <p>
     * Negative tests.
     * </p>
     */
    public void test_10_negative() throws Exception
    {
        Connection conn = getConnection();

        //
        // Input operand must agree with input type of aggregate
        //
        goodStatement
            (
             conn,
             "create derby aggregate intMode_10 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode'\n"
             );
        goodStatement
            (
             conn,
             "create table intMode_10_inputs( a int, b varchar( 10 ) )"
             );
        expectCompilationError( INPUT_MISMATCH, "select intMode_10( b ) from intMode_10_inputs" );

        // aggregates not allowed in generated columns
        expectCompilationError( BAD_GEN_COL, "create table t_shouldFail( a int, b int generated always as ( intMode_10( a ) ) )" );

        //
        // Input type not within bounds of aggregator class.
        //
        goodStatement
            (
             conn,
             "create type Price_10 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             conn,
             "create table t_price_10( a int, b Price_10 )"
             );
        goodStatement
            (
             conn,
             "create derby aggregate priceMode_10 for Price_10\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode'\n"
             );
        expectCompilationError( INPUT_OUTSIDE_BOUNDS, "select priceMode_10( b ) from t_price_10" );
        expectCompilationError( INPUT_OUTSIDE_BOUNDS, "select a, priceMode_10( b ) from t_price_10 group by a" );

        //
        // Return type not within bounds of aggregator class.
        //
        goodStatement
            (
             conn,
             "create table t_price_10_1( a int, b int )"
             );
        goodStatement
            (
             conn,
             "create derby aggregate priceMode_10_1 for int returns Price_10\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode'"
             );
        expectCompilationError( RETURN_OUTSIDE_BOUNDS, "select priceMode_10_1( b ) from t_price_10_1" );
        expectCompilationError( RETURN_OUTSIDE_BOUNDS, "select a, priceMode_10_1( b ) from t_price_10_1 group by a" );

        // aggregates not allowed inside aggregates
        expectCompilationError( NESTED_AGGS, "select max( intMode_10( columnnumber ) ) from sys.syscolumns" );
        expectCompilationError( NESTED_AGGS, "select intMode_10( max( columnnumber ) ) from sys.syscolumns" );

        // untyped nulls not allowed as args to an aggregate
        expectCompilationError( UNTYPED_NULL, "select intMode_10( null ) from sys.syscolumns" );

        // missing Aggregator class
        goodStatement
            (
             conn,
             "create derby aggregate intMode_missing_10 for int external name 'missing.Missing'"
             );
        expectCompilationError( MISSING_CLASS, "select intMode_missing_10( columnnumber ) from sys.syscolumns" );

        // invalid context for an aggregate
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from sys.syscolumns where intMode_10( columnnumber ) = 1" );
        expectCompilationError
            ( BAD_AGG_PLACEMENT,
              "select case when columnnumber = 1 then 1 else intMode_10( columnnumber ) end from sys.syscolumns" );
        expectCompilationError
            ( BAD_AGG_PLACEMENT,
              "select case when columnnumber = 1 then intMode_10( columnnumber ) else 1 end from sys.syscolumns" );
        expectCompilationError
            ( BAD_AGG_PLACEMENT,
              "select case when columnnumber = intMode_10( columnnumber ) then 0 else 1 end from sys.syscolumns" );
        expectCompilationError
            ( BAD_AGGREGATE_USAGE, "values ( intMode_10( 1 ) )" );
        expectCompilationError
            ( BAD_CONSTRAINT,
              "create table badTable( a int, b int check ( intMode_10( b ) > 1 ) )" );

        // aggregates not permitted in ON clause
        expectCompilationError
            ( AGG_IN_ON_CLAUSE,
              "select * from sys.syscolumns l join sys.syscolumns r on intMode_10( r.columnnumber ) = l.columnnumber" );

        // aggregates not allowed in the SET clause of an UPDATE statement
        goodStatement( conn, "create table intMode_10_inputs_1( a int, b int )" );
        expectCompilationError
            ( BAD_AGG_PLACEMENT, "update intMode_10_inputs_1 set b = intMode_10( b )" );
    }

    /**
     * <p>
     * Test datatype coverage. Verify that you can declare user-defined aggregates on all
     * Derby data types except for XML.
     * </p>
     */
    public void test_11_datatypes() throws Exception
    {
        Connection conn = getConnection();

        // if this fails, then we need to add a new data type to this test
        vetDatatypeCount( conn, 22 );
        
        vetParameterizedAggregate
            (
             conn,
             "booleanMode_11",
             "boolean",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BooleanMode",
             "( 1, false ), ( 1, true ), (1, false ), ( 2, true ), ( 2, true ), ( 3, false ), ( 3, true ), ( 3, true )",
             new String[][]
             {
                 { "true" },
             },
             new String[][]
             {
                 { "1", "false" },
                 { "2", "true" },
                 { "3", "true" },
             },
             new String[][]
             {
                 { "true" },
             },
             new String[][]
             {
                 { "1", "true" },
                 { "2", "true" },
                 { "3", "true" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "bigintMode_11",
             "bigint",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BigintMode",
             "( 1, 1 ), ( 1, 2 ), (1, 2 ), ( 2, 2 ), ( 2, 3 ), ( 3, 3 ), ( 3, 4 ), ( 3, 5 )",
             new String[][]
             {
                 { "2" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             },
             new String[][]
             {
                 { "5" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "smallintMode_11",
             "smallint",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode",
             "( 1, 1 ), ( 1, 2 ), (1, 2 ), ( 2, 2 ), ( 2, 3 ), ( 3, 3 ), ( 3, 4 ), ( 3, 5 )",
             new String[][]
             {
                 { "2" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             },
             new String[][]
             {
                 { "5" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "intMode_11",
             "int",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode",
             "( 1, 1 ), ( 1, 2 ), (1, 2 ), ( 2, 2 ), ( 2, 3 ), ( 3, 3 ), ( 3, 4 ), ( 3, 5 )",
             new String[][]
             {
                 { "2" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             },
             new String[][]
             {
                 { "5" },
             },
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "decimalMode_11",
             "decimal( 5, 2 )",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BigDecimalMode",
             "( 1, 1.11 ), ( 1, 2.22 ), (1, 2.22 ), ( 2, 2.22 ), ( 2, 3.33 ), ( 3, 3.33 ), ( 3, 4.44 ), ( 3, 5.55 )",
             new String[][]
             {
                 { "2.22" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             },
             new String[][]
             {
                 { "5.55" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "numericMode_11",
             "numeric( 5, 2 )",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BigDecimalMode",
             "( 1, 1.11 ), ( 1, 2.22 ), (1, 2.22 ), ( 2, 2.22 ), ( 2, 3.33 ), ( 3, 3.33 ), ( 3, 4.44 ), ( 3, 5.55 )",
             new String[][]
             {
                 { "2.22" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             },
             new String[][]
             {
                 { "5.55" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "doubleMode_11",
             "double",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$DoubleMode",
             "( 1, 1.11 ), ( 1, 2.22 ), (1, 2.22 ), ( 2, 2.22 ), ( 2, 3.33 ), ( 3, 3.33 ), ( 3, 4.44 ), ( 3, 5.55 )",
             new String[][]
             {
                 { "2.22" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             },
             new String[][]
             {
                 { "5.55" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "floatMode_11",
             "float",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$DoubleMode",
             "( 1, 1.11 ), ( 1, 2.22 ), (1, 2.22 ), ( 2, 2.22 ), ( 2, 3.33 ), ( 3, 3.33 ), ( 3, 4.44 ), ( 3, 5.55 )",
             new String[][]
             {
                 { "2.22" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             },
             new String[][]
             {
                 { "5.55" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "realMode_11",
             "real",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$RealMode",
             "( 1, 1.11 ), ( 1, 2.22 ), (1, 2.22 ), ( 2, 2.22 ), ( 2, 3.33 ), ( 3, 3.33 ), ( 3, 4.44 ), ( 3, 5.55 )",
             new String[][]
             {
                 { "2.22" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             },
             new String[][]
             {
                 { "5.55" },
             },
             new String[][]
             {
                 { "1", "2.22" },
                 { "2", "3.33" },
                 { "3", "5.55" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "charMode_11",
             "char( 4 )",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$StringMode",
             "( 1, 'aaaa' ), ( 1, 'abaa' ), ( 1, 'aaaa' ), ( 2, 'abaa' ), ( 2, 'abaa' ), ( 2, 'abca' ), ( 3, 'abaa' ), ( 3, 'abaa' ), ( 3, 'abcd' )",
             new String[][]
             {
                 { "abaa" },
             },
             new String[][]
             {
                 { "1", "aaaa" },
                 { "2", "abaa" },
                 { "3", "abaa" },
             },
             new String[][]
             {
                 { "abcd" },
             },
             new String[][]
             {
                 { "1", "abaa" },
                 { "2", "abca" },
                 { "3", "abcd" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "varcharMode_11",
             "varchar( 4 )",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$StringMode",
             "( 1, 'aaaa' ), ( 1, 'abaa' ), ( 1, 'aaaa' ), ( 2, 'abaa' ), ( 2, 'abaa' ), ( 2, 'abca' ), ( 3, 'abaa' ), ( 3, 'abaa' ), ( 3, 'abcd' )",
             new String[][]
             {
                 { "abaa" },
             },
             new String[][]
             {
                 { "1", "aaaa" },
                 { "2", "abaa" },
                 { "3", "abaa" },
             },
             new String[][]
             {
                 { "abcd" },
             },
             new String[][]
             {
                 { "1", "abaa" },
                 { "2", "abca" },
                 { "3", "abcd" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "longvarcharMode_11",
             "long varchar",
             "org.apache.derbyTesting.functionTests.tests.lang.GenericMode$StringMode",
             "( 1, 'aaaa' ), ( 1, 'abaa' ), ( 1, 'aaaa' ), ( 2, 'abaa' ), ( 2, 'abaa' ), ( 2, 'abca' ), ( 3, 'abaa' ), ( 3, 'abaa' ), ( 3, 'abcd' )",
             new String[][]
             {
                 { "abaa" },
             },
             new String[][]
             {
                 { "1", "aaaa" },
                 { "2", "abaa" },
                 { "3", "abaa" },
             },
             null,
             null
             );

        goodStatement
            (
             conn,
             "create function makeBlob_11( contents varchar( 32672 ) ) returns blob\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UserDefinedAggregatesTest.makeBlob'\n"
             );
        vetParameterizedAggregate
            (
             conn,
             "blobMode_11",
             "blob",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$BlobMode",
             "( 1, makeBlob_11( 'a' ) ),\n" +
             "( 1, makeBlob_11( 'ab' ) ),\n" +
             "( 1, makeBlob_11( 'ab' ) ),\n" +
             "( 2, makeBlob_11( 'ab' ) ),\n" +
             "( 2, makeBlob_11( 'abc' ) ),\n" +
             "( 3, makeBlob_11( 'a' ) ),\n" +
             "( 3, makeBlob_11( 'ab' ) ),\n" +
             "( 3, makeBlob_11( 'abcd' ) )",
             new String[][]
             {
                 { "6162" },
             },
             new String[][]
             {
                 { "1", "6162" },
                 { "2", "616263" },
                 { "3", "61626364" },
             },
             null,
             null
             );

        goodStatement
            (
             conn,
             "create function makeClob_11( contents varchar( 32672 ) ) returns clob\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UserDefinedAggregatesTest.makeClob'\n"
             );
        vetParameterizedAggregate
            (
             conn,
             "clobMode_11",
             "clob",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$ClobMode",
             "( 1, makeClob_11( 'a' ) ),\n" +
             "( 1, makeClob_11( 'ab' ) ),\n" +
             "( 1, makeClob_11( 'ab' ) ),\n" +
             "( 2, makeClob_11( 'ab' ) ),\n" +
             "( 2, makeClob_11( 'abc' ) ),\n" +
             "( 3, makeClob_11( 'a' ) ),\n" +
             "( 3, makeClob_11( 'ab' ) ),\n" +
             "( 3, makeClob_11( 'abcd' ) )",
             new String[][]
             {
                 { "ab" },
             },
             new String[][]
             {
                 { "1", "ab" },
                 { "2", "abc" },
                 { "3", "abcd" },
             },
             null,
             null
             );

        goodStatement
            (
             conn,
             "create function makeBinary_11( contents varchar( 32672 ) ) returns char( 4 ) for bit data\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UserDefinedAggregatesTest.makeBinary'\n"
             );
        vetParameterizedAggregate
            (
             conn,
             "binaryMode_11",
             "char( 4 ) for bit data",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$BinaryMode",
             "( 1, makeBinary_11( 'abaa' ) ),\n" +
             "( 1, makeBinary_11( 'abaa' ) ),\n" +
             "( 1, makeBinary_11( 'abca' ) ),\n" +
             "( 2, makeBinary_11( 'abaa' ) ),\n" +
             "( 2, makeBinary_11( 'abca' ) ),\n" +
             "( 2, makeBinary_11( 'abaa' ) ),\n" +
             "( 3, makeBinary_11( 'aaaa' ) ),\n" +
             "( 3, makeBinary_11( 'abcd' ) ),\n" +
             "( 3, makeBinary_11( 'abcd' ) )\n",
             new String[][]
             {
                 { "61626161" },
             },
             new String[][]
             {
                 { "1", "61626161" },
                 { "2", "61626161" },
                 { "3", "61626364" },
             },
             new String[][]
             {
                 { "61626364" },
             },
             new String[][]
             {
                 { "1", "61626361" },
                 { "2", "61626361" },
                 { "3", "61626364" },
             }
             );

        goodStatement
            (
             conn,
             "create function makeVarbinary_11( contents varchar( 32672 ) ) returns varchar( 4 ) for bit data\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UserDefinedAggregatesTest.makeBinary'\n"
             );
        vetParameterizedAggregate
            (
             conn,
             "varbinaryMode_11",
             "varchar( 4 ) for bit data",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$BinaryMode",
             "( 1, makeVarbinary_11( 'abaa' ) ),\n" +
             "( 1, makeVarbinary_11( 'abaa' ) ),\n" +
             "( 1, makeVarbinary_11( 'abca' ) ),\n" +
             "( 2, makeVarbinary_11( 'abaa' ) ),\n" +
             "( 2, makeVarbinary_11( 'abca' ) ),\n" +
             "( 2, makeVarbinary_11( 'abaa' ) ),\n" +
             "( 3, makeVarbinary_11( 'aaaa' ) ),\n" +
             "( 3, makeVarbinary_11( 'abcd' ) ),\n" +
             "( 3, makeVarbinary_11( 'abcd' ) )\n",
             new String[][]
             {
                 { "61626161" },
             },
             new String[][]
             {
                 { "1", "61626161" },
                 { "2", "61626161" },
                 { "3", "61626364" },
             },
             new String[][]
             {
                 { "61626364" },
             },
             new String[][]
             {
                 { "1", "61626361" },
                 { "2", "61626361" },
                 { "3", "61626364" },
             }
             );

        goodStatement
            (
             conn,
             "create function makeLongvarbinary_11( contents varchar( 32672 ) ) returns long varchar for bit data\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UserDefinedAggregatesTest.makeBinary'\n"
             );
        vetParameterizedAggregate
            (
             conn,
             "longvarbinaryMode_11",
             "long varchar for bit data",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$BinaryMode",
             "( 1, makeLongvarbinary_11( 'abaa' ) ),\n" +
             "( 1, makeLongvarbinary_11( 'abaa' ) ),\n" +
             "( 1, makeLongvarbinary_11( 'abca' ) ),\n" +
             "( 2, makeLongvarbinary_11( 'abaa' ) ),\n" +
             "( 2, makeLongvarbinary_11( 'abca' ) ),\n" +
             "( 2, makeLongvarbinary_11( 'abaa' ) ),\n" +
             "( 3, makeLongvarbinary_11( 'aaaa' ) ),\n" +
             "( 3, makeLongvarbinary_11( 'abcd' ) ),\n" +
             "( 3, makeLongvarbinary_11( 'abcd' ) )\n",
             new String[][]
             {
                 { "61626161" },
             },
             new String[][]
             {
                 { "1", "61626161" },
                 { "2", "61626161" },
                 { "3", "61626364" },
             },
             null,
             null
             );

        vetParameterizedAggregate
            (
             conn,
             "dateMode_11",
             "date",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$DateMode",
             "( 1, date( '1994-02-23' ) ),\n" +
             "( 1, date( '1994-02-23' ) ),\n" +
             "( 1, date( '1995-02-23' ) ),\n" +
             "( 2, date( '1995-02-23' ) ),\n" +
             "( 2, date( '1995-02-23' ) ),\n" +
             "( 2, date( '1996-02-23' ) ),\n" +
             "( 3, date( '1993-02-23' ) ),\n" +
             "( 3, date( '1993-02-23' ) ),\n" +
             "( 3, date( '1995-02-23' ) )\n",
             new String[][]
             {
                 { "1995-02-23" },
             },
             new String[][]
             {
                 { "1", "1994-02-23" },
                 { "2", "1995-02-23" },
                 { "3", "1993-02-23" },
             },
             new String[][]
             {
                 { "1996-02-23" },
             },
             new String[][]
             {
                 { "1", "1995-02-23" },
                 { "2", "1996-02-23" },
                 { "3", "1995-02-23" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "timestampMode_11",
             "timestamp",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$TimestampMode",
             "( 1, timestamp( '1994-02-23 03:20:20' ) ),\n" +
             "( 1, timestamp( '1994-02-23 03:20:20' ) ),\n" +
             "( 1, timestamp( '1995-02-23 03:20:20' ) ),\n" +
             "( 2, timestamp( '1995-02-23 03:20:20' ) ),\n" +
             "( 2, timestamp( '1995-02-23 03:20:20' ) ),\n" +
             "( 2, timestamp( '1996-02-23 03:20:20' ) ),\n" +
             "( 3, timestamp( '1993-02-23 03:20:20' ) ),\n" +
             "( 3, timestamp( '1993-02-23 03:20:20' ) ),\n" +
             "( 3, timestamp( '1995-02-23 03:20:20' ) )\n",
             new String[][]
             {
                 { "1995-02-23 03:20:20.0" },
             },
             new String[][]
             {
                 { "1", "1994-02-23 03:20:20.0" },
                 { "2", "1995-02-23 03:20:20.0" },
                 { "3", "1993-02-23 03:20:20.0" },
             },
             new String[][]
             {
                 { "1996-02-23 03:20:20.0" },
             },
             new String[][]
             {
                 { "1", "1995-02-23 03:20:20.0" },
                 { "2", "1996-02-23 03:20:20.0" },
                 { "3", "1995-02-23 03:20:20.0" },
             }
             );

        vetParameterizedAggregate
            (
             conn,
             "timeMode_11",
             "time",
             "org.apache.derbyTesting.functionTests.tests.lang.LobMode$TimeMode",
             "( 1, time( '03:20:20' ) ),\n" +
             "( 1, time( '03:20:20' ) ),\n" +
             "( 1, time( '04:20:20' ) ),\n" +
             "( 2, time( '04:20:20' ) ),\n" +
             "( 2, time( '04:20:20' ) ),\n" +
             "( 2, time( '05:20:20' ) ),\n" +
             "( 3, time( '04:20:20' ) ),\n" +
             "( 3, time( '04:20:20' ) ),\n" +
             "( 3, time( '06:20:20' ) )\n",
             new String[][]
             {
                 { "04:20:20" },
             },
             new String[][]
             {
                 { "1", "03:20:20" },
                 { "2", "04:20:20" },
                 { "3", "04:20:20" },
             },
             new String[][]
             {
                 { "06:20:20" },
             },
             new String[][]
             {
                 { "1", "04:20:20" },
                 { "2", "05:20:20" },
                 { "3", "06:20:20" },
             }
             );

        // XML is not a valid data type for UDA inputs and return values
        expectCompilationError
            (
             XML_TYPE,
             "create derby aggregate xmlMode_11 for xml\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.LobMode'\n"
             );
    }

   /** Blob-creating function */
    public  static  Blob    makeBlob( String contents ) throws Exception
    {
        return new HarmonySerialBlob( makeBinary( contents ) );
    }

    private int vetDatatypeCount( Connection conn, int expectedTypeCount ) throws Exception
    {
        //
        // If this fails, it means that we need to add another datatype to the
        // calling test.
        //
        
        ResultSet rs = conn.getMetaData().getTypeInfo();
        int actualTypeCount = 0;
        while ( rs.next() ) { actualTypeCount++; }
        rs.close();

        assertEquals( expectedTypeCount, actualTypeCount );

        return actualTypeCount;
    }
    
   /** Blob-creating function */
    public  static  byte[]    makeBinary( String contents ) throws Exception
    {
        return contents.getBytes( "UTF-8" );
    }

   /** Clob-creating function */
    public  static  Clob    makeClob( String contents ) throws Exception
    {
        return new HarmonySerialClob( contents );
    }

    /**
     * <p>
     * Test implicit casts of input types.
     * </p>
     */
    public void test_12_coercion() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create derby aggregate charMode_12 for char( 4 )\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$StringMode'\n"
             );
        goodStatement
            (
             conn,
             "create table charMode_12_mode_inputs_small( a int, b char( 3 ) )"
             );
        goodStatement
            (
             conn,
             "insert into charMode_12_mode_inputs_small( a, b ) values ( 1, 'aaa' ), ( 1, 'aba' ), ( 1, 'aaa' ), ( 2, 'aba' ), ( 2, 'aba' ), ( 2, 'aba' ), ( 3, 'aba' ), ( 3, 'aba' ), ( 3, 'abc' )"
             );
        goodStatement
            (
             conn,
             "create table charMode_12_mode_inputs_big( a int, b char( 5 ) )"
             );
        goodStatement
            (
             conn,
             "insert into charMode_12_mode_inputs_big( a, b ) values ( 1, 'aaaaa' ), ( 1, 'abaaa' ), ( 1, 'aaaaa' ), ( 2, 'abaaa' ), ( 2, 'abaaa' ), ( 2, 'abcaa' ), ( 3, 'abaaa' ), ( 3, 'abaaa' ), ( 3, 'abcde' )"
             );

        // undersized char values are space-padded at the end
        assertResults
            (
             conn,
             "select charMode_12( b ) from charMode_12_mode_inputs_small",
             new String[][]
             {
                 { "aba " },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, charMode_12( b ) from charMode_12_mode_inputs_small group by a",
             new String[][]
             {
                 { "1", "aaa " },
                 { "2", "aba " },
                 { "3", "aba " },
             },
             false
             );
        assertResults
            (
             conn,
             "select charMode_12( distinct b ) from charMode_12_mode_inputs_small",
             new String[][]
             {
                 { "abc " },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, charMode_12( distinct b ) from charMode_12_mode_inputs_small group by a",
             new String[][]
             {
                 { "1", "aba " },
                 { "2", "aba " },
                 { "3", "abc " },
             },
             false
             );

        // oversized char values raise truncation errors
        expectExecutionError
            ( conn, STRING_TRUNCATION, "select charMode_12( b ) from charMode_12_mode_inputs_big" );
        expectExecutionError
            ( conn, STRING_TRUNCATION, "select a, charMode_12( b ) from charMode_12_mode_inputs_big group by a" );
        expectExecutionError
            ( conn, STRING_TRUNCATION, "select charMode_12( distinct b ) from charMode_12_mode_inputs_big" );
        expectExecutionError
            ( conn, STRING_TRUNCATION, "select a, charMode_12( distinct b ) from charMode_12_mode_inputs_big group by a" );

        // no problem running a BIGINT aggregator on INTs
        goodStatement
            (
             conn,
             "create derby aggregate bigintMode_12 for bigint\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BigintMode'\n"
             );
        goodStatement
            (
             conn,
             "create table ints_12( a int, b int )"
             );
        goodStatement
            (
             conn,
             "insert into ints_12( a, b ) values ( 1, 1 ), ( 1, 2 ), (1, 2 ), ( 2, 2 ), ( 2, 3 ), ( 3, 3 ), ( 3, 4 ), ( 3, 5 )"
             );
        assertResults
            (
             conn,
             "select bigintMode_12( b ) from ints_12",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, bigintMode_12( b ) from ints_12 group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             },
             false
             );
        assertResults
            (
             conn,
             "select bigintMode_12( distinct b ) from ints_12",
             new String[][]
             {
                 { "5" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, bigintMode_12( distinct b ) from ints_12 group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
                 { "3", "5" },
             },
             false
             );

        // but you can get runtime errors if you run an INT aggregate on oversized BIGINTs
        goodStatement
            (
             conn,
             "create derby aggregate intMode_12 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode'\n"
             );
        goodStatement
            (
             conn,
             "create table bigints_12( a int, b bigint )"
             );
        goodStatement
            (
             conn,
             "insert into bigints_12( a, b ) values ( 1, 1000000000001 ), ( 1, 1000000000002 ), (1, 1000000000002 ), ( 2, 1000000000002 ), ( 2, 1000000000003 ), ( 3, 1000000000003 ), ( 3, 1000000000004 ), ( 3, 1000000000005 )"
             );
        expectExecutionError
            ( conn, INT_TRUNCATION, "select intMode_12( b ) from bigints_12" );
        expectExecutionError
            ( conn, INT_TRUNCATION, "select a, intMode_12( b ) from bigints_12 group by a" );
        expectExecutionError
            ( conn, INT_TRUNCATION, "select intMode_12( distinct b ) from bigints_12" );
        expectExecutionError
            ( conn, INT_TRUNCATION, "select a, intMode_12( distinct b ) from bigints_12 group by a" );

        // implicit cast from char to int fails
        expectCompilationError( INPUT_MISMATCH, "select intMode_12( b ) from charMode_12_mode_inputs_small" );

        // explict cast from char to int can fail at runtime
        expectExecutionError
            ( conn, CAST_FAILURE, "select intMode_12( cast (b as int) ) from charMode_12_mode_inputs_small" );
        
    }

    /**
     * <p>
     * Test aggregates whose input and return types are different.
     * </p>
     */
    public void test_13_differentReturnType() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create derby aggregate intMagnitude_13 for int returns bigint\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.LongMagnitude'\n"
             );
        goodStatement
            (
             conn,
             "create derby aggregate stringMagnitude_13 for int returns varchar( 10 )\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.LongMagnitude'\n"
             );
        goodStatement
            (
             conn,
             "create table intValues_13( a int, b int )"
             );
        goodStatement
            (
             conn,
             "insert into intValues_13 values ( 1, 1 ), ( 2, -100 ), ( 1, 2 ), ( 2, -1234 )"
             );

        assertResults
            (
             conn,
             "select intMagnitude_13( b ) from intValues_13",
             new String[][]
             {
                 { "1234" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, intMagnitude_13( b ) from intValues_13 group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "1234" },
             },
             false
             );

        // the declared return type of the aggregate does not match the actual return type
        expectCompilationError( RETURN_OUTSIDE_BOUNDS, "select stringMagnitude_13( b ) from intValues_13" );
        expectCompilationError( RETURN_OUTSIDE_BOUNDS, "select a, stringMagnitude_13( b ) from intValues_13 group by a" );
    }
    
    /**
     * <p>
     * Verify that user-defined aggregates are not allowed in GROUP BY clauses.
     * </p>
     */
    public void test_14_inGroupBy() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create derby aggregate intMode_14 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode'\n"
             );
        goodStatement
            (
             conn,
             "create table intMode_14_mode_inputs( a int, b int )"
             );

        expectCompilationError
            ( AGG_IN_GROUP_BY,
              "select intMode_14( b ) from intMode_14_mode_inputs group by intMode_14( b )" );
    }
    
    /**
     * <p>
     * Verify precision mismatches.
     * </p>
     */
    public void test_15_precisionMismatch() throws Exception
    {
        Connection conn = getConnection();

        // truncating string types
        goodStatement
            (
             conn,
             "create derby aggregate varcharMode_15 for varchar( 4 )\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$StringMode'\n"
             );
        goodStatement
            (
             conn,
             "create table varcharMode_15_mode_inputs_big( a int, b varchar( 5 ) )"
             );
        goodStatement
            (
             conn,
             "insert into varcharMode_15_mode_inputs_big( a, b ) values ( 1, 'aaaaa' ), ( 1, 'abaaa' ), ( 1, 'aaaaa' ), ( 2, 'abaaa' ), ( 2, 'abaaa' ), ( 2, 'abcaa' ), ( 3, 'abaaa' ), ( 3, 'abaaa' ), ( 3, 'abcda' )"
             );

        expectExecutionError
            ( conn, STRING_TRUNCATION, "select varcharMode_15( b ) from varcharMode_15_mode_inputs_big" );

        // truncating numeric precision
        goodStatement
            (
             conn,
             "create derby aggregate numericMode_15_bigger for numeric( 5, 3 )\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BigDecimalMode'\n"
             );
        goodStatement
            (
             conn,
             "create derby aggregate numericMode_15 for numeric( 5, 1 )\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$BigDecimalMode'\n"
             );
        goodStatement
            (
             conn,
             "create table numericMode_15_mode_inputs_big( a int, b numeric( 5, 2 ) )"
             );
        goodStatement
            (
             conn,
             "insert into numericMode_15_mode_inputs_big( a, b ) values ( 1, 1.11 ), ( 1, 1.12 ), ( 1, 1.13 ), ( 1, 2.12 ), (1, 2.22 ), ( 2, 2.22 ), ( 2, 3.33 ), ( 3, 3.33 ), ( 3, 4.44 ), ( 3, 5.55 )"
             );
        assertResults
            (
             conn,
             "select numericMode_15_bigger( b ) from numericMode_15_mode_inputs_big",
             new String[][]
             {
                 { "3.330" },
             },
             false
             );
        assertResults
            (
             conn,
             "select numericMode_15( b ) from numericMode_15_mode_inputs_big",
             new String[][]
             {
                 { "1.1" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, numericMode_15_bigger( b ) from numericMode_15_mode_inputs_big group by a",
             new String[][]
             {
                 { "1", "2.220" },
                 { "2", "3.330" },
                 { "3", "5.550" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, numericMode_15( b ) from numericMode_15_mode_inputs_big group by a",
             new String[][]
             {
                 { "1", "1.1" },
                 { "2", "3.3" },
                 { "3", "5.5" },
             },
             false
             );
    }
    
    /**
     * <p>
     * Verify that types fit within the most exact bound possible.
     * </p>
     */
    public void test_16_exactBound() throws Exception
    {
        Connection conn = getConnection();

        // input bounds
        goodStatement
            (
             conn,
             "create derby aggregate bigintMode_16 for bigint\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode'\n"
             );
        goodStatement
            (
             conn,
             "create table bigintMode_16_mode_inputs( a int, b bigint )"
             );
        expectCompilationError
            ( INPUT_OUTSIDE_BOUNDS,
              "select bigintMode_16( b ) from bigintMode_16_mode_inputs" );

        // return bounds
        goodStatement
            (
             conn,
             "create derby aggregate intMode_16 for int returns varchar( 10 )\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$IntMode'\n"
             );
        goodStatement
            (
             conn,
             "create table intMode_16_mode_inputs( a int, b int )"
             );
        expectCompilationError
            ( RETURN_OUTSIDE_BOUNDS,
              "select intMode_16( b ) from intMode_16_mode_inputs" );
        
    }

    /**
     * <p>
     * Verify that you can't drop a user-defined type if a user-defined aggregate depends on it.
     * </p>
     */
    public void test_17_udtDependencies() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create type Price_17 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             conn,
             "create type Price_17_2 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             conn,
             "create derby aggregate priceMode_17 for Price_17 returns Price_17_2\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode'\n"
             );

        // can't drop the types because the aggregate depends on them
        expectExecutionError
            ( conn, DEPENDENCY_VIOLATION, "drop type Price_17 restrict" );
        expectExecutionError
            ( conn, DEPENDENCY_VIOLATION, "drop type Price_17_2 restrict" );

        // once you drop the aggregate, you can drop the types
        goodStatement
            (
             conn,
             "drop derby aggregate priceMode_17 restrict"
             );
        goodStatement
            (
             conn,
             "drop type Price_17 restrict"
             );
        goodStatement
            (
             conn,
             "drop type Price_17_2 restrict"
             );
    }

}
