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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;

import junit.framework.Test;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

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
    public static final String BAD_ORDER_BY = "42Y35";

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
        return TestConfiguration.defaultSuite(UserDefinedAggregatesTest.class);
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
        goodStatement( conn, "create derby aggregate mode for int external name 'foo.bar.Wibble'" );
        goodStatement( conn, "create derby aggregate uda_schema.mode2 for int external name 'foo.bar.Wibble'" );

        // can't create an aggregate with an existing name
        expectExecutionError
            ( conn, OBJECT_EXISTS, "create derby aggregate mode for int external name 'foo.bar.Wibble'" );
        expectExecutionError
            ( conn, OBJECT_EXISTS, "create derby aggregate uda_schema.mode2 for int external name 'foo.bar.Wibble'" );
        
        // only RESTRICTed drops allowed now
        expectCompilationError( SYNTAX_ERROR, "drop derby aggregate mode" );

        // successfully drop an aggregate
        goodStatement( conn, "drop derby aggregate mode restrict" );

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
        goodStatement( conn, "drop derby aggregate uda_schema.mode2 restrict" );
        goodStatement( conn, "drop schema uda_schema restrict" );

        // can't drop a non-existent aggregate
        expectCompilationError( NONEXISTENT_OBJECT, "drop derby aggregate mode restrict" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop derby aggregate mode1 restrict" );
        expectCompilationError( NONEXISTENT_OBJECT, "drop derby aggregate uda_schema2.mode restrict" );
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
            ( conn, "create derby aggregate mode for int\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'" );
        goodStatement
            ( conn, "create derby aggregate agg_schema.mode2 for int\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'" );
        goodStatement( conn, "create table mode_inputs( a int, b int )" );
        goodStatement( conn, "insert into mode_inputs( a, b ) values ( 1, 1 ), ( 1, 2 ), ( 1, 2 ), ( 1, 2 ), ( 2, 3 ), ( 2, 3 ), ( 2, 4 )" );

        // scalar aggregate
        assertResults
            (
             conn,
             "select mode( b ) from mode_inputs",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select app.mode( b ) from mode_inputs",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select agg_schema.mode2( b ) from mode_inputs",
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
             "select a, mode( b ) from mode_inputs group by a",
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
             "select a, app.mode( b ) from mode_inputs group by a",
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
             "select mode( distinct b ) from mode_inputs",
             new String[][]
             {
                 { "4" },
             },
             false
             );
        assertResults
            (
             conn,
             "select agg_schema.mode2( distinct b ) from mode_inputs",
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
             "select a, mode( distinct b ) from mode_inputs group by a",
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
             "select a, agg_schema.mode2( distinct b ) from mode_inputs group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "4" },
             },
             false
             );

        // some negative tests for missing aggregates
        expectCompilationError( MISSING_FUNCTION, "select agg_schema.mode( b ) from mode_inputs" );
        expectCompilationError( OBJECT_DOES_NOT_EXIST, "select agg_schema.mode( distinct b ) from mode_inputs" );
        expectCompilationError( MISSING_SCHEMA, "select missing_schema.mode( b ) from mode_inputs" );
        expectCompilationError( MISSING_SCHEMA, "select missing_schema.mode( distinct b ) from mode_inputs" );

        // some negative tests for aggregates in the WHERE clause
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_inputs where mode( b ) = 4" );
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_inputs where mode( distinct b ) = 4" );
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_inputs where app.mode( b ) = 4" );
        expectCompilationError( BAD_AGGREGATE_USAGE, "select * from mode_inputs where app.mode( distinct b ) = 4" );

        // negative test: can't put an aggregate in an ORDER BY list unless it's in the SELECT list too
        expectCompilationError( BAD_ORDER_BY, "select * from mode_inputs order by mode( b )" );

        // various other syntactically correct placements of user-defined aggregates
        assertResults
            (
             conn,
             "select mode( b ) from mode_inputs order by mode( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode( b ) from mode_inputs group by a order by mode( b )",
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
             "select a, mode( b ) from mode_inputs group by a order by mode( b ) desc",
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
             "select a, mode( b ) from mode_inputs group by a having mode( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, count( b ) from mode_inputs group by a having mode( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, sum( b ) from mode_inputs group by a having mode( b ) = 3",
             new String[][]
             {
                 { "2", "10" },
             },
             false
             );

        assertResults
            (
             conn,
             "select mode( b ) from mode_inputs order by app.mode( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, mode( b ) from mode_inputs group by a order by app.mode( b )",
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
             "select a, mode( b ) from mode_inputs group by a order by app.mode( b ) desc",
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
             "select a, mode( b ) from mode_inputs group by a having app.mode( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, count( b ) from mode_inputs group by a having app.mode( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, sum( b ) from mode_inputs group by a having app.mode( b ) = 3",
             new String[][]
             {
                 { "2", "10" },
             },
             false
             );

        assertResults
            (
             conn,
             "select app.mode( b ) from mode_inputs order by app.mode( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode( b ) from mode_inputs group by a order by app.mode( b )",
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
             "select a, app.mode( b ) from mode_inputs group by a order by app.mode( b ) desc",
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
             "select a, app.mode( b ) from mode_inputs group by a having app.mode( b ) = 3",
             new String[][]
             {
                 { "2", "3" },
             },
             false
             );

        assertResults
            (
             conn,
             "select app.mode( b ) from mode_inputs order by mode( b )",
             new String[][]
             {
                 { "2" },
             },
             false
             );
        assertResults
            (
             conn,
             "select a, app.mode( b ) from mode_inputs group by a order by mode( b )",
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
             "select a, app.mode( b ) from mode_inputs group by a order by mode( b ) desc",
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
             "select a, app.mode( b ) from mode_inputs group by a having mode( b ) = 3",
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
             "IntMode",
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
             "StringMode",
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
         String nestedClassName,
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
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode$" + nestedClassName + "'" );
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

        assertResults
            (
             conn,
             "select " + aggName + "( distinct b ) from " + tableName,
             distinctScalarResult,
             false
             );

        assertResults
            (
             conn,
             "select a, " + aggName + "( distinct b ) from " + tableName + " group by a",
             distinctGroupedResult,
             false
             );
    }

}
