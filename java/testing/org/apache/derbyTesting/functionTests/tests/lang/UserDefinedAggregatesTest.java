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
    public static final String INPUT_MISMATCH = "42Y22";

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
        expectCompilationError( BAD_ORDER_BY, "select * from mode_05_inputs order by mode_05( b )" );

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

    }

}
