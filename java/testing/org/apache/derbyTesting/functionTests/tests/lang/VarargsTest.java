/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.VarargsTest

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

/**
 * <p>
 * Test routines with varargs. See DERBY-3069.
 * </p>
 */
public class VarargsTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  NEEDS_DERBY_STYLE = "42ZC9";
    private static  final   String  NEEDS_JAVA_STYLE = "42ZCA";
    private static  final   String  RETURNS_RESULT_SETS = "42ZCB";
    private static  final   String  AMBIGUOUS = "42X73";
    private static  final   String  NO_SUCH_METHOD = "42X50";
    private static  final   String  BAD_TIME_FORMAT = "22007";
    private static  final   String  BAD_BOOLEAN_FORMAT = "22018";
    private static  final   String  NEEDS_DJRS_STYLE = "42ZB2";

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

    public VarargsTest(String name)
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
        TestSuite       suite = new TestSuite( "UserDefinedAggregatesTest" );

        suite.addTest( TestConfiguration.defaultSuite( VarargsTest.class ) );

        return suite;
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

        goodStatement
            ( conn,
              "create function varargsDerbyStyle( a int ... ) returns int\n" +
              "parameter style derby language java no sql\n" +
              "external name 'Foo.foo'\n"
              );
        goodStatement
            ( conn,
              "create function varargsTableFunction( a int ... )\n" +
              "returns table( b int )\n" +
              "language java parameter style derby_jdbc_result_set no sql\n" +
              "external name 'Foo.foo'\n"
              );
        goodStatement
            ( conn,
              "create function nonvarargsJavaStyle( a int ) returns int\n" +
              "parameter style java language java no sql\n" +
              "external name 'Foo.foo'\n"
              );
        goodStatement
            ( conn,
              "create procedure varargsDerbyStyle( a int ... )\n" +
              "language java parameter style derby no sql\n" +
              "external name 'Foo.foo'\n"
              );
        goodStatement
            ( conn,
              "create procedure nonvarargsJavaStyle( a int )\n" +
              "language java parameter style java no sql\n" +
              "external name 'Foo.foo'\n"
              );

        // bad parameter style
        expectCompilationError
            ( NEEDS_DERBY_STYLE,
              "create function varargsJavaStyle( a int ... ) returns int\n" +
              "parameter style java language java no sql\n" +
              "external name 'Foo.foo'\n"
              );
        expectCompilationError
            ( NEEDS_JAVA_STYLE,
              "create function nonvarargsDerbyStyle( a int ) returns int\n" +
              "parameter style derby language java no sql\n" +
              "external name 'Foo.foo'\n"
              );
        expectCompilationError
            ( NEEDS_DERBY_STYLE,
              "create procedure varargsDerbyStyle( a int ... )\n" +
              "language java parameter style java no sql\n" +
              "external name 'Foo.foo'\n"
              );
        expectCompilationError
            ( NEEDS_JAVA_STYLE,
              "create procedure nonvarargsDerbyStyle( a int )\n" +
              "language java parameter style derby no sql\n" +
              "external name 'Foo.foo'\n"
              );

        // need at least one parameter in order to use varargs
        expectCompilationError
            ( SYNTAX_ERROR,
              "create function varargsDerbyStyleNoParam( ... ) returns int\n" +
              "parameter style derby language java no sql\n" +
              "external name 'Foo.foo'\n"
              );
        expectCompilationError
            ( SYNTAX_ERROR,
              "create procedure varargsDerbyStyleNoParam( ... )\n" +
              "language java parameter style derby no sql\n" +
              "external name 'Foo.foo'\n"
              );

        // bad because returns result sets
        expectCompilationError
            ( RETURNS_RESULT_SETS,
              "create procedure varargsDerbyStyle( a int ... )\n" +
              "language java parameter style derby no sql result sets 1\n" +
              "external name 'Foo.foo'\n"
              );
    }

    /**
     * <p>
     * Simple invocations to verify that varargs routines can be invoked.
     * </p>
     */
    public void test_02_simple() throws Exception
    {
        if ( !vmSupportsVarargs() ) { return; }
        
        Connection conn = getConnection();

        goodStatement
            ( conn,
              "create function maximum( a int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.max'\n"
              );
        goodStatement
            ( conn,
              "create function formatMessage( message varchar( 32672 ),  args varchar( 32672 ) ... ) returns varchar( 32672 )\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.formatMessage'\n"
              );

        // 0 args
        assertResults
            (
             conn,
             "values maximum()",
             new String[][]
             {
                 { null },
             },
             false
             );
        
        // a null argument
        assertResults
            (
             conn,
             "values maximum( null )",
             new String[][]
             {
                 { null },
             },
             false
             );
        
        // one non-null argument
        assertResults
            (
             conn,
             "values maximum( 1 )",
             new String[][]
             {
                 { "1" },
             },
             false
             );
         
        // multiple arguments
        assertResults
            (
             conn,
             "values maximum( 1, 3, 2 )",
             new String[][]
             {
                 { "3" },
             },
             false
             );
         
        // verify that arguments are passed in the correct order
        assertResults
            (
             conn,
             "values formatMessage( 'First {0} then {1} then {2}', 'one', 'two', 'three' )",
             new String[][]
             {
                 { "First one then two then three" },
             },
             false
             );
    }

    /**
     * <p>
     * Misc tests for varargs routines.
     * </p>
     */
    public void test_03_misc() throws Exception
    {
        if ( !vmSupportsVarargs() ) { return; }

        Connection conn = getConnection();

        // primitive and wrapper overloads make method resolution ambiguous

        goodStatement
            ( conn,
              "create function ambiguousTypes( a int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.ambiguousTypes'\n"
              );
        expectCompilationError( AMBIGUOUS, "values ambiguousTypes( 1, 2, 3 )" );

        // can resolve to a primitive-typed vararg
        goodStatement
            ( conn,
              "create function maxInts( a int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.maxInts'\n"
              );
        assertResults
            (
             conn,
             "values maxInts( 3 )",
             new String[][]
             {
                 { "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "values maxInts( 1, 2, 5, 4, 3 )",
             new String[][]
             {
                 { "5" },
             },
             false
             );

        // error if the matching method isn't varargs
        goodStatement
            ( conn,
              "create function nonVarargsMethod( a int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.nonVarargsMethod'\n"
              );
        expectCompilationError( NO_SUCH_METHOD, "values nonVarargsMethod( 3 )" );
        
        // correctly disambiguate similar varargs and non-varargs methods
        goodStatement
            ( conn,
              "create function vnvr_vararg( a int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.vnvr'\n"
              );
        goodStatement
            ( conn,
              "create function vnvr_nonvararg( a int ) returns int\n" +
              "language java parameter style java no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.vnvr'\n"
              );
        assertResults
            (
             conn,
             "values vnvr_vararg( 3 )",
             new String[][]
             {
                 { "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "values vnvr_nonvararg( 3 )",
             new String[][]
             {
                 { "-3" },
             },
             false
             );
        
        // correctly disambiguate overloads with different numbers of leading non-vararg arguments
        goodStatement
            ( conn,
              "create function lnv( a int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.lnv'\n"
              );
        goodStatement
            ( conn,
              "create function lnv_1( a int, b int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.lnv'\n"
              );
        goodStatement
            ( conn,
              "create function lnv_2( a int, b int, c int ... ) returns int\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.lnv'\n"
              );
        assertResults
            (
             conn,
             "values lnv( 5, 4, 3, 2, 1 )",
             new String[][]
             {
                 { "5" },
             },
             false
             );
        assertResults
            (
             conn,
             "values lnv_1( 5, 4, 3, 2, 1 )",
             new String[][]
             {
                 { "4" },
             },
             false
             );
        assertResults
            (
             conn,
             "values lnv_2( 5, 4, 3, 2, 1 )",
             new String[][]
             {
                 { "3" },
             },
             false
             );
    }
    
    /**
     * <p>
     * Test in, out, and in/out procedure arguments which are varargs.
     * </p>
     */
    public void test_04_inOut() throws Exception
    {
        if ( !vmSupportsVarargs() ) { return; }

        Connection conn = getConnection();
        CallableStatement   cs =  null;

        // one input vararg
        goodStatement
            ( conn,
              "create procedure inVarargs( out result varchar( 32672 ), b int ... )\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.inVarargs'\n"
              );
        cs = chattyPrepareCall
            ( conn, "call inVarargs( ?, ? )" );
        cs.registerOutParameter( 1, java.sql.Types.VARCHAR );
        cs.setInt( 2, 5 );
        cs.execute();
        assertEquals( "RESULT:  5", cs.getString( 1 ) );

        cs = chattyPrepareCall
            ( conn, "call inVarargs( ?, ?, ? )" );
        cs.registerOutParameter( 1, java.sql.Types.VARCHAR );
        cs.setInt( 2, 5 );
        cs.setInt( 3, 4 );
        cs.execute();
        assertEquals( "RESULT:  5 4", cs.getString( 1 ) );

        cs = chattyPrepareCall
            ( conn, "call inVarargs( ?, ?, ?, ? )" );
        cs.registerOutParameter( 1, java.sql.Types.VARCHAR );
        cs.setInt( 2, 5 );
        cs.setInt( 3, 4 );
        cs.setInt( 4, 3 );
        cs.execute();
        assertEquals( "RESULT:  5 4 3", cs.getString( 1 ) );

        // output vararg
        goodStatement
            ( conn,
              "create procedure outVarargs( seed int, out b int ... )\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.outVarargs'\n"
              );
        cs = chattyPrepareCall
            ( conn, "call outVarargs( ? )" );
        cs.setInt( 1, 5 );
        cs.execute();

        cs = chattyPrepareCall
            ( conn, "call outVarargs( ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.INTEGER );
        cs.setInt( 1, 5 );
        cs.execute();
        assertEquals( 5, cs.getInt( 2 ) );

        cs = chattyPrepareCall
            ( conn, "call outVarargs( ?, ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.INTEGER );
        cs.registerOutParameter( 3, java.sql.Types.INTEGER );
        cs.setInt( 1, 5 );
        cs.execute();
        assertEquals( 5, cs.getInt( 2 ) );
        assertEquals( 6, cs.getInt( 3 ) );

        cs = chattyPrepareCall
            ( conn, "call outVarargs( ?, ?, ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.INTEGER );
        cs.registerOutParameter( 3, java.sql.Types.INTEGER );
        cs.registerOutParameter( 4, java.sql.Types.INTEGER );
        cs.setInt( 1, 5 );
        cs.execute();
        assertEquals( 5, cs.getInt( 2 ) );
        assertEquals( 6, cs.getInt( 3 ) );
        assertEquals( 7, cs.getInt( 4 ) );

        // in/out vararg
        goodStatement
            ( conn,
              "create procedure inoutVarargs( seed int, inout b int ... )\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.inoutVarargs'\n"
              );
        cs = chattyPrepareCall
            ( conn, "call inoutVarargs( ? )" );
        cs.setInt( 1, 5 );
        cs.execute();

        cs = chattyPrepareCall
            ( conn, "call inoutVarargs( ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.INTEGER );
        cs.setInt( 1, 5 );
        cs.setInt( 2, 3 );
        cs.execute();
        assertEquals( 8, cs.getInt( 2 ) );

        cs = chattyPrepareCall
            ( conn, "call inoutVarargs( ?, ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.INTEGER );
        cs.registerOutParameter( 3, java.sql.Types.INTEGER );
        cs.setInt( 1, 5 );
        cs.setInt( 2, 3 );
        cs.setInt( 3, 10 );
        cs.execute();
        assertEquals( 8, cs.getInt( 2 ) );
        assertEquals( 15, cs.getInt( 3 ) );

        cs = chattyPrepareCall
            ( conn, "call inoutVarargs( ?, ?, ?, ? )" );
        cs.registerOutParameter( 2, java.sql.Types.INTEGER );
        cs.registerOutParameter( 3, java.sql.Types.INTEGER );
        cs.registerOutParameter( 4, java.sql.Types.INTEGER );
        cs.setInt( 1, 5 );
        cs.setInt( 2, 3 );
        cs.setInt( 3, 10 );
        cs.setInt( 4, 100 );
        cs.execute();
        assertEquals( 8, cs.getInt( 2 ) );
        assertEquals( 15, cs.getInt( 3 ) );
        assertEquals( 105, cs.getInt( 4 ) );

    }
    
    /**
     * <p>
     * Test varargs with all datatypes.
     * </p>
     */
    public void test_05_datatypes() throws Exception
    {
        if ( !vmSupportsVarargs() ) { return; }

        Connection conn = getConnection();

        goodStatement
            ( conn,
              "create function makeBlob( a varchar( 32672 ) ) returns blob\n" +
              "language java parameter style java no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.makeBlob'\n"
              );
        goodStatement
            ( conn,
              "create function makeBytes( a varchar( 32672 ) ) returns char(1) for bit data\n" +
              "language java parameter style java no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.makeBytes'\n"
              );
        goodStatement
            ( conn,
              "create function makeVarBytes( a varchar( 32672 ) ) returns varchar(10) for bit data\n" +
              "language java parameter style java no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.makeBytes'\n"
              );
        goodStatement
            ( conn,
              "create function makeLongVarBytes( a varchar( 32672 ) ) returns long varchar for bit data\n" +
              "language java parameter style java no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.makeBytes'\n"
              );
        goodStatement
            ( conn,
              "create function makeClob( a varchar( 32672 ) ) returns clob\n" +
              "language java parameter style java no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.makeClob'\n"
              );
        goodStatement( conn, "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n" );
        goodStatement
            ( conn,
              "create function makePrice( currencyCode char( 3 ), amount decimal( 31, 5 ), timeInstant Timestamp )\n" +
              "returns Price language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice'\n");

        vetDatatype( conn, "boolean", "boolean", "true", "false", "true", "1", "1", "2" );

        vetDatatype( conn, "long", "bigint", "1", "2", "3", "1", "3", "6" );
        vetDatatype( conn, "int", "int", "1", "2", "3", "1", "3", "6" );
        vetDatatype( conn, "short", "smallint", "1", "2", "3", "1", "3", "6" );

        vetDatatype( conn, "bigdecimal", "decimal(5,2)", "1.0", "2.0", "3.0", "1", "3", "6" );
        vetDatatype( conn, "bigdecimal", "numeric(5,2)", "1.0", "2.0", "3.0", "1", "3", "6" );

        vetDatatype( conn, "double", "double", "1.0", "2.0", "3.0", "1.0", "3.0", "6.0" );
        vetDatatype( conn, "float", "real", "1.0", "2.0", "3.0", "1.0", "3.0", "6.0" );
        vetDatatype( conn, "double", "float", "1.0", "2.0", "3.0", "1.0", "3.0", "6.0" );

        vetDatatype( conn, "blob", "blob", "makeBlob( '1' )", "makeBlob( '2' )", "makeBlob( '3' )", "1", "3", "6" );
        vetDatatype( conn, "clob", "clob", "makeClob( '1' )", "makeClob( '2' )", "makeClob( '3' )", "1", "12", "123" );

        vetDatatype( conn, "byte[]", "char(1) for bit data", "makeBytes( '1' )", "makeBytes( '2' )", "makeBytes( '3' )", "1", "3", "6" );
        vetDatatype( conn, "byte[]", "varchar(10) for bit data", "makeVarBytes( '1' )", "makeVarBytes( '2' )", "makeVarBytes( '3' )", "1", "3", "6" );
        vetDatatype( conn, "byte[]", "long varchar for bit data", "makeLongVarBytes( '1' )", "makeLongVarBytes( '2' )", "makeLongVarBytes( '3' )", "1", "3", "6" );

        vetDatatype( conn, "String", "char(1)", "'1'", "'2'", "'3'", "1", "12", "123" );
        vetDatatype( conn, "String", "varchar(10)", "'1'", "'2'", "'3'", "1", "12", "123" );
        vetDatatype( conn, "String", "long varchar", "'1'", "'2'", "'3'", "1", "12", "123" );

        vetDatatype( conn, "date", "date", "date('1994-02-23')", "date('1995-02-23')", "date('1996-02-23')", "1", "2", "3" );
        vetDatatype( conn, "time", "time", "time('15:09:02')", "time('14:09:02')", "time('13:09:02')", "1", "2", "3" );
        vetDatatype( conn, "timestamp", "timestamp", "timestamp('1962-09-23 03:23:34.234')", "timestamp('1963-09-23 03:23:34.234')", "timestamp('1964-09-23 03:23:34.234')", "1", "2", "3" );

        vetDatatype
            (
             conn, "Price", "Price",
             "makePrice( 'USD', cast( 9.99 as decimal( 31, 5 ) ), timestamp('2009-10-16 14:24:43') )",
             "makePrice( 'USD', cast( 10.99 as decimal( 31, 5 ) ), timestamp('2009-10-16 14:24:43') )",
             "makePrice( 'USD', cast( 11.99 as decimal( 31, 5 ) ), timestamp('2009-10-16 14:24:43') )",
             "1", "2", "3" );

        //
        // Check that implicit coercions work correctly
        //
        goodStatement
            ( conn,
              "create table all_types\n" +
              "(\n" +
              "    bigint_col  bigint,\n" +
              "    blob_col    blob,\n" +
              "    boolean_col boolean,\n" +
              "    char_col    char( 1 ),\n" +
              "    char_for_bit_data_col   char( 1 ) for bit data,\n" +
              "    clob_col    clob,\n" +
              "    date_col    date,\n" +
              "    decimal_col decimal,\n" +
              "    double_col  double,\n" +
              "    float_col   float,\n" +
              "    integer_col int,\n" +
              "    long_varchar_col    long varchar,\n" +
              "    long_varchar_for_bit_data_col   long varchar for bit data,\n" +
              "    numeric_col    numeric,\n" +
              "    real_col       real,\n" +
              "    smallint_col   smallint,\n" +
              "    time_col       time,\n" +
              "    timestamp_col  timestamp,\n" +
              "    varchar_col    varchar( 5 ),\n" +
              "    varchar_for_bit_data_col  varchar( 10 ) for bit data,\n" +
              "    price_col    price\n" +
              ")\n"
              );
        goodStatement
            ( conn,
              "insert into all_types values\n" +
              "(\n" +
              "    1,\n" +
              "    makeBlob( '1' ),\n" +
              "    true,\n" +
              "    '1',\n" +
              "    makeBytes( '1' ),\n" +
              "    makeClob( '1' ),\n" +
              "    date( '1994-02-23' ),\n" +
              "    1.0,\n" +
              "    1.0,\n" +
              "    1.0,\n" +
              "    1,\n" +
              "    '1',\n" +
              "    makeLongVarBytes( '1' ),\n" +
              "    1.0,\n" +
              "    1.0,\n" +
              "    1,\n" +
              "    time( '15:09:02' ),\n" +
              "    timestamp( '1962-09-23 03:23:34.234' ),\n" +
              "    '1',\n" +
              "    makeVarBytes( '1' ),\n" +
              "    makePrice( 'USD', cast( 9.99 as decimal( 31, 5 ) ), timestamp('2009-10-16 14:24:43') )\n" +
              ")\n"
              );
        
        vetNumericCoercions( conn, "long", "bigint", "1" );
        vetNumericCoercions( conn, "int", "int", "1" );
        vetNumericCoercions( conn, "short", "smallint", "1" );
        vetNumericCoercions( conn, "bigdecimal", "decimal", "1" );
        vetNumericCoercions( conn, "bigdecimal", "numeric", "1" );
        vetNumericCoercions( conn, "float", "real", "1.0" );
        vetNumericCoercions( conn, "double", "double", "1.0" );
        vetNumericCoercions( conn, "double", "float", "1.0" );

        vetStringCoercions( conn, "String", "char(50)" );
        vetStringCoercions( conn, "clob", "clob" );
        vetStringCoercions( conn, "String", "varchar(50)" );
        vetStringCoercions( conn, "String", "long varchar" );

        vetTimeCoercions( conn, "date", "date" );
        vetTimeCoercions( conn, "time", "time" );
        vetTimeCoercions( conn, "timestamp", "timestamp" );

        vetBinaryCoercions( conn, "byte[]", "char(1) for bit data" );
        vetBinaryCoercions( conn, "byte[]", "varchar(10) for bit data" );
        vetBinaryCoercions( conn, "byte[]", "long varchar for bit data" );

        vetBooleanCoercions( conn, "boolean", "boolean" );

        vetPriceCoercions( conn, "Price", "Price" );
    }
    private void    vetDatatype
        (
         Connection conn,
         String javatype,
         String sqltype,
         String arg1,
         String arg2,
         String arg3,
         String result1,
         String result2,
         String result3
         )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        // no varargs
        assertResults
            (
             conn,
             "values addFunc( '" + sqltype + "' )",
             new String[][]
             {
                 { null },
             },
             false
             );

        // one vararg
        assertResults
            (
             conn,
             "values addFunc( '" + sqltype + "', " + arg1 + " )",
             new String[][]
             {
                 { javatype + " " + sqltype + " " + result1 },
             },
             false
             );

        // two varargs
        assertResults
            (
             conn,
             "values addFunc( '" + sqltype + "', " + arg1 + ", " + arg2 + " )",
             new String[][]
             {
                 { javatype + " " + sqltype + " " + result2 },
             },
             false
             );

        // three varargs
        assertResults
            (
             conn,
             "values addFunc( '" + sqltype + "', " + arg1 + ", " + arg2 + ", " + arg3 + " )",
             new String[][]
             {
                 { javatype + " " + sqltype + " " + result3 },
             },
             false
             );

        dropAddFunc( conn );
    }
    private void    createAddFunc( Connection conn, String sqltype )
        throws Exception
    {
        goodStatement
            ( conn,
              "create function addFunc( seed varchar( 50 ), a " + sqltype + " ... ) returns varchar( 50 )\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.add'\n"
              );
    }
    private void    dropAddFunc( Connection conn )  throws Exception
    {
        goodStatement( conn, "drop function addFunc" );
    }

    private void    vetNumericCoercions
        ( Connection conn, String javatype, String sqltype, String expectedValue )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        vetGoodCoercion( conn, javatype, sqltype, "bigint_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "decimal_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "double_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "float_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "integer_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "numeric_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "real_col", expectedValue );
        vetGoodCoercion( conn, javatype, sqltype, "smallint_col", expectedValue );

        vetBadCoercion( conn, sqltype, "blob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "boolean_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "char_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "char_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "clob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "date_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "time_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "timestamp_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "price_col", ILLEGAL_STORAGE );

        dropAddFunc( conn );
    }
    private void    vetGoodCoercion
        ( Connection conn, String javatype, String sqltype, String colname, String expectedValue )
        throws Exception
    {
        assertResults
            (
             conn,
             "select addFunc( '" + sqltype + "', " + colname + " ) from all_types",
             new String[][]
             {
                 { javatype + " " + sqltype + " " + expectedValue },
             },
             true
             );
    }
    private void    vetBadCoercion
        ( Connection conn, String sqltype, String colname, String sqlstate )
        throws Exception
    {
        expectCompilationError( sqlstate, "select addFunc( '" + sqltype + "', " + colname + " ) from all_types" );
    }
    
    private void    vetStringCoercions
        ( Connection conn, String javatype, String sqltype )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        boolean isClob = javatype.equals( "clob" ) || sqltype.equals( "long varchar" );

        vetGoodCoercion( conn, javatype, sqltype, "char_col", "1" );
        vetGoodCoercion( conn, javatype, sqltype, "clob_col", "1" );
        vetGoodCoercion( conn, javatype, sqltype, "long_varchar_col", "1" );
        vetGoodCoercion( conn, javatype, sqltype, "varchar_col", "1" );

        vetGoodCoercion( conn, javatype, sqltype, "boolean_col", "true" );

        if ( isClob )
        {
            vetBadCoercion( conn, sqltype, "date_col", ILLEGAL_STORAGE );
            vetBadCoercion( conn, sqltype, "time_col", ILLEGAL_STORAGE );
            vetBadCoercion( conn, sqltype, "timestamp_col", ILLEGAL_STORAGE );
        }
        else
        {
            vetGoodCoercion( conn, javatype, sqltype, "date_col", "1994-02-23" );
            vetGoodCoercion( conn, javatype, sqltype, "time_col", "15:09:02" );
            vetGoodCoercion( conn, javatype, sqltype, "timestamp_col", "1962-09-23 03:23:34.234" );
        }
        
        vetBadCoercion( conn, sqltype, "bigint_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "decimal_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "double_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "float_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "integer_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "numeric_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "real_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "smallint_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "blob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "char_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "price_col", ILLEGAL_STORAGE );

        dropAddFunc( conn );
    }

    private void    vetTimeCoercions
        ( Connection conn, String javatype, String sqltype )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        if ( javatype.equals( "date" ) )
        {
            vetGoodCoercion( conn, javatype, sqltype, "date_col", "1" );
        }
        else
        {
            vetBadCoercion( conn, sqltype, "date_col", ILLEGAL_STORAGE );
        }

        if ( javatype.equals( "time" ) )
        {
            vetGoodCoercion( conn, javatype, sqltype, "time_col", "1" );
        }
        else
        {
            vetBadCoercion( conn, sqltype, "time_col", ILLEGAL_STORAGE );
        }

        if ( javatype.equals( "timestamp" ) )
        {
            vetGoodCoercion( conn, javatype, sqltype, "timestamp_col", "1" );
        }
        else
        {
            vetBadCoercion( conn, sqltype, "timestamp_col", ILLEGAL_STORAGE );
        }

        vetBadFormat( conn, sqltype, "char_col", BAD_TIME_FORMAT );
        vetBadFormat( conn, sqltype, "varchar_col", BAD_TIME_FORMAT );

        vetBadCoercion( conn, sqltype, "clob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "boolean_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "bigint_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "decimal_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "double_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "float_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "integer_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "numeric_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "real_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "smallint_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "blob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "char_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "price_col", ILLEGAL_STORAGE );

        dropAddFunc( conn );
    }
    private void    vetBadFormat
        ( Connection conn, String sqltype, String colname, String sqlstate )
        throws Exception
    {
        expectExecutionError( conn, sqlstate, "select addFunc( '" + sqltype + "', " + colname + " ) from all_types" );
    }

    private void    vetBinaryCoercions
        ( Connection conn, String javatype, String sqltype )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        vetGoodCoercion( conn, javatype, sqltype, "char_for_bit_data_col", "1" );
        vetGoodCoercion( conn, javatype, sqltype, "varchar_for_bit_data_col", "1" );
        vetGoodCoercion( conn, javatype, sqltype, "long_varchar_for_bit_data_col", "1" );

        vetBadCoercion( conn, sqltype, "char_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "clob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "date_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "time_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "timestamp_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "boolean_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "bigint_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "decimal_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "double_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "float_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "integer_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "numeric_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "real_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "smallint_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "blob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "price_col", ILLEGAL_STORAGE );

        dropAddFunc( conn );
    }

    private void    vetBooleanCoercions
        ( Connection conn, String javatype, String sqltype )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        vetGoodCoercion( conn, javatype, sqltype, "boolean_col", "1" );

        vetBadFormat( conn, sqltype, "char_col", BAD_BOOLEAN_FORMAT );
        vetBadFormat( conn, sqltype, "varchar_col", BAD_BOOLEAN_FORMAT );
        vetBadFormat( conn, sqltype, "clob_col", BAD_BOOLEAN_FORMAT );
        vetBadFormat( conn, sqltype, "long_varchar_col", BAD_BOOLEAN_FORMAT );

        vetBadCoercion( conn, sqltype, "char_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_for_bit_data_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "date_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "time_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "timestamp_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "bigint_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "decimal_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "double_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "float_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "integer_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "numeric_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "real_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "smallint_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "blob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "price_col", ILLEGAL_STORAGE );

        dropAddFunc( conn );
    }

    private void    vetPriceCoercions
        ( Connection conn, String javatype, String sqltype )
        throws Exception
    {
        createAddFunc( conn, sqltype );

        vetGoodCoercion( conn, javatype, sqltype, "price_col", "1" );

        vetBadCoercion( conn, sqltype, "char_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "clob_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "boolean_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "char_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "varchar_for_bit_data_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "long_varchar_for_bit_data_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "date_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "time_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "timestamp_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "bigint_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "decimal_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "double_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "float_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "integer_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "numeric_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "real_col", ILLEGAL_STORAGE );
        vetBadCoercion( conn, sqltype, "smallint_col", ILLEGAL_STORAGE );

        vetBadCoercion( conn, sqltype, "blob_col", ILLEGAL_STORAGE );

        dropAddFunc( conn );
    }

    /**
     * <p>
     * Test vararg table functions.
     * </p>
     */
    public void test_06_tableFunctions() throws Exception
    {
        if ( !vmSupportsVarargs() ) { return; }

        Connection conn = getConnection();

        // although varargs are allowed with table functions, the parameter style
        // must still be derby_jdbc_result_set
        expectCompilationError
            ( NEEDS_DJRS_STYLE,
              "create function tf_derby( rowValues varchar( 32672 ) )\n" +
              "returns table\n" +
              "(\n" +
              "    col1 varchar( 32672 )\n" +
              ")\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.oneColumnRows'\n"
              );
        expectCompilationError
            ( NEEDS_DJRS_STYLE,
              "create function tf_derby_varargs( rowValues varchar( 32672 ) ... )\n" +
              "returns table\n" +
              "(\n" +
              "    col1 varchar( 32672 )\n" +
              ")\n" +
              "language java parameter style derby no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.oneColumnRows'\n"
              );

        // vararg table functions ok if the parameter style is derby_jdbc_result_set
        goodStatement
            ( conn,
              "create function oneColumnRows( rowValues varchar( 32672 ) ... )\n" +
              "returns table\n" +
              "(\n" +
              "    col1 varchar( 32672 )\n" +
              ")\n" +
              "language java parameter style derby_jdbc_result_set no sql deterministic\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.oneColumnRows'\n"
              );
        assertResults
            (
             conn,
             "select * from table( oneColumnRows() ) s",
             new String[][]
             {
             },
             false
             );
        assertResults
            (
             conn,
             "select * from table( oneColumnRows( 'one' ) ) s",
             new String[][]
             {
                 { "one" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from table( oneColumnRows( 'one', 'two' ) ) s",
             new String[][]
             {
                 { "one" },
                 { "two" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from table( oneColumnRows( 'one', 'two', 'three' ) ) s",
             new String[][]
             {
                 { "one" },
                 { "two" },
                 { "three" },
             },
             false
             );

    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if the VM supports vararg methods */
    private boolean vmSupportsVarargs() { return JDBC.vmSupportsJDBC3(); }

}
