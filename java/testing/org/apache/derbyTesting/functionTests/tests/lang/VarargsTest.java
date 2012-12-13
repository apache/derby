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
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if the VM supports vararg methods */
    private boolean vmSupportsVarargs() { return JDBC.vmSupportsJDBC3(); }

}
