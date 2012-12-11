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

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if the VM supports vararg methods */
    private boolean vmSupportsVarargs() { return JDBC.vmSupportsJDBC3(); }

}
