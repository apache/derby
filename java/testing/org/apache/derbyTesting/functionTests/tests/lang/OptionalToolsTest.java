/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.OptionalToolsTest

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
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test optional tools. See DERBY-6022.
 * </p>
 */
public class OptionalToolsTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected   static  final   String  NO_SUCH_TABLE_FUNCTION = "42ZB4";
    protected   static  final   String  UNEXPECTED_USER_EXCEPTION = "38000";
    protected   static  final   String  MISSING_SCHEMA = "42Y07";
    protected   static  final   String  UNKNOWN_TOOL = "X0Y88";
    protected   static  final   String  UNKNOWN_ROUTINE = "42Y03";

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK  };

    private static  final   String      FOREIGN_DB = "foreignDB";

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

    public OptionalToolsTest(String name)
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
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            OptionalToolsTest.class);

        Test        test = DatabasePropertyTestSetup.builtinAuthentication
            ( suite, LEGAL_USERS, "optionalToolsPermissions" );

        test = TestConfiguration.sqlAuthorizationDecorator( test );
        test = TestConfiguration.additionalDatabaseDecorator( test, FOREIGN_DB );

        return test;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the optional package of routines which wrap the DatabaseMetaData methods.
     * </p>
     */
    public void test_01_dbmdWrapper() throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        String  getTypeInfo = "select type_name, minimum_scale, maximum_scale from table( getTypeInfo() ) s";

        // only the dbo can register tools
        expectExecutionError
            (
             ruthConnection,
             LACK_EXECUTE_PRIV,
             "call syscs_util.syscs_register_tool( 'databaseMetaData', true )"
             );

        // create a dummy table just to force the schema to be created
        goodStatement( dboConnection, "create table t( a int )" );

        // the routines don't exist unless you register them
        expectCompilationError( dboConnection, NO_SUCH_TABLE_FUNCTION, getTypeInfo );

        // now register the database metadata wrappers
        goodStatement( dboConnection, "call syscs_util.syscs_register_tool( 'databaseMetaData', true )" );

        // now the routine exists
        assertResults
            (
             dboConnection,
             getTypeInfo,
             new String[][]
             {
                 { "BIGINT", "0", "0" },
                 { "LONG VARCHAR FOR BIT DATA", null, null },
                 { "VARCHAR () FOR BIT DATA", null, null },
                 { "CHAR () FOR BIT DATA", null, null },
                 { "LONG VARCHAR", null, null },
                 { "CHAR", null, null },
                 { "NUMERIC", "0", "31" },
                 { "DECIMAL", "0", "31" },
                 { "INTEGER", "0", "0" },
                 { "SMALLINT", "0", "0" },
                 { "FLOAT", null, null },
                 { "REAL", null, null },
                 { "DOUBLE", null, null },
                 { "VARCHAR", null, null },
                 { "BOOLEAN", null, null },
                 { "DATE", "0", "0" },
                 { "TIME", "0", "0" },
                 { "TIMESTAMP", "0", "9" },
                 { "OBJECT", null, null },
                 { "BLOB", null, null }, 
                 { "CLOB", null, null },
                 { "XML", null, null },
             },
             false
             );

        // now unregister the database metadata wrappers
        goodStatement( dboConnection, "call syscs_util.syscs_register_tool( 'databaseMetaData', false )" );

        // the routines don't exist anymore
        expectCompilationError( dboConnection, NO_SUCH_TABLE_FUNCTION, getTypeInfo );
    }
    
    /**
     * <p>
     * Test the optional package of views on an external database.
     * </p>
     */
    public void test_02_foreignDBViews() throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  foreignFrankConnection = getTestConfiguration().openConnection( FOREIGN_DB, FRANK, FRANK );
        Connection  foreignAliceConnection = getTestConfiguration().openConnection( FOREIGN_DB, ALICE, ALICE );

        //
        // Create the foreign database.
        //
        goodStatement
            (
             foreignFrankConnection,
             "create table employee\n" +
             "(\n" +
             "    firstName   varchar( 50 ),\n" +
             "    lastName    varchar( 50 ),\n" +
             "    employeeID  int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             foreignFrankConnection,
             "insert into employee values ( 'Billy', 'Goatgruff', 1 )\n"
             );
        goodStatement
            (
             foreignFrankConnection,
             "insert into employee values ( 'Mary', 'Hadalittlelamb', 2 )\n"
             );
        goodStatement
            (
             foreignAliceConnection,
             "create table stars\n" +
             "(\n" +
             "    name   varchar( 50 ),\n" +
             "    magnitude int,\n" +
             "    starID  int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             foreignAliceConnection,
             "insert into stars values ( 'Polaris', 100, 1 )\n"
             );
        
        // now work in the database where we will create views
        String      foreignURL = "jdbc:derby:" +
            getTestConfiguration().getPhysicalDatabaseName( FOREIGN_DB ) +
            ";user=" + TEST_DBO + ";password=" + TEST_DBO;
        String      employeeSelect = "select * from frank.employee order by employeeID";
        String      starSelect = "select * from alice.stars order by starID";
        String[][]   employeeResult = new String[][]
            {
                { "Billy", "Goatgruff", "1" },
                { "Mary", "Hadalittlelamb", "2" },
            };
        String[][]  starResult = new String[][]
            {
                { "Polaris", "100", "1" },
            };

        // create a function to count the number of connections
        // managed by ForeignTableVTI
        goodStatement
            (
             dboConnection,
             "create function countConnections() returns int\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derby.vti.ForeignTableVTI.countConnections'\n"
             );

        // wrong number of arguments
        expectExecutionError
            (
             dboConnection,
             UNEXPECTED_USER_EXCEPTION,
             "call syscs_util.syscs_register_tool( 'foreignViews', true )"
             );

        // should fail because the view and its schema don't exist
        expectCompilationError
            (
             dboConnection,
             MISSING_SCHEMA,
             employeeSelect
             );
        expectCompilationError
            (
             dboConnection,
             MISSING_SCHEMA,
             starSelect
             );

        // should work
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'foreignViews', true, '" + foreignURL + "' )"
             );

        // views should have been created against the foreign database
        assertResults
            (
             dboConnection,
             employeeSelect,
             employeeResult,
             false
             );
        assertResults
            (
             dboConnection,
             starSelect,
             starResult,
             false
             );
        assertResults
            (
             dboConnection,
             "values countConnections()",
             new String[][] { { "1" } },
             false
             );
        
        // wrong number of arguments
        expectExecutionError
            (
             dboConnection,
             UNEXPECTED_USER_EXCEPTION,
             "call syscs_util.syscs_register_tool( 'foreignViews', false )"
             );

        // should work
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'foreignViews', false, '" + foreignURL + "' )"
             );
        assertResults
            (
             dboConnection,
             "values countConnections()",
             new String[][] { { "0" } },
             false
             );

        // should fail because the view and its schema were dropped when the tool was unloaded
        expectCompilationError
            (
             dboConnection,
             MISSING_SCHEMA,
             employeeSelect
             );
        expectCompilationError
            (
             dboConnection,
             MISSING_SCHEMA,
             starSelect
             );

        // unregistration should be idempotent
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'foreignViews', false, '" + foreignURL + "' )"
             );

        // register with a schema prefix
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'foreignViews', true, '" + foreignURL + "', 'XYZ_' )"
             );
        employeeSelect = "select * from xyz_frank.employee order by employeeID";
        starSelect = "select * from xyz_alice.stars order by starID";

        // views should have been created against the foreign database
        assertResults
            (
             dboConnection,
             employeeSelect,
             employeeResult,
             false
             );
        assertResults
            (
             dboConnection,
             starSelect,
             starResult,
             false
             );

        // drop the views
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'foreignViews', false, '" + foreignURL + "', 'XYZ_' )"
             );
        expectCompilationError
            (
             dboConnection,
             MISSING_SCHEMA,
             employeeSelect
             );
        expectCompilationError
            (
             dboConnection,
             MISSING_SCHEMA,
             starSelect
             );
        assertResults
            (
             dboConnection,
             "values countConnections()",
             new String[][] { { "0" } },
             false
             );
        goodStatement( dboConnection, "drop function countConnections" );
    }

    /**
     * <p>
     * Test loading custom, user-supplied tools.
     * </p>
     */
    public void test_03_customTool() throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        // unknown tool name
        expectExecutionError
            (
             dboConnection,
             UNKNOWN_TOOL,
             "call syscs_util.syscs_register_tool( 'uknownToolName', true )"
             );

        // no custom class name supplied
        expectExecutionError
            (
             dboConnection,
             UNKNOWN_TOOL,
             "call syscs_util.syscs_register_tool( 'customTool', true )"
             );

        // supplied class does not implement OptionalTool
        expectExecutionError
            (
             dboConnection,
             UNKNOWN_TOOL,
             "call syscs_util.syscs_register_tool( 'customTool', true, 'java.lang.String' )"
             );

        //
        // Register a custom tool.
        //

        // first verify that the tool hasn't been run yet
        expectCompilationError
            (
             dboConnection,
             UNKNOWN_ROUTINE,
             "values toString( 100 )"
             );

        // now register the tool
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'customTool', true, 'org.apache.derbyTesting.functionTests.tests.lang.OptionalToolExample' )"
             );

        // run it
        assertResults
            (
             dboConnection,
             "values toString( 100 )",
             new String[][]
             {
                 { "100" },
             },
             false
             );

        // unregister the tool
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'customTool', false, 'org.apache.derbyTesting.functionTests.tests.lang.OptionalToolExample' )"
             );

        // verify that the tool was unregistered
        expectCompilationError
            (
             dboConnection,
             UNKNOWN_ROUTINE,
             "values toString( 100 )"
             );

        //
        // Register a custom tool with a custom parameter.
        //

        // first verify that the tool hasn't been run yet
        expectCompilationError
            (
             dboConnection,
             UNKNOWN_ROUTINE,
             "values foobar( 100 )"
             );

        // now register the tool
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool\n" +
             "(\n" +
             "    'customTool',\n" +
             "    true,\n" +
             "    'org.apache.derbyTesting.functionTests.tests.lang.OptionalToolExample$VariableName',\n" +
             "    'foobar'\n" +
             ")\n"
             );

        // run it
        assertResults
            (
             dboConnection,
             "values foobar( 100 )",
             new String[][]
             {
                 { "100" },
             },
             false
             );

        // unregister the tool
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool\n" +
             "(\n" +
             "    'customTool',\n" +
             "    false,\n" +
             "    'org.apache.derbyTesting.functionTests.tests.lang.OptionalToolExample$VariableName',\n" +
             "    'foobar'\n" +
             ")\n"
             );

        // verify that the tool was unregistered
        expectCompilationError
            (
             dboConnection,
             UNKNOWN_ROUTINE,
             "values foobar( 100 )"
             );

    }
    
    /**
     * <p>
     * Test loading a customized optimizer tracer. See DERBY-6211.
     * </p>
     */
    public void test_04_customOptimizerTrace() throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        goodStatement
            (
             dboConnection,
             "create function fullTrace() returns varchar( 32672 )\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.DummyOptTrace.fullTrace'\n"
             );

        // install a custom tracer for the optimizer
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool\n" +
             "(\n" +
             "    'optimizerTracing', true, 'custom',\n" +
             "    'org.apache.derbyTesting.functionTests.tests.lang.DummyOptTrace'\n" +
             ")\n"
             );
        // run a couple queries
        goodStatement
            (
             dboConnection,
             "select tablename from sys.systables where 1=2"
             );
        goodStatement
            (
             dboConnection,
             "select columnname from sys.syscolumns where 1=2"
             );
        // unload the tracer
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', false )"
             );

        // verify that it actually did something
        assertResults
            (
             dboConnection,
             "values fullTrace()",
             new String[][]
             {
                 { "<text>select tablename from sys.systables where 1=2</text><text>select columnname from sys.syscolumns where 1=2</text><text>call syscs_util.syscs_register_tool( 'optimizerTracing', false )</text>" },
             },
             false
             );

        // drop the function
        goodStatement
            (
             dboConnection,
             "drop function fullTrace"
             );

        // no classname given
        expectExecutionError
            (
             dboConnection,
             UNEXPECTED_USER_EXCEPTION,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', true, 'custom' )"
             );
        // class can't be found
        expectExecutionError
            (
             dboConnection,
             UNEXPECTED_USER_EXCEPTION,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', true, 'custom', 'foo.bar.Wibble' )"
             );
        // error because class doesn't implement OptTrace
        expectExecutionError
            (
             dboConnection,
             UNEXPECTED_USER_EXCEPTION,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', true, 'custom', 'java.lang.String' )"
             );
        // error because class doesn't have a 0-arg constructor
        expectExecutionError
            (
             dboConnection,
             UNEXPECTED_USER_EXCEPTION,
             "call syscs_util.syscs_register_tool\n" +
             "(\n" +
             "    'optimizerTracing', true, 'custom',\n" +
             "    'org.apache.derbyTesting.functionTests.tests.lang.DummyOptTrace$BadSubclass'\n" +
             ")\n"
             );
    }

}
