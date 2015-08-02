/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SimpleJsonPermsTest

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

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

/**
 * <p>
 * Test permissions on objects created by the simpleJson optional tool.
 * </p>
 */
public class SimpleJsonPermsTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      DB_NAME = "simplejsonpermsdb";

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      ALICE = "ALICE";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE  };

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'simpleJson', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'simpleJson', false )";

    private static  final   String      LACK_EXECUTE_PRIV = "42504";

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

    public SimpleJsonPermsTest(String name)
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
        BaseTestSuite suite = (BaseTestSuite) TestConfiguration.embeddedSuite
            ( SimpleJsonPermsTest.class );

        Test    customTest = new SupportFilesSetup
            (
             suite,
             new String[]
             { 
                "functionTests/tests/lang/json.dat",
             }
            );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( customTest, LEGAL_USERS, "SimpleJsonPermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecoratorSingleUse( authenticatedTest, DB_NAME, true );

        return authorizedTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that the simpleJson tool granst public access to its UDT and FUNCTIONs.
     * </p>
     */
    public  void    test_001_basic()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  aliceConnection = openUserConnection( ALICE );

        // create a dummy table in order to create the schema
        goodStatement( aliceConnection, "create table t( a int )" );

        // alice does not have permission to load the tool
        expectExecutionError( aliceConnection, LACK_EXECUTE_PRIV, LOAD_TOOL );

        // but the dbo has permission
        goodStatement( dboConnection, LOAD_TOOL );

        // alice can create a table function referencing the JSONArray udt
        goodStatement
            (
             aliceConnection,
             "create function f_double( jsonArray test_dbo.JSONArray )\n" +
             "returns table\n" +
             "(\n" +
             "  str_col varchar( 10 ),\n" +
             "  bool_col boolean,\n" +
             "  num_col double\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set contains sql\n" +
             "external name 'org.apache.derby.optional.api.SimpleJsonVTI.readArray'\n"
             );

        // alice can use the simpleJson functions
        String[][]  stringResults = new String[][]
            {
                { "abc","true", "127.0" },
            };
        String[][]  fileResults = new String[][]
            {
                { "abc","true", "127.0" },
                { "def", "false", "1.2" },
                { "ghi", null, "345.67" },
                { "lmn", "true", "9.223372036854776E18" },    
            };
        
        assertResults
            (
             aliceConnection,
             "select * from table\n" +
             "(\n" +
             "    f_double\n" +
             "    (\n" +
             "        test_dbo.readArrayFromString\n" +
             "        ( '[{ \"STR_COL\" : \"abc\", \"BOOL_COL\" : true, \"NUM_COL\" : 127 }]' )\n" +
             "    )\n" +
             ") t\n",
             stringResults,
             false
             );

        PreparedStatement   ps = null;
        ResultSet           rs = null;
        File                inputFile = SupportFilesSetup.getReadOnly( "json.dat" );
        
        ps = aliceConnection.prepareStatement
            (
             "select * from table\n" +
             "( f_double( test_dbo.readArrayFromFile( ?, 'UTF-8' ) ) ) t"
             );
        ps.setString( 1, PrivilegedFileOpsForTests.getAbsolutePath( inputFile ) );
        rs = ps.executeQuery();
        assertResults( rs, fileResults, false );
        rs.close();
        ps.close();

        ps = aliceConnection.prepareStatement
            (
             "select * from table\n" +
             "( f_double( test_dbo.readArrayFromURL( ?, 'UTF-8' ) ) ) t"
             );
        String  inputFileURL = PrivilegedFileOpsForTests.toURI( inputFile ).toURL().toString();
        ps.setString( 1, inputFileURL);
        rs = ps.executeQuery();
        assertResults( rs, fileResults, false );
        rs.close();
        ps.close();

        // tear down the test
        goodStatement( aliceConnection, "drop table t" );
        goodStatement( aliceConnection, "drop function f_double" );

        // alice cannot unload the tool
        expectExecutionError( aliceConnection, LACK_EXECUTE_PRIV, UNLOAD_TOOL );

        // but the dbo can
        goodStatement( dboConnection, UNLOAD_TOOL );
    }

}
