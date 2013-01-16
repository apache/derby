/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UDAPermsTest

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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.util.ArrayList;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derby.catalog.types.RoutineAliasInfo;

/**
 * <p>
 * Test permissions on user-defined aggregates. See DERBY-672.
 * </p>
 */
public class UDAPermsTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String      TONY = "TONY";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK, TONY  };

    private static  final   String      MISSING_ROUTINE = "42Y03";

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

    public UDAPermsTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(UDAPermsTest.class);

        Test        cleanTest = new CleanDatabaseTestSetup( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "udaPermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecorator( authenticatedTest );

        return authorizedTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that you need USAGE privilege on an aggregate in order to invoke it.
     * and in order to declare objects which mention that type.
     * </p>
     */
    public  void    test_001_basicGrant()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        //
        // Create an aggregate and table.
        //
        goodStatement
            (
             ruthConnection,
             "create derby aggregate mode_01 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             ruthConnection,
             "create table mode_inputs_01( a int, b int )\n"
             );
        goodStatement
            (
             ruthConnection,
             "insert into mode_inputs_01( a, b ) values ( 1, 1 ), ( 1, 2 ), ( 1, 2 ), ( 1, 2 ), ( 2, 3 ), ( 2, 3 ), ( 2, 4 )\n"
             );
        goodStatement
            (
             ruthConnection,
             "grant select on mode_inputs_01 to public\n"
             );

        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "select a, ruth.mode_01( b ) from ruth.mode_inputs_01 group by a\n"
             );
        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "create view v_alice_01( a, modeOfA ) as select a, ruth.mode_01( b ) from ruth.mode_inputs_01 group by a\n"
             );

        //
        // The DBO however is almighty.
        //
        assertResults
            (
             dboConnection,
             "select a, ruth.mode_01( b ) from ruth.mode_inputs_01 group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );
        goodStatement
            (
             dboConnection,
             "create view v_dbo_01( a, modeOfA ) as select a, ruth.mode_01( b ) from ruth.mode_inputs_01 group by a\n"
             );
        assertResults
            (
             dboConnection,
             "select * from v_dbo_01",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );

        //
        // Now grant USAGE on the user-defined aggregate. User Alice should now have all the
        // privileges she needs.
        //
        goodStatement
            (
             ruthConnection,
             "grant usage on derby aggregate mode_01 to public\n"
             );
        
        assertResults
            (
             aliceConnection,
             "select a, ruth.mode_01( b ) from ruth.mode_inputs_01 group by a",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );

        goodStatement
            (
             aliceConnection,
             "create view v_alice_01( a, modeOfA ) as select a, ruth.mode_01( b ) from ruth.mode_inputs_01 group by a\n"
             );
        assertResults
            (
             aliceConnection,
             "select * from v_alice_01",
             new String[][]
             {
                 { "1", "2" },
                 { "2", "3" },
             },
             false
             );

    }
    
   /**
     * <p>
     * Test that USAGE privilege can't be revoked if it would make objects
     * unusable by their owners.
     * </p>
     */
    public  void    test_002_basicRevoke()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        //
        // Create an aggregate and table.
        //
        goodStatement
            (
             ruthConnection,
             "create derby aggregate mode_02 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             ruthConnection,
             "create table mode_inputs_02( a int, b int )\n"
             );
        goodStatement
            (
             ruthConnection,
             "grant select on mode_inputs_02 to public\n"
             );
        goodStatement
            (
             ruthConnection,
             "insert into mode_inputs_02( a, b ) values ( 1, 1 ), ( 1, 2 ), ( 1, 2 ), ( 1, 2 ), ( 2, 3 ), ( 2, 3 ), ( 2, 4 )\n"
             );

        // only RESTRICTed revocations allowed
        expectCompilationError( ruthConnection, SYNTAX_ERROR, "revoke usage on derby aggregate mode_02 from ruth\n" );

        // can't revoke USAGE from owner
        expectCompilationError
            (
             ruthConnection,
             GRANT_REVOKE_NOT_ALLOWED,
             "revoke usage on derby aggregate mode_02 from ruth restrict\n"
             );

        String grantUsage = "grant usage on derby aggregate mode_02 to alice\n";
        String revokeUsage = "revoke usage on derby aggregate mode_02 from alice restrict\n";
        String createStatement;
        String dropStatement;
        String badRevokeSQLState;
        
        // can't revoke USAGE if a view depends on it
        createStatement =
             "create view v_alice_02( a, modeOfA ) as select a, ruth.mode_02( b ) from ruth.mode_inputs_02 group by a"
            ;
        dropStatement = "drop view v_alice_02\n";
        badRevokeSQLState = VIEW_DEPENDENCY;
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );

        // can't revoke USAGE if a trigger depends on it
        goodStatement( aliceConnection, "create table t_source_02( a int )\n" );
        goodStatement( aliceConnection, "create table t_target_02( a int )\n" );
        createStatement =
            "create trigger t_insert_trigger_02\n" +
            "after insert on t_source_02\n" +
            "for each row\n" +
            "insert into t_target_02( a ) select ruth.mode_02( b ) from ruth.mode_inputs_02\n";
        dropStatement = "drop trigger t_insert_trigger_02\n";
        badRevokeSQLState = OPERATION_FORBIDDEN;
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );
    }
    
   /**
     * <p>
     * Test that you need USAGE privilege on user-defined types in order to use them in
     * user-defined aggregates.
     * </p>
     */
    public  void    test_003_typePrivs()
        throws Exception
    {
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        // can't revoke USAGE on a type if an aggregate's input/return depends on it
        goodStatement
            (
             ruthConnection,
             "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );

        String grantUsage = "grant usage on type Price to public";
        String revokeUsage = "revoke usage on type Price from public restrict";
        String createStatement =
            "create derby aggregate priceMode for ruth.Price\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode'\n";
        String dropStatement = "drop derby aggregate priceMode restrict";
        String badRevokeSQLState = ROUTINE_DEPENDS_ON_TYPE;

        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );
        
        // can't revoke USAGE on a type if an aggregate's input depends on it
        goodStatement
            (
             ruthConnection,
             "create type Price_input external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             ruthConnection,
             "create type Price_return external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             ruthConnection,
             "grant usage on type Price_return to public"
             );

        grantUsage = "grant usage on type Price_input to public";
        revokeUsage = "revoke usage on type Price_input from public restrict";
        createStatement =
            "create derby aggregate priceMode for ruth.Price_input returns ruth.Price_return\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode'\n";
        dropStatement = "drop derby aggregate priceMode restrict";
        badRevokeSQLState = ROUTINE_DEPENDS_ON_TYPE;
        
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );
        
        // can't revoke USAGE on a type if an aggregate's return value depends on it
        goodStatement
            (
             ruthConnection,
             "create type Price_input_2 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             ruthConnection,
             "create type Price_return_2 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             ruthConnection,
             "grant usage on type Price_input_2 to public"
             );

        grantUsage = "grant usage on type Price_return_2 to public";
        revokeUsage = "revoke usage on type Price_return_2 from public restrict";
        createStatement =
            "create derby aggregate priceMode for ruth.Price_input_2 returns ruth.Price_return_2\n" +
            "external name 'org.apache.derbyTesting.functionTests.tests.lang.GenericMode'\n";
        dropStatement = "drop derby aggregate priceMode restrict";
        badRevokeSQLState = ROUTINE_DEPENDS_ON_TYPE;
        
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );
    }
    
   /**
     * <p>
     * Test that we fixed an NPE in resolving function names when the
     * schema hasn't been created yet.
     * </p>
     */
    public  void    test_004_emptySchema()
        throws Exception
    {
        Connection  tonyConnection = openUserConnection( TONY );

        expectCompilationError( tonyConnection, MISSING_ROUTINE, "values toString( 100 )" );
    }

}
