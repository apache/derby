/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UDAPermsTestz

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
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

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
    private static  final   String      IMPLICIT_CAST_ERROR = "42Y22";
    private static  final   String      PARSE_ERROR = "42X01";
    private static  final   String      BAD_DISTINCT = "42XAS";

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            UDAPermsTest.class);

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

   /**
     * <p>
     * Test that anyone can run the modern, builtin system aggregates
     * which implement org.apache.derby.agg.Aggregator.
     * </p>
     */
    public  void    test_005_builtinAggregators()
//IC see: https://issues.apache.org/jira/browse/DERBY-5466
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        createSchema_005( ruthConnection );

        vetStatsBuiltins_005( dboConnection );
        vetStatsBuiltins_005( ruthConnection );
        
        dropSchema_005( ruthConnection );
    }
    private void vetStatsBuiltins_005( Connection conn )
        throws Exception
    {
        vetBuiltinAgg_005
            (
             conn,
             "var_pop",
             new String[][] { { "8.079999999999991" } },
             new String[][] { { "8.0" } }
             );
        vetBuiltinAgg_005
            (
             conn,
             "var_samp",
             new String[][] { { "10.099999999999994" } },
             new String[][] { { "10.0" } }
             );
        vetBuiltinAgg_005
            (
             conn,
             "stddev_pop",
             new String[][] { { "2.8425340807103776" } },
             new String[][] { { "2.8284271247461903" } }
             );
        vetBuiltinAgg_005
            (
             conn,
             "stddev_samp",
             new String[][] { { "3.1780497164141397" } },
             new String[][] { { "3.1622776601683795" } }
             );
    }
    private void vetBuiltinAgg_005
        (
         Connection conn,
         String aggName,
         String[][] expectedInexactResults,
         String[][] expectedExactResults
         )
        throws Exception
    {
        vetBuiltinAgg_005( conn, aggName, "doubles", expectedInexactResults );
        vetBuiltinAgg_005( conn, aggName, "floats", expectedInexactResults );
        vetBuiltinAgg_005( conn, aggName, "bigints", expectedExactResults );
        vetBuiltinAgg_005( conn, aggName, "ints", expectedExactResults );
        vetBuiltinAgg_005( conn, aggName, "smallints", expectedExactResults );
        vetBuiltinNegative_005( conn, aggName );
    }
    private void vetBuiltinAgg_005
        (
         Connection conn,
         String aggName,
         String tableName,
         String[][] expectedResults
         )
        throws Exception
    {
        assertResults
            (
             conn,
             "select " + aggName + "( a ) from ruth." + tableName,
             expectedResults,
             false
             );
    }
    private void vetBuiltinNegative_005
        (
         Connection conn,
         String aggName
         )
        throws Exception
    {
        // varchar can not be implicitly cast to double
        expectCompilationError( conn, IMPLICIT_CAST_ERROR,
                                "select " + aggName + "( a ) from ruth.varchars" );

        // cannot schema-qualify a builtin aggregate name
        expectCompilationError( conn, MISSING_ROUTINE,
                                "select sys." + aggName + "( a ) from ruth.doubles" );

        // cannot use ALL or DISTINCT with a builtin aggregate
//IC see: https://issues.apache.org/jira/browse/DERBY-5466
        expectCompilationError( conn, PARSE_ERROR,
                                "select " + aggName + "( all a ) from ruth.doubles" );
        expectCompilationError( conn, BAD_DISTINCT,
                                "select " + aggName + "( distinct a ) from ruth.doubles" );
    }
    private void createSchema_005( Connection ruthConnection )
        throws Exception
    {
        goodStatement
            (
             ruthConnection,
             "create table doubles( a double )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into doubles values ( 1.2 ), ( 3.4 ), (5.6), (7.8), (9.0)"
             );
        goodStatement
            (
             ruthConnection,
             "create table floats( a double )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into floats values ( 1.2 ), ( 3.4 ), (5.6), (7.8), (9.0)"
             );
        goodStatement
            (
             ruthConnection,
             "create table bigints( a bigint )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into bigints values ( 1 ), ( 3 ), (5), (7), (9)"
             );
        goodStatement
            (
             ruthConnection,
             "create table ints( a bigint )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into ints values ( 1 ), ( 3 ), (5), (7), (9)"
             );
        goodStatement
            (
             ruthConnection,
             "create table smallints( a bigint )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into smallints values ( 1 ), ( 3 ), (5), (7), (9)"
             );
        goodStatement
            (
             ruthConnection,
             "create table varchars( a varchar( 10 ) )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into varchars values ( '1' ), ( '3' ), ( '5' ), ( '7' ), ( '9' )"
             );
    }
    private void dropSchema_005( Connection ruthConnection )
        throws Exception
    {
        // drop schema
        goodStatement
            (
             ruthConnection,
             "drop table varchars"
             );
        goodStatement
            (
             ruthConnection,
             "drop table smallints"
             );
        goodStatement
            (
             ruthConnection,
             "drop table ints"
             );
        goodStatement
            (
             ruthConnection,
             "drop table bigints"
             );
        goodStatement
            (
             ruthConnection,
             "drop table floats"
             );
        goodStatement
            (
             ruthConnection,
             "drop table doubles"
             );
    }
}
