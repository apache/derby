/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsPermsTest

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
 * Test permissions on generated columns. See DERBY-481.
 * </p>
 */
public class GeneratedColumnsPermsTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      JANET = "JANET";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, JANET };

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

    public GeneratedColumnsPermsTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(GeneratedColumnsPermsTest.class);

        Test        cleanTest = new CleanDatabaseTestSetup( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "generatedColumnsPermissions" );
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
     * Test that you need select/insert/update/delete privileges on a generated column and not just on
     * the columns it references.
     * </p>
     */
    public  void    test_001_basicPermissions()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        //
        // Verify correct behavior when have granted only SELECT and UPDATE on the referenced column.
        //
        goodStatement
            (
             dboConnection,
             "create table t_bp_1( a int, b int generated always as ( -a ) )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t_bp_1( a ) values ( 1 )"
             );
        goodStatement
            (
             dboConnection,
             "grant select ( a ) on t_bp_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant update ( a ) on t_bp_1 to public"
             );

        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "insert into test_dbo.t_bp_1( a ) values ( 100 )"
             );
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_bp_1 set a = a+ 1"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "delete from test_dbo.t_bp_1 where a = 2"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_COLUMN_PRIV,
             "select * from test_dbo.t_bp_1 order by a"
             );
        assertResults
            (
             janetConnection,
             "select a from test_dbo.t_bp_1 order by a",
             new String[][]
             {
                 { "2", },
             },
             false
             );
        
        //
        // Verify correct behavior when we also grant SELECT on the generated column.
        //
        goodStatement
            (
             dboConnection,
             "grant select ( b ) on t_bp_1 to public"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "insert into test_dbo.t_bp_1( a ) values ( 100 )"
             );
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_bp_1 set a = a+ 1"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "delete from test_dbo.t_bp_1 where a = 2"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_bp_1 order by a",
             new String[][]
             {
                 { "3", "-3", },
             },
             false
             );
        
        //
        // Verify correct behavior when we also grant UPDATE on the generated column.
        //
        goodStatement
            (
             dboConnection,
             "grant update ( b ) on t_bp_1 to public"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "insert into test_dbo.t_bp_1( a ) values ( 100 )"
             );
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_bp_1 set a = a+ 1"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "delete from test_dbo.t_bp_1 where a = 2"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_bp_1 order by a",
             new String[][]
             {
                 { "4", "-4", },
             },
             false
             );
        
        //
        // Verify correct behavior when we also grant INSERT on the table.
        //
        goodStatement
            (
             dboConnection,
             "grant insert on t_bp_1 to public"
             );
        goodStatement
            (
             janetConnection,
             "insert into test_dbo.t_bp_1( a ) values ( 100 )"
             );
        expectExecutionError
            (
             janetConnection,
             LACK_TABLE_PRIV,
             "delete from test_dbo.t_bp_1 where a = 2"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_bp_1 order by a",
             new String[][]
             {
                 { "4", "-4", },
                 { "100", "-100", },
             },
             false
             );
        
        //
        // Verify correct behavior when we also grant DELETE on the table.
        //
        goodStatement
            (
             dboConnection,
             "grant delete on t_bp_1 to public"
             );
        goodStatement
            (
             janetConnection,
             "delete from test_dbo.t_bp_1 where a = 4"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_bp_1 order by a",
             new String[][]
             {
                 { "100", "-100", },
             },
             false
             );
   }

    /**
     * <p>
     * Test that you need execute privilege to run functions mentioned in
     * generation clauses.
     * </p>
     */
    public  void    test_002_functionPermissions()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        //
        // Verify correct behavior when EXECUTE privilege is not granted.
        //
        goodStatement
            (
             dboConnection,
             "create function f_fp_minus\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.minus'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t_fp_1( a int, b int generated always as ( test_dbo.f_fp_minus( a ) ) )"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t_fp_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on t_fp_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant select on t_fp_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "insert into test_dbo.t_fp_1( a ) values ( 100 )"
             );

        goodStatement
            (
             janetConnection,
             "create function f_fp_minus\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_fp_1 set a = a + 1"
             );

        // this is a wrong result. see DERBY-6434
        expectExecutionError
            (
             janetConnection,
             LACK_EXECUTE_PRIV,
             "insert into test_dbo.t_fp_1( a ) values ( 200 )"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_fp_1 order by a",
             new String[][]
             {
                 { "101", "-101", },
             },
             false
             );

        //
        // Verify correct behavior when EXECUTE privilege is granted.
        //
        goodStatement
            (
             dboConnection,
             "grant execute on function f_fp_minus to public"
             );
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_fp_1 set a = a + 1"
             );
        goodStatement
            (
             janetConnection,
             "insert into test_dbo.t_fp_1( a ) values ( 200 )"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_fp_1 order by a",
             new String[][]
             {
                 { "102", "-102", },
                 { "200", "-200", },
             },
             false
             );
    }
    
    /**
     * <p>
     * Test ddl that can only be issued when authorization is turned on.
     * </p>
     */
    public  void    test_003_ddl()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Verify that current_role is not allowed in generation clauses.
        //
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_ddl_1( a int, b varchar( 128 ) generated always as ( current_role ) )"
             );
    }
    
    /**
     * <p>
     * Test that unqualified function references in generation clauses resolve
     * to the current schema in effect when the generated column was added.
     * See DERBY-3945.
     * </p>
     */
    public  void    test_004_functionSchema()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        //
        // Schema.
        //
        goodStatement
            (
             dboConnection,
             "create function f_fsch_1\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.minus'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t_fsch_1( a int, b generated always as ( f_fsch_1( a ) ) )"
             );
        goodStatement
            (
             dboConnection,
             "create table t_fsch_2( a int )"
             );
        goodStatement
            (
             dboConnection,
             "alter table t_fsch_2 add column b generated always as ( f_fsch_1( a ) )"
             );
        goodStatement
            (
             dboConnection,
             "grant execute on function f_fsch_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant select on t_fsch_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on t_fsch_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t_fsch_1 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant select on t_fsch_2 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on t_fsch_2 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t_fsch_2 to public"
             );

        goodStatement
            (
             janetConnection,
             "create function f_fsch_1\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );

        //
        // Populate
        //
        goodStatement
            (
             dboConnection,
             "insert into test_dbo.t_fsch_1( a ) values ( 1 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into test_dbo.t_fsch_2( a ) values ( 2 )"
             );

        //
        // Verify insert by other user
        //
        goodStatement
            (
             janetConnection,
             "insert into test_dbo.t_fsch_1( a ) values ( 1 )"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_fsch_1 order by a",
             new String[][]
             {
                 { "1", "-1", },
                 { "1", "-1", },
             },
             false
             );
        goodStatement
            (
             janetConnection,
             "insert into test_dbo.t_fsch_2( a ) values ( 2 )"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_fsch_2 order by a",
             new String[][]
             {
                 { "2", "-2", },
                 { "2", "-2", },
             },
             false
             );

        //
        // Verify update by other user
        //
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_fsch_1 set a = 100 + a"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_fsch_1 order by a",
             new String[][]
             {
                 { "101", "-101", },
                 { "101", "-101", },
             },
             false
             );
        goodStatement
            (
             janetConnection,
             "update test_dbo.t_fsch_2 set a = 100 + a"
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.t_fsch_2 order by a",
             new String[][]
             {
                 { "102", "-102", },
                 { "102", "-102", },
             },
             false
             );

    }
    
    /**
     * <p>
     * Test that unqualified function references in check constraints resolve
     * to the current schema in effect when the constraint was declared.
     * See DERBY-3944, which is related to the bug above, DERBY-3945.
     * </p>
     */
    public  void    test_005_functionSchema()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        goodStatement
            (
             dboConnection,
             "create function f_3944\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.minus'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t_3944( a int, constraint t_3944_check check ( f_3944( a ) < 0 ) )"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on t_3944 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant execute on function f_3944 to public"
             );

        expectExecutionError
            (
             janetConnection,
             CONSTRAINT_VIOLATION,
             "insert into test_dbo.t_3944( a ) values ( -100 )"
             );
        goodStatement
            (
             janetConnection,
             "insert into test_dbo.t_3944( a ) values ( 200 )"
             );
        assertResults
            (
             dboConnection,
             "select * from t_3944 order by a",
             new String[][]
             {
                 { "200", },
             },
             false
             );
    }

   /**
     * <p>
     * Test that unqualified function references in views resolve
     * to the view's schema.
     * See DERBY-3953, which is related to the bugs above, DERBY-3944 and DERBY-3945.
     * </p>
     */
    public  void    test_006_functionSchema()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        goodStatement
            (
             dboConnection,
             "create function f_3953\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.minus'\n"
             );
        goodStatement
            (
             dboConnection,
             "create view v_3953( a, b ) as values ( f_3953( 1 ), f_3953( 2 ) )"
             );
        goodStatement
            (
             dboConnection,
             "grant select on v_3953 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant execute on function f_3953 to public"
             );
        
        goodStatement
            (
             janetConnection,
             "create function f_3953\n" +
             "(\n" +
             "    a int\n" +
             ")\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        
        assertResults
            (
             janetConnection,
             "values ( f_3953( 1 ), f_3953( 2 ) )",
             new String[][]
             {
                 { "1", "2" },
             },
             false
             );
        assertResults
            (
             janetConnection,
             "select * from test_dbo.v_3953",
             new String[][]
             {
                 { "-1", "-2" },
             },
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}
