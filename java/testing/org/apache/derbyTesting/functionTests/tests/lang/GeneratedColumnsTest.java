/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest

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
 * Test generated columns. See DERBY-481.
 * </p>
 */
public class GeneratedColumnsTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  REDUNDANT_CLAUSE = "42613";
    private static  final   String  ILLEGAL_AGGREGATE = "42XA1";
    private static  final   String  UNSTABLE_RESULTS = "42XA2";
    private static  final   String  CANT_OVERRIDE_GENERATION_CLAUSE = "42XA3";
    private static  final   String  CANT_REFERENCE_GENERATED_COLUMN = "42XA4";
    private static  final   String  ROUTINE_CANT_ISSUE_SQL = "42XA5";
    private static  final   String  BAD_FOREIGN_KEY_ACTION = "42XA6";
    private static  final   String  NOT_NULL_VIOLATION = "23502";
    private static  final   String  CONSTRAINT_VIOLATION = "23513";
    private static  final   String  FOREIGN_KEY_VIOLATION = "23503";
    private static  final   String  ILLEGAL_DUPLICATE = "23505";
    private static  final   String  SYNTAX_ERROR = "42X01";
    private static  final   String  COLUMN_OUT_OF_SCOPE = "42X04";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  int _minusCounter;

    private static  ArrayList   _triggerReports = new ArrayList();

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Create a new instance.
     */

    public GeneratedColumnsTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(GeneratedColumnsTest.class);

        return new CleanDatabaseTestSetup( suite );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that the stored system procedures and functions are non-deterministic. If you want
     * a particular procedure/function to be deterministic, add some logic here.
     * </p>
     *
     * <p>
     * Also test that, by default, user-defined routines are created as NOT DETERMINISTIC.
     * </p>
     */
    public  void    test_001_determinism_of_stored_system_routines()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Create a user-defined function and procedure and verify
        // that they too are NOT DETERMINISTIC.
        // 
        PreparedStatement functionCreate = conn.prepareStatement
            (
             "create function f1()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        functionCreate.execute();
        functionCreate.close();

        PreparedStatement procedureCreate = conn.prepareStatement
            (
             "create procedure p1()\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        procedureCreate.execute();
        procedureCreate.close();

        //
        // OK, now verify that all routines in the catalogs are NOT DETERMINISTIC 
        //
        PreparedStatement   ps = conn.prepareStatement
            (
             "select s.schemaname, a.alias, a.aliastype, a.systemalias, a.aliasinfo\n" +
             "from sys.sysschemas s, sys.sysaliases a\n" +
             "where s.schemaid = a.schemaid\n" +
             "order by s.schemaname, a.alias\n"
             );
        ResultSet               rs = ps.executeQuery();

        while ( rs.next() )
        {
            String    aliasName = rs.getString( 2 );
            boolean isSystemAlias = rs.getBoolean( 4 );

            RoutineAliasInfo    rai = (RoutineAliasInfo) rs.getObject( 5 );

            if ( isSystemAlias ) { assertFalse( aliasName, rai.isDeterministic() ); }
        }

        rs.close();
        ps.close();
    }

    /**
     * <p>
     * Basic positive tests for DETERMINISTIC keyword.
     * </p>
     */
    public  void    test_002_determinism_positive()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement
            (
             conn,
             "create function f11()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "F11", false );
        
        goodStatement
            (
             conn,
             "create function f12()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "F12", true );
        
        goodStatement
            (
             conn,
             "create function f13()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "not deterministic\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "F13", false );
        
        goodStatement
            (
             conn,
             "create procedure p11()\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "P11", false );
        
        goodStatement
            (
             conn,
             "create procedure p12()\n" +
             "deterministic\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "P12", true );
        
        goodStatement
            (
             conn,
             "create procedure p13()\n" +
             "language java\n" +
             "not deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "P13", false );
    }

    /**
     * <p>
     * Verify that we get errors when there is more than one determinism clause
     * in a routine declaration.
     * </p>
     */
    public  void    test_003_determinism_redundantClause()
        throws Exception
    {
        Connection  conn = getConnection();

        expectCompilationError
            (
             REDUNDANT_CLAUSE,
             "create function f_fail()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "deterministic\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        expectCompilationError
            (
             REDUNDANT_CLAUSE,
             "create function f_fail()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "not deterministic\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        expectCompilationError
            (
             REDUNDANT_CLAUSE,
             "create procedure p_fail()\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "deterministic\n" +
             "external name 'foo.bar.wibble'\n"
             );
        expectCompilationError
            (
             REDUNDANT_CLAUSE,
             "create procedure p_fail()\n" +
             "language java\n" +
             "not deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "not deterministic\n" +
             "external name 'foo.bar.wibble'\n"
             );
    }
    
    /**
     * <p>
     * Verify basic parse/bind logic for declaring generated columns.
     * </p>
     */
    public  void    test_004_basicParser()
        throws Exception
    {
        Connection  conn = getConnection();
        
        goodStatement
            (
             conn,
             "create function f_parse_deterministic( a int )\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             conn,
             "create function f_parse_non_deterministic( a int )\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             conn,
             "create table t_parse_1\n" +
             "(\n" +
             "   a int,\n" +
             "   b int generated always as ( f_parse_deterministic( a ) ),\n" +
             "   c int\n" +
             ")"
             );
        
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_parse_shouldFail\n" +
             "(\n" +
             "   a int,\n" +
             "   b int generated always as ( f_parse_non_deterministic( a ) ),\n" +
             "   c int\n" +
             ")\n"
             );
    }

    /**
     * <p>
     * Verify basic insert behavior for generated columns.
     * </p>
     */
    public  void    test_005_basicInsert()
        throws Exception
    {
        Connection  conn = getConnection();
        
        goodStatement
            (
             conn,
             "create table t_insert_1( a int,  b int  default 1, c int )"
             );
        goodStatement
            (
             conn,
             "create table t_insert_2( a int,  b int  generated always as( -a ) check ( b < 0 ), c int )"
             );
        goodStatement
            (
             conn,
             "create unique index t_insert_2_b on t_insert_2( b )"
             );
        goodStatement
            (
             conn,
             "create table t_insert_3( a int, b int generated always as ( -a ) )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_insert_1( a, c ) values ( 100, 1000 ), ( 200, 2000 ), ( 300, 3000 )"
             );
        
        // insert one row
        goodStatement
            (
             conn,
             "insert into t_insert_2( a, c ) values ( 2, 200 )"
             );
        
        // insert multiple rows
        goodStatement
            (
             conn,
             "insert into t_insert_2( a, c ) values ( 1, 100 ), ( 3, 300 ), ( 4, 400 ), ( 5, 500 )"
             );
        
        // insert by selecting from another table
        goodStatement
            (
             conn,
             "insert into t_insert_2( a, c ) select a, c from t_insert_1"
             );
        
        // insert using a default clause on the generated column
        goodStatement
            (
             conn,
             "insert into t_insert_2( a, b ) values ( 6, default )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_insert_3 values ( 1, default )"
             );
        goodStatement
            (
             conn,
             "insert into t_insert_3 values ( 2, default ), ( 3, default ), ( 4, default )"
             );
        
        //
        // Verify that all of the expected rows are in the table having the
        // generated column.
        //
        assertResults
            (
             conn,
             "select * from t_insert_2 order by a",
             new String[][]
             {
                 { "1" ,         "-1" ,        "100" },
                 { "2" ,         "-2" ,        "200" },
                 { "3" ,         "-3" ,        "300" },
                 { "4" ,         "-4" ,        "400" },
                 { "5" ,         "-5" ,        "500" },
                 { "6" ,         "-6" ,        null },
                 { "100",        "-100" ,      "1000" },
                 { "200" ,       "-200" ,      "2000" },
                 { "300" ,       "-300" ,      "3000" },
             },
             false
             );
        
        assertResults
            (
             conn,
             "select * from t_insert_3 order by a",
             new String[][]
             {
                 { "1" ,         "-1" },
                 { "2" ,         "-2" },
                 { "3" ,         "-3" },
                 { "4" ,         "-4" },
             },
             false
             );
        
        // fails trying to override a generation clause
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_insert_2( a, b ) values ( 7, 70 )"
             );
        
        // fails on a violation of the check constraint on the generated column
        expectExecutionError
            (
             conn,
             CONSTRAINT_VIOLATION,
             "insert into t_insert_2( a ) values ( -8 )"
             );
        
        // fails because it violates the unique index on the generated column
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_insert_2( a ) values ( 2 )"
             );
        
    }

    /**
     * <p>
     * Verify basic update behavior for generated columns.
     * </p>
     */
    public  void    test_006_basicUpdate()
        throws Exception
    {
        Connection  conn = getConnection();
        int             counter;
        
        goodStatement
            (
             conn,
             "create function f_minus\n" +
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
             conn,
             "create function f_readMinusCounter()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.readMinusCounter'\n"
             );
        goodStatement
            (
             conn,
             "create table t_update_1( a int,  b int  generated always as( f_minus(a) ) check ( b < 0 ), c int )"
             );
        goodStatement
            (
             conn,
             "create unique index t_update_1_b on t_update_1( b )"
             );
        
        counter = readMinusCounter( conn );
        goodStatement
            (
             conn,
             "insert into t_update_1( a, c ) values ( 1, 100 ), ( 2, 200 ), ( 3, 300 )"
             );
        assertEquals( counter + 3, readMinusCounter( conn ) );
        
        counter = readMinusCounter( conn );
        goodStatement
            (
             conn,
             "update t_update_1\n" +
             "set a = a + 10 where a > 1\n"
             );
        assertEquals( counter + 2, readMinusCounter( conn ) );
        
        // you can use the DEFAULT keyword to set a generated column
        goodStatement
            (
             conn,
             "update t_update_1\n" +
             "set a = a + 10, b = default where c = 300\n"
             );
        
        // fails trying to override a generation clause
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "update t_update_1\n" +
             "set a = a + 10, b = -3 where c = 300\n"
             );
        
        // fails on a violation of the check constraint on the generated column
        expectExecutionError
            (
             conn,
             CONSTRAINT_VIOLATION,
             "update t_update_1\n" +
             "set a = -100\n" +
             "where a = 1\n"
             );
        
        // fails because it violates the unique index on the generated column
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "update t_update_1\n" +
             "set a = 12\n" +
             "where a = 1\n"
             );
        
        //
        // Verify that all of the expected rows are in the table having the
        // generated column.
        //
        assertResults
            (
             conn,
             "select * from t_update_1 order by c",
             new String[][]
             {
                 { "1" ,         "-1" ,        "100" },
                 { "12" ,         "-12" ,        "200" },
                 { "23" ,         "-23" ,        "300" },
             },
             false
             );
    }

    /**
     * <p>
     * Verify basic trigger interaction with generated columns
     * </p>
     */
    public  void    test_007_basicTriggers()
        throws Exception
    {
        Connection  conn = getConnection();
        
        //
        // Setup schema for test
        //
        goodStatement
            (
             conn,
             "create function f_bt_minus\n" +
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
             conn,
             "create function triggerReports()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     contents varchar( 100 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.triggerReport'\n"
             );
        goodStatement
            (
             conn,
             "create procedure clearTriggerReports\n" +
             "()\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.clearTriggerReports'\n"
             );
        goodStatement
            (
             conn,
             "create procedure report_proc\n" +
             "( tag varchar( 40 ), a int, b int, c int )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.showValues'\n"
             );
        goodStatement
            (
             conn,
             "create procedure wide_report_proc\n" +
             "( tag varchar( 40 ), old_a int, old_b int, old_c int, new_a int, new_b int, new_c int )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.showValues'\n"
             );
        goodStatement
            (
             conn,
             "create table t1_trig( a int, b int generated always as ( f_bt_minus(a) ), c int )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_before_insert_row_trigger\n" +
             "no cascade before insert on t1_trig\n" +
             "referencing new as ar\n" +
             "for each row\n" +
             "call report_proc( 'before_insert_row_trigger', ar.a, ar.b, ar.c )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_after_insert_row_trigger\n" +
             "after insert on t1_trig\n" +
             "referencing new as ar\n" +
             "for each row\n" +
             "call report_proc( 'after_insert_row_trigger', ar.a, ar.b, ar.c ) \n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_before_update_row_trigger\n" +
             "no cascade before update on t1_trig\n" +
             "referencing old as br new as ar\n" +
             "for each row\n" +
             "call wide_report_proc( 'before_update_row_trigger', br.a, br.b, br.c, ar.a, ar.b, ar.c )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_after_update_row_trigger\n" +
             "after update on t1_trig\n" +
             "referencing old as br new as ar\n" +
             "for each row\n" +
             "call wide_report_proc( 'after_update_row_trigger', br.a, br.b, br.c, ar.a, ar.b, ar.c )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_before_delete_row_trigger\n" +
             "no cascade before delete on t1_trig\n" +
             "referencing old as br\n" +
             "for each row\n" +
             "call report_proc( 'before_delete_row_trigger', br.a, br.b, br.c )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_after_delete_row_trigger\n" +
             "after delete on t1_trig\n" +
             "referencing old as br\n" +
             "for each row\n" +
             "call report_proc( 'after_delete_row_trigger', br.a, br.b, br.c )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_before_insert_statement_trigger\n" +
             "no cascade before insert on t1_trig\n" +
             "for each statement\n" +
             "call report_proc( 'before_insert_statement_trigger', -1, -1, -1 )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_after_insert_statement_trigger\n" +
             "after insert on t1_trig\n" +
             "for each statement\n" +
             "call report_proc( 'after_insert_statement_trigger', -1, -1, -1 )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_before_update_statement_trigger\n" +
             "no cascade before update on t1_trig\n" +
             "for each statement\n" +
             "call report_proc( 'before_update_statement_trigger', -1, -1, -1 )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_before_delete_statement_trigger\n" +
             "no cascade before delete on t1_trig\n" +
             "for each statement\n" +
             "call report_proc( 'before_delete_statement_trigger', -1, -1, -1 )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_after_update_statement_trigger\n" +
             "after update on t1_trig\n" +
             "for each statement\n" +
             "call report_proc( 'after_update_statement_trigger', -1, -1, -1 )\n"
             );
        goodStatement
            (
             conn,
             "create trigger t1_trig_after_delete_statement_trigger\n" +
             "after delete on t1_trig\n" +
             "for each statement\n" +
             "call report_proc( 'after_delete_statement_trigger', -1, -1, -1 )\n"
             );
        
        //
        // Now run the tests.
        //
        assertTriggerStatus
            (
             conn,
             "insert into t1_trig( a ) values ( 1 ), ( 2 ), ( 3 )",
             new String[][]
             {
                 { "before_insert_row_trigger: [ 1, -1, null ]" },
                 { "before_insert_row_trigger: [ 2, -2, null ]" },
                 { "before_insert_row_trigger: [ 3, -3, null ]" },
                 { "before_insert_statement_trigger: [ -1, -1, -1 ]" },
                 { "after_insert_row_trigger: [ 1, -1, null ]" },
                 { "after_insert_row_trigger: [ 2, -2, null ]" },
                 { "after_insert_row_trigger: [ 3, -3, null ]" },                                                           
                 { "after_insert_statement_trigger: [ -1, -1, -1 ]" },
             }
             );
        assertTriggerStatus
            (
             conn,
             "update t1_trig set a = a + 10",
             new String[][]
             {
                 { "before_update_row_trigger: [ 1, -1, null, 11, -11, null ]" },
                 { "before_update_row_trigger: [ 2, -2, null, 12, -12, null ]" },
                 { "before_update_row_trigger: [ 3, -3, null, 13, -13, null ]" },
                 { "before_update_statement_trigger: [ -1, -1, -1 ]" },
                 { "after_update_row_trigger: [ 1, -1, null, 11, -11, null ]" },
                 { "after_update_row_trigger: [ 2, -2, null, 12, -12, null ]" },
                 { "after_update_row_trigger: [ 3, -3, null, 13, -13, null ]" },
                 { "after_update_statement_trigger: [ -1, -1, -1 ]" },
             }
             );
        assertTriggerStatus
            (
             conn,
             "delete from t1_trig where a > 11",
             new String[][]
             {
                 { "before_delete_row_trigger: [ 12, -12, null ]" },
                 { "before_delete_row_trigger: [ 13, -13, null ]" },
                 { "before_delete_statement_trigger: [ -1, -1, -1 ]" },
                 { "after_delete_row_trigger: [ 12, -12, null ]" },
                 { "after_delete_row_trigger: [ 13, -13, null ]" },
                 { "after_delete_statement_trigger: [ -1, -1, -1 ]" },
             }
             );
        
    }
    
    /**
     * <p>
     * Verify basic interaction of foreign keys with generated columns
     * </p>
     */
    public  void    test_008_basicForeignKeys()
        throws Exception
    {
        Connection  conn = getConnection();
        
        //
        // Setup schema for test
        //
        goodStatement
            (
             conn,
             "create function f_bfk_minus\n" +
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
             conn,
             "create table t1_for( a int, b int generated always as ( f_bfk_minus(a) ) primary key, c int )"
             );
        goodStatement
            (
             conn,
             "create table t2_for( a int, b int references t1_for( b ), c int )"
             );
        goodStatement
            (
             conn,
             "create table t3_for( a int, b int primary key, c int )"
             );
        goodStatement
            (
             conn,
             "create table t4_for( a int, b int generated always as ( f_bfk_minus(a) ) references t3_for( b ), c int )"
             );
        
        //
        // Initial data.
        //
        goodStatement
            (
             conn,
             "insert into t1_for( a ) values ( 1 ), ( 2 ), ( 3 )"
             );
        goodStatement
            (
             conn,
             "insert into t2_for( b ) values ( -1 ), ( -3 )"
             );
        goodStatement
            (
             conn,
             "insert into t3_for( b ) values ( 1 ), ( 2 ), ( 3 )"
             );
        goodStatement
            (
             conn,
             "insert into t4_for( a ) values ( -1 ), ( -2 ), ( -3 )"
             );
        
        //
        // Let's violate some foreign keys.
        //
        expectExecutionError
            (
             conn,
             FOREIGN_KEY_VIOLATION,
             "update t1_for set a = a + 10 where a = 1"
             );
        expectExecutionError
            (
             conn,
             FOREIGN_KEY_VIOLATION,
             "update t4_for set a = a + 10 where a = -1"
             );
        expectExecutionError
            (
             conn,
             FOREIGN_KEY_VIOLATION,
             "insert into t4_for( a ) values ( -4 )"
             );
    }
    
    /**
     * <p>
     * Verify that column defaults look good for generated columns.
     * </p>
     */
    public  void    test_009_basicDefaultInfo()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement
            (
             conn,
             "create table t_di_1\n" +
             "(\n" +
             "   a int,\n" +
             "   b int generated always as ( 1 ),\n" +
             "   c int\n" +
             ")"
             );
        goodStatement
            (
             conn,
             "create table t_di_2\n" +
             "(\n" +
             "   a int,\n" +
             "   b int generated always as ( -a ),\n" +
             "   c int\n" +
             ")"
             );
        goodStatement
            (
             conn,
             "create table t_di_3\n" +
             "(\n" +
             "   a int,\n" +
             "   b int generated always as ( a + c ),\n" +
             "   c int\n" +
             ")"
             );

        assertDefaultInfo
            (
             conn, "T_DI_1", "B",
             new int[] {},
             "1"
             );
        assertDefaultInfo
            (
             conn, "T_DI_2", "B",
             new int[] { 1 },
             "-a"
             );
        assertDefaultInfo
            (
             conn, "T_DI_3", "B",
             new int[] { 1, 3 },
             "a + c"
             );
             
    }
        
    /**
     * <p>
     * Various tests involving the DEFAULT literal in UPDATE statements.
     * </p>
     */
    public  void    test_010_updateDefaultLiteral()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t_ud_1( a int, b int generated always as ( a*a ) , c int )"
             );
        goodStatement
            (
             conn,
             "create table t_ud_2( a int, b int generated always as ( a*c ) , c int )"
             );

        // initial values
        goodStatement
            (
             conn,
             "insert into t_ud_1( a ) values ( 1 ), ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_ud_2( a ) values ( 1 ), ( 2 )"
             );

        //
        // Tests of generated column depending on one other column.
        //
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "update t_ud_1 set b = a*a"
             );
        assertResults
            (
             conn,
             "select * from t_ud_1 order by a",
             new String[][]
             {
                 { "1" ,         "1" ,        null },
                 { "2" ,         "4" ,        null },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "update t_ud_1 set c = -1, b = default"
             );
        assertResults
            (
             conn,
             "select * from t_ud_1 order by a",
             new String[][]
             {
                 { "1" ,         "1" ,        "-1" },
                 { "2" ,         "4" ,        "-1" },
             },
             false
             );

        goodStatement
            (
             conn,
             "update t_ud_1 set a = 2*a, b = default"
             );
        assertResults
            (
             conn,
             "select * from t_ud_1 order by a",
             new String[][]
             {
                 { "2" ,         "4" ,        "-1" },
                 { "4" ,         "16" ,        "-1" },
             },
             false
             );


        //
        // Tests of generated column depending on two other columns.
        //
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "update t_ud_2 set b = a*c"
             );
        assertResults
            (
             conn,
             "select * from t_ud_2 order by a",
             new String[][]
             {
                 { "1" ,         null,        null },
                 { "2" ,         null,        null },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "update t_ud_2 set b = default"
             );
        assertResults
            (
             conn,
             "select * from t_ud_2 order by a",
             new String[][]
             {
                 { "1" ,         null,        null },
                 { "2" ,         null,        null },
             },
             false
             );

        goodStatement
            (
             conn,
             "update t_ud_2 set c = -5"
             );
        assertResults
            (
             conn,
             "select * from t_ud_2 order by a",
             new String[][]
             {
                 { "1" ,         "-5" ,        "-5" },
                 { "2" ,         "-10" ,        "-5" },
             },
             false
             );

        goodStatement
            (
             conn,
             "update t_ud_2 set c = -3, b = default"
             );
        assertResults
            (
             conn,
             "select * from t_ud_2 order by a",
             new String[][]
             {
                 { "1" ,         "-3" ,        "-3" },
                 { "2" ,         "-6" ,        "-3" },
             },
             false
             );

        goodStatement
            (
             conn,
             "update t_ud_2 set a = 2*a, b = default"
             );
        assertResults
            (
             conn,
             "select * from t_ud_2 order by a",
             new String[][]
             {
                 { "2" ,         "-6" ,        "-3" },
                 { "4" ,         "-12" ,        "-3" },
             },
             false
             );

        goodStatement
            (
             conn,
             "update t_ud_2 set a = a - 1, b = default, c = 4"
             );
        assertResults
            (
             conn,
             "select * from t_ud_2 order by a",
             new String[][]
             {
                 { "1" ,         "4" ,        "4" },
                 { "3" ,         "12" ,        "4" },
             },
             false
             );

    }

    /**
     * <p>
     * Basic tests for altering a table and adding a generated column.
     * </p>
     */
    public  void    test_011_basicAlter()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t_alt_1( a int, c int )"
             );
        goodStatement
            (
             conn,
             "create function f_alt_deterministic( a int )\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             conn,
             "create function f_alt_non_deterministic( a int )\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );

        //
        // Initial values
        //
        goodStatement
            (
             conn,
             "insert into t_alt_1( a ) values ( 1 ), ( 2 )"
             );

        //
        // Now alter the table and add a generated column.
        //
        goodStatement
            (
             conn,
             "alter table t_alt_1 add column b int generated always as ( -a )"
             );
        assertResults
            (
             conn,
             "select * from t_alt_1 order by a",
             new String[][]
             {
                 { "1" ,         null,        "-1" },
                 { "2" ,         null,        "-2" },
             },
             false
             );

        goodStatement
            (
             conn,
             "insert into t_alt_1( a ) values ( 3 ), ( 4 )"
             );
        assertResults
            (
             conn,
             "select * from t_alt_1 order by a",
             new String[][]
             {
                 { "1" ,         null,        "-1" },
                 { "2" ,         null,        "-2" },
                 { "3" ,         null,        "-3" },
                 { "4" ,         null,        "-4" },
             },
             false
             );

        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "alter table t_alt_1 add column d int generated always as ( f_alt_non_deterministic( a ) )"
             );

        goodStatement
            (
             conn,
             "alter table t_alt_1 add column d int generated always as ( f_alt_deterministic( a ) )"
             );
        assertResults
            (
             conn,
             "select * from t_alt_1 order by a",
             new String[][]
             {
                 { "1" ,         null,        "-1",    "1" },
                 { "2" ,         null,        "-2",    "2" },
                 { "3" ,         null,        "-3",    "3" },
                 { "4" ,         null,        "-4",    "4" },
             },
             false
             );

    }
    
    /**
     * <p>
     * Verify that generated columns can't refer to one another.
     * </p>
     */
    public  void    test_012_referencedColumns()
        throws Exception
    {
        Connection  conn = getConnection();

        expectCompilationError
            (
             CANT_REFERENCE_GENERATED_COLUMN,
             "create table t_recurse_1( a int, b int generated always as ( -a ), c int generated always as ( b*b) )"
             );
        expectCompilationError
            (
             CANT_REFERENCE_GENERATED_COLUMN,
             "create table t_recurse_1( a int, b int generated always as ( -b ) )"
             );
        
        goodStatement
            (
             conn,
             "create table t_recurse_1( a int, b int generated always as ( -a ) )"
             );
        expectCompilationError
            (
             CANT_REFERENCE_GENERATED_COLUMN,
             "alter table t_recurse_1 add column c int generated always as ( b*b )"
             );
        expectCompilationError
            (
             CANT_REFERENCE_GENERATED_COLUMN,
             "alter table t_recurse_1 add column c int generated always as ( -c )"
             );
    }

    /**
     * <p>
     * Various miscellaneous illegal create/alter table statements.
     * </p>
     */
    public  void    test_013_badReferences()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement
            (
             conn,
             "create table t_br_1( a int )"
             );
        goodStatement
            (
             conn,
             "create table t_br_3( a int )"
             );
        goodStatement
            (
             conn,
             "create function f_br_reads_sql( a int )\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "reads sql data\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             conn,
             "create function f_br_contains_sql( a int )\n" +
             "returns int\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "contains sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        
        expectCompilationError
            (
             COLUMN_OUT_OF_SCOPE,
             "create table t_br_2( a int, b int generated always as ( t_br_1.a ) )"
             );
        expectCompilationError
            (
             SYNTAX_ERROR,
             "create table t_br_2( a int, b int generated always as ( select a from t_br_1 ) )"
             );
        expectCompilationError
            (
             COLUMN_OUT_OF_SCOPE,
             "alter table t_br_3 add column b int generated always as ( t_br_1.a )"
             );
        expectCompilationError
            (
             SYNTAX_ERROR,
             "alter table t_br_3 add column b int generated always as ( select a from t_br_1 )"
             );
        expectCompilationError
            (
             ROUTINE_CANT_ISSUE_SQL,
             "create table t_br_2( a int, b int generated always as ( f_br_reads_sql( a ) ) )"
             );
        expectCompilationError
            (
             ROUTINE_CANT_ISSUE_SQL,
             "create table t_br_2( a int, b int generated always as ( f_br_contains_sql( a ) ) )"
             );
        expectCompilationError
            (
             ILLEGAL_AGGREGATE,
             "create table t_br_2( a int, b int generated always as ( sum( a ) ) )"
             );
        expectCompilationError
            (
             ILLEGAL_AGGREGATE,
             "create table t_br_2( a int, b int generated always as ( max( a ) ) )"
             );
        expectCompilationError
            (
             ILLEGAL_AGGREGATE,
             "create table t_br_2( a int, b int generated always as ( min( a ) ) )"
             );
        expectCompilationError
            (
             ILLEGAL_AGGREGATE,
             "create table t_br_2( a int, b int generated always as ( count( a ) ) )"
             );
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b date generated always as ( current_date ) )"
             );
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b time generated always as ( current_time ) )"
             );
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b timestamp generated always as ( current_timestamp ) )"
             );
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b varchar( 128 ) generated always as ( current_user ) )"
             );
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b varchar( 128 ) generated always as ( session_user ) )"
             );
    }

    /**
     * <p>
     * Test that the declared datatype of a generated column can be anything that the
     * generation clause can be assigned to.
     * </p>
     */
    public  void    test_014_assignment()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t_dt_smallint\n" +
             "(\n" +
             "   a  smallint,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
            );
        goodStatement
            (
             conn,
             "create table t_dt_int\n" +
             "(\n" +
             "   a  int,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_bigint\n" +
             "(\n" +
             "   a  bigint,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_decimal\n" +
             "(\n" +
             "   a  decimal,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_real\n" +
             "(\n" +
             "   a  real,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_double\n" +
             "(\n" +
             "   a  double,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_float\n" +
             "(\n" +
             "   a  float,\n" +
             "   b  smallint generated always as ( -a ),\n" +
             "   c  int generated always as ( -a ),\n" +
             "   d bigint generated always as ( -a ),\n" +
             "   e decimal generated always as ( -a ),\n" +
             "   f real generated always as ( -a ),\n" +
             "   g double generated always as ( -a ),\n" +
             "   h float generated always as ( -a )\n" +
             ")\n"
             );

        goodStatement
            (
             conn,
             "create table t_dt_char\n" +
             "(\n" +
             "   a  char( 20 ),\n" +
             "   b  char( 20 ) generated always as ( upper( a ) ),\n" +
             "   c  varchar( 20 ) generated always as ( upper( a ) ),\n" +
             "   d long varchar generated always as ( upper( a ) ),\n" +
             "   e clob generated always as ( upper( a ) ),\n" +
             "   f date generated always as ( a ),\n" +
             "   g time generated always as ( '15:09:02' ),\n" +
             "   h timestamp generated always as ( trim( a ) || ' 03:23:34.234' )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_varchar\n" +
             "(\n" +
             "   a  varchar( 20 ),\n" +
             "   b  char( 20 ) generated always as ( upper( a ) ),\n" +
             "   c  varchar( 20 ) generated always as ( upper( a ) ),\n" +
             "   d long varchar generated always as ( upper( a ) ),\n" +
             "   e clob generated always as ( upper( a ) ),\n" +
             "   f date generated always as ( a ),\n" +
             "   g time generated always as ( '15:09:02' ),\n" +
             "   h timestamp generated always as ( trim( a ) || ' 03:23:34.234' )\n" +
             ")\n"
             );
        
        goodStatement
            (
             conn,
             "create table t_dt_longvarchar\n" +
             "(\n" +
             "   a  long varchar,\n" +
             "   b  char( 20 ) generated always as ( upper( a ) ),\n" +
             "   c  varchar( 20 ) generated always as ( upper( a ) ),\n" +
             "   d long varchar generated always as ( upper( a ) ),\n" +
             "   e clob generated always as ( upper( a ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_clob\n" +
             "(\n" +
             "   a  clob,\n" +
             "   b  char( 20 ) generated always as ( upper( a ) ),\n" +
             "   c  varchar( 20 ) generated always as ( upper( a ) ),\n" +
             "   d long varchar generated always as ( upper( a ) ),\n" +
             "   e clob generated always as ( upper( a ) )\n" +
             ")\n"
             );
        
        goodStatement
            (
             conn,
             "create table t_dt_charforbitdata\n" +
             "(\n" +
             "   a  char( 4 ) for bit data,\n" +
             "   b  char( 4) for bit data generated always as ( a ),\n" +
             "   c  varchar( 4 ) for bit data generated always as ( a ),\n" +
             "   d long varchar for bit data generated always as ( a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_varcharforbitdata\n" +
             "(\n" +
             "   a  varchar( 4 ) for bit data,\n" +
             "   b  char( 4) for bit data generated always as ( a ),\n" +
             "   c  varchar( 4 ) for bit data generated always as ( a ),\n" +
             "   d long varchar for bit data generated always as ( a )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_dt_longvarcharforbitdata\n" +
             "(\n" +
             "   a  long varchar for bit data,\n" +
             "   b  char( 4) for bit data generated always as ( a ),\n" +
             "   c  varchar( 4 ) for bit data generated always as ( a ),\n" +
             "   d long varchar for bit data generated always as ( a )\n" +
             ")\n"
             );
        
        goodStatement
            (
             conn,
             "create table t_dt_date\n" +
             "(\n" +
             "   a  date,\n" +
             "   b  char( 20 ) generated always as ( a ),\n" +
             "   c  varchar( 20 ) generated always as ( a ),\n" +
             "   d date generated always as ( a )\n" +
             ")\n"
             );

        goodStatement
            (
             conn,
             "create table t_dt_time\n" +
             "(\n" +
             "   a  time,\n" +
             "   b  char( 20 ) generated always as ( a ),\n" +
             "   c  varchar( 20 ) generated always as ( a ),\n" +
             "   d time generated always as ( a )\n" +
             ")\n"
             );

        goodStatement
            (
             conn,
             "create table t_dt_timestamp\n" +
             "(\n" +
             "   a  timestamp,\n" +
             "   b  char( 30 ) generated always as ( a ),\n" +
             "   c  varchar( 30 ) generated always as ( a ),\n" +
             "   d timestamp generated always as ( a )\n" +
             ")\n"
             );

        //
        // Populate
        //
        goodStatement
            (
             conn,
             "insert into t_dt_smallint( a ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_int( a ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_bigint( a ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_decimal( a ) values ( 1.0 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_real( a ) values ( 1.0 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_double( a ) values ( 1.0 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_float( a ) values ( 1.0 )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_dt_char( a ) values ( '1994-02-23' )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_varchar( a ) values ( '1994-02-23' )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_dt_longvarchar( a ) values ( 'foo' )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_clob( a ) values ( 'foo' )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_dt_charforbitdata( a ) values ( X'ABCDEFAB' )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_varcharforbitdata( a ) values ( X'ABCDEFAB' )"
             );
        goodStatement
            (
             conn,
             "insert into t_dt_longvarcharforbitdata( a ) values ( X'ABCDEFAB' )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_dt_date( a ) values ( date('1994-02-23') )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_dt_time( a ) values ( time('15:09:02') )"
             );
        
        goodStatement
            (
             conn,
             "insert into t_dt_timestamp( a ) values ( timestamp('1962-09-23 03:23:34.234') )"
             );
        
        //
        // Verify that the correct results were inserted.
        //
        assertResults
            (
             conn,
             "select * from t_dt_smallint order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dt_int order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dt_bigint order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dt_decimal order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dt_real order by a",
             new String[][]
             {
                 { "1.0" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dt_double order by a",
             new String[][]
             {
                 { "1.0" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dt_float order by a",
             new String[][]
             {
                 { "1.0" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        assertResults
            (
             conn,
             "select * from t_dt_char order by a",
             new String[][]
             {
                 { "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "15:09:02", "1994-02-23 03:23:34.234" },
             },
             true
             );
        assertResults
            (
             conn,
             "select * from t_dt_varchar order by a",
             new String[][]
             {
                 { "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "15:09:02", "1994-02-23 03:23:34.234" },
             },
             true
             );

        assertResults
            (
             conn,
             "select * from t_dt_longvarchar",
             new String[][]
             {
                 { "foo", "FOO", "FOO", "FOO", "FOO", },
             },
             true
             );
        assertResults
            (
             conn,
             "select * from t_dt_clob",
             new String[][]
             {
                 { "foo", "FOO", "FOO", "FOO", "FOO", },
             },
             true
             );

        assertResults
            (
             conn,
             "select * from t_dt_charforbitdata",
             new String[][]
             {
                 { "abcdefab", "abcdefab", "abcdefab", "abcdefab", },
             },
             true
             );
        assertResults
            (
             conn,
             "select * from t_dt_varcharforbitdata",
             new String[][]
             {
                 { "abcdefab", "abcdefab", "abcdefab", "abcdefab", },
             },
             true
             );
        assertResults
            (
             conn,
             "select * from t_dt_longvarcharforbitdata",
             new String[][]
             {
                 { "abcdefab", "abcdefab", "abcdefab", "abcdefab", },
             },
             true
             );

        assertResults
            (
             conn,
             "select * from t_dt_date order by a",
             new String[][]
             {
                 { "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", },
             },
             true
             );

        assertResults
            (
             conn,
             "select * from t_dt_time order by a",
             new String[][]
             {
                 { "15:09:02", "15:09:02", "15:09:02", "15:09:02", },
             },
             true
             );

        assertResults
            (
             conn,
             "select * from t_dt_timestamp order by a",
             new String[][]
             {
                 { "1962-09-23 03:23:34.234", "1962-09-23 03:23:34.234", "1962-09-23 03:23:34.234", "1962-09-23 03:23:34.234", },
             },
             true
             );

    }

    /**
     * <p>
     * Test that delete/update referential actions don't override generated columns.
     * </p>
     */
    public  void    test_015_foreignKeyActions()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t1_for_ra( a int, b int primary key )"
             );
        goodStatement
            (
             conn,
             "create table t3_for_ra( a int, b int, constraint t3_for_ra_pk primary key( a, b ) )"
             );
        goodStatement
            (
             conn,
             "create table t6_for_ra( a int, b int generated always as (-a) )"
             );

        //
        // Can't override a generated value via a cascading foreign key action.
        //
        expectCompilationError
            (
             BAD_FOREIGN_KEY_ACTION,
             "create table t2_for_ra( a int, b int generated always as ( -a ) references t1_for_ra( b ) on delete set null )"
             );
        expectCompilationError
            (
             BAD_FOREIGN_KEY_ACTION,
             "create table t2_for_ra( a int, b int generated always as ( -a ) references t1_for_ra( b ) on delete set default )"
             );
        expectCompilationError
            (
             BAD_FOREIGN_KEY_ACTION,
             "create table t4_for_ra\n" +
             "(\n" +
             "   aa int,\n" +
             "   bb int generated always as ( -aa ),\n" +
             "   cc int,\n" +
             "   constraint t4_for_ra_fk foreign key( aa, bb ) references t3_for_ra( a, b ) on delete set null\n" +
             ")\n"
             );
        expectCompilationError
            (
             BAD_FOREIGN_KEY_ACTION,
             "create table t4_for_ra\n" +
             "(\n" +
             "   aa int,\n" +
             "   bb int generated always as ( -aa ),\n" +
             "   cc int,\n" +
             "   constraint t4_for_ra_fk foreign key( aa, bb ) references t3_for_ra( a, b ) on delete set default\n" +
             ")\n"
             );
        expectCompilationError
            (
             BAD_FOREIGN_KEY_ACTION,
             "alter table t6_for_ra\n" +
             "  add constraint t6_for_ra_fk foreign key( b ) references t1_for_ra( b ) on delete set null\n"
             );
        expectCompilationError
            (
             BAD_FOREIGN_KEY_ACTION,
             "alter table t6_for_ra\n" +
             "  add constraint t6_for_ra_fk foreign key( b ) references t1_for_ra( b ) on delete set default\n"
             );

        //
        // We don't currently support this syntax. But when we do, we want to
        // make sure that we remember to disallow this use of generated columns.
        // This will involve adding some more logic to TableElementList.validateForeignKeysOnGenerationClauses().
        //
        expectCompilationError
            (
             SYNTAX_ERROR,
             "alter table t6_for_ra\n" +
             "  add constraint t6_for_ra_fk foreign key( b ) references t1_for_ra( b ) on update cascade\n"
             );
    }
    
    /**
     * <p>
     * Test NOT NULL constraints on generated columns.
     * </p>
     */
    public  void    test_016_notNull()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t1_nn( a int, b int generated always as (-a) not null, c int )"
             );
        goodStatement
            (
             conn,
             "create table t2_nn( a int, c int )"
             );

        //
        // Populate first table
        //
        goodStatement
            (
             conn,
             "insert into t1_nn( a ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             NOT_NULL_VIOLATION,
             "insert into t1_nn( c ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "update t1_nn set a = a + 1"
             );
        expectExecutionError
            (
             conn,
             NOT_NULL_VIOLATION,
             "update t1_nn set a = null"
             );
        assertResults
            (
             conn,
             "select * from t1_nn order by a",
             new String[][]
             {
                 { "2", "-2", null, },
             },
             true
             );
        
        //
        // Populate and alter second table
        //
        goodStatement
            (
             conn,
             "insert into t2_nn values ( 1, 1 )"
             );
        goodStatement
            (
             conn,
             "alter table t2_nn\n" +
             "  add column b int generated always as (-a) not null\n"
             );
        goodStatement
            (
             conn,
             "insert into t2_nn( a ) values ( 2 )"
             );
        expectExecutionError
            (
             conn,
             NOT_NULL_VIOLATION,
             "insert into t2_nn( c ) values ( 10 )"
             );
        goodStatement
            (
             conn,
             "update t2_nn set a = a + 1"
             );
        expectExecutionError
            (
             conn,
             NOT_NULL_VIOLATION,
             "update t2_nn set a = null"
             );
        assertResults
            (
             conn,
             "select * from t2_nn order by a",
             new String[][]
             {
                 { "2", "1",   "-2", },
                 { "3", null, "-3", },
             },
             true
             );
    }

    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Run good DDL.
     * @throws SQLException 
     */
    private void    goodStatement( Connection conn, String ddl ) throws SQLException
    {
        PreparedStatement    ps = chattyPrepare( conn, ddl );

        ps.execute();
        ps.close();
    }
    
    /**
     * Prepare a statement and report its sql text.
     */
    private PreparedStatement   chattyPrepare( Connection conn, String text )
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return conn.prepareStatement( text );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    private void    expectCompilationError( String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        assertCompileError( sqlState, query );
    }

    /**
     * Assert that the statement text, when executed, raises an error.
     */
    private void    expectExecutionError( Connection conn, String sqlState, String query )
        throws Exception
    {
        println( "\nExpecting " + sqlState + " when executing:\n\t"  );
        PreparedStatement   ps = chattyPrepare( conn, query );

        assertStatementError( sqlState, ps );
    }

    /**
     * Assert that triggers fire correctly
     */
    private void assertTriggerStatus( Connection conn, String query, String[][] rows )
        throws Exception
    {
        goodStatement
            (
             conn,
             "call clearTriggerReports()\n"
             );
        goodStatement
            (
             conn,
             query
             );
        PreparedStatement   ps = chattyPrepare( conn, "select * from table( triggerReports() ) s" );
        ResultSet                   rs = ps.executeQuery();

        assertResults( rs, rows, true );

        rs.close();
        ps.close();
    }

    /**
     * <p>
     * Assert whether a routine is expected to be DETERMINISTIC.
     * </p>
     */
    public  void    assertDeterministic( Connection conn, String routineName, boolean isDeterministic )
        throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement
            (
             "select a.aliasinfo\n" +
             "from sys.sysaliases a\n" +
             "where alias =  ?"
             );
        ps.setString( 1, routineName );
        ResultSet               rs = ps.executeQuery();

        rs.next();
        RoutineAliasInfo    rai = (RoutineAliasInfo) rs.getObject( 1 );

        assertEquals( isDeterministic, rai.isDeterministic() );

        rs.close();
        ps.close();
    }

    /**
     * Assert that the statement returns the correct results.
     */
    private void assertResults( Connection conn, String query, String[][] rows, boolean trimResults )
        throws Exception
    {
        PreparedStatement   ps = chattyPrepare( conn, query );
        ResultSet                   rs = ps.executeQuery();

        assertResults( rs, rows, trimResults );

        rs.close();
        ps.close();
    }
        
    /**
     * Assert that the ResultSet returns the desired rows.
     */
    private void assertResults( ResultSet rs, String[][] rows, boolean trimResults )
        throws Exception
    {
        int     rowCount = rows.length;

        for ( int i = 0; i < rowCount; i++ )
        {
            String[]    row = rows[ i ];
            int             columnCount = row.length;

            assertTrue( rs.next() );

            for ( int j = 0; j < columnCount; j++ )
            {
                String  expectedValue =  row[ j ];
                String  actualValue = null;
                int         column = j+1;

                actualValue = rs.getString( column );
                if ( rs.wasNull() ) { actualValue = null; }

                if ( (actualValue != null) && trimResults ) { actualValue = actualValue.trim(); }
                
                assertEquals( (expectedValue == null), rs.wasNull() );
                
                if ( expectedValue == null )    { assertNull( actualValue ); }
                else { assertEquals(expectedValue, actualValue); }
            }
        }

        assertFalse( rs.next() );
    }

    // read the counter of the number of times that the minus function has been
    // called
    private int readMinusCounter( Connection conn )
        throws Exception
    {
        PreparedStatement   ps = chattyPrepare( conn, "values ( f_readMinusCounter() )" );
        ResultSet                   rs = ps.executeQuery();

        rs.next();

        int     result = rs.getInt( 1 );

        rs.close();
        ps.close();

        return result;
    }

    
    /**
     * <p>
     * Assert that a column has the expected generation clause.
     * </p>
     */
    private void assertDefaultInfo
        ( Connection conn, String tableName, String columnName, int[] expectedReferenceColumns, String expectedDefaultText )
        throws Exception
    {
        DefaultInfo di = getColumnDefault( conn, tableName, columnName );

        assertEquals
            ( StringUtil.stringify( expectedReferenceColumns ), StringUtil.stringify( di.getReferencedColumnIDs() ) );
        assertEquals( expectedDefaultText, di.getDefaultText() );

        assertTrue( di.isGeneratedColumn() );
    }
    
    /**
     * <p>
     * Returns the column default for a column.
     * </p>
     */
    public  DefaultInfo  getColumnDefault( Connection conn, String tableName, String columnName )
        throws SQLException
    {
        PreparedStatement   ps = chattyPrepare
            (
             conn,
             "select c.columndefault\n" +
             "from sys.syscolumns c, sys.systables t\n" +
             "where t.tableid = c.referenceid\n" +
             "and t.tablename = ?\n" +
             "and c.columnname = ?"
             );
        ps.setString( 1, tableName );
        ps.setString( 2, columnName );

        ResultSet       rs = ps.executeQuery();
        rs.next();

        DefaultInfo result = (DefaultInfo) rs.getObject( 1 );

        rs.close();
        ps.close();

        return result;
    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static   int minus( int a )
    {
        _minusCounter++;

        return -a;
    }
    
    public static   int readMinusCounter()
    {
        return _minusCounter;
    }

    public  static  void    clearTriggerReports()
    {
        _triggerReports.clear();
    }

    public  static  ResultSet  triggerReport()
    {
        int             count = _triggerReports.size();
        String[][]      rows = new String[ count ][];

        for ( int i = 0; i < count; i++ )
        {
            rows[ i ] = new String[] { (String) _triggerReports.get( i ) };
        }

        return new StringArrayVTI( new String[] { "contents" }, rows );
    }

    public  static  void    showValues( String tag, Integer a, Integer b, Integer c )
    {
        StringBuffer    buffer = new StringBuffer();

        buffer.append( tag );
        buffer.append( ": [ " );
        buffer.append( a ); buffer.append( ", " );
        buffer.append( b ); buffer.append( ", " );
        buffer.append( c );
        buffer.append( " ]" );

        String  result = buffer.toString();

        _triggerReports.add( result );
    }
    
    public  static  void    showValues
        ( String tag, Integer old_a, Integer old_b, Integer old_c, Integer new_a, Integer new_b, Integer new_c )
    {
        StringBuffer    buffer = new StringBuffer();

        buffer.append( tag );
        buffer.append( ": [ " );
        buffer.append( old_a ); buffer.append( ", " );
        buffer.append( old_b ); buffer.append( ", " );
        buffer.append( old_c ); buffer.append( ", " );
        buffer.append( new_a ); buffer.append( ", " );
        buffer.append( new_b ); buffer.append( ", " );
        buffer.append( new_c );
        buffer.append( " ]" );

        String  result = buffer.toString();

        _triggerReports.add( result );
    }


}