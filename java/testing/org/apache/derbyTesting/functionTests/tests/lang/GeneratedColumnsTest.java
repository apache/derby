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
    private static  final   String  UNSTABLE_RESULTS = "42XA2";
    private static  final   String  CANT_OVERRIDE_GENERATION_CLAUSE = "42XA3";
    private static  final   String  CONSTRAINT_VIOLATION = "23513";
    private static  final   String  FOREIGN_KEY_VIOLATION = "23503";
    private static  final   String  ILLEGAL_DUPLICATE = "23505";

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
             "create table t1_trig( a int, b int generated always as ( f_minus(a) ), c int )\n"
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
             "create table t1_for( a int, b int generated always as ( f_minus(a) ) primary key, c int )"
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
             "create table t4_for( a int, b int generated always as ( f_minus(a) ) references t3_for( b ), c int )"
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