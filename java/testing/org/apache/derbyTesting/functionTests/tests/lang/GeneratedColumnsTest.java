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
import org.apache.derbyTesting.junit.SupportFilesSetup;

import org.apache.derby.catalog.types.RoutineAliasInfo;

/**
 * <p>
 * Test generated columns. See DERBY-481.
 * </p>
 */
public class GeneratedColumnsTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  IMPORT_FILE_NAME = "t_bi_1.dat";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  int _minusCounter;

    private static  ArrayList   _triggerReports = new ArrayList();

    private String  _clearingProcName;
    private String  _triggerReportVTIName;

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
        Test        cleanDatabaseSuite = new CleanDatabaseTestSetup( suite );

        //
        // Copies the data file to a location which can be read.
        //
        Test        result = new SupportFilesSetup
            (
             cleanDatabaseSuite,
             new String [] { "functionTests/tests/lang/" + IMPORT_FILE_NAME }
             );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Test for DERBY-4448 and DERBY-4451: removal of explicitly given values
     * for generated column failed if there is more than one row in the VALUES
     * clause.
     */
    public void testDerby_4448_4451() throws SQLException {

        //  DERBY-4451

        Statement s = createStatement();
        ResultSet rs = null;
        setAutoCommit(false);

        s.execute("create table t(a int, b generated always as (-a))");
        s.execute("insert into t(b,a) values (default,1)");


        // Trying to override a generation clause

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) select a,b from t union select a,b from t"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(a,b) select * from t union select * from t"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) select * from t union select * from t"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) select * from t intersect select * from t"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) select * from t except select * from t"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) select a,b from t"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(a,b) values (1,1)"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) values (default,1), (2, 2)"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) values (default,1), (default, 2),(3,3)"
             );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b,a) values (1,1), (default, 2),(default,3)"
             );

        // Originally repro: failed prior to fix with array out of bounds
        // (insane), or ASSERT (sane):
        s.execute("insert into t(b,a) values (default,1), (default, 2)");

        rs = s.executeQuery("select * from t");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1", "-1"},
                {"1", "-1"},
                {"2", "-2"}});

        // DERBY-4448:
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b) values (2)"
            );

        // Originally repro for DERBY-4448: failed with array out of bounds
        // prior to fix:
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b) values (default), (2)"
            );

        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t(b) values (default), (default), (2)"
            );

        rollback();
    }


    /**
     * Test for DERBY-4426
     */
    public void testDerby_4426() throws SQLException {

        Statement s = createStatement();
        ResultSet rs = null;
        setAutoCommit(false);

        s.execute("create table t(a int, b generated always as (-a))");
        s.execute("insert into t(b,a) values (default,1)");

        // Wrong use of default
        expectCompilationError
            (
             LANG_INVALID_USE_OF_DEFAULT,
             "insert into t(b,a) values (default,3) intersect " +
             "                   values (default,3)"
             );

        expectCompilationError
            (
             LANG_INVALID_USE_OF_DEFAULT,
             "insert into t(a,b) values (3,default) except values (3,default)"
             );

        expectCompilationError
            (
             LANG_INVALID_USE_OF_DEFAULT,
             "insert into t values (3,default) union values (3,default)"
             );


        rollback();
    }

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
             "call report_proc( 'before_insert_row_trigger', ar.a, ar.a, ar.c )\n"
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
             "call wide_report_proc( 'before_update_row_trigger', br.a, br.b, br.c, ar.a, ar.a, ar.c )\n"
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

        _clearingProcName = "clearTriggerReports";
        _triggerReportVTIName = "triggerReports";
        
        //
        // Now run the tests.
        //
        assertTriggerStatus
            (
             conn,
             "insert into t1_trig( a ) values ( 1 ), ( 2 ), ( 3 )",
             new String[][]
             {
                 { "before_insert_row_trigger: [ 1, 1, null ]" },
                 { "before_insert_row_trigger: [ 2, 2, null ]" },
                 { "before_insert_row_trigger: [ 3, 3, null ]" },
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
                 { "before_update_row_trigger: [ 1, -1, null, 11, 11, null ]" },
                 { "before_update_row_trigger: [ 2, -2, null, 12, 12, null ]" },
                 { "before_update_row_trigger: [ 3, -3, null, 13, 13, null ]" },
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
             new String[] {},
             "1"
             );
        assertDefaultInfo
            (
             conn, "T_DI_2", "B",
             new String[] { "A" },
             "-a"
             );
        assertDefaultInfo
            (
             conn, "T_DI_3", "B",
             new String[] { "A", "C" },
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
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b date generated always as ( current schema ) )"
             );
        expectCompilationError
            (
             UNSTABLE_RESULTS,
             "create table t_br_2( a int, b date generated always as ( current sqlid ) )"
             );
    }

    /**
     * <p>
     * Test that the declared datatype of a generated column can be anything that the
     * generation clause can be assigned to. The legal assignments are described
     * in the Reference Guide in the section titled "Data type assignments and comparison, sorting, and ordering".
     * Note that this is a subset of the legal casts described for the CAST
     * operator. Here we are verifying the behavior described in the SQL
     * standard, Part 2, section 11.4, syntax rule 9.
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

    /**
     * <p>
     * Test padding and truncation of character and fixed decimal data.
     * </p>
     */
    public  void    test_017_padding()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t_cc_char\n" +
             "(\n" +
             "    a char( 10 ),\n" +
             "    b char( 5 ) generated always as( cast(upper( a ) as char(5))),\n" +
             "    c char( 10 ) generated always as( upper( a ) ),\n" +
             "    d char( 15 ) generated always as( upper( a ) ),\n" +
             "    e varchar( 5 ) generated always as( cast(upper( a ) as varchar(5))),\n" +
             "    f varchar( 10 ) generated always as( upper( a ) ),\n" +
             "    g varchar( 15 ) generated always as( upper( a ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_cc_varchar\n" +
             "(\n" +
             "    a varchar( 10 ),\n" +
             "    b char( 5 ) generated always as( cast(upper( a ) as char(5))),\n" +
             "    c char( 10 ) generated always as( upper( a ) ),\n" +
             "    d char( 15 ) generated always as( upper( a ) ),\n" +
             "    e varchar( 5 ) generated always as( cast(upper( a ) as varchar(5))),\n" +
             "    f varchar( 10 ) generated always as( upper( a ) ),\n" +
             "    g varchar( 15 ) generated always as( upper( a ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table t_cc_decimal\n" +
             "(\n" +
             "    a decimal( 6, 2 ),\n" +
             "    b decimal( 5, 1 ) generated always as ( a ),\n" +
             "    c decimal( 5, 2 ) generated always as ( a ),\n" +
             "    d decimal( 5, 3 ) generated always as ( a ),\n" +
             "    e decimal( 6, 1 ) generated always as ( a ),\n" +
             "    f decimal( 6, 2 ) generated always as ( a ),\n" +
             "    g decimal( 6, 3 ) generated always as ( a ),\n" +
             "    h decimal( 7, 1 ) generated always as ( a ),\n" +
             "    i decimal( 7, 2 ) generated always as ( a ),\n" +
             "    j decimal( 7, 3 ) generated always as ( a )\n" +
             ")\n"
             );

        //
        // Populate
        //
        goodStatement
            (
             conn,
             "insert into t_cc_char( a ) values ( 'abcdefghij' )"
             );
        goodStatement
            (
             conn,
             "insert into t_cc_varchar( a ) values ( 'abcdefghij' )"
             );
        goodStatement
            (
             conn,
             "insert into t_cc_decimal( a ) values ( 12.345 )"
             );

        //
        // Verify
        //
        assertResults
            (
             conn,
             "select * from t_cc_char order by a",
             new String[][]
             {
                 { "abcdefghij", "ABCDE", "ABCDEFGHIJ", "ABCDEFGHIJ     ", "ABCDE", "ABCDEFGHIJ", "ABCDEFGHIJ", },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_cc_varchar order by a",
             new String[][]
             {
                 { "abcdefghij", "ABCDE", "ABCDEFGHIJ", "ABCDEFGHIJ     ", "ABCDE", "ABCDEFGHIJ", "ABCDEFGHIJ", },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_cc_decimal order by a",
             new String[][]
             {
                 { "12.34", "12.3", "12.34", "12.340", "12.3", "12.34", "12.340", "12.3", "12.34", "12.340", },
             },
             true
             );
    }
    
    /**
     * <p>
     * Test datatype alteration
     * </p>
     */
    public  void    test_018_alterDatatype()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t_atac_1( a char( 5 ), b varchar( 5 ) generated always as ( upper( a )  ) )"
             );
        goodStatement
            (
             conn,
             "create table t_atac_2( a char( 5 ) for bit data,  b varchar( 5 ) for bit data generated always as ( a )  )"
             );
        goodStatement
            (
             conn,
             "create table t_atac_3( a varchar( 5 ), b varchar( 5 ) generated always as ( cast(upper( a ) as varchar(5)) ) )"
             );
        goodStatement
            (
             conn,
             "create table t_atac_4( a varchar( 5 ) for bit data,  b varchar( 5 ) for bit data generated always as ( cast(a as varchar( 5 ) for bit data))  )"
             );

        //
        // Populate
        //
        goodStatement
            (
             conn,
             "insert into t_atac_1( a ) values ( 'abc' )"
             );
        goodStatement
            (
             conn,
             "insert into t_atac_2( a ) values ( X'AB' )"
             );
        goodStatement
            (
             conn,
             "insert into t_atac_3( a ) values ( 'abc' )"
             );
        goodStatement
            (
             conn,
             "insert into t_atac_3( a ) values ( 'abcde' )"
             );
        goodStatement
            (
             conn,
             "insert into t_atac_4( a ) values ( X'AB' )"
             );
        goodStatement
            (
             conn,
             "insert into t_atac_4( a ) values ( X'ABCDEFABCD' )"
             );
        
        //
        // Change the lengths of columns
        //
        goodStatement
            (
             conn,
             "alter table t_atac_1\n" +
             "  alter column b set data type varchar( 10 )\n"
             );
        goodStatement
            (
             conn,
             "alter table t_atac_2\n" +
             "  alter column b set data type varchar( 10 ) for bit data\n"
             );
        goodStatement
            (
             conn,
             "alter table t_atac_3\n" +
             "  alter column a set data type varchar( 10 )\n"
             );
        goodStatement
            (
             conn,
             "alter table t_atac_4\n" +
             "  alter column a set data type varchar( 10 ) for bit data\n"
             );
        
        //
        // Insert some more data
        //
        goodStatement
            (
             conn,
             "insert into t_atac_3( a ) values ( 'abcdefg' )"
             );
        goodStatement
            (
             conn,
             "insert into t_atac_4( a ) values ( X'ABCDEFABCDAB' )"
             );
        
        //
        // Verify contents
        //
        assertResults
            (
             conn,
             "select * from t_atac_1 order by a",
             new String[][]
             {
                 { "abc  ", "ABC  " },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_atac_2 order by a",
             new String[][]
             {
                 { "ab20202020", "ab20202020" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_atac_3 order by a",
             new String[][]
             {
                 { "abc", "ABC" },
                 { "abcde", "ABCDE" },
                 { "abcdefg", "ABCDE" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_atac_4 order by a",
             new String[][]
             {
                 { "ab", "ab" },
                 { "abcdefabcd", "abcdefabcd" }, 
                 { "abcdefabcdab", "abcdefabcd" },
             },
             false
             );
    }
    
    /**
     * <p>
     * Test ALTER TABLE DROP COLUMN
     * </p>
     */
    public  void    test_019_dropColumn()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Verify that you can directly drop generated columns
        //
        goodStatement
            (
             conn,
             "create table t_dc_1( a int, b int, c int generated always as ( -b ), d int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_1( b, d ) values ( 1, 1 )"
             );
        goodStatement
            (
             conn,
             "alter table t_dc_1 drop column c restrict"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_1( b, d ) values ( 1, 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_1 order by d",
             new String[][]
             {
                 { null, "1", "1", },
                 { null, "1", "1", },
             },
             false
             );

        //
        // Verify that a generated column blocks the RESTRICTed drop of columns
        // that it references.
        //
        goodStatement
            (
             conn,
             "create table t_dc_2( a int, b int, c int generated always as ( -b ), d int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_2( b, d ) values ( 1, 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_2( d ) values ( 2 )"
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "alter table t_dc_2 drop column b restrict"
             );
        expectExecutionWarning
            (
             conn,
             CASCADED_COLUMN_DROP_WARNING,
             "alter table t_dc_2 drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_2( d ) values ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_2( a, d ) values ( 3, 3 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_2 order by d",
             new String[][]
             {
                 { null, "1", },
                 { null, "2", },
                 { null, "2", },
                 { "3", "3", },
             },
             false
             );
        goodStatement
            (
             conn,
             "alter table t_dc_2 drop column a restrict"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_2( d ) values ( 4 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_2 order by d",
             new String[][]
             {
                 { "1", },
                 { "2", },
                 { "2", },
                 { "3", },
                 { "4", },
             },
             false
             );

        //
        // Verify that dropping columns before and after a generated column
        // correctly recompiles INSERT statements on the table.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_3( a int, b int, c int, d int generated always as ( -c ), e int, f int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( b, c, f ) values ( 1, 1, 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( c ) values ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( f ) values ( 3 )"
             );
        goodStatement
            (
             conn,
             "alter table t_dc_3 drop column a restrict"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( b, c, f ) values ( 1, 1, 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( c ) values ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( f ) values ( 3 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_3 order by f",
             new String[][]
             {
                 { "1", "1", "-1", null, "1", },
                 { "1", "1", "-1", null, "1", },
                 { null, null, null, null, "3", },
                 { null, null, null, null, "3", },
                 { null, "2", "-2", null, null, },
                 { null, "2", "-2", null, null, },
             },
             false
             );
        goodStatement
            (
             conn,
             "alter table t_dc_3 drop column e restrict"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( b, c, f ) values ( 1, 1, 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( c ) values ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( f ) values ( 3 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_3 order by f",
             new String[][]
             {
                 { "1", "1", "-1", "1", },
                 { "1", "1", "-1", "1", },
                 { "1", "1", "-1", "1", },
                 { null, null, null, "3", },
                 { null, null, null, "3", },
                 { null, null, null, "3", },
                 { null, "2", "-2", null, },
                 { null, "2", "-2", null, },
                 { null, "2", "-2", null, },
             },
             false
             );
        goodStatement
            (
             conn,
             "alter table t_dc_3 drop column c"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_3( f ) values ( 3 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_3 order by f",
             new String[][]
             {
                 { "1", "1", },
                 { "1", "1", },
                 { "1", "1", },
                 { null, "3", },
                 { null, "3", },
                 { null, "3", },
                 { null, "3", },
                 { null, null, },
                 { null, null, },
                 { null, null, },
             },
             false
             );
        
        //
        // Verify that a generated column blocks the RESTRICTed drop of the
        // columns it depends on even when there's more than one.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_4( a int, b int, c int, d int generated always as ( -(a + e) ), e int, f int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_4( a, f ) values ( 1, 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_4( a, e, f ) values ( 2, 2, 2 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_4 order by f",
             new String[][]
             {
                 { "1", null, null, null, null, "1", },
                 { "2", null, null, "-4", "2", "2", },
             },
             false
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "alter table t_dc_4 drop column a restrict"
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "alter table t_dc_4 drop column e restrict"
             );
        expectExecutionWarning
            (
             conn,
             CASCADED_COLUMN_DROP_WARNING,
             "alter table t_dc_4 drop column e"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_4( a, f ) values ( 1, 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_4 order by f",
             new String[][]
             {
                 { "1", null, null, "1", },
                 { "1", null, null, "1", },
                 { "2", null, null, "2", },
             },
             false
             );
        
        //
        // Verify that the cascaded drop of a generated column raises a warning
        // noting that the dependent column was dropped.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_5( a int generated always as ( -b ), b int, c int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_5( b, c ) values ( 100, 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_5( c ) values ( 2 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_5 order by c",
             new String[][]
             {
                 { "-100", "100", "1", },
                 { null, null, "2", },
             },
             false
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "alter table t_dc_5 drop column b restrict"
             );
        expectExecutionWarning
            (
             conn,
             CASCADED_COLUMN_DROP_WARNING,
             "alter table t_dc_5 drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_5( c ) values ( 2 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_5 order by c",
             new String[][]
             {
                 { "1", },
                 { "2", },
                 { "2", },
             },
             false
             );

        //
        // Verify that the cascaded drop of a generated column also drops
        // primary and foreign keys which depend on it.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_6_prim( a int generated always as ( -b ) primary key, b int, c int )"
             );
        goodStatement
            (
             conn,
             "create table t_dc_6_for( a int references t_dc_6_prim( a ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_6_prim( b, c ) values ( 100, 1 )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_dc_6_prim( b, c ) values ( 100, 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_6_for( a ) values ( -100 )"
             );
        expectExecutionError
            (
             conn,
             FOREIGN_KEY_VIOLATION,
             "insert into t_dc_6_for( a ) values ( -101 )"
             );
        expectExecutionWarnings
            (
             conn,
             new String[] { CASCADED_COLUMN_DROP_WARNING, CONSTRAINT_DROPPED_WARNING, CONSTRAINT_DROPPED_WARNING, },
             "alter table t_dc_6_prim drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_6_prim( c ) values ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_6_for( a ) values ( -101 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_6_prim order by c",
             new String[][]
             {
                 { "1", },
                 { "2", },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dc_6_for order by a",
             new String[][]
             {
                 { "-101", },
                 { "-100", },
             },
             false
             );
        
        //
        // Verify that cascaded drops of generated columns drop triggers which
        // mention the generated columns in their UPDATE OF clauses.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_7( a int generated always as ( -b ), b int, c int )"
             );
        goodStatement
            (
             conn,
             "create function dc_triggerReports()\n" +
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
             "create procedure dc_clearTriggerReports\n" +
             "()\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.clearTriggerReports'\n"
             );
        goodStatement
            (
             conn,
             "create procedure dc_report_proc\n" +
             "( tag varchar( 40 ), a int, b int, c int )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.showValues'\n"
             );
        goodStatement
            (
             conn,
             "create trigger t_dc_7_trig_after_update\n" +
             "after update of a\n" +
             "on t_dc_7\n" +
             "referencing new as ar\n" +
             "for each row\n" +
             "call dc_report_proc( 'after_update_row_trigger', ar.a, ar.a, ar.a ) \n"
             );
        _clearingProcName = "dc_clearTriggerReports";
        _triggerReportVTIName = "dc_triggerReports";
        goodStatement
            (
             conn,
             "insert into t_dc_7( b, c ) values ( 100, 1 )"
             );
        assertTriggerStatus
            (
             conn,
             "update t_dc_7 set b = 101",
             new String[][]
             {
                 { "after_update_row_trigger: [ -101, -101, -101 ]" },
             }
             );
        
        expectExecutionWarnings
            (
             conn,
             new String[] { CASCADED_COLUMN_DROP_WARNING, TRIGGER_DROPPED_WARNING, },
             "alter table t_dc_7 drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_7( c ) values ( 2 )"
             );
        assertTriggerStatus
            (
             conn,
             "update t_dc_7 set c = c + 1000",
             new String[][] {}
             );
        assertResults
            (
             conn,
             "select * from t_dc_7 order by c",
             new String[][]
             {
                 { "1001", },
                 { "1002", },
             },
             false
             );

        //
        // Verify that cascaded drops of generated columns prevent you from
        // dropping columns that they reference if you would end up with a table
        // that has no columns in it.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_8( a int generated always as ( -b ), b int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_8( b ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "alter table t_dc_8 drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_8( b ) values ( 2 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_8 order by b",
             new String[][]
             {
                 { "-1", "1", },
                 { "-2", "2", },
             },
             false
             );
        
        //
        // Verify that cascaded drops of generated columns drop the indexes
        // built on them.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_9( a int generated always as ( -b ), b int, c int )"
             );
        goodStatement
            (
             conn,
             "create unique index t_dc_9_a_idx on t_dc_9( a )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_9( c ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_dc_9( c ) values ( 1 )"
             );
        expectExecutionWarning
            (
             conn,
             CASCADED_COLUMN_DROP_WARNING,
             "alter table t_dc_9 drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_9( c ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_9 order by c",
             new String[][]
             {
                 { "1", },
                 { "1", },
             },
             false
             );
        
        //
        // Verify that dropping a generated column also drops check constraints
        // on it.
        //        
        goodStatement
            (
             conn,
             "create table t_dc_10( a int generated always as ( -b ) check ( a is not null ), b int, c int )"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_10( b, c ) values ( 1, 1 )"
             );
        expectExecutionError
            (
             conn,
             CONSTRAINT_VIOLATION,
             "insert into t_dc_10( c ) values ( 2 )"
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "alter table t_dc_10 drop column a restrict"
             );
        expectExecutionWarnings
            (
             conn,
             new String[] { CASCADED_COLUMN_DROP_WARNING, CONSTRAINT_DROPPED_WARNING, },
             "alter table t_dc_10 drop column b"
             );
        goodStatement
            (
             conn,
             "insert into t_dc_10( c ) values ( 2 )"
             );
        assertResults
            (
             conn,
             "select * from t_dc_10 order by c",
             new String[][]
             {
                 { "1", },
                 { "2", },
             },
             false
             );
    }
    
    /**
     * <p>
     * Test ALTER TABLE ALTER COLUMN
     * </p>
     */
    public  void    test_020_alterColumn()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Verify that you can't add a default to a generated column.
        //        
        goodStatement
            (
             conn,
             "create table t_ad_1( a int generated always as ( -b ), b int, c int )"
             );
        expectCompilationError
            (
             ILLEGAL_ADD_DEFAULT,
             "alter table t_ad_1 alter column a with default 1"
             );
        
        //
        // Verify that you can't rename a column which is mentioned by a
        // generation clause.
        //        
        expectCompilationError
            (
             ILLEGAL_RENAME,
             "rename column t_ad_1.b to d"
             );
    }

    /**
     * <p>
     * Test that generation clauses block the dropping of routines that they
     * depend on.
     * </p>
     */
    public  void    test_021_dropFunction()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement
            (
             conn,
             "create function f_fd_minus\n" +
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
             "create function f_fd_minus_2\n" +
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
             "create table t_fd_1( a int generated always as ( f_fd_minus( b ) ), b int )"
             );
        goodStatement
            (
             conn,
             "insert into t_fd_1( b ) values ( 1 )"
             );
        
        //
        // Verify that the generation clause blocks our dropping the function.
        //
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "drop function f_fd_minus"
             );
        goodStatement
            (
             conn,
             "alter table t_fd_1 drop column a"
             );
        goodStatement
            (
             conn,
             "drop function f_fd_minus"
             );
        goodStatement
            (
             conn,
             "insert into t_fd_1( b ) values ( 1 )"
             );
        
        //
        // Verify that same behavior in case the generated column was added via
        // an ALTER TABLE statement.
        //
        goodStatement
            (
             conn,
             "alter table t_fd_1 add column c int generated always as ( f_fd_minus_2( b ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_fd_1( b ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             OPERATION_FORBIDDEN,
             "drop function f_fd_minus_2"
             );
        goodStatement
            (
             conn,
             "alter table t_fd_1 drop column c"
             );
        goodStatement
            (
             conn,
             "drop function f_fd_minus_2"
             );
        goodStatement
            (
             conn,
             "insert into t_fd_1( b ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_fd_1 order by b",
             new String[][]
             {
                 { "1", },
                 { "1", },
                 { "1", },
                 { "1", },
             },
             false
             );
    }

    /**
     * <p>
     * Test that CREATE/ALTER TABLE can omit the column datatype if there is a
     * generation clause.
     * </p>
     */
    public  void    test_022_omitDatatype()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Verify basic ALTER TABLE without a column datatype
        //
        goodStatement
            (
             conn,
             "create table t_nd_1( a int )"
             );
        goodStatement
            (
             conn,
             "alter table t_nd_1 add b generated always as ( -a )"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_1( a ) values ( 1 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_1",
             new String[][]
             {
                 { "A", "INTEGER" },
                 { "B", "INTEGER" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_1 order by a",
             new String[][]
             {
                 { "1", "-1" },
             },
             false
             );

        //
        // Verify that you can't omit the datatype for other types of columns.
        //
        expectCompilationError
            (
             NEED_EXPLICIT_DATATYPE,
             "create table t_nd_2( a generated always as identity )"
             );
        expectCompilationError
            (
             CANT_ADD_IDENTITY,
             "alter table t_nd_1 add c generated always as identity"
             );

        //
        // Verify basic CREATE TABLE omitting datatype on generated column
        //
        goodStatement
            (
             conn,
             "create table t_nd_3( a int, b generated always as ( -a ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_3( a ) values ( 100 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_3",
             new String[][]
             {
                 { "A", "INTEGER" },
                 { "B", "INTEGER" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_3 order by a",
             new String[][]
             {
                 { "100", "-100" },
             },
             false
             );

        //
        // Now verify various datatypes are correctly resolved.
        //
        goodStatement
            (
             conn,
             "create table t_nd_smallint\n" +
             "(\n" +
             "   a smallint,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_smallint( a ) values ( 1 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_SMALLINT",
             new String[][]
             {
                 { "A", "SMALLINT" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_smallint order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_int\n" +
             "(\n" +
             "   a int,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_int( a ) values ( 1 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_INT",
             new String[][]
             {
                 { "A", "INTEGER" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_int order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_bigint\n" +
             "(\n" +
             "   a bigint,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_bigint( a ) values ( 1 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_BIGINT",
             new String[][]
             {
                 { "A", "BIGINT" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_bigint order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_decimal\n" +
             "(\n" +
             "   a decimal,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_decimal( a ) values ( 1.0 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_DECIMAL",
             new String[][]
             {
                 { "A", "DECIMAL(5,0)" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_decimal order by a",
             new String[][]
             {
                 { "1" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_real\n" +
             "(\n" +
             "   a real,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_real( a ) values ( 1.0 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_REAL",
             new String[][]
             {
                 { "A", "REAL" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_real order by a",
             new String[][]
             {
                 { "1.0" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_double\n" +
             "(\n" +
             "   a double,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_double( a ) values ( 1.0 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_DOUBLE",
             new String[][]
             {
                 { "A", "DOUBLE" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_double order by a",
             new String[][]
             {
                 { "1.0" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_float\n" +
             "(\n" +
             "   a float,\n" +
             "   b generated always as ( cast ( -a  as smallint ) ),\n" +
             "   c generated always as ( cast ( -a as int ) ),\n" +
             "   d generated always as ( cast( -a as bigint ) ),\n" +
             "   e generated always as ( cast ( -a as decimal ) ),\n" +
             "   f generated always as ( cast ( -a as real ) ),\n" +
             "   g generated always as ( cast ( -a as double ) ),\n" +
             "   h generated always as ( cast ( -a as float ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_float( a ) values ( 1.0 )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_FLOAT",
             new String[][]
             {
                 { "A", "DOUBLE" },
                 { "B", "SMALLINT" },
                 { "C", "INTEGER" },
                 { "D", "BIGINT" },
                 { "E", "DECIMAL(5,0)" },
                 { "F", "REAL" },
                 { "G", "DOUBLE" },
                 { "H", "DOUBLE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_float order by a",
             new String[][]
             {
                 { "1.0" , "-1", "-1", "-1", "-1", "-1.0", "-1.0", "-1.0" },
             },
             false
             );
        
        goodStatement
            (
             conn,
             "create table t_nd_char\n" +
             "(\n" +
             "   a char( 20 ),\n" +
             "   b generated always as ( cast ( upper( a ) as char( 20 ) ) ),\n" +
             "   c generated always as ( cast ( upper( a ) as varchar( 20 ) ) ),\n" +
             "   d generated always as ( cast ( upper( a ) as long varchar ) ),\n" +
             "   e generated always as ( cast ( upper( a ) as clob ) ),\n" +
             "   f generated always as ( cast( a as date ) ),\n" +
             "   g generated always as ( cast( '15:09:02' as time ) ),\n" +
             "   h generated always as ( cast( ( trim( a ) || ' 03:23:34.234' ) as timestamp ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_char( a ) values ( '1994-02-23' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_CHAR",
             new String[][]
             {
                 { "A", "CHAR(20)" },
                 { "B", "CHAR(20)" },
                 { "C", "VARCHAR(20)" },
                 { "D", "LONG VARCHAR" },
                 { "E", "CLOB(2147483647)" },
                 { "F", "DATE" },
                 { "G", "TIME NOT NULL" },
                 { "H", "TIMESTAMP" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_char order by a",
             new String[][]
             {
                 { "1994-02-23          " , "1994-02-23          ", "1994-02-23          ", "1994-02-23          ", "1994-02-23          ", "1994-02-23", "15:09:02", "1994-02-23 03:23:34.234" },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_varchar\n" +
             "(\n" +
             "   a varchar( 20 ),\n" +
             "   b generated always as ( cast ( upper( a ) as char( 20 ) ) ),\n" +
             "   c generated always as ( cast ( upper( a ) as varchar( 20 ) ) ),\n" +
             "   d generated always as ( cast ( upper( a ) as long varchar ) ),\n" +
             "   e generated always as ( cast ( upper( a ) as clob ) ),\n" +
             "   f generated always as ( cast( a as date ) ),\n" +
             "   g generated always as ( cast( '15:09:02' as time ) ),\n" +
             "   h generated always as ( cast( ( trim( a ) || ' 03:23:34.234' ) as timestamp ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_varchar( a ) values ( '1994-02-23' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_VARCHAR",
             new String[][]
             {
                 { "A", "VARCHAR(20)" },
                 { "B", "CHAR(20)" },
                 { "C", "VARCHAR(20)" },
                 { "D", "LONG VARCHAR" },
                 { "E", "CLOB(2147483647)" },
                 { "F", "DATE" },
                 { "G", "TIME NOT NULL" },
                 { "H", "TIMESTAMP" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_varchar order by a",
             new String[][]
             {
                 { "1994-02-23" , "1994-02-23          ", "1994-02-23", "1994-02-23", "1994-02-23", "1994-02-23", "15:09:02", "1994-02-23 03:23:34.234" },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_longvarchar\n" +
             "(\n" +
             "   a long varchar,\n" +
             "   b generated always as ( cast ( upper( a ) as char( 20 ) ) ),\n" +
             "   c generated always as ( cast ( upper( a ) as varchar( 20 ) ) ),\n" +
             "   d generated always as ( cast ( upper( a ) as long varchar ) ),\n" +
             "   e generated always as ( cast ( upper( a ) as clob ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_longvarchar( a ) values ( 'foo' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_LONGVARCHAR",
             new String[][]
             {
                 { "A", "LONG VARCHAR" },
                 { "B", "CHAR(20)" },
                 { "C", "VARCHAR(20)" },
                 { "D", "LONG VARCHAR" },
                 { "E", "CLOB(2147483647)" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_longvarchar",
             new String[][]
             {
                 { "foo" , "FOO                 ", "FOO", "FOO", "FOO", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_clob\n" +
             "(\n" +
             "   a clob,\n" +
             "   b generated always as ( cast ( upper( a ) as char( 20 ) ) ),\n" +
             "   c generated always as ( cast ( upper( a ) as varchar( 20 ) ) ),\n" +
             "   d generated always as ( cast ( upper( a ) as long varchar ) ),\n" +
             "   e generated always as ( cast ( upper( a ) as clob ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_clob( a ) values ( 'foo' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_CLOB",
             new String[][]
             {
                 { "A", "CLOB(2147483647)" },
                 { "B", "CHAR(20)" },
                 { "C", "VARCHAR(20)" },
                 { "D", "LONG VARCHAR" },
                 { "E", "CLOB(2147483647)" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_clob",
             new String[][]
             {
                 { "foo" , "FOO                 ", "FOO", "FOO", "FOO", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_charforbitdata\n" +
             "(\n" +
             "   a char( 4 ) for bit data,\n" +
             "   b generated always as ( cast ( a as char( 4 ) for bit data ) ),\n" +
             "   c generated always as ( cast ( a as varchar( 4 ) for bit data ) ),\n" +
             "   d generated always as ( cast ( a as long varchar for bit data ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_charforbitdata( a ) values ( X'ABCDEFAB' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_CHARFORBITDATA",
             new String[][]
             {
                 { "A", "CHAR (4) FOR BIT DATA" },
                 { "B", "CHAR (4) FOR BIT DATA" },
                 { "C", "VARCHAR (4) FOR BIT DATA" },
                 { "D", "LONG VARCHAR FOR BIT DATA" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_charforbitdata",
             new String[][]
             {
                 { "abcdefab", "abcdefab", "abcdefab", "abcdefab", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_varcharforbitdata\n" +
             "(\n" +
             "   a varchar( 4 ) for bit data,\n" +
             "   b generated always as ( cast ( a as char( 4 ) for bit data ) ),\n" +
             "   c generated always as ( cast ( a as varchar( 4 ) for bit data ) ),\n" +
             "   d generated always as ( cast ( a as long varchar for bit data ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_varcharforbitdata( a ) values ( X'ABCDEFAB' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_VARCHARFORBITDATA",
             new String[][]
             {
                 { "A", "VARCHAR (4) FOR BIT DATA" },
                 { "B", "CHAR (4) FOR BIT DATA" },
                 { "C", "VARCHAR (4) FOR BIT DATA" },
                 { "D", "LONG VARCHAR FOR BIT DATA" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_varcharforbitdata",
             new String[][]
             {
                 { "abcdefab", "abcdefab", "abcdefab", "abcdefab", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_longvarcharforbitdata\n" +
             "(\n" +
             "   a long varchar for bit data,\n" +
             "   b generated always as ( cast ( a as char( 4 ) for bit data ) ),\n" +
             "   c generated always as ( cast ( a as varchar( 4 ) for bit data ) ),\n" +
             "   d generated always as ( cast ( a as long varchar for bit data ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_longvarcharforbitdata( a ) values ( X'ABCDEFAB' )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_LONGVARCHARFORBITDATA",
             new String[][]
             {
                 { "A", "LONG VARCHAR FOR BIT DATA" },
                 { "B", "CHAR (4) FOR BIT DATA" },
                 { "C", "VARCHAR (4) FOR BIT DATA" },
                 { "D", "LONG VARCHAR FOR BIT DATA" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_longvarcharforbitdata",
             new String[][]
             {
                 { "abcdefab", "abcdefab", "abcdefab", "abcdefab", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_date\n" +
             "(\n" +
             "   a date,\n" +
             "   b generated always as ( cast ( a as char( 20 ) ) ),\n" +
             "   c generated always as ( cast ( a as varchar( 20 ) ) ),\n" +
             "   d generated always as ( cast ( a as date ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_date( a ) values ( date('1994-02-23') )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_DATE",
             new String[][]
             {
                 { "A", "DATE" },
                 { "B", "CHAR(20)" },
                 { "C", "VARCHAR(20)" },
                 { "D", "DATE" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_date",
             new String[][]
             {
                 { "1994-02-23", "1994-02-23          ", "1994-02-23", "1994-02-23", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_time\n" +
             "(\n" +
             "   a time,\n" +
             "   b generated always as ( cast ( a as char( 20 ) ) ),\n" +
             "   c generated always as ( cast ( a as varchar( 20 ) ) ),\n" +
             "   d generated always as ( cast ( a as time ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_time( a ) values (  time('15:09:02')  )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_TIME",
             new String[][]
             {
                 { "A", "TIME" },
                 { "B", "CHAR(20)" },
                 { "C", "VARCHAR(20)" },
                 { "D", "TIME" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_time",
             new String[][]
             {
                 { "15:09:02", "15:09:02            ", "15:09:02", "15:09:02", },
             },
             false
             );

        goodStatement
            (
             conn,
             "create table t_nd_timestamp\n" +
             "(\n" +
             "   a  timestamp,\n" +
             "   b  char( 30 ) generated always as ( cast( a as char( 30 ) ) ),\n" +
             "   c  varchar( 30 ) generated always as ( cast ( a as varchar( 30 ) ) ),\n" +
             "   d timestamp generated always as ( cast ( a as timestamp ) )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into t_nd_timestamp( a ) values (  timestamp('1962-09-23 03:23:34.234')  )"
             );
        assertColumnTypes
            (
             conn,
             "T_ND_TIMESTAMP",
             new String[][]
             {
                 { "A", "TIMESTAMP" },
                 { "B", "CHAR(30)" },
                 { "C", "VARCHAR(30)" },
                 { "D", "TIMESTAMP" },
             }
             );
        assertResults
            (
             conn,
             "select * from t_nd_timestamp",
             new String[][]
             {
                 { "1962-09-23 03:23:34.234", "1962-09-23 03:23:34.234       ", "1962-09-23 03:23:34.234", "1962-09-23 03:23:34.234", },
             },
             false
             );

    }
    
    /**
     * <p>
     * Test that you cannot override the value of a generated column via
     * a driving SELECT--except where the value in the driving SELECT is the
     * DEFAULT literal. Make sure that generation clauses behave like
     * autoincrement columns in this respect.
     * </p>
     */
    public  void    test_023_drivingSelect()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema and pre-population.
        //
        goodStatement
            (
             conn,
             "create table t_ds_source( a int, b int )"
             );
        goodStatement
            (
             conn,
             "create table t_ds_id( a int, b int generated always as identity )"
             );
        goodStatement
            (
             conn,
             "create table t_ds_gc( a int, b generated always as ( -a ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_ds_source( a, b ) values ( 1, 1 )"
             );
        
        //
        // DEFAULT literals ok.
        //
        goodStatement
            (
             conn,
             "insert into t_ds_id values ( 3, default )"
             );
        goodStatement
            (
             conn,
             "insert into t_ds_gc values ( 3, default )"
             );
        
        //
        // Inserts into non-generated columns OK.
        //
        goodStatement
            (
             conn,
             "insert into t_ds_id( a ) select a from t_ds_source"
             );
        goodStatement
            (
             conn,
             "insert into t_ds_gc( a ) select a from t_ds_source"
             );
        
        //
        // Other literals raise an error.
        //
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_ds_id values ( 2, 2 )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_ds_gc values ( 2, 2 )"
             );
        
        //
        // You can't stuff an overriding value from a nested SELECT
        //
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_ds_id select * from t_ds_source"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_ds_gc select * from t_ds_source"
             );
        
        //
        // You can't stuff an overriding value from a literal in a nested SELECT
        //
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_ds_id select a, 3 from t_ds_source"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_ds_gc select a, 3 from t_ds_source"
             );

        //
        // DEFAULT literal in the SELECT list is just a syntax error.
        //
        expectCompilationError
            (
             SYNTAX_ERROR,
             "insert into t_ds_id select a, default from t_ds_source"
             );
        expectCompilationError
            (
             SYNTAX_ERROR,
             "insert into t_ds_gc select a, default from t_ds_source"
             );

        //
        // Verify contents of tables.
        //
        assertResults
            (
             conn,
             "select * from t_ds_id order by b",
             new String[][]
             {
                 { "3", "1", },
                 { "1", "2", },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_ds_gc order by b",
             new String[][]
             {
                 { "3", "-3", },
                 { "1", "-1", },
             },
             false
             );

    }
    
    /**
     * <p>
     * Test that the NEW variables of BEFORE triggers do not mention generated columns.
     * </p>
     */
    public  void    test_024_beforeTriggers()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema.
        //
        goodStatement
            (
             conn,
             "create procedure t_tba_report_proc\n" +
             "( tag varchar( 40 ), a int, b int, c int )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.showValues'\n"
             );
        goodStatement
            (
             conn,
             "create procedure t_tba_wide_report_proc\n" +
             "( tag varchar( 40 ), old_a int, old_b int, old_c int, new_a int, new_b int, new_c int )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.showValues'\n"
             );
        goodStatement
            (
             conn,
             "create table t_tba_1( a int, b int generated always as ( -a ), c int )"
             );

        // BEFORE INSERT trigger that DOESN'T mention generated columns
        goodStatement
            (
             conn,
             "create trigger trig_tba_good_before_insert\n" +
             "no cascade before insert on t_tba_1\n" +
             "referencing new as ar\n" +
             "for each row\n" +
             "call t_tba_report_proc( 'before_insert_row_trigger', ar.a, ar.a, ar.a )\n"
             );

        // BEFORE INSERT trigger that DOES mention generated columns
        expectCompilationError
            (
             BAD_BEFORE_TRIGGER,
             "create trigger trig_tba_bad_before_insert\n" +
             "no cascade before insert on t_tba_1\n" +
             "referencing new as ar\n" +
             "for each row\n" +
             "call t_tba_report_proc( 'before_insert_row_trigger', ar.a, ar.b, ar.c )\n"
             );

        // AFTER INSERT trigger that DOES mention generated columns
        goodStatement
            (
             conn,
             "create trigger trig_tba_good_after_insert\n" +
             "after insert on t_tba_1\n" +
             "referencing new as ar\n" +
             "for each row\n" +
             "call t_tba_report_proc( 'after_insert_row_trigger', ar.a, ar.b, ar.c ) \n"
             );

        // BEFORE UPDATE trigger that DOESN'T mention generated columns in its
        // NEW variable
        goodStatement
            (
             conn,
             "create trigger trig_tba_good_before_update\n" +
             "no cascade before update on t_tba_1\n" +
             "referencing old as br new as ar\n" +
             "for each row\n" +
             "call t_tba_wide_report_proc( 'before_update_row_trigger', br.a, br.b, br.c, ar.a, ar.a, ar.a )\n"
             );

        // BEFORE UPDATE trigger that DOES mention generated columns in its NEW variable
        expectCompilationError
            (
             BAD_BEFORE_TRIGGER,
             "create trigger trig_tba_bad_before_update\n" +
             "no cascade before update on t_tba_1\n" +
             "referencing old as br new as ar\n" +
             "for each row\n" +
             "call t_tba_wide_report_proc( 'before_update_row_trigger', br.a, br.b, br.c, ar.a, ar.b, ar.c )\n"
             );

        // AFTER UPDATE trigger that DOES mention generated columns in its NEW
        // variable
        goodStatement
            (
             conn,
             "create trigger trig_tba_good_after_update\n" +
             "after update on t_tba_1\n" +
             "referencing old as br new as ar\n" +
             "for each row\n" +
             "call t_tba_wide_report_proc( 'after_update_row_trigger', br.a, br.b, br.c, ar.a, ar.b, ar.c )\n"
             );

        // BEFORE DELETE trigger which DOES mention generated columns
        goodStatement
            (
             conn,
             "create trigger trig_tba_before_delete\n" +
             "no cascade before delete on t_tba_1\n" +
             "referencing old as br\n" +
             "for each row\n" +
             "call t_tba_report_proc( 'before_delete_row_trigger', br.a, br.b, br.c )\n"
             );

    }

    /**
     * <p>
     * Test that you can't use updatable ResultSets to corrupt generated columns.
     * </p>
     */
    public  void    test_025_basicUpdatableResultSets()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Setup
        //
        conn.setAutoCommit( false );
        goodStatement
            (
             conn,
             "create table t_urs_1 ( a int, b generated always as ( -a ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_urs_1( a ) values ( 1 )"
             );
        conn.commit();
        Statement   stmt = conn.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE );
        ResultSet   rs = null;
        
        //
        // Verify that updates to the updatable column trigger the generation clause.
        //
        rs = executeQuery( stmt, "select * from t_urs_1 for update" );
        rs.next();
        println( "Initially ( a, b ) = ( " + rs.getInt( 1 ) + ", " + rs.getInt( 2 ) + " )" );
        rs.updateInt( 1, 2 );
        rs.updateRow();
        rs.close();
        conn.commit();
        assertResults
            (
             conn,
             "select * from t_urs_1 order by a",
             new String[][]
             {
                 { "2", "-2" },
             },
             false
             );
        conn.commit();

        //
        // Verify that updates to the generated column raise an exception.
        //
        rs = executeQuery( stmt, "select * from t_urs_1 for update" );
        rs.next();
        println( "Initially ( a, b ) = ( " + rs.getInt( 1 ) + ", " + rs.getInt( 2 ) + " )" );
        rs.updateInt( 2, 2 );
        expectUpdateRowError( rs, CANT_OVERRIDE_GENERATION_CLAUSE );
        rs.close();
        conn.commit();
        assertResults
            (
             conn,
             "select * from t_urs_1 order by a",
             new String[][]
             {
                 { "2", "-2" },
             },
             false
             );
        conn.commit();

        //
        // Verify that inserts succeed and trigger the generation clause if they
        // just poke values into non-generated columns.
        //
        rs = executeQuery( stmt, "select * from t_urs_1 for update" );
        rs.next();
        rs.moveToInsertRow();
        rs.updateInt( 1, 10 );
        rs.insertRow();
        rs.close();
        conn.commit();
        assertResults
            (
             conn,
             "select * from t_urs_1 order by a",
             new String[][]
             {
                 { "2", "-2" },
                 { "10", "-10" },
             },
             false
             );
        conn.commit();

        //
        // Verify that in-place inserts raise an exception if you try to poke a
        // value into a generated column.
        //
        rs = executeQuery( stmt, "select * from t_urs_1 for update" );
        rs.next();
        rs.moveToInsertRow();
        rs.updateInt( 2, 10 );
        expectInsertRowError( rs, CANT_OVERRIDE_GENERATION_CLAUSE );
        rs.close();
        conn.commit();
        assertResults
            (
             conn,
             "select * from t_urs_1 order by a",
             new String[][]
             {
                 { "2", "-2" },
                 { "10", "-10" },
             },
             false
             );
        conn.commit();
        conn.setAutoCommit( true );

        stmt.close();
    }

    /**
     * <p>
     * Test that we correctly handle foreign keys with ON DELETE SET NULL
     * clauses. DERBY-3964.
     * </p>
     */
    public  void    test_026_onDeleteSetNull()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Setup
        //
        goodStatement
            (
             conn,
             "create table t_dhw_1( a int primary key )"
             );
        goodStatement
            (
             conn,
             "create table t_dhw_2( a int references t_dhw_1( a ) on delete set null check ( a is null or a > 0 ), b int generated always as ( -a ) check ( b is null or b < 0 ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_dhw_1( a ) values ( 1 ), ( 2 )"
             );
        goodStatement
            (
             conn,
             "insert into t_dhw_2( a ) values( 1 )"
             );

        //
        // Verify that when you delete from the primary table, the foreign key
        // table is updated and the update percolates through to the generated column.
        //
        goodStatement
            (
             conn,
             "delete from t_dhw_1 where a = 1"
             );
        assertResults
            (
             conn,
             "select * from t_dhw_1 order by a",
             new String[][]
             {
                 { "2", },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_dhw_2 order by a",
             new String[][]
             {
                 { null, null, },
             },
             false
             );
    }
    
    /**
     * <p>
     * Test that we can put constraints on generated columns when we omit the datatype.
     * DERBY-3969.
     * </p>
     */
    public  void    test_027_constraintsNoDatatype()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Verify that we can declare check constraints on generated columns
        // which omit the datatype.
        //
        goodStatement
            (
             conn,
             "create table t_ccnd_1( a int, b generated always as ( -a ) check ( b < 0 ) )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_1( a ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             CONSTRAINT_VIOLATION,
             "insert into t_ccnd_1( a ) values ( -1 )"
             );
        goodStatement
            (
             conn,
             "alter table t_ccnd_1 add column c generated always as ( -a ) check ( c > -10 )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_1( a ) values ( 2 )"
             );
        expectExecutionError
            (
             conn,
             CONSTRAINT_VIOLATION,
             "insert into t_ccnd_1( a ) values ( 20 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_1 order by a",
             new String[][]
             {
                 { "1", "-1", "-1" },
                 { "2", "-2", "-2" },
             },
             false
             );

        //
        // Verify that we can declare foreign keys on generated columns
        // which omit the datatype.
        //
        goodStatement
            (
             conn,
             "create table t_ccnd_2( b int primary key )"
             );
        goodStatement
            (
             conn,
             "create table t_ccnd_3( a int, b generated always as ( -a ) references t_ccnd_2( b ) )"
             );
        goodStatement
            (
             conn,
             "create table t_ccnd_4( a int )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_2( b ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_3( a ) values ( -1 )"
             );
        expectExecutionError
            (
             conn,
             FOREIGN_KEY_VIOLATION,
             "insert into t_ccnd_3( a ) values ( -2 )"
             );
        goodStatement
            (
             conn,
             "alter table t_ccnd_4 add column b generated always as ( -a ) references t_ccnd_2( b )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_4( a ) values ( -1 )"
             );
        expectExecutionError
            (
             conn,
             FOREIGN_KEY_VIOLATION,
             "insert into t_ccnd_4( a ) values ( -2 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_3 order by a",
             new String[][]
             {
                 { "-1", "1", },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_4 order by a",
             new String[][]
             {
                 { "-1", "1", },
             },
             false
             );

        //
        // Verify that we can declare primary keys on generated columns
        // which omit the datatype.
        //
        goodStatement
            (
             conn,
             "create table t_ccnd_5( a int, b generated always as ( -a ) primary key )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_5( a ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_ccnd_5( a ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_5 order by a",
             new String[][]
             {
                 { "1", "-1", },
             },
             false
             );
        
        //
        // Verify that you CANNOT declare a generated column to be NOT NULL
        // if you omit the datatype.
        //
        expectCompilationError
            (
             NOT_NULL_NEEDS_DATATYPE,
             "create table t_ccnd_6( a int, b generated always as ( -a ) not null )"
             );
        goodStatement
            (
             conn,
             "create table t_ccnd_6( a int )"
             );
        expectCompilationError
            (
             NOT_NULL_NEEDS_DATATYPE,
             "alter table t_ccnd_6 add column b generated always as ( -a ) not null"
             );
        
        //
        // Verify that you CAN declare a generated column to be NOT NULL
        // if you include the datatype.
        //
        goodStatement
            (
             conn,
             "create table t_ccnd_7( a int, b int generated always as ( -a ) not null )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_7( a ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             NOT_NULL_VIOLATION,
             "insert into t_ccnd_7( a ) values ( null )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_7 order by a",
             new String[][]
             {
                 { "1", "-1", },
             },
             false
             );
        
        //
        // Verify that we can add generated columns with primary keys
        // but only if you include the datatype or if the resolved datatype
        // is not nullable.
        //
        goodStatement
            (
             conn,
             "create table t_ccnd_8( a int )"
             );
        goodStatement
            (
             conn,
             "create table t_ccnd_9( a int not null )"
             );
        expectCompilationError
            (
             CANT_CONTAIN_NULLS,
             "alter table t_ccnd_8 add column b generated always as ( -a ) primary key"
             );
        goodStatement
            (
             conn,
             "alter table t_ccnd_8 add column b int not null generated always as ( -a ) primary key"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_8( a ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             NOT_NULL_VIOLATION,
             "insert into t_ccnd_8( a ) values ( null )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_ccnd_8( a ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_8 order by a",
             new String[][]
             {
                 { "1", "-1", },
             },
             false
             );

        goodStatement
            (
             conn,
             "alter table t_ccnd_9 add column b generated always as ( -a ) primary key"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_9( a ) values ( 1 )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_ccnd_9( a ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_9 order by a",
             new String[][]
             {
                 { "1", "-1", },
             },
             false
             );
        
        //
        // Verify that we can create generated columns with unique constraints.
        //
        goodStatement
            (
             conn,
             "create table t_ccnd_10( a int, b generated always as ( -a ) unique )"
             );
        goodStatement
            (
             conn,
             "create table t_ccnd_11( a int )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_10( a ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_10( a ) values ( null )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_10( a ) values ( null )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_ccnd_10( a ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_10 order by a",
             new String[][]
             {
                 { "1", "-1", },
                 { null, null, },
                 { null, null, },
             },
             false
             );

        goodStatement
            (
             conn,
             "alter table t_ccnd_11 add column b generated always as ( -a ) unique"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_11( a ) values ( 1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_11( a ) values ( null )"
             );
        goodStatement
            (
             conn,
             "insert into t_ccnd_11( a ) values ( null )"
             );
        expectExecutionError
            (
             conn,
             ILLEGAL_DUPLICATE,
             "insert into t_ccnd_11( a ) values ( 1 )"
             );
        assertResults
            (
             conn,
             "select * from t_ccnd_11 order by a",
             new String[][]
             {
                 { "1", "-1", },
                 { null, null, },
                 { null, null, },
             },
             false
             );

    }
    
    /**
     * <p>
     * Test that bulk import works with generated columns.
     * </p>
     */
    public  void    test_028_bulkImport()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Initial setup
        //
        goodStatement
            (
             conn,
             "create table t_bi_1( a int, b int, c generated always as ( a + b ) )"
             );

        //
        // Should fail because we can't override the value of a generated column.
        //
        expectExecutionError
            (
             conn,
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "call syscs_util.syscs_import_table( null, 'T_BI_1', 'extin/" + IMPORT_FILE_NAME + "', null, null, null, 0 )"
             );

        //
        // Should be able to import partial column list which doesn't include
        // generated column. In this case, one of the referenced columns is not
        // imported so it is stuffed with a null, which then propagates through
        // the generation clause.
        //
        goodStatement
            (
             conn,
             "call syscs_util.syscs_import_data( null, 'T_BI_1', 'A', '1',  'extin/" + IMPORT_FILE_NAME + "', null, null, null, 0 )"
             );

        //
        // Partial import including all of the referenced columns.
        //
        goodStatement
            (
             conn,
             "call syscs_util.syscs_import_data( null, 'T_BI_1', 'A, B', '1, 2', 'extin/" + IMPORT_FILE_NAME + "', null, null, null, 0 )"
             );

        assertResults
            (
             conn,
             "select * from t_bi_1 order by a, b",
             new String[][]
             {
                 { "2", "3", "5", },
                 { "2", null, null, },
             },
             false
             );
    }

    /**
     * <p>
     * Test that we don't get a null pointer exception when generation clauses
     * have forward references to other generated columns.
     * </p>
     */
    public  void    test_029_derby_4145()
        throws Exception
    {
        expectCompilationError
            (
             CANT_REFERENCE_GENERATED_COLUMN,
             "create table t_4145(c1 int, c2 int, c3 generated always as (c1 + c4), c4 generated always as (-c1))"
             );
    }
    
    /**
     * <p>
     * Test that a generated column can refer to an identity column.
     * </p>
     */
    public  void    test_030_derby_4146()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
             conn,
             "create table t_4146 (c1 int generated always as identity, c2 generated always as (c1+100))"
             );
        goodStatement
            (
             conn,
             "create table t_4146_2 (c1 int generated always as identity, c2 generated always as (c1+100), c3 int default 1000)"
             );

        goodStatement
            (
             conn,
             "insert into t_4146 values ( default, default )"
             );
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_4146 values ( -1, default )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_4146 values ( default, -1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_4146 (c1, c2) values ( default, default )"
             );
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_4146 (c1, c2) values ( -1, default )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_4146 (c1, c2) values ( default, -1 )"
             );
        assertResults
            (
             conn,
             "select * from t_4146 order by c1",
             new String[][]
             {
                 { "1", "101", },
                 { "2", "102", },
             },
             false
             );

        goodStatement
            (
             conn,
             "insert into t_4146_2 values ( default, default, default )"
             );
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_4146_2 values ( -1, default, default )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_4146_2 values ( default, -1, default )"
             );
        goodStatement
            (
             conn,
             "insert into t_4146_2 (c1, c2) values ( default, default )"
             );
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_4146_2 (c1, c2) values ( -1, default )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_4146_2 (c1, c2) values ( default, -1 )"
             );
        goodStatement
            (
             conn,
             "insert into t_4146_2 (c1, c2, c3) values ( default, default, default )"
             );
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_4146_2 (c1, c2, c3) values ( -1, default, default )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_4146_2 (c1, c2, c3) values ( default, -1, default )"
             );
        goodStatement
            (
             conn,
             "insert into t_4146_2 (c1, c2, c3) values ( default, default, 2000 )"
             );
        expectCompilationError
            (
             CANT_MODIFY_IDENTITY,
             "insert into t_4146_2 (c1, c2, c3) values ( -1, default, 3000 )"
             );
        expectCompilationError
            (
             CANT_OVERRIDE_GENERATION_CLAUSE,
             "insert into t_4146_2 (c1, c2, c3) values ( default, -1, 4000 )"
             );
        assertResults
            (
             conn,
             "select * from t_4146_2 order by c1",
             new String[][]
             {
                 { "1", "101", "1000", },
                 { "2", "102", "1000", },
                 { "3", "103", "1000", },
                 { "4", "104", "2000", },
             },
             false
             );

    }


   /**
    * Test INSERT INTO .. select distinct in presence of generated column.
    * Cf DERBY-4413.
    */
    public  void    test_031_derby_4413()
            throws Exception
    {
        Connection  conn = getConnection();

        //
        // Schema
        //
        goodStatement
            (
                conn,
                "create table t_4413 (" +
                "     i integer, " +
                "     j integer not null generated always as (i*2))"
            );
        goodStatement
            (
                conn,
                "insert into t_4413(i) values 1,2,1"
            );

        goodStatement
            (
                conn,
                "insert into t_4413(i) select distinct i from t_4413"
            );
        assertResults
            (
                conn,
                "select * from t_4413 order by i, j",
                new String[][]
                {
                    { "1", "2", },
                    { "1", "2", },
                    { "1", "2", },
                    { "2", "4", },
                    { "2", "4", },
                },
                false
            );

    }

    public void test_derby_4425()
        throws Exception
    {
        Connection conn = getConnection();
        goodStatement(conn, "create table t4425_1(x int)");
        goodStatement(conn, "create table t4425_2(x int)");
        goodStatement(conn, "insert into t4425_1 values 1,2");
        goodStatement(conn, "insert into t4425_2 values 2,3");
        goodStatement(conn, "create table t4425_3 (x int, " +
                "y generated always as (2*x))");
        goodStatement(conn, "insert into t4425_3(x) " +
                "select * from t4425_1 union select * from t4425_2");
        assertResults(conn, "select * from t4425_3",
                new String[][] { {"1","2"},{"2","4"},{"3","6"}}, false);
    }

    // Derby 4779
    public void test_derby_4779()
        throws Exception
    {
    	Connection conn = getConnection();

        goodStatement
        (
         conn,
         "create function f_getRegion\n" +
         "(\n" +
         "    v int\n" +
         ")\n" +
         "returns int\n" +
         "language java\n" +
         "parameter style java\n" +
         "deterministic\n" +
         "no sql\n" +
         "external name 'org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest.signum'\n"
        );

        goodStatement
        (
         conn,
         "create table t1_orders( price int, region generated always as " +
         "( f_getRegion(price) ) )\n"
        );
        
        goodStatement
        (
         conn,
         "create table t1_dummy(a int)\n"
        );

        goodStatement
        (
         conn,
         "create trigger t1_trig_after_insert_row_trigger_4779\n" +
         "after insert on t1_orders\n" +
         "referencing new as ar\n" +
         "for each row\n" +
         "insert into t1_dummy( a ) values ( 1 )\n"
        );

        goodStatement
        (
         conn, 
         "insert into t1_orders(price) values (1), (2)"
        );

        assertResults
        ( 
         conn,
         "select a from t1_dummy",
         new String[][]
                      {
                          { "1" },
                          { "1" }
                      },
                      false
         
        );
    }


    // Derby 5749
    public void test_derby_5749()
        throws Exception
    {
        Connection conn = getConnection();

        goodStatement
        (
            conn,
            "create table t_5749\n" +
            "(c varchar(5) generated always as ('--' || b), b varchar(5))\n"
        );

        // fails on truncation
        expectExecutionError
        (
            conn,
            STRING_TRUNCATION,
            "insert into t_5749 values (default, '12345')"
        );

        // Try an update case:
        goodStatement
        (
            conn,
            "insert into t_5749 values (default, '123')"
        );

        expectExecutionError
        (
            conn,
            STRING_TRUNCATION,
            "update t_5749 set b='12345'"
        );

    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////


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
     * Assert that triggers fire correctly
     */
    private void assertTriggerStatus( Connection conn, String query, String[][] rows )
        throws Exception
    {
        goodStatement
            (
             conn,
             "call " + _clearingProcName + "()\n"
             );
        goodStatement
            (
             conn,
             query
             );
        PreparedStatement   ps = chattyPrepare( conn, "select * from table( " + _triggerReportVTIName + "() ) s" );
        ResultSet                   rs = ps.executeQuery();

        assertResults( rs, rows, true );

        rs.close();
        ps.close();
    }

    
    /**
     * <p>
     * Assert that a column has the expected generation clause.
     * </p>
     */
    private void assertDefaultInfo
        ( Connection conn, String tableName, String columnName, String[] expectedReferenceColumns, String expectedDefaultText )
        throws Exception
    {
        DefaultInfo di = getColumnDefault( conn, tableName, columnName );
        String[]        actualReferenceColumns = di.getReferencedColumnNames();

        assertEquals
            ( fill( expectedReferenceColumns ).toString(), fill( actualReferenceColumns ).toString() );
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

    public static   Integer minus( Integer a )
    {
        _minusCounter++;

        if ( a == null ) { return null; }
        else { return new Integer( -a.intValue() ); }
    }
    
    public static   int readMinusCounter()
    {
        return _minusCounter;
    }

    public static   int signum( int i )
    {
        if ( i > 0 ) { return 1; }
        else if ( i == 0 ) { return 0; }
        else { return -1; }
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
