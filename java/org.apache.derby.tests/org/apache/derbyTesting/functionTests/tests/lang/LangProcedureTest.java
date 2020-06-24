/*
 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LangProcedureTest

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
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

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ProcedureTest;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the syntax for creating procedures and semantics when
 * executing them.
 */
public class LangProcedureTest extends BaseJDBCTestCase {

    private final static String LANG_STRING_TRUNCATION = "22001";
    private final static String LANG_INVALID_CALL_STATEMENT = "42X74";
    private final String thisClassName = getClass().getName();

    public LangProcedureTest(String name) {
        super(name);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Default suite for running this test (embedded and client).
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (JDBC.vmSupportsJSR169()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite(
                "Empty LangProcedureTest. " +
                "JSR169 does not support jdbc:default:connection");
        } else {
            // fix locale since we need to check error messages
            return TestConfiguration.singleUseDatabaseDecorator(
                new LocaleTestSetup(
                    TestConfiguration.defaultSuite(LangProcedureTest.class),
                    Locale.ENGLISH));
        }
    }

    /**
     * Verifies the exception that gets raised for bad syntax or unsupported
     * features.
     * 
     * @throws SQLException
     */
    public void testCreateRoutineErrors() throws SQLException {
        Statement s = createStatement();

        // The expected format is path.method (no '.').
        assertStatementError("42Y04", s,
                "create procedure asdf() language java "
                        + "external name 'asdfasdf' parameter style java");

        // The expected format is path.method (there's a trailing '.').
        assertStatementError("42Y04", s,
                "create procedure asdf() language java "
                        + "external name 'asdfasdf.' parameter style java");

        // The procedure name exceeds the max length.
        assertStatementError(
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            "42622",
            s,
            "create procedure a234567890123456789012345678901234567890123"
            + "456789012345678901234567890123456789012345678901234567890123"
            + "45678901234567890123456789() language java external name "
            + "'asdf.asdf' parameter style java");

        // "LANGUAGE C" is an incorrect language.
        assertStatementError(
            "42X01",
            s,
            "create procedure ASSEMBLY_PARTS (in ASSEMBLY_NUM integer, "
            + "out NUM_PARTS integer, out COST DOUBLE) external name "
            + "'parts!assembly' dynamic result sets 1 language C "
            + "parameter style GENERAL");

        // Not allowed to create a routine in the SYS schema.
        assertStatementError(
            "42X62",
            s,
            "create procedure sys.proc1() language java "
            + "external name 'java.lang.System.gc' parameter style java");

        // The 'LANGUAGE' clause has multiple or conflicting keywords ('java').
        assertStatementError(
            "42613",
            s,
            "create procedure noclass() language java "
            + "external name 'asdf.asdf' parameter style java language java");

        assertStatementError(
            "42613", s,
            "create procedure noclass() parameter style java language java "
            + "external name 'asdf.asdf' parameter style java");

        assertStatementError(
            "42613",
            s,
            "create procedure noclass() external name 'asdf.xxxx' "
            + "language java external name 'asdf.asdf' parameter style java");

        assertStatementError(
            "42X01",
            s,
            "create procedure noclass() parameter style java language java "
            + "external name 'asdf.asdf' parameter style derby_rs_collection");

        // The create statement is missing an element.
        assertStatementError("42X01", s, "create procedure missing01()");
        assertStatementError("42X01", s,
                "create procedure missing02() language java");

        assertStatementError(
            "42X01", s,
            "create procedure missing03() language java parameter style java");

        assertStatementError("42X01", s,
                "create procedure missing04() language java "
                        + "external name 'foo.bar'");
        assertStatementError("42X01", s,
                "create procedure missing05() parameter style java");
        assertStatementError("42X01", s,
                "create procedure missing06() parameter style java "
                        + "external name 'foo.bar'");
        assertStatementError("42X01", s,
                "create procedure missing07() external name 'goo.bar'");
        assertStatementError("42X01", s,
                "create procedure missing08() dynamic result sets 1");
        assertStatementError("0A000", s,
                "create procedure missing09() specific name fred");

        // RETURNS NULL ON NULL INPUT isn't allowed in procedures.
        assertStatementError(
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            "42X01",
            s,
            "create procedure nullinput2() returns null on null input "
            + "language java parameter style java external name 'foo.bar'");


        // Duplicate parameter names.
        assertStatementError("42734", s, "create procedure DUP_P1"
                + "(in FRED int, out RON char(10), in FRED int) "
                + "language java parameter style java external name 'no.dup1'");
        assertStatementError("42734", s, "create procedure D2.DUP_P2"
                + "(in \"FreD\" int, out RON char(10), in \"FreD\" int) "
                + "language java parameter style java external name 'no.dup2'");
        assertStatementError("42734", s, "create procedure D3.DUP_P3"
                + "(in \"FRED\" int, out RON char(10), in fred int) "
                + "language java parameter style java external name 'no.dup3'");

        // This one should succeed.
        s.execute(
            "create procedure DUP_POK"
            + "(in \"FreD\" int, out RON char(10), in fred int) "
            + "language java parameter style java external name 'no.dupok'");

        s.execute("drop procedure DUP_POK");

        // procedure not found with explicit schema name
        assertStatementError("42Y03", s, "call APP.NSP(?, ?)");

        // Long ago this caused a null pointer exception.
        assertStatementError(
            "42X15", s,
            "call syscs_util.syscs_set_database_property(\"foo\", \"bar\")");

        // Not so long ago (DERBY-6212) this caused a NullPointerException.
        assertCompileError(LANG_INVALID_CALL_STATEMENT,
          "call syscs_util.syscs_set_database_property('foo', (values 'bar'))");

        s.close();
    }

    /**
     * Verifies the fix for DERBY-258: incorrect method resolution if an
     * explicit method signature has Java type that does not match the correct
     * SQL to Java mapping as defined by JDBC.
     * 
     * @throws SQLException
     */
    public void testMethodSignatureDerby258() throws SQLException {
        Statement s = createStatement();

        // int doesn't match String
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure SIGNATURE_BUG_DERBY_258_A(in A int) "
                + "language java parameter style java "
                + "external name 'java.lang.System.load(java.lang.String)'");
        assertStatementError("22005", s,
                "call APP.SIGNATURE_BUG_DERBY_258_A(4)");
        s.execute("drop PROCEDURE SIGNATURE_BUG_DERBY_258_A");

        // Signature with too many arguments.
        s.execute("create function SIGNATURE_BUG_DERBY_258_B(A int) "
                + "RETURNS varchar(128) language java parameter style java "
                + "external name 'java.lang.Integer.toString(int, int)'");
        assertStatementError("46J02", s,
                "values APP.SIGNATURE_BUG_DERBY_258_B(4)");
        s.execute("drop FUNCTION SIGNATURE_BUG_DERBY_258_B");

        // Signature with too few arguments.
        s.execute("create procedure SIGNATURE_BUG_DERBY_258_C(in A int) "
                + "language java parameter style java "
                + "external name 'java.lang.System.gc()'");
        assertStatementError("46J02", s,
                "call APP.SIGNATURE_BUG_DERBY_258_C(4)");
        s.execute("drop PROCEDURE SIGNATURE_BUG_DERBY_258_C");

        // Java method signature has only a leading parenthesis.
        s.execute("create procedure SIGNATURE_BUG_DERBY_258_F(in A int) "
                + "language java parameter style java "
                + "external name 'java.lang.System.gc('");
        assertStatementError("46J01", s,
                "call APP.SIGNATURE_BUG_DERBY_258_F(4)");
        s.execute("drop PROCEDURE SIGNATURE_BUG_DERBY_258_F");

        // Java method signature of (,,)
        s.execute("create procedure SIGNATURE_BUG_DERBY_258_G(in A int) "
                + "language java parameter style java "
                + "external name 'java.lang.System.gc(,,)'");
        assertStatementError("46J01", s,
                "call APP.SIGNATURE_BUG_DERBY_258_G(4)");
        s.execute("drop PROCEDURE SIGNATURE_BUG_DERBY_258_G");

        // Java method signature of (, ,)
        s.execute("create procedure SIGNATURE_BUG_DERBY_258_H(in A int) "
                + "language java parameter style java "
                + "external name 'java.lang.System.gc(, ,)'");
        assertStatementError("46J01", s,
                "call APP.SIGNATURE_BUG_DERBY_258_H(4)");
        s.execute("drop PROCEDURE SIGNATURE_BUG_DERBY_258_H");

        // Java method signature of (int,)
        s.execute("create procedure SIGNATURE_BUG_DERBY_258_I(in A int) "
                + "language java parameter style java "
                + "external name 'java.lang.System.gc(int ,)'");
        assertStatementError("46J01", s,
                "call APP.SIGNATURE_BUG_DERBY_258_I(4)");
        s.execute("drop PROCEDURE SIGNATURE_BUG_DERBY_258_I");

        s.execute("create procedure DERBY_3304() "
                + " dynamic result sets 1 language java parameter style java "
                + " external name '" + getClass().getName() + ".DERBY_3304'"
                + " modifies sql data");
        String[][] t1Results = { { "APP"} };
        ResultSet rs = s.executeQuery("call APP.DERBY_3304()");
        JDBC.assertFullResultSet(rs, t1Results);
        s.execute("drop PROCEDURE DERBY_3304");

        s.close();
    }

    /**
     * This procedure does an explicit commit and then creates a resultset
     * to be passed back to the caller. As part of commit, we should not
     * close the resultset that will be returned by this procedure.
     * 
     * @param rs1
     * @throws SQLException
     */
    public static void DERBY_3304(ResultSet[] rs1) throws SQLException 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");
        Statement stm = conn.createStatement();
        conn.commit();
        ResultSet rs = stm.executeQuery("values current_user");
        rs1[0] = rs;
    }

    /**
     * Tests the exception that gets thrown at runtime when the external method
     * for a SQL procedure doesn't exist -- there's no check for existence at
     * CREATE time, the check occurs at runtime.
     * 
     * @throws SQLException
     */
    public void testDelayedClassChecking() throws SQLException {
        Statement s = createStatement();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute(
            "create procedure noclass() language java " +
            "external name 'asdf.asdf' parameter style java");

        s.execute(
            "create procedure nomethod() language java " +
            "external name 'java.lang.Integer.asdf' parameter style java");

        s.execute(
            "create procedure notstatic() language java " +
            "external name 'java.lang.Integer.equals' parameter style java");

        s.execute(
            "create procedure notvoid() language java " +
            "external name " +
            "'java.lang.Runtime.getRuntime' parameter style java");

        assertCallError("42X51", "call noclass()");
        assertCallError("42X50", "call nomethod()");
        assertCallError("42X50", "call notstatic()");
        assertCallError("42X50", "call notvoid()");

        // Comment from old test: CHECK SYSALIAS
        s.execute("drop procedure noclass");
        s.execute("drop procedure nomethod");
        s.execute("drop procedure notstatic");
        s.execute("drop procedure notvoid");

        s.close();
    }

    public void testDuplicates() throws SQLException {
        Connection conn = getConnection();

        Statement s = createStatement();

        s.execute("create schema S1");
        s.execute("create schema S2");

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure    PROCDUP() language java " +
                  "external name 'okAPP.ok0' parameter style java");

        s.execute("create procedure s1.PROCDUP() language java " +
                  "external name 'oks1.ok0' parameter style java");

        s.execute("create procedure s2.PROCDUP() language java " +
                  "external name 'oks2.ok0' parameter style java");

        assertStatementError(
            "X0Y68",
            s,
            "create procedure PROCDUP() language java " +
            "external name 'failAPP.fail0' parameter style java");

        assertStatementError(
            "X0Y68",
            s,
            "create procedure s1.PROCDUP() language java " +
            "external name 'fails1.fail0' parameter style java");

        assertStatementError(
            "X0Y68",
            s,
            "create procedure s2.PROCDUP() language java " +
            "external name 'fails2.fail0' parameter style java");

        String[] sysAliasDefinition = {
            "APP.PROCDUP AS okAPP.ok0() " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA",
            "S1.PROCDUP AS oks1.ok0() " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA",
            "S2.PROCDUP AS oks2.ok0() " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA"};

        String[] DBMetaDefinition = {
                "APP.PROCDUP AS okAPP.ok0 type procedureNoResult",
                "S1.PROCDUP AS oks1.ok0 type procedureNoResult",
                "S2.PROCDUP AS oks2.ok0 type procedureNoResult" };
        checkMatchingProcedures(conn, "PROCDUP", sysAliasDefinition,
                DBMetaDefinition, null);

        assertStatementError(
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            "0A000",
            s,
            "create procedure S1.NOTYET() SPECIFIC fred language java " +
            "external name 'failAPP.fail0' parameter style java");

//IC see: https://issues.apache.org/jira/browse/DERBY-6039
        s.execute("drop procedure PROCDUP");
        s.execute("drop procedure s1.PROCDUP");
        s.execute("drop procedure s2.PROCDUP");

        s.execute("drop schema S1 RESTRICT");
        s.execute("drop schema S2 RESTRICT");
        s.close();

    }

    public void testAmbigiousMethods() throws SQLException {

        Statement s = createStatement();

        // ambiguous resolution - with result sets
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure ambiguous01(p1 integer, p2 char(20)) " +
                  "dynamic result sets 1 language java parameter style java " +
                  "external name '" + thisClassName + ".ambiguous1'");
        assertCallError("42X73", "call AMBIGUOUS01(?, ?)");
        s.execute("drop procedure AMBIGUOUS01");

        // ambiguous in defined parameters
        s.execute("create procedure ambiguous02(p1 integer, p2 integer) " +
                  "dynamic result sets 1 language java parameter style java " +
                  "external name '" + thisClassName + ".ambiguous2'");
        assertCallError("42X50", "call AMBIGUOUS02(?, ?)");
        s.execute("drop procedure AMBIGUOUS02");

        // After DERBY-3652  these are also ambiguous:
        s.execute("create procedure ambiguous03(p1 integer, p2 integer) " +
                "dynamic result sets 1 language java parameter style java " +
                "external name '" + thisClassName +
                ".ambiguous2(int,java.lang.Integer)'");
        assertCallError("42X73", "{call ambiguous03(1,NULL)}");
        s.execute("drop procedure AMBIGUOUS03");

        s.execute("create procedure ambiguous04(p1 integer, p2 integer) " +
                "dynamic result sets 1 language java parameter style java " +
                "external name '" + thisClassName +
                ".ambiguous2(java.lang.Integer,int)'");
        assertCallError("42X73", "{call ambiguous04(NULL, 1)}");
        s.execute("drop procedure AMBIGUOUS04");
        s.close();
    }

    private static void checkMatchingProcedures(Connection conn,
            String procedureName, String[] sysAliasDefinition,
            String[] DBMetaDefinition, String[] columnDefinition)
            throws SQLException {
        // Until cs defaults to hold cursor we need to turn autocommit off
        // while we do this because one metadata call will close the other's
        // cursor
        boolean saveAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        PreparedStatement ps = conn.prepareStatement(
            "select schemaname, alias, " +
            "    cast (((javaclassname || '.' ) || " +
            "    cast (aliasinfo as varchar(1000))) as varchar(2000))" +
            "        as signature " +
            "from sys.sysaliases A, sys.sysschemas S " +
            "where alias like ? and A.schemaid = S.schemaid order by 1,2,3");

        ps.setString(1, procedureName);

        ResultSet rs = ps.executeQuery();
        int i = 0;
        while (rs.next()) {
            assertEquals(sysAliasDefinition[i++], rs.getString(1) + "."
                    + rs.getString(2) + " AS " + rs.getString(3));
        }
        rs.close();

        DatabaseMetaData dmd = conn.getMetaData();

        rs = dmd.getProcedures(null, null, procedureName);
        // with jcc 2.1 for now this will fail on the second round,
        // because the resultset gets closed when we do getProcedureColumns.
        // thus, catch that gracefully...
        i = 0;
        while (rs.next()) {
            String schema = rs.getString(2);
            String name = rs.getString(3);
            assertEquals(schema + "." + name + " AS " + rs.getString(7)
                    + " type " + TYPE(rs.getShort(8)), DBMetaDefinition[i++]);
            // get the column information.
            ResultSet rsc = dmd.getProcedureColumns(null, schema, name, null);
            int j = 0;
            while (rsc.next()) {
                assertEquals(PARAMTYPE(rsc.getShort(5)) + " "
                        + rsc.getString(4) + " " + rsc.getString(7),
                        columnDefinition[j++]);
            }
            rsc.close();
        }
        rs.close();
        // restore previous autocommit mode
        conn.setAutoCommit(saveAutoCommit);
    }

    public void testZeroArgProcedures() throws SQLException {
        Connection conn = getConnection();

        Statement s = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure za() language java external name '" +
                  thisClassName + ".zeroArg' parameter style java");

        s.execute("call za()");
        assertUpdateCountForProcedureWithNoResults(s);

        PreparedStatement ps = prepareStatement("call za()");
        ps.execute();
        assertUpdateCountForProcedureWithNoResults(ps);
        ps.close();

        ps = prepareStatement("{call za()}");
        ps.execute();
        assertUpdateCountForProcedureWithNoResults(ps);
        ps.close();

        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            prepareStatement("call za(?)");
            fail("FAIL - prepareStatement call za(?)");
        } catch (SQLException sqle) {
            assertSQLState("42Y03", sqle);
        }

        CallableStatement cs = prepareCall("call za()");
        cs.execute();
        assertUpdateCountForProcedureWithNoResults(cs);
        cs.close();

        cs = prepareCall("{call za()}");
        cs.execute();
        assertUpdateCountForProcedureWithNoResults(cs);
        cs.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        String[] sysAliasDefinition = {
            "APP.ZA AS " + thisClassName + ".zeroArg() " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA"};

        String[] dbMetadataDefinition = {
            "APP.ZA AS " + thisClassName + ".zeroArg " +
            "type procedureNoResult"};

        checkMatchingProcedures(conn, "ZA", sysAliasDefinition,
                dbMetadataDefinition, null);
        s.execute("drop procedure za");
        checkMatchingProcedures(conn, "ZA", null, null, null);

        s.close();

    }

    public void testSqlProcedures() throws SQLException {
        Connection conn = getConnection();
        Statement s = createStatement();

        s.execute("create table t1(i int not null primary key, b char(15))");
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute(
                "create procedure ir(p1 int) " +
                "modifies sql data dynamic result sets 0 language java " +
                "external name '" + thisClassName + ".insertRow' " +
                "parameter style java");
        s.execute(
                "create procedure ir2(p1 int, p2 char(10)) " +
                " modifies sql data language java " +
                "external name '" + thisClassName + ".insertRow' " +
                "parameter style java");

        String[] sysaliasDefinition = {
            "APP.IR AS " + thisClassName + ".insertRow" +
            "(IN \"P1\" INTEGER) LANGUAGE JAVA " +
            "PARAMETER STYLE JAVA MODIFIES SQL DATA" };
        String[] dbMetadataDefinition = {
            "APP.IR AS " + thisClassName + ".insertRow" +
                " type procedureNoResult" };
        String[] columnDefinition = { "procedureColumnIn P1 INTEGER" };
        checkMatchingProcedures(conn, "IR", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        sysaliasDefinition = new String[] {
            "APP.IR2 AS " + thisClassName + ".insertRow" +
            "(IN \"P1\" INTEGER,IN \"P2\" CHAR(10)) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA" };
        dbMetadataDefinition = new String[] {
            "APP.IR2 AS " + thisClassName + ".insertRow" +
                " type procedureNoResult" };
        columnDefinition = new String[] { "procedureColumnIn P1 INTEGER",
                "procedureColumnIn P2 CHAR" };
        checkMatchingProcedures(conn, "IR2", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);
        assertCallError("42Y03", "call IR()");

        CallableStatement ir1 = prepareCall("call IR(?)");

        ir1.setInt(1, 1);
        ir1.execute();

        ir1.setInt(1, 2);
        ir1.execute();
        try {
            ir1.execute();
            fail("FAIL - duplicate key insertion through ir");
        } catch (SQLException sqle) {
            assertSQLState("23505", sqle);
        }

        ir1.setString(1, "3");
        ir1.execute();

        ir1.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        ir1 = conn.prepareCall("call APP.IR(?)");
        ir1.setInt(1, 7);
        ir1.execute();

        CallableStatement ir2 = conn.prepareCall("call IR2(?, ?)");

        ir2.setInt(1, 4);
        ir2.setInt(2, 4);
        ir2.execute();

        ir2.setInt(1, 5);
        ir2.setString(2, "ir2");
        ir2.execute();

        ir2.setInt(1, 6);
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        ir2.setString(2, "'012345678");
        ir2.execute();

        ir1.close();
        ir2.close();

        if (!conn.getAutoCommit()) {
            conn.commit();
        }

        String[][] t1Results = { { "1", "int" }, { "2", "int" },
                { "3", "int" }, { "7", "int" }, { "4", "4" }, { "5", "ir2" },
                { "6", "'012345678" } };
        ResultSet rs = s.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, t1Results);

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (!conn.getAutoCommit()) {
            conn.commit();
        }

        assertCallError("38000", "call IR2(2, 'no way')");
        assertCallError("07000", "call IR2(?, 'no way')");
        assertCallError("07000", "call IR2(2, ?)");

        s.execute("drop procedure IR");
        s.execute("drop procedure IR2");
        s.execute("drop table t1");
        commit();
    }

    /**
     * 1. basic testing 2. correct auto commit logic 3. correct holdability
     * (JDBC 3)
     */
    public void testDynamicResultSets() throws SQLException {
        Connection conn = getConnection();
        Connection conn2 = openDefaultConnection();

        Statement s = createStatement();

        assertStatementError(
                "42X01",
                s,
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                "create procedure DRS(p1 int) " +
                "parameter style java reads sql data dynamic result sets -1 " +
                "language java external name '" +
                thisClassName + ".selectRows'");

        s.execute("create procedure DRS(p1 int) " +
                  "parameter style java reads sql data dynamic result sets 1 " +
                  "language java external name '" +
                  thisClassName + ".selectRows'");

        // Create a test table with some data that can be accessed by the
        // DRS procedure.
//IC see: https://issues.apache.org/jira/browse/DERBY-6039
        s.execute("create table t1(i int not null primary key, b char(15))");
        s.execute("insert into t1 values (1, 'int'), (2, 'int'), (3, 'int'), "
                + "(7, 'int'), (4, '4'), (5, 'ir2'), (6, '''012345678')");

        String[] sysaliasDefinition = {
            "APP.DRS AS " + thisClassName + ".selectRows" +
            "(IN \"P1\" INTEGER) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA " +
            "READS SQL DATA DYNAMIC RESULT SETS 1" };

        String[] dbMetadataDefinition = {"APP.DRS AS " + thisClassName +
                                         ".selectRows type procedureNoResult" };

        String[] columnDefinition = { "procedureColumnIn P1 INTEGER" };

        checkMatchingProcedures(conn, "DRS", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);
        assertCallError("42Y03", "call DRS()");
        assertCallError("42Y03","call DRS(?,?)");

        CallableStatement drs1 = prepareCall("call DRS(?)");

        drs1.setInt(1, 3);
        drs1.execute();
        ResultSet rs = drs1.getResultSet();
        String[][] drsResult = { { "3", "int" } };
        JDBC.assertFullResultSet(rs, drsResult);
        drs1.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure DRS2(p1 int, p2 int) " +
                  "parameter style java reads sql data " +
                  "dynamic result sets 2 language java " +
                  "external name '" + thisClassName + ".selectRows'");

        sysaliasDefinition = new String[] {
            "APP.DRS2 AS " + thisClassName + ".selectRows" +
            "(IN \"P1\" INTEGER,IN \"P2\" INTEGER) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA " +
            "READS SQL DATA DYNAMIC RESULT SETS 2" };

        dbMetadataDefinition = new String[] {
            "APP.DRS2 AS " +
            thisClassName + ".selectRows type procedureNoResult" };

        columnDefinition = new String[] { "procedureColumnIn P1 INTEGER",
                "procedureColumnIn P2 INTEGER" };

        checkMatchingProcedures(conn, "DRS2", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        CallableStatement drs2;
        drs2 = conn.prepareCall("call DRS2(?, ?)");
        drs2.setInt(1, 2);
        drs2.setInt(2, 6);
        drs2.execute();
        rs = drs2.getResultSet();
        String[][] drs2Results = { { "2", "int" } };
        JDBC.assertFullResultSet(rs, drs2Results);
        assertTrue(drs2.getMoreResults());
        drs2Results = new String[][] { { "6", "'012345678" }, { "7", "int" } };
        rs = drs2.getResultSet();
        JDBC.assertFullResultSet(rs, drs2Results);

        // execute it returning one closed result set
        drs2.setInt(1, 2);
        drs2.setInt(2, 99); // will close the second result set
        assertTrue(drs2.execute());
        rs = drs2.getResultSet();
        drs2Results = new String[][] { { "2", "int" } };
        JDBC.assertFullResultSet(rs, drs2Results);
        assertFalse(drs2.getMoreResults());

        // execute it returning no result sets
        drs2.setInt(1, 2);
        drs2.setInt(2, 199); // return no results at all
        assertFalse(drs2.execute());
        assertFalse(drs2.getMoreResults());

        // execute it returning two result sets but with the order swapped in
        // the parameters
        // doesnot affect display order.
        drs2.setInt(1, 2);
        drs2.setInt(2, 299); // swap results
        assertTrue(drs2.execute());
        drs2Results = new String[][] { { "2", "int" } };

        rs = drs2.getResultSet();
        JDBC.assertFullResultSet(rs, drs2Results);
        assertTrue(drs2.getMoreResults());
        rs = drs2.getResultSet();
        JDBC.assertEmpty(rs);

        drs2.setInt(1, 2);
        drs2.setInt(2, 2);
        drs2.execute();
        ResultSet lastResultSet = null;
        int pass = 1;
        do {

            if (lastResultSet != null) {
                try {
                    lastResultSet.next();
                    fail("FAILED - result set should be closed");
                } catch (SQLException sqle) {
                    assertSQLState("XCL16", sqle);
                }
            }

            lastResultSet = drs2.getResultSet();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            if ((pass == 1) || (pass == 2)) {
                assertNotNull("expected resultset pass " + pass, lastResultSet);
            } else if (pass == 3) {
                assertNull(lastResultSet);
            }

            pass++;

        } while (drs2.getMoreResults() || lastResultSet != null);

        checkCommitWithMultipleResultSets(drs2, conn2, "autocommit");
        checkCommitWithMultipleResultSets(drs2, conn2, "noautocommit");
        checkCommitWithMultipleResultSets(drs2, conn2, "statement");

        drs2.close();

        // use escape syntax
        drs2 = conn.prepareCall("{call DRS2(?, ?)}");
        drs2.setInt(1, 2);
        drs2.setInt(2, 6);
        drs2.execute();
        rs = drs2.getResultSet();
        String[][] expectedRows = { { "2", "int" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        drs2.close();

        // check that a procedure with dynamic result sets can not resolve to a
        // method with no ResultSet argument.
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute(
            "create procedure irdrs(p1 int) dynamic result sets 1 " +
            "language java parameter style java external name '" +
            thisClassName + ".missingDynamicParameter'");

        assertCallError("42X50", "call IRDRS(?)");
        s.execute("drop procedure irdrs");

        // check that a procedure with dynamic result sets can not resolve to a
        // method with an argument that is a ResultSet impl,
        s.execute(
            "create procedure rsi(p1 int) dynamic result sets 1 " +
            "language java parameter style java external name " +
            "'org.apache.derbyTesting.functionTests.util." +
            "ProcedureTest.badDynamicParameter'");

        assertCallError("42X50", "call rsi(?)");
        s.execute("drop procedure rsi");

        // simple check for a no-arg method that has dynamic result sets but
        // does not return any
        // System.out.println("no dynamic result sets");
        s.execute(
            "create procedure zadrs() dynamic result sets 4 " +
            "language java parameter style java external name '" +
            thisClassName + ".zeroArgDynamicResult'");

        CallableStatement zadrs = conn.prepareCall("call ZADRS()");
        zadrs.execute();

        // DERBY-211
        if (usingEmbedded()) {
            assertEquals(0, zadrs.getUpdateCount());
        } else {
            assertEquals(-1, zadrs.getUpdateCount());
        }
        zadrs.close();
        s.execute("drop procedure ZADRS");

        // return too many result sets

        s.execute(
            "create procedure way.toomany(p1 int, p2 int) reads sql data " +
            "dynamic result sets 1 language java parameter style java " +
            "external name '" +
            thisClassName + ".selectRows'");

        CallableStatement toomany = conn.prepareCall("call way.toomany(?, ?)");
        toomany.setInt(1, 2);
        toomany.setInt(2, 6);
        toomany.execute();
        SQLWarning warn = toomany.getWarnings();

        // DERBY-159. Network Server does not get warning
        if (usingEmbedded()) {
            assertNotNull(warn);
            assertEquals("0100E", warn.getSQLState());
        }
        rs = toomany.getResultSet();
        JDBC.assertFullResultSet(rs, new String[][] { { "2", "int" } });
        JDBC.assertNoMoreResults(toomany);

        toomany.setInt(1, 2);
        toomany.setInt(2, 99); // will close the second result set.
        toomany.execute();
        rs = toomany.getResultSet();
        // Single result set returned, therefore no warnings.
        JDBC.assertNoWarnings(toomany.getWarnings());
        JDBC.assertFullResultSet(rs, new String[][] { { "2", "int" } });
        JDBC.assertNoMoreResults(toomany);
        toomany.close();
        s.execute("drop procedure way.toomany");
        s.execute("drop schema way restrict");
//IC see: https://issues.apache.org/jira/browse/DERBY-6039

        // Run following test in embedded only until DERBY-3414 is fixed. As
        // identified in DERBY-3414, the rollback inside the java procedure
        // is not closing all the resultsets when run in network server mode.
        if (usingEmbedded()) {
            boolean oldAutoCommit = conn.getAutoCommit();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            s.execute(
                "create table dellater1(i int not null primary key, " +
                "                       b char(15))");

            s.executeUpdate(
                "insert into dellater1 " +
                "    values(1,'a'),(2,'b'),(3,'c'),(4,'d')");

            s.executeUpdate("create table dellater2(c11 int)");
            s.executeUpdate("insert into dellater2 values(1),(2),(3),(4)");

            conn.setAutoCommit(false);

            ResultSet rs1 = s.executeQuery("select * from dellater2");
            rs1.next();

            Statement s1 =
                conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                    ResultSet.CONCUR_READ_ONLY,
                                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            ResultSet resultSet = s1.executeQuery("values (1, 2), (3, 4)");
            resultSet.next();

            s.execute(
                "create procedure procWithRollback(p1 int) " +
                "parameter style java reads sql data dynamic result sets 1 " +
                "language java external name '" +
                thisClassName + ".rollbackInsideProc'");

            drs1 = prepareCall("call procWithRollback(3)");
            drs1.execute();
            // Following shows that the rollback inside the java procedure will
            // cause procedure to return no resultset (A procedure does
            // not return closed resultsets). In 10.2 codeline though, java
            // procedure returns a closed resultset if there is a rollback
            // inside the java procedure.
            JDBC.assertNoMoreResults(drs1);
            JDBC.assertClosed(rs1);
            JDBC.assertClosed(resultSet);

            // Following shows that the rollback inside the java procedure will
            // only close the resultset created before the rollback. The
            // resultset created after the rollback will remain open and if it
            // is a resultset returned through the procedure then it will be
            // available to the caller of the procedure. Notice that even though
            // the procedure is defined to 2 return dynamic resultsets, only one
            // is returned because the other one was closed as a result of
            // rollback.
            s.execute(
                "create procedure procWithRollbackAnd2Resulsets"+
                "(p1 int) parameter style java reads sql data dynamic "+
                "result sets 2 language java external name "+
                "'" + thisClassName + ".rollbackInsideProcWith2ResultSets'");

            drs1 = prepareCall("call procWithRollbackAnd2Resulsets(3)");
            drs1.execute();
            rs = drs1.getResultSet();
            JDBC.assertDrainResults(rs);
            JDBC.assertNoMoreResults(drs1);

            // Create a procedure which does an insert into a table. Then call
            // it with parameters such that insert will fail because of
            // duplicate key. The procedure also has couple select statements
            // The exception thrown for duplicate key should close the
            // dynamic result sets in the procedure, and we should be
            // able to drop the tables used in the select queries without
            // running into locking issues.
            s.execute(
                    "create procedure insertCausingRollback"+
                    "(p1 int, p2 char(20))  modifies sql data "+
                    "dynamic result sets 1 language java external name '" +
                    thisClassName + ".insertCausingRollback' "+
                    "parameter style java");
            
            s.executeUpdate("create table DELLATER3(c31 int)");
            s.executeUpdate("insert into DELLATER3 values(1),(2),(3),(4)");
            conn.commit();
            drs1 = prepareCall("call insertCausingRollback(3,'3')");
            assertStatementError("23505",drs1);
            JDBC.assertNoMoreResults(drs1);
            s.execute("drop table DELLATER1");
            s.execute("drop table DELLATER2");
            s.execute("drop table DELLATER3");
            s.execute("drop procedure insertCausingRollback");
//IC see: https://issues.apache.org/jira/browse/DERBY-6039

            conn.setAutoCommit(oldAutoCommit);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6039
        s.execute("drop procedure drs");
        s.execute("drop procedure drs2");
//IC see: https://issues.apache.org/jira/browse/DERBY-6039
        s.execute("drop table t1");
        commit();

        conn2.close();
    }


    public void testResultSetsWithLobs() throws SQLException {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        // Create objects.
        Statement s = createStatement();

        // Clob.
        s.execute("create table lobCheckOne (c clob(30))");
        s.execute("insert into lobCheckOne values (cast " +
                "('yayorsomething' as clob(30)))");
        s.execute("insert into lobCheckOne values (cast " +
                "('yayorsomething2' as clob(30)))");
        s.execute(
                "create procedure clobproc () parameter style java " +
                "language java dynamic result sets 3 reads sql data " +
                "external name " +
                "'org.apache.derbyTesting.functionTests.util." +
                "ProcedureTest.clobselect'");

        // Blob.
        s.execute("create table lobCheckTwo (b blob(30))");
        s.execute("insert into lobCheckTwo values (cast " + "(" +
                TestUtil.stringToHexLiteral("101010001101") +
                " as blob(30)))");
        s.execute("insert into lobCheckTwo values (cast " +
                "(" +
                TestUtil.stringToHexLiteral("101010001101") +
                " as blob(30)))");
        s.execute("create procedure blobproc () parameter style java " +
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.util." +
                "ProcedureTest.blobselect' " +
                "dynamic result sets 1 reads sql data");

        // Clobs.

        CallableStatement cs = conn.prepareCall("call clobproc()");
        cs.execute();
        ResultSet rs = cs.getResultSet();
        JDBC.assertFullResultSet(rs,
                new String[][] {{"yayorsomething"},{"yayorsomething2"}});
        cs.close();

        // Blobs.

        cs = conn.prepareCall("call blobproc()");
        cs.execute();
        rs = cs.getResultSet();
        String [][] expectedRows =
           {{"003100300031003000310030003000300031003100300031"},
            {"003100300031003000310030003000300031003100300031"}};

        JDBC.assertFullResultSet(rs, expectedRows);
        cs.close();

        // Clean up.
        s.execute("drop table lobCheckOne");
        s.execute("drop table lobCheckTwo");
        s.execute("drop procedure clobproc");
        s.execute("drop procedure blobproc");
        s.close();
    }


    /**
     * Original harness results transcribed here:
     -auto commit is true
     -lock count before execution 0
     -lock count after execution 1
     -lock count after next on first rs 3 -&gt; 2 now FIXME: explain
     -lock count after first getMoreResults() 2
     -lock count after next on second rs 7
     -lock count after second getMoreResults() 0

     -auto commit is false
     -lock count before execution 0
     -lock count after execution 1
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
     -lock count after next on first rs 3 -&gt; 2 now FIXME: explain
     -lock count after first getMoreResults() 2
     -lock count after next on second rs 7
     -lock count after second getMoreResults() 7

     -auto commit is true
     -lock count before execution 0
     -lock count after execution 1
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
     -lock count after next on first rs 3 -&gt; 2 now FIXME: explain
     -executing statement to force auto commit on open call statement
     -lock count after statement execution 0
     -lock count after first getMoreResults() 0
     -lock count after next on second rs 0
     -lock count after second getMoreResults() 0
    */

    private static void checkCommitWithMultipleResultSets(
            CallableStatement drs1, Connection conn2, String action)
            throws SQLException {
        Connection conn = drs1.getConnection();

        // Do not run with client until DERBY-2510 is fixed
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (usingDerbyNetClient()) {
            return;
        }

        try {
            conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        } catch (Exception e) {
            fail("shouldn't get that error " + e.getMessage());
        }
        // check to see that the commit of the transaction happens at the
        // correct time.
        // switch isolation levels to keep the locks around.
        int oldIsolation = conn.getTransactionIsolation();

        boolean oldAutoCommit = conn.getAutoCommit();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (action.equals("noautocommit")) {
            conn.setAutoCommit(false);
        } else {
            conn.setAutoCommit(true);
        }

        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        if (action.equals("noautocommit")) {
            assertFalse(conn.getAutoCommit());
        } else {
            assertTrue(conn.getAutoCommit());
        }

        PreparedStatement psLocks = conn2
                .prepareStatement("select count(*) from SYSCS_DIAG.LOCK_TABLE AS LT");
        ResultSet lrs = psLocks.executeQuery();
        // lock count before execution
        JDBC.assertFullResultSet(lrs, new String[][] { { "0" } });

        drs1.execute();
        // lock count after execution
        lrs = psLocks.executeQuery();

        JDBC.assertFullResultSet(lrs, new String[][] { { "1" } });

        ResultSet rs = drs1.getResultSet();
        rs.next();
        // lock count after next on first rs
        lrs = psLocks.executeQuery();

        JDBC.assertFullResultSet(lrs, new String[][] { { "2" } });
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        boolean expectClosed = false;

        // execute another statement to ensure that the result sets close.
        if (action.equals("statement")) {
            // executing statement to force auto commit on open call statement")

            conn.createStatement().executeQuery("values 1").next();
            expectClosed = true;
            lrs = psLocks.executeQuery();

            JDBC.assertFullResultSet(lrs, new String[][] { { "0" } });

            try {
                rs.next();
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                if (action.equals("autocommit")) {
                    fail("FAIL - result set open in auto commit mode after " +
                         "another statement execution");
                }
            } catch (SQLException sqle) {
                assertSQLState("XCL16", sqle);
            }
        }

        boolean anyMore = drs1.getMoreResults();
        assertTrue("is there a second result", anyMore);
        lrs = psLocks.executeQuery();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (action.equals("statement")) {
            JDBC.assertFullResultSet(lrs, new String[][] { { "0" } });
        } else {
            JDBC.assertFullResultSet(lrs, new String[][] { { "2" } });
        }

        if (anyMore) {

            rs = drs1.getResultSet();
            try {
                rs.next();
                if (expectClosed && !usingDerbyNetClient()) {
                    fail("FAIL - result set open in auto commit mode " +
                         "after another statement execution");
                }
            } catch (SQLException sqle) {
                if (expectClosed) {
                    assertSQLState("XCL16", sqle);
                } else {
                    throw sqle;
                }
            }

            // lock count after next on second rs
            lrs = psLocks.executeQuery();

            if (action.equals("statement")) {
                JDBC.assertFullResultSet(lrs, new String[][] { { "0" } });
            } else {
                JDBC.assertFullResultSet(lrs, new String[][] { { "7" } });
            }

            // should commit here since all results are closed
            boolean more = drs1.getMoreResults();
            assertFalse("more results (should be false) ", more);
            lrs = psLocks.executeQuery();

            if (action.equals("autocommit") || action.equals("statement")) {
                JDBC.assertFullResultSet(lrs, new String[][] { { "0" } });
             }else {
                JDBC.assertFullResultSet(lrs, new String[][] { { "7" } });
            }

            conn.setTransactionIsolation(oldIsolation);
            conn.setAutoCommit(oldAutoCommit);
        }

        psLocks.close();
    }

    private void assertUpdateCountForProcedureWithNoResults(Statement s)
            throws SQLException {
        // DERBY-211 Network Server returns no result sets for a procedure call
        // that returns no result
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (usingEmbedded()) {
            assertEquals(0, s.getUpdateCount());
        } else {
            assertEquals(-1, s.getUpdateCount());
        }
    }

    static String TYPE(short type) {
        switch (type) {
        case DatabaseMetaData.procedureResultUnknown:
            return "procedureResultUnknown";
        case DatabaseMetaData.procedureNoResult:
            return "procedureNoResult";
        case DatabaseMetaData.procedureReturnsResult:
            return "procedureReturnsResult";
        default:
            return "??????";
        }

    }

    static String PARAMTYPE(short type) {
        switch (type) {
        case DatabaseMetaData.procedureColumnUnknown:
            return "procedureColumnUnknown";
        case DatabaseMetaData.procedureColumnIn:
            return "procedureColumnIn";
        case DatabaseMetaData.procedureColumnInOut:
            return "procedureColumnInOut";
        case DatabaseMetaData.procedureColumnOut:
            return "procedureColumnOut";
        case DatabaseMetaData.procedureColumnReturn:
            return "procedureColumnReturn";
        case DatabaseMetaData.procedureColumnResult:
            return "procedureColumnResult";
        default:
            return "???";
        }
    }

    // PROCEDURES for ambiguous testing
    public static void ambiguous1(int p1, String p2, ResultSet[] data1,
            ResultSet[] data2) {
    }

    public static void ambiguous1(int p1, String p2, ResultSet[] data1) {
    }

    public static void ambiguous2(int p1, Integer p2) {
        // System.out.println("ambiguous2(int,Integer) called");
    };

    public static void ambiguous2(Integer p1, int p2) {
        // System.out.println("ambiguous2(Integer,int) called");

    };

    // PROCEDURES for zeroArgProcedures

    public static void zeroArg() {
        // System.out.println("zeroArg() called");
    }

    // PROCEDURES for sqlProcedures

    public static void insertRow(int p1) throws SQLException {
        insertRow(p1, "int");
    }

    public static void insertRow(int p1, String p2) throws SQLException {
        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");
        PreparedStatement ps = conn
                .prepareStatement("insert into t1 values (?, ?)");
        ps.setInt(1, p1);
        ps.setString(2, p2);
        ps.executeUpdate();
        ps.close();
        conn.close();
    }

    // PROCEDURES for dynamic results

    public static void selectRows(int p1, ResultSet[] data) throws SQLException {

        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");
        PreparedStatement ps = conn
                .prepareStatement("select * from t1 where i = ?");
        ps.setInt(1, p1);
        data[0] = ps.executeQuery();
        conn.close();
    }

    /**
     * A test case for DERBY-3304. An explicit rollback inside the procedure
     * should close all the resultsets created before the call to the
     * procedure and any resultsets created inside the procedure including
     * the dynamic resultsets.
     * 
     * @param p1
     * @param data
     * @throws SQLException
     */
    public static void rollbackInsideProc(int p1, ResultSet[] data) 
    throws SQLException {
        Connection conn = DriverManager.getConnection(
        		"jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement(
        		"select * from dellater1 where i = ?");
        ps.setInt(1, p1);
        data[0] = ps.executeQuery();
        conn.rollback();
        conn.close();
    }

    /**
     * A test case for DERBY-3304. An explicit rollback inside the procedure
     * should close all the resultsets created before the call to the
     * procedure and any resultsets created inside the procedure including
     * the dynamic resultsets. But the resultset created after the rollback
     * should stay open
     * 
     * @param p1
     * @param data1
     * @param data2
     * @throws SQLException
     */
    public static void rollbackInsideProcWith2ResultSets(int p1, 
    		ResultSet[] data1,
            ResultSet[] data2) 
    throws SQLException {
        Connection conn = DriverManager.getConnection(
        		"jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement(
        		"select * from t1 where i = ?");
        ps.setInt(1, p1);
        data1[0] = ps.executeQuery();
        conn.rollback();
        ps = conn.prepareStatement(
        		"select * from dellater1 where i = ?");
        ps.setInt(1, p1);
        data2[0] = ps.executeQuery();
        conn.close();
    }

    /**
     * A test case for DERBY-3304. The procedure is attempting to insert a
     * duplicate key into a table which causes an internal rollback (vs a
     * user-initiated rollback). This internal rollback should close the
     * internal CallStatementResultSet associated with the Java procedure
     * and the dynamic result set.
     * 
     * @param p1
     * @param p2
     * @param data
     * @throws SQLException
     */
    public static void insertCausingRollback(int p1, String p2, ResultSet[] data) throws SQLException {
        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");

        // The resultset created here is a dynamic resultset and will be
        // available to the caller of the java procedure (provided that there
        // is no SQL exception thrown inside of this procedure. An exception
        // will cause Derby to close this resultset).
        PreparedStatement ps = conn.prepareStatement(
        		"select * from dellater2 where c11 = ?");
        ps.setInt(1, p1);
        data[0] = ps.executeQuery();

        // The resultset created here has the lifetime of this procedure
        // and is not available to the caller of the procedure.
        PreparedStatement ps1 = conn.prepareStatement(
        		"select * from dellater3 where c31 = ?");
        ps1.setInt(1, p1);
        ResultSet rs = ps1.executeQuery();

        // DERBY-6038: When the procedure fails because of duplicate key
        // exception below, all dynamic results set will be closed. Other
        // open result sets will stay open until they have been garbage
        // collected and finalized. Their staying open may cause problems
        // later in the test, so close non-dynamic result sets before
        // returning.
        rs.close();

        // Depending on the value of p1, following may throw duplicate key
        // exception. If that happens, both the dynamic resultset and local
        // resultset created above will get closed and locks held by them
        // and insert statement will be released
        PreparedStatement ps2 = conn
                .prepareStatement("insert into dellater1 values (?, ?)");
        ps2.setInt(1, p1);
        ps2.setString(2, p2);
        ps2.executeUpdate();
        ps2.close();
        conn.close();
    }

    public static void selectRows(int p1, int p2, ResultSet[] data1,
            ResultSet[] data2) throws SQLException {

        // selectRows - 2 arg - 2 rs

        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");
        PreparedStatement ps = conn
                .prepareStatement("select * from t1 where i = ?");
        ps.setInt(1, p1);
        data1[0] = ps.executeQuery();

        ps = conn.prepareStatement("select * from t1 where i >= ?");
        ps.setInt(1, p2);
        data2[0] = ps.executeQuery();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (p2 == 99) {
            data2[0].close();
        }

        // return no results
        if (p2 == 199) {
            data1[0].close();
            data1[0] = null;
            data2[0].close();
            data2[0] = null;
        }

        // swap results
        if (p2 == 299) {
            ResultSet rs = data1[0];
            data1[0] = data2[0];
            data2[0] = rs;
        }

        conn.close();
    }

    // select all rows from a table
    public static void selectRows(String table, ResultSet[] rs)
            throws SQLException {
        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");
        Statement stmt = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        rs[0] = stmt.executeQuery("select * FROM " + table);
        conn.close();
    }

    public void testParameterTypes() throws SQLException {

        Connection conn = getConnection();
        Statement s = createStatement();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create table PT1(A integer not null primary key, " +
                  "                 B char(10), C varchar(20))");
        s.execute(
            "create procedure PT1(in a int, in b char(10), c varchar(20)) " +
            "parameter style java dynamic result sets 1 language java " +
            "modifies sql data " +
            "external name '" + thisClassName + ".parameter1'");

        String[] sysaliasDefinition = {
            "APP.PT1 AS " + thisClassName + ".parameter1" +
            "(IN \"A\" INTEGER,IN \"B\" CHAR(10),IN \"C\" VARCHAR(20)) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA " +
            "MODIFIES SQL DATA DYNAMIC RESULT SETS 1" };

        String[] dbMetadataDefinition = {
            "APP.PT1 AS " + thisClassName + ".parameter1 " +
            "type procedureNoResult" };

        String[] columnDefinition = {
            "procedureColumnIn A INTEGER",
            "procedureColumnIn B CHAR", "procedureColumnIn C VARCHAR" };

        checkMatchingProcedures(conn, "PT1", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        CallableStatement pt1 = conn.prepareCall("call PT1(?, ?, ?)");

        pt1.setInt(1, 20);
        pt1.setString(2, "abc");
        pt1.setString(3, "efgh");
        pt1.execute();
        JDBC.assertFullResultSet(pt1.getResultSet(), new String[][] { { "20",
                "abc", "10", "efgh", "4" } });

        pt1.setInt(1, 30);
        pt1.setString(2, "abc   ");
        pt1.setString(3, "efgh  ");
        pt1.execute();
        JDBC.assertFullResultSet(pt1.getResultSet(), new String[][] { { "30",
                "abc", "10", "efgh", "6" } });

        pt1.setInt(1, 40);

        // end blanks truncation of arguments
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        char[] c75 = new char[75]; Arrays.fill(c75, ' ');
        char[] c77 = new char[77]; Arrays.fill(c77, ' ');
        pt1.setString(2, "abc" + new String(c75));
        pt1.setString(3, "efgh" + new String(c77));

        pt1.execute();
        JDBC.assertFullResultSet(pt1.getResultSet(), new String[][] { { "40",
                "abc", "10", "efgh", "20" } });

        pt1.setInt(1, 50);
        pt1.setString(2, "0123456789X");
        pt1.setString(3, "efgh  ");
        assertPreparedStatementError(LANG_STRING_TRUNCATION, pt1);
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        pt1.setInt(1, 50);
        pt1.setString(2, "0123456789");
        pt1.setString(3, "efgh  ");
        pt1.execute();
        JDBC.assertFullResultSet(pt1.getResultSet(), new String[][] { { "50",
                "0123456789", "10", "efgh", "6" } });
        pt1.close();

        s.execute("drop procedure PT1");

        s.execute(
            "create procedure PT2(in a int, in b DECIMAL(4), c DECIMAL(7,3)) " +
            "parameter style java dynamic result sets 1 language java " +
            "modifies sql data " +
            "external name '" + thisClassName + ".parameter2'");

        sysaliasDefinition = new String[] {
            "APP.PT2 AS " + thisClassName + ".parameter2" +
            "(IN \"A\" INTEGER,IN \"B\" DECIMAL(4,0),IN \"C\" DECIMAL(7,3)) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA " +
            "MODIFIES SQL DATA DYNAMIC RESULT SETS 1" };

        dbMetadataDefinition = new String[] {
            "APP.PT2 AS " +
            thisClassName + ".parameter2 type procedureNoResult" };

        columnDefinition = new String[] { "procedureColumnIn A INTEGER",
                "procedureColumnIn B DECIMAL", "procedureColumnIn C DECIMAL" };

        checkMatchingProcedures(conn, "PT2", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        CallableStatement pt2 = conn.prepareCall("call PT2(?, ?, ?)");

        pt2.setInt(1, 60);
        pt2.setString(2, "34");
        pt2.setString(3, "54.1");
        boolean firstIsResultSet = pt2.execute();
        ResultSet rs = pt2.getResultSet();
        JDBC.assertFullResultSet(rs,
                new String[][] { { "60", "34", "54.100" } });

        pt2.setInt(1, 70);
        pt2.setBigDecimal(2, new BigDecimal("831"));
        pt2.setBigDecimal(3, new BigDecimal("45.7"));
        pt2.execute();
        rs = pt2.getResultSet();
        JDBC.assertFullResultSet(rs,
                new String[][] { { "70", "831", "45.700" } });

        pt2.setInt(1, -1);
        pt2.setBigDecimal(2, new BigDecimal("10243"));
        pt2.setBigDecimal(3, null);

        try {
            pt2.execute();
            fail("FAIL - too many digits in decimal value accepted");
        } catch (SQLException sqle) {
            assertSQLState("22003", sqle);
        }
        pt2.setInt(1, 80);
        pt2.setBigDecimal(2, new BigDecimal("993"));
        pt2.setBigDecimal(3, new BigDecimal("1234.5678"));
        pt2.execute();
        rs = pt2.getResultSet();
        JDBC.assertFullResultSet(rs,
                new String[][] { { "80", "993", "1234.567" } });
        pt2.close();

        s.execute("drop procedure PT2");
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        s.execute(
            "create procedure PTSMALLINT2(" +
            "    in    p_in    SMALLINT, " +
            "    inout p_inout SMALLINT, " +
            "    out   p_out   SMALLINT) " +
            "parameter style java dynamic result sets 0 language java " +
            "no sql " +
            "external name '" + thisClassName + ".pSMALLINT'");

        sysaliasDefinition = new String[] {
            "APP.PTSMALLINT2 AS " + thisClassName + ".pSMALLINT" +
            "(IN \"P_IN\" SMALLINT," +
            "INOUT \"P_INOUT\" SMALLINT," +
            "OUT \"P_OUT\" SMALLINT) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA NO SQL" };

        dbMetadataDefinition = new String[] {
            "APP.PTSMALLINT2 AS " +
            thisClassName + ".pSMALLINT type procedureNoResult" };

        columnDefinition = new String[] { "procedureColumnIn P_IN SMALLINT",
                "procedureColumnInOut P_INOUT SMALLINT",
                "procedureColumnOut P_OUT SMALLINT" };

        checkMatchingProcedures(conn, "PT2", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        CallableStatement ptsi = conn.prepareCall("call PTSMALLINT2(?, ?, ?)");
        ptsi.registerOutParameter(2, Types.SMALLINT);
        ptsi.registerOutParameter(3, Types.SMALLINT);

        ptsi.setNull(1, Types.SMALLINT);
        ptsi.setShort(2, (short) 7);
        try {
            ptsi.execute();
            fail("FAIL NULL PASSED to  primitive");
        } catch (SQLException sqle) {
            assertSQLState("39004", sqle);
        }

        ptsi.setShort(1, (short) 4);
        ptsi.setNull(2, Types.SMALLINT);
        try {
            ptsi.execute();
            fail("FAIL NULL PASSED to  primitive");
        } catch (SQLException sqle) {
            assertSQLState("39004", sqle);
        }

        ptsi.setShort(1, (short) 6);
        ptsi.setShort(2, (short) 3);
        ptsi.execute();
        assertEquals("wrong value for p_inout", "9", ptsi.getObject(2)
                .toString());
        assertEquals("wrong value for p_out", "6", ptsi.getObject(3).toString());

        ptsi.setShort(2, (short) 3);
        ptsi.execute();
        assertEquals("wrong value for p_inout", "9", ptsi.getObject(2)
                .toString());
        assertEquals("wrong value for p_out", "6", ptsi.getObject(3).toString());

        // with setObject . Beetle 5439
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        ptsi.setObject(1, 6);
        ptsi.setObject(2, 3);

        ptsi.execute();
        assertEquals("wrong value for p_inout", "9", ptsi.getObject(2)
                .toString());
        assertFalse(ptsi.wasNull());
        assertEquals("wrong value for p_out", "6", ptsi.getObject(3).toString());
        assertFalse(ptsi.wasNull());

        ptsi.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("drop procedure PTSMALLINT2");
        s.execute("drop table PT1");

        s.close();
    }

    public void testOutparams() throws SQLException {
        Connection conn = getConnection();
        Statement s = createStatement();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure OP1(out a int, in b int) " +
                "parameter style java language java " +
                "external name '" + thisClassName + ".outparams1'");

        String[] sysaliasDefinition = {
            "APP.OP1 AS " + thisClassName + ".outparams1" +
            "(OUT \"A\" INTEGER,IN \"B\" INTEGER) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA" };
        String[] dbMetadataDefinition = {
            "APP.OP1 AS " + thisClassName + ".outparams1 " +
            "type procedureNoResult" };
        String[] columnDefinition = { "procedureColumnOut A INTEGER",
                "procedureColumnIn B INTEGER" };

        checkMatchingProcedures(conn, "OP1", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        // check execute via a Statement fails for use of out parameter

        try {
            s.execute("call OP1(?, ?)");
            fail("FAIL execute succeeded on out param with Statement");
        } catch (SQLException sqle) {
            String expectedSQLState = "XJ009";
            if (usingDerbyNetClient()) {
                expectedSQLState = "07004";
            }
            assertSQLState(expectedSQLState, sqle);
        }

        // check execute via a PreparedStatement fails for use of out parameter
        // DERBY-2512 Network client allows prepare of a stored procedure with
        // an output parameter using a PreparedStatement
        if (usingEmbedded()) {
            try {
                prepareStatement("call OP1(?, ?)");
                fail("FAIL prepare succeeded on out param " +
                     "with PreparedStatement");
            } catch (SQLException sqle) {
                String expectedSQLState = "XJ009";
                assertSQLState(expectedSQLState, sqle);
            }
        }
        CallableStatement op = prepareCall("call OP1(?, ?)");

        op.registerOutParameter(1, Types.INTEGER);
        op.setInt(2, 7);
        op.execute();

        assertEquals(14, op.getInt(1));
        assertFalse(op.wasNull());

        op.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create procedure OP2(inout a int, in b int) " +
            "parameter style java language java " +
            "external name '" + thisClassName + ".inoutparams2'");

        sysaliasDefinition = new String[] {
            "APP.OP2 AS " + thisClassName + ".inoutparams2" +
            "(INOUT \"A\" INTEGER,IN \"B\" INTEGER) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA" };

        dbMetadataDefinition = new String[] {
            "APP.OP2 AS " +
            thisClassName + ".inoutparams2 type procedureNoResult"};

        columnDefinition = new String[] { "procedureColumnInOut A INTEGER",
                "procedureColumnIn B INTEGER" };

        checkMatchingProcedures(conn, "OP2", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            s.execute("call OP2(?,?)");
            fail("FAIL execute succeeded on inout param with Statement");
        } catch (SQLException sqle) {
            String expectedSQLState = "XJ009";
            if (usingDerbyNetClient()) {
                expectedSQLState = "07004";
            }
            assertSQLState(expectedSQLState, sqle);
        }

        if (!usingDerbyNetClient()) { // bug DERBY-2512
            // check execute via a PreparedStatement fails for use of inout
            // parameter
            try {
                prepareStatement("call OP2(?, ?)");
                fail("FAIL prepare succeeded on inout param " +
                     "with PreparedStatement");
            } catch (SQLException sqle) {
                String expectedSQLState = "XJ009";
                assertSQLState(expectedSQLState, sqle);

            }
        }

        op = prepareCall("call OP2(?, ?)");
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        op.registerOutParameter(1, Types.INTEGER);
        op.setInt(1, 3);
        op.setInt(2, 7);
        op.execute();
        assertEquals(17, op.getInt(1));
        assertFalse(op.wasNull());
        op.close();

        // inout & out procedures with variable length
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute(
            "create procedure OP3(inout a char(10), in b int) " +
            "parameter style java language java " +
            "external name '" + thisClassName + ".inoutparams3'");

        op = prepareCall("call OP3(?, ?)");

        op.registerOutParameter(1, Types.CHAR);
        op.setString(1, "dan");
        op.setInt(2, 8);
        op.execute();
        assertEquals("nad       ", op.getString(1));
        assertFalse(op.wasNull());
        op.close();

        // inout & out DECIMAL procedures with variable length
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute(
            "create procedure OP4(out a DECIMAL(4,2), in b varchar(255)) " +
            "parameter style java language java " +
            "external name '" + thisClassName + ".inoutparams4'");

        sysaliasDefinition = new String[] {
            "APP.OP4 AS " + thisClassName + ".inoutparams4" +
            "(OUT \"A\" DECIMAL(4,2),IN \"B\" VARCHAR(255)) " +
            "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA" };

        dbMetadataDefinition = new String[] {
            "APP.OP4 AS " +
            thisClassName + ".inoutparams4 type procedureNoResult"};

        columnDefinition = new String[] { "procedureColumnOut A DECIMAL",
                "procedureColumnIn B VARCHAR", };

        checkMatchingProcedures(conn, "OP4", sysaliasDefinition,
                dbMetadataDefinition, columnDefinition);

        op = prepareCall("call OP4(?, ?)");
        op.registerOutParameter(1, Types.DECIMAL);
        op.setString(2, null);
        op.execute();
        assertNull(op.getBigDecimal(1));
        assertTrue(op.wasNull());

        op.setString(2, "14");
        op.execute();
        assertEquals("31.00", op.getBigDecimal(1).toString());
        assertFalse(op.wasNull());

        op.setString(2, "11.3");
        op.execute();
        assertEquals("28.30", op.getBigDecimal(1).toString());
        assertFalse(op.wasNull());

        op.setString(2, "39.345");
        op.execute();
        assertEquals("56.34", op.getBigDecimal(1).toString());
        assertFalse(op.wasNull());

        op.setString(2, "83");
        try {
            op.execute();
            fail("FAIL - execution ok on out of range out parameter");
        } catch (SQLException sqle) {
            assertSQLState("22003", sqle);
        }

        op.clearParameters();
        try {
            // b not set
            op.execute();

            fail("FAIL - b not set");
        } catch (SQLException sqle) {
            assertSQLState("07000", sqle);
        }
        // DERBY-2513 Network client allows out parameter to be set
        if (usingEmbedded()) {
            // try to set an out param
            try {
                op.setBigDecimal(1, new BigDecimal("22.32"));
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                fail("FAIL - set out param to value");
            } catch (SQLException sqle) {
                assertSQLState("XCL27", sqle);
            }

            try {
                op.setBigDecimal(1, null);
                fail("FAIL - set out param to null");
            } catch (SQLException sqle) {
                assertSQLState("XCL27", sqle);
            }
            try {
                op.setNull(1, Types.DECIMAL);
                fail("FAIL - set out param to null");
            } catch (SQLException sqle) {
                assertSQLState("XCL27", sqle);

            }
        }

        // can we get an in param?
        op.setString(2, "49.345");
        op.execute();
        assertEquals("66.34", op.getBigDecimal(1).toString());
        assertFalse(op.wasNull());

        try {
            op.getString(2);
            fail("FAIL OP4 GET 49.345 >" + op.getString(2) + "< null ? "
                    + op.wasNull());
        } catch (SQLException sqle) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            if (usingDerbyNetClient()) {
                assertSQLState("XJ091", sqle);
            } else {
                assertSQLState("XCL26", sqle);
            }
        }
        op.close();

        // check to see that a registration is required first for the out
        // parameter.
        op = conn.prepareCall("call OP4(?, ?)");
        op.setString(2, "14");
        try {
            op.execute();
            fail("FAIL - execute succeeded without registration of out parameter");
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertSQLState("07004", sqle);
            } else {
                assertSQLState("07000", sqle);
            }
        }
        op.close();

        s.execute(
            "create procedure OP4INOUT(" +
            "    inout a DECIMAL(4,2), in b varchar(255)) " +
            "parameter style java language java " +
            "external name '" + thisClassName + ".inoutparams4'");

        op = conn.prepareCall("call OP4INOUT(?, ?)");
        op.registerOutParameter(1, Types.DECIMAL);

        op.setString(2, null);

        op.setBigDecimal(1, null);
        op.execute();
        assertNull(op.getBigDecimal(1));
        assertTrue(op.wasNull());

        op.setBigDecimal(1, new BigDecimal("99"));
        op.execute();
        assertNull(op.getBigDecimal(1));
        assertTrue(op.wasNull());

        op.setString(2, "23.5");
        op.setBigDecimal(1, new BigDecimal("14"));
        op.execute();
        assertEquals("37.50", op.getBigDecimal(1).toString());

        op.setString(2, "23.505");
        op.setBigDecimal(1, new BigDecimal("9"));
        op.execute();
        assertEquals("32.50", op.getBigDecimal(1).toString());

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        op.execute();
        assertEquals("56.00", op.getBigDecimal(1).toString());
        assertFalse(op.wasNull());

        op.setString(2, "67.99");
        op.setBigDecimal(1, new BigDecimal("32.01"));
        try {
            op.execute();
            fail("FAIL OP4INOUT 32.01+67.99 >" + op.getBigDecimal(1)
                    + "< null ? " + op.wasNull());
        } catch (SQLException sqle) {
            assertSQLState("22003", sqle);
        }

        op.setString(2, "1");
        op.setBigDecimal(1, new BigDecimal("102.33"));
        try {
            op.execute();
            fail("FAIL OP4INOUT 1+102.33 >" + op.getBigDecimal(1) + "< null ? "
                    + op.wasNull());
        } catch (SQLException sqle) {
            assertSQLState("22003", sqle);
        }

        // now some checks to requirements for parameter setting.
        op.clearParameters();
        try {
            // a,b not set
            op.execute();
            fail("FAIL - a,b not set");
        } catch (SQLException sqle) {
            assertSQLState("07000", sqle);
        }
        op.clearParameters();
        op.setString(2, "2");
        try {
            // a not set
            op.execute();
            fail("FAIL - a  not set");
        } catch (SQLException sqle) {
            assertSQLState("07000", sqle);
        }
        op.clearParameters();
        op.setBigDecimal(1, new BigDecimal("33"));
        try {
            // b not set
            op.execute();
            fail("FAIL - b  not set");
        } catch (SQLException sqle) {
            assertSQLState("07000", sqle);
        }

        op.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        op = conn.prepareCall("call OP4INOUT(?, ?)");
        op.setString(2, "14");
        try {
            op.execute();
            fail("FAIL - execute succeeded without registration of " +
                 "inout parameter");
        } catch (SQLException sqle) {
            if (usingDerbyNetClient()) {
                assertSQLState("07000", sqle);
            } else {
                assertSQLState("07004", sqle);
            }
        }
        op.close();

        s.execute("drop PROCEDURE OP1");
        s.execute("drop PROCEDURE OP2");
        s.execute("drop PROCEDURE OP3");
        s.execute("drop PROCEDURE OP4");
        s.execute("drop PROCEDURE OP4INOUT");
        s.close();

    }

    public void testSQLControl() throws SQLException {
        Connection conn = getConnection();
        Statement s = createStatement();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create schema SQLC");
        s.execute("create table SQLC.SQLCONTROL_DML(I int)");
        s.execute("insert into SQLC.SQLCONTROL_DML values 4");

        String[] control = { "", "no sql", "contains sql", "reads sql data",
                "modifies sql data" };

        for (int i = 0; i < control.length; i++) {

            StringBuilder cp = new StringBuilder(256);
            cp.append("create procedure sqlc.sqlcontrol1_");
            cp.append(i);
            cp.append(
                "(out e1 varchar(128), " +
                " out e2 varchar(128), " +
                " out e3 varchar(128), " +
                " out e4 varchar(128), " +
                " out e5 varchar(128), " +
                " out e6 varchar(128), " +
                " out e7 varchar(128)) ");
            cp.append(control[i]);
            cp.append(" parameter style java language java ");
            cp.append("external name '");
            cp.append(thisClassName);
            cp.append(".sqlControl'");

            String cpsql = cp.toString();

            s.execute(cpsql);

            cp.setLength(0);
            cp.append("create procedure sqlc.sqlcontrol2_");
            cp.append(i);
            cp.append(
                "(out e1 varchar(128)," +
                " out e2 varchar(128)," +
                " out e3 varchar(128)," +
                " out e4 varchar(128)," +
                " out e5 varchar(128)," +
                " out e6 varchar(128)," +
                " out e7 varchar(128)) ");
            cp.append(control[i]);
            cp.append(" parameter style java language java ");
            cp.append("external name '");
            cp.append(thisClassName);
            cp.append(".sqlControl2'");

            cpsql = cp.toString();

            s.execute(cpsql);

            cp.setLength(0);
            cp.append("create procedure sqlc.sqlcontrol3_");
            cp.append(i);
            cp.append(
                "(out e1 varchar(128)," +
                " out e2 varchar(128)," +
                " out e3 varchar(128)," +
                " out e4 varchar(128)," +
                " out e5 varchar(128)," +
                " out e6 varchar(128)," +
                " out e7 varchar(128)) ");

            cp.append(control[i]);
            cp.append(" parameter style java language java ");
            cp.append("external name '");
            cp.append(thisClassName);
            cp.append(".sqlControl3'");

            cpsql = cp.toString();

            s.execute(cpsql);

            cp.setLength(0);
            cp.append("create procedure sqlc.sqlcontrol4_");
            cp.append(i);
            cp.append(
                "(in sqlc integer," +
                " out e1 varchar(128)," +
                " out e2 varchar(128)," +
                " out e3 varchar(128)," +
                " out e4 varchar(128)," +
                " out e5 varchar(128)," +
                " out e6 varchar(128)," +
                " out e7 varchar(128)," +
                " out e8 varchar(128)) ");

            cp.append(control[i]);
            cp.append(" parameter style java language java ");
            cp.append("external name '");
            cp.append(thisClassName);
            cp.append(".sqlControl4'");

            cpsql = cp.toString();

            s.execute(cpsql);
        }

        if (!conn.getAutoCommit()) {
            conn.commit();
        }

        String[][] sqlControl_0 = /* sqlControl_0 */{
                { "CREATE TABLE SQ-UPDATE 0-EXECUTE OK",
                        "ALTER TABLE SQL-UPDATE 0-EXECUTE OK",
                        "INSERT INTO SQL-UPDATE 1-EXECUTE OK",
                        "UPDATE SQLC.SQL-UPDATE 2-EXECUTE OK",
                        "SELECT * FROM S- ROW(15)- ROW(12)-EXECUTE OK",
                        "DELETE FROM SQL-UPDATE 2-EXECUTE OK",
                        "DROP TABLE SQLC-UPDATE 0-EXECUTE OK" },
                { "CREATE VIEW SQL-42X05", "DROP VIEW SQLCO-X0X05",
                        "LOCK TABLE SQLC-42X05",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-UPDATE 0-EXECUTE OK",
                        "DROP SCHEMA SQL-UPDATE 0-EXECUTE OK" },
                { "DERBY FEATURE", "DERBY FEATURE",
                        "SET ISOLATION C-UPDATE 0-EXECUTE OK",
                        "SET RUNTIMESTAT-42X01", "SET STATISTICS -42X01",
                        "VALUES 1- ROW(1)-EXECUTE OK",
                        "VALUES 1- ROW(1)-EXECUTE OK" } };

        String[][] sqlControl_1 = /* sqlControl_1 */{
                { "CREATE TABLE SQ-38001", "ALTER TABLE SQL-42Y55",
                        "INSERT INTO SQL-42X05", "UPDATE SQLC.SQL-42X05",
                        "SELECT * FROM S-42X05", "DELETE FROM SQL-42X05",
                        "DROP TABLE SQLC-42Y55" },
                { "CREATE VIEW SQL-42X05", "DROP VIEW SQLCO-38001",
                        "LOCK TABLE SQLC-42X05", "VALUES 1,2,3-38001",
                        "SET SCHEMA SQLC-38001", "CREATE SCHEMA S-38001",
                        "DROP SCHEMA SQL-38001" },
                { "DERBY FEATURE", "DERBY FEATURE", "SET ISOLATION C-38001",
                        "SET RUNTIMESTAT-42X01", "SET STATISTICS -42X01",
                        "VALUES 1-38001", "VALUES 1-38001" } };

        String[][] sqlControl_2 = /* sqlControl_2 */{
                { "CREATE TABLE SQ-38002", "ALTER TABLE SQL-42Y55",
                        "INSERT INTO SQL-42X05", "UPDATE SQLC.SQL-42X05",
                        "SELECT * FROM S-42X05", "DELETE FROM SQL-42X05",
                        "DROP TABLE SQLC-42Y55" },
                { "CREATE VIEW SQL-42X05", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-42X05", "VALUES 1,2,3-38004",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "DERBY FEATURE", "DERBY FEATURE",
                        "SET ISOLATION C-UPDATE 0-EXECUTE OK",
                        "SET RUNTIMESTAT-42X01", "SET STATISTICS -42X01",
                        "VALUES 1-38004", "VALUES 1-38004" } };

        String[][] sqlControl_3 = /* sqlControl_3 */{
                { "CREATE TABLE SQ-38002", "ALTER TABLE SQL-42Y55",
                        "INSERT INTO SQL-42X05", "UPDATE SQLC.SQL-42X05",
                        "SELECT * FROM S-42X05", "DELETE FROM SQL-42X05",
                        "DROP TABLE SQLC-42Y55" },
                { "CREATE VIEW SQL-42X05", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-42X05",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "DERBY FEATURE", "DERBY FEATURE",
                        "SET ISOLATION C-UPDATE 0-EXECUTE OK",
                        "SET RUNTIMESTAT-42X01", "SET STATISTICS -42X01",
                        "VALUES 1- ROW(1)-EXECUTE OK",
                        "VALUES 1- ROW(1)-EXECUTE OK" } };

        String[][] sqlControl_4 =
        /* sqlControl_4 */{
                { "CREATE TABLE SQ-UPDATE 0-EXECUTE OK",
                        "ALTER TABLE SQL-42Y55", "INSERT INTO SQL-42X05",
                        "UPDATE SQLC.SQL-42X05", "SELECT * FROM S-42X05",
                        "DELETE FROM SQL-42X05", "DROP TABLE SQLC-42Y55" },
                { "CREATE VIEW SQL-42X05", "DROP VIEW SQLCO-X0X05",
                        "LOCK TABLE SQLC-42X05",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-UPDATE 0-EXECUTE OK",
                        "DROP SCHEMA SQL-UPDATE 0-EXECUTE OK" },
                { "DERBY FEATURE", "DERBY FEATURE",
                        "SET ISOLATION C-UPDATE 0-EXECUTE OK",
                        "SET RUNTIMESTAT-42X01", "SET STATISTICS -42X01",
                        "VALUES 1- ROW(1)-EXECUTE OK",
                        "VALUES 1- ROW(1)-EXECUTE OK" } };

        String[][][] sqlControl = { sqlControl_0, sqlControl_1, sqlControl_2,
                sqlControl_3, sqlControl_4 };
        for (int i = 0; i < control.length; i++) {
            for (int k = 1; k <= 3; k++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                CallableStatement cs = conn.prepareCall("call SQLC.SQLCONTROL"
                        + k + "_" + i + " (?, ?, ?, ?, ?, ?, ?)");
                // System.out.println("{");
                for (int rop = 1; rop <= 7; rop++) {
                    cs.registerOutParameter(rop, Types.VARCHAR);
                }
                cs.execute();
                for (int p = 1; p <= 7; p++) {
                    // System.out.print("\"" + cs.getString(p) + "\"");
                    /*
                     * if (p != 7) System.out.println(","); else
                     * System.out.println("}");
                     */
                    assertEquals(sqlControl[i][k - 1][p - 1], cs.getString(p));
                }

                cs.close();
                /*
                 * if (k < 3) System.out.println(","); else
                 * System.out.println("}");
                 */
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            if (control[i].length() == 0) {
                // This default case (see test above) succeeds in
                // creating the SQLCONTROL_DDL table, so remove it to
                // avoid tripping the explicit "modifies sql data".
                s.execute("drop table SQLCONTROL_DDL");
            }
            // System.out.println("}");
        }

        // test procedures that call others, e.g. to ensure that within a READS
        // SQL DATA procedure, a "modifies sql data" cannot be called.
        // table was dropped by previous executions.
        String[][] dmlSqlControl_0 = /* dmlSqlControl_0 */{
                { "call SQLC.SQLCONTROL2_0 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-UPDATE 0-EXECUTE OK",
                        "DROP VIEW SQLCO-UPDATE 0-EXECUTE OK",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-UPDATE 0-EXECUTE OK",
                        "DROP SCHEMA SQL-UPDATE 0-EXECUTE OK" },
                { "call SQLC.SQLCONTROL2_1 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38001", "DROP VIEW SQLCO-38001",
                        "LOCK TABLE SQLC-38001", "VALUES 1,2,3-38001",
                        "SET SCHEMA SQLC-38001", "CREATE SCHEMA S-38001",
                        "DROP SCHEMA SQL-38001" },
                { "call SQLC.SQLCONTROL2_2 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3-38004",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_3 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_4 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-UPDATE 0-EXECUTE OK",
                        "DROP VIEW SQLCO-UPDATE 0-EXECUTE OK",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-UPDATE 0-EXECUTE OK",
                        "DROP SCHEMA SQL-UPDATE 0-EXECUTE OK" } };

        String[][] dmlSqlControl_1 = /* dmlSqlControl_1 */{
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                { "call SQLC.SQLCONTROL2_0 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38001", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_1 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38001", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_2 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38001", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_3 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38001", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_4 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38001", "null", "null", "null", "null", "null",
                        "null" }

        };

        String[][] dmlSqlControl_2 = /* dmlSqlControl_2 */{
                { "call SQLC.SQLCONTROL2_0 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38002", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_1 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38001", "DROP VIEW SQLCO-38001",
                        "LOCK TABLE SQLC-38001", "VALUES 1,2,3-38001",
                        "SET SCHEMA SQLC-38001", "CREATE SCHEMA S-38001",
                        "DROP SCHEMA SQL-38001" },
                { "call SQLC.SQLCONTROL2_2 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3-38004",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_3 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38004", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_4 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38002", "null", "null", "null", "null", "null",
                        "null" } };

        String[][] dmlSqlControl_3 = /* dmlSqlControl_3 */{
                { "call SQLC.SQLCONTROL2_0 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38002", "null", "null", "null", "null", "null",
                        "null" },
                { "call SQLC.SQLCONTROL2_1 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38001", "DROP VIEW SQLCO-38001",
                        "LOCK TABLE SQLC-38001", "VALUES 1,2,3-38001",
                        "SET SCHEMA SQLC-38001", "CREATE SCHEMA S-38001",
                        "DROP SCHEMA SQL-38001" },
                { "call SQLC.SQLCONTROL2_2 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3-38004",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_3 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_4 (?, ?, ?, ?, ?, ?, ?) ",
                        "STATE-38002", "null", "null", "null", "null", "null",
                        "null" } };

        String[][] dmlSqlControl_4 = /* dmlSqlControl_4 */{
                { "call SQLC.SQLCONTROL2_0 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-UPDATE 0-EXECUTE OK",
                        "DROP VIEW SQLCO-UPDATE 0-EXECUTE OK",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-UPDATE 0-EXECUTE OK",
                        "DROP SCHEMA SQL-UPDATE 0-EXECUTE OK" },
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                { "call SQLC.SQLCONTROL2_1 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38001", "DROP VIEW SQLCO-38001",
                        "LOCK TABLE SQLC-38001", "VALUES 1,2,3-38001",
                        "SET SCHEMA SQLC-38001", "CREATE SCHEMA S-38001",
                        "DROP SCHEMA SQL-38001" },
                { "call SQLC.SQLCONTROL2_2 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3-38004",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_3 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-38002", "DROP VIEW SQLCO-38002",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-38002", "DROP SCHEMA SQL-38002" },
                { "call SQLC.SQLCONTROL2_4 (?, ?, ?, ?, ?, ?, ?) ",
                        "CREATE VIEW SQL-UPDATE 0-EXECUTE OK",
                        "DROP VIEW SQLCO-UPDATE 0-EXECUTE OK",
                        "LOCK TABLE SQLC-UPDATE 0-EXECUTE OK",
                        "VALUES 1,2,3- ROW(1)- ROW(2)- ROW(3)-EXECUTE OK",
                        "SET SCHEMA SQLC-UPDATE 0-EXECUTE OK",
                        "CREATE SCHEMA S-UPDATE 0-EXECUTE OK",
                        "DROP SCHEMA SQL-UPDATE 0-EXECUTE OK" } };

        String[][][] dmlSqlControl = { dmlSqlControl_0, dmlSqlControl_1,
                dmlSqlControl_2, dmlSqlControl_3, dmlSqlControl_4 };

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        s.execute("create table SQLC.SQLCONTROL_DML(I int)");
        s.execute("insert into SQLC.SQLCONTROL_DML values 4");
        for (int i = 0; i < control.length; i++) {
            for (int t = 0; t < control.length; t++) {
                CallableStatement cs = conn
                        .prepareCall("call SQLC.SQLCONTROL4_" + i
                                + " (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                cs.setInt(1, t);
                for (int rop = 2; rop <= 9; rop++) {
                    cs.registerOutParameter(rop, Types.VARCHAR);
                }

                cs.execute();
                for (int p = 2; p <= 9; p++) {
                    String so = cs.getString(p);
                    // System.out.print("\"" + so + "\"");
                    /*
                     * if (p < 9) System.out.println(","); else
                     * System.out.println("}");
                     */
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                    if (so == null) {
                        continue;
                    }

                    assertEquals(dmlSqlControl[i][t][p - 2], so);
                }
                /*
                 * if (t< control.length -1) System.out.println(","); else
                 * System.out.println("}");
                 */
                cs.close();
            }
        }
        // Make sure we throw proper error with network server
        // if params are not registered
        assertCallError(
                usingEmbedded() ? "07004" : "07000",
                "call SQLC.SQLCONTROL3_0 (?, ?, ?, ?, ?, ?, ?)");
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        s.execute("drop table SQLC.SQLCONTROL_DML");

        for (int i = 0; i < control.length; i++) {
            s.execute("drop PROCEDURE SQLC.SQLCONTROL1_" + i);
            s.execute("drop PROCEDURE SQLC.SQLCONTROL2_" + i);
            s.execute("drop PROCEDURE SQLC.SQLCONTROL3_" + i);
            s.execute("drop PROCEDURE SQLC.SQLCONTROL4_" + i);
        }
        s.execute("drop table SQLCONTROL_DDL");
        s.execute("set schema APP");
        s.execute("drop schema SQLC RESTRICT");

        s.close();
    }


    /**
     * Better diagnostics when a function is being used as a procedure
     * or vice versa
     */
    public void testDerby5945() throws SQLException {
        setAutoCommit(false);
        Connection c = getConnection();
        Statement s = c.createStatement();
        s.executeUpdate(
            "create procedure PROC( inout ret int ) parameter style java" +
            "    modifies sql data language java external name " +
            "'" + thisClassName + ".PROC'");
        s.executeUpdate(
            "create function FUNC (i int) returns int parameter style java" +
            "    reads sql data language java external name " +
            "'" + thisClassName + ".FUNC'");


        try {
            CallableStatement cs = c.prepareCall("{?=call PROC(?)}");
            fail();
        } catch (SQLException e) {
            assertEquals(
                "'PROC' is a procedure but it is being used as a function.",
                e.getMessage());
            assertSQLState("42Y03", e);
        }

        ResultSet rs = s.executeQuery("values func(3)");
        rs.next();
        assertEquals(9, rs.getInt(1));

        try {
            s.executeUpdate("call func(3)");
            fail();
        } catch (SQLException e) {
            assertEquals(
                "'FUNC' is a function but it is being called as a procedure.",
                e.getMessage());
            assertSQLState("42Y03", e);
        }

        rollback();
    }

    public static void PROC(int[] i) {}

    public static int FUNC(int i) {
        return i*i;
    }

    public static void pSMALLINT(short in, short[] inout, short[] out)
            throws SQLException {

        out[0] = in;
        inout[0] += in;
    }

    public static void parameter1(int a, String b, String c,
            java.sql.ResultSet[] rs) throws SQLException {

        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");
        PreparedStatement ps = conn
                .prepareStatement("insert into PT1 values (?, ?, ?)");
        ps.setInt(1, a);
        ps.setString(2, b);
        ps.setString(3, c);
        ps.executeUpdate();
        ps.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        ps = conn.prepareStatement(
            "select a,b, length(b), c, length(c) from PT1 where a = ?");
        ps.setInt(1, a);
        rs[0] = ps.executeQuery();
        conn.close();
    }

    public static void parameter2(int a, java.math.BigDecimal b,
            java.math.BigDecimal c, java.sql.ResultSet[] rs)
            throws SQLException {
        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");
        PreparedStatement ps = conn
                .prepareStatement("insert into PT1 values (?, ?, ?)");
        ps.setInt(1, a);
        ps.setString(2, b.toString());
        ps.setString(3, c.toString());
        ps.executeUpdate();
        ps.close();
        ps = conn.prepareStatement("select a,b,c from PT1 where a = ?");
        ps.setInt(1, a);
        rs[0] = ps.executeQuery();
        conn.close();
    }

    public static void outparams1(int[] p1, int p2) {

        p1[0] = p2 * 2;
    }

    public static void inoutparams2(int[] p1, int p2) {

        p1[0] = p1[0] + (p2 * 2);
    }

    public static void inoutparams3(String[] p1, int p2) {

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        if (p2 == 8) {
            p1[0] = "nad";
        } else if (p2 == 9) {
            p1[0] = null;
        } else if (p2 == 10) {
            p1[0] = "abcdefghijklmnopqrstuvwzyz";
        }
    }

    public static void inoutparams4(java.math.BigDecimal[] p1, String p2) {
        if (p2 == null) {
            p1[0] = null;
        } else {
            if (p1[0] == null) {
                p1[0] = new BigDecimal(p2).add(new BigDecimal("17"));
            } else {
                p1[0] = new BigDecimal(p2).add(p1[0]);
            }
        }
    }

    public static void sqlControl(String[] e1, String[] e2, String[] e3,
            String[] e4, String[] e5, String[] e6, String[] e7)
            throws SQLException {

        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");

        Statement s = conn.createStatement();

        executeStatement(s, "CREATE TABLE SQLCONTROL_DDL (I INT)", e1);
        executeStatement(
                s,
                "ALTER TABLE SQLC.SQLCONTROL_DML ADD COLUMN B INT DEFAULT NULL",
                e2);

        executeStatement(s, "INSERT INTO SQLC.SQLCONTROL_DML(I) VALUES (1)", e3);
        executeStatement(s, "UPDATE SQLC.SQLCONTROL_DML SET I = I + 11", e4);
        executeStatement(s, "SELECT * FROM SQLC.SQLCONTROL_DML", e5);
        executeStatement(s, "DELETE FROM SQLC.SQLCONTROL_DML", e6);

        executeStatement(s, "DROP TABLE SQLC.SQLCONTROL_DML", e7);

        conn.close();

    }

    public static void sqlControl2(String[] e1, String[] e2, String[] e3,
            String[] e4, String[] e5, String[] e6, String[] e7)
            throws SQLException {

        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");

        Statement s = conn.createStatement();

        executeStatement(
                s,
                "CREATE VIEW SQLCONTROL_VIEW AS SELECT * FROM SQLC.SQLCONTROL_DML",
                e1);
        executeStatement(s, "DROP VIEW SQLCONTROL_VIEW", e2);

        executeStatement(s, "LOCK TABLE SQLC.SQLCONTROL_DML IN EXCLUSIVE MODE",
                e3);
        executeStatement(s, "VALUES 1,2,3", e4);
        executeStatement(s, "SET SCHEMA SQLC", e5);
        executeStatement(s, "CREATE SCHEMA SQLC_M", e6);
        executeStatement(s, "DROP SCHEMA SQLC_M RESTRICT", e7);

        conn.close();

    }

    public static void sqlControl3(String[] e1, String[] e2, String[] e3,
            String[] e4, String[] e5, String[] e6, String[] e7)
            throws SQLException {

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        Statement s = conn.createStatement();

        e1[0] = "DERBY FEATURE";
        e2[0] = "DERBY FEATURE";

        executeStatement(s, "SET ISOLATION CS", e3);
        executeStatement(s, "SET RUNTIMESTATISTICS OFF", e4);
        executeStatement(s, "SET STATISTICS TIMING OFF", e5);
        executeStatement(s, "VALUES 1", e6);

        executeStatement(s, "VALUES 1", e7);

        conn.close();

    }

    public static void sqlControl4(int sqlc, String[] e1, String[] e2,
            String[] e3, String[] e4, String[] e5, String[] e6, String[] e7,
            String[] e8) throws SQLException {

        Connection conn = DriverManager
                .getConnection("jdbc:default:connection");

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
        String sql = "call SQLC.SQLCONTROL2_" + sqlc
                + " (?, ?, ?, ?, ?, ?, ?) ";

        e1[0] = sql;

        CallableStatement cs1 = conn.prepareCall(sql);
        try {
            for (int rop = 1; rop <= 7; rop++) {
                cs1.registerOutParameter(rop, Types.VARCHAR);
            }
            cs1.execute();

            e2[0] = cs1.getString(1);
            e3[0] = cs1.getString(2);
            e4[0] = cs1.getString(3);
            e5[0] = cs1.getString(4);
            e6[0] = cs1.getString(5);
            e7[0] = cs1.getString(6);
            e8[0] = cs1.getString(7);
        } catch (SQLException sqle) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5945
            StringBuilder sb = new StringBuilder(128);
            sb.append("STATE");
            do {
                sb.append("-");
                String ss = sqle.getSQLState();

                if (ss == null) {
                    ss = "?????";
                }

                sb.append(ss);
                sqle = sqle.getNextException();
            } while (sqle != null);

            e2[0] = sb.toString();
        }

        cs1.close();

        conn.close();

    }

    private static void executeStatement(Statement s, String sql,
            String[] result) {

        StringBuilder sb = new StringBuilder(128);
//IC see: https://issues.apache.org/jira/browse/DERBY-5945

        int len = sql.length();

        if (len > 15) {
            len = 15;
        }

        sb.append(sql.substring(0, len));
        try {
            if (s.execute(sql)) {
                ResultSet rs = s.getResultSet();

                while (rs.next()) {
                    sb.append("- ROW(");
                    sb.append(rs.getString(1));
                    sb.append(")");
                }

                rs.close();
            } else {
                sb.append("-UPDATE ");
                sb.append(s.getUpdateCount());
            }

            sb.append("-EXECUTE OK");

        } catch (SQLException sqle) {

            do {
                sb.append("-");
                String ss = sqle.getSQLState();

//IC see: https://issues.apache.org/jira/browse/DERBY-5945
                if (ss == null) {
                    ss = "?????";
                }

                sb.append(ss);
                sqle = sqle.getNextException();
            } while (sqle != null);
        }
        result[0] = sb.toString();
    }

    public static void missingDynamicParameter(int p1) {
    }

    public static void missingDynamicParameter(int p1, Object p2) {
    }

    public static void badDynamicParameter(int p1, ProcedureTest[] data) {
    }

    public static void zeroArgDynamicResult(ResultSet[] data1,
            ResultSet[] data2, ResultSet[] data3, ResultSet[] data4) {

    }
}
