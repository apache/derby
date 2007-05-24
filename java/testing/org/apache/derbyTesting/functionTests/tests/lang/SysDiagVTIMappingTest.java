/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SysDiagVTIMappingTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class SysDiagVTIMappingTest extends BaseJDBCTestCase {

    // Name of the log file to use when testing VTIs that expect one.
    private static final String testLogFile = "sys_vti_test_derby.tstlog";

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public SysDiagVTIMappingTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("Diagnostic VTI Table Mappings");
        suite.addTest(TestConfiguration.
            defaultSuite(SysDiagVTIMappingTest.class));

        /* Some of the VTIs that are tested in this class require a derby.log
         * file.  We have a test log file stored in the tests/lang directory,
         * and since the VTIs are going to try to read it, the test log file
         * must be in a directory for which Derby has read access.  By
         * using a SupportFilesSetup wrapper, we copy the test log file to
         * the "extin" directory, which has the required permissions.
         */
        return SecurityManagerSetup.noSecurityManager(
            new SupportFilesSetup(suite,
                new String [] {
                    "functionTests/tests/lang/" + testLogFile
                }));
    }

    public void setUp() throws Exception
    {
        createStatement().execute("create table app.t1 (i int, c varchar(10))");
        createStatement().execute("insert into app.t1 values (1, 'one'), "
            + "(2, 'two'), (4, 'four')");
    }

    public void tearDown() throws Exception
    {
        createStatement().execute("drop table app.t1");
    }

    /**
     * We use the SpaceTable VTI as our primary test VTI to verify that
     * that VTI table mappings in general are working as expected.  So
     * this method does a lot more than the other VTI test methods;
     * the other test methods just do sanity checks to make sure that
     * the mapping from "SYSCS_DIAG.<vti_table>" to the actual VTI
     * class names in question is working correctly.
     */
    public void testSpaceTable() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set schema APP");
        
        // Should fail because SPACE_TABLE is not defined in APP 
        // schema.
        
        assertStatementError("42Y03", st,
            "select * from TABLE(SPACE_TABLE('APP')) x");
        
        assertStatementError("42Y03", st,
            "select * from TABLE(APP.SPACE_TABLE('APP', 'T1')) x");
        
        // Should fail due to extra "TABLE" keyword.
        
        assertStatementError("42X01", st,
            "select * from TABLE TABLE(SYSCS_DIAG.SPACE_TABLE('T1')) x");
        
        assertStatementError("42X01", st,
            "select * from TABLE TABLE (select * from t1) x");
        
        // Should fail because the specified schema does not exist.
        
        assertStatementError("42Y07", st,
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE('T1', 'APP')) x");
        
        assertStatementError("42Y07", st,
            "select * from "
            + "TABLE(SYSCS_DIAG.SPACE_TABLE('NOTTHERE', 'T1')) x");
        
        // Should fail because SPACE_TABLE is not defined in APP schema.
        
        st.executeUpdate("set schema SYSCS_DIAG");
        assertStatementError("42Y03", st,
            "select * from TABLE(APP.SPACE_TABLE('APP', 'T1')) x");
        
        // All remaining test cases in this method should succeed.
        
        st.executeUpdate("set schema APP");

        // These should all return 1 row for APP.T1.
        
        // Two-argument direct call.
        ResultSet rs = st.executeQuery(
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE('APP', 'T1')) x");
        
        String [] expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Single-argument direct execution.
        rs = st.executeQuery(
            " select * from TABLE(SYSCS_DIAG.SPACE_TABLE('T1')) x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Two-argument prepare and execute.
        PreparedStatement pSt = prepareStatement(
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE(?, ?)) x");

        pSt.setString(1, "APP");
        pSt.setString(2, "T1");

        rs = pSt.executeQuery();
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
             "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Single-argument prepare and execute.
        pSt = prepareStatement(
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE(?)) x");

        pSt.setString(1, "T1");

        rs = pSt.executeQuery();
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Statment should succeed but there will be no rows 
        // because the tables do not exist.
        
        rs = st.executeQuery(
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE('APP')) x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE('APP', "
            + "'NOTTHERE')) x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            "select * from "
            + "TABLE(SYSCS_DIAG.SPACE_TABLE('SYSCS_DIAG', 'NOTTHERE')) x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // Should see zero rows since LOCK_TABLE does not exist as 
        // an actual base table (it's another VTI).
        
        rs = st.executeQuery(
            "select * from "
            + "TABLE(SYSCS_DIAG.SPACE_TABLE('SYSCS_DIAG', 'LOCK_TABLE')) x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // Similar tests but in the SYSCS_DIAG schema, in which 
        // case the schema-name for SPACE_TABLE should not be required.
        
        st.executeUpdate("set schema syscs_diag");
        
        // Should see 1 row for APP.T1.
        
        pSt = prepareStatement(
            "select * from TABLE(SPACE_TABLE(?, ?)) x");
        
        pSt.setString(1, "APP");
        pSt.setString(2, "T1");

        rs = pSt.executeQuery();
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Should see zero rows since T1 does not exist within 
        // schema "SYSCS_DIAG".
        
        pSt = prepareStatement("select * from TABLE(SPACE_TABLE(?)) x");
        pSt.setString(1, "T1");

        rs = pSt.executeQuery();
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // Should see zero rows since LOCK_TABLE does not exist as 
        // an actual base table (it's another VTI).
        
        rs = st.executeQuery(
            "select * from TABLE(SPACE_TABLE('LOCK_TABLE')) x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // Simple check to ensure that we we can join with the VTI.
        
        st.executeUpdate("set schema app");
        rs = st.executeQuery(
            "select cast (conglomeratename as varchar(30)), t1.* from"
            + "  TABLE(SYSCS_DIAG.SPACE_TABLE('APP', 'T1')) x,"
            + "  t1"
            + " where x.conglomeratename is not null");
        
        expColNames = new String [] {"1", "I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "1", "one"},
            {"T1", "2", "two"},
            {"T1", "4", "four"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // Make sure old way of calling still works until it is 
        // deprecated.
        
        st.executeUpdate("set schema APP");
        rs = st.executeQuery(
            "SELECT * FROM NEW org.apache.derby.diag.SpaceTable('T1') as x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "SELECT * FROM NEW "
            + "org.apache.derby.diag.SpaceTable('APP', 'T1') as x");
        
        expColNames = new String [] {
            "CONGLOMERATENAME", "ISINDEX", "NUMALLOCATEDPAGES", "NUMFREEPAGES",
            "NUMUNFILLEDPAGES", "PAGESIZE", "ESTIMSPACESAVING"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"T1", "0", "1", "0", "1", "4096", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Now do some sanity checking to make sure SPACE_TABLE cannot be
        // used in any illegal ways.        

        checkIllegalUses(st, "space_table", "SpaceTable",
            "('APP', 'T1')", "conglomeratename");

        // Clean-up.
        getConnection().rollback();
        st.close();
    }

    /**
     * Just run a couple of sanity checks to makes sure the table
     * mapping for org.apache.derby.diag.StatementDuration() works
     * correctly and fails where it is supposed to.
     */
    public void testStatementDuration() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set schema APP");

        // Do a simple check to make sure the VTI mapping works.
        
        java.net.URL logURL = SupportFilesSetup.getReadOnlyURL(testLogFile);
        String vtiArg = "('" + logURL.getFile() + "')";
        ResultSet rs = st.executeQuery(
            "select * from "
            + "TABLE(SYSCS_DIAG.STATEMENT_DURATION" + vtiArg + ") x");
        
        String [] expColNames = new String [] {
            "TS", "THREADID", "XID", "LCCID", "LOGTEXT", "DURATION"};

        JDBC.assertColumnNames(rs, expColNames);
        String [][] expRS = new String [][]
        {
            {"2006-12-15 16:14:58.280", "main,5,main", "1111", "0",
                "(DATABASE = ugh), (DRDAID = null), Cleanup action starting",
                "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // Make sure old way of calling still works until it is 
        // deprecated.
        
        rs = st.executeQuery(
            "SELECT * FROM NEW "
            + "org.apache.derby.diag.StatementDuration" + vtiArg + " as x");
        
        expColNames = new String [] {
            "TS", "THREADID", "XID", "LCCID", "LOGTEXT", "DURATION"};

        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
        {
            {"2006-12-15 16:14:58.280", "main,5,main", "1111", "0",
                "(DATABASE = ugh), (DRDAID = null), Cleanup action starting",
                "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // And finally, do the usual checks for invalid uses.

        checkIllegalUses(st, "statement_duration", "StatementDuration",
            vtiArg, "logtext");
    }

    /**
     * Just run a couple of sanity checks to makes sure the table
     * mapping for org.apache.derby.diag.ErrorLogReader() works
     * correctly and fails where it is supposed to.
     */
    public void testErrorLogReader() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set schema APP");

        // Do a simple check to make sure the VTI mapping works.
        
        java.net.URL logURL = SupportFilesSetup.getReadOnlyURL(testLogFile);
        String vtiArg = "('" + logURL.getFile() + "')";
        ResultSet rs = st.executeQuery(
            " select * from "
            + "TABLE(SYSCS_DIAG.ERROR_LOG_READER" + vtiArg + ") x");
        
        String [] expColNames = new String [] {
            "TS", "THREADID", "XID", "LCCID", "DATABASE", "DRDAID", "LOGTEXT"};

        JDBC.assertColumnNames(rs, expColNames);
        String [][] expRS = new String [][]
        {
            {"2006-12-15 16:14:58.280", "main,5,main", "1111", "0", "ugh",
                "null", "Cleanup action starting"},
            {"2006-12-15 16:14:58.280", "main,5,main", "1111", "0", "ugh",
                "null", "Failed Statement is: select * from oops"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // Make sure old way of calling still works until it is 
        // deprecated.
        
        rs = st.executeQuery(
            "SELECT * FROM NEW" +
            " org.apache.derby.diag.ErrorLogReader" + vtiArg + " as x");
        
        expColNames = new String [] {
            "TS", "THREADID", "XID", "LCCID", "DATABASE", "DRDAID", "LOGTEXT"};

        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
        {
            {"2006-12-15 16:14:58.280", "main,5,main", "1111", "0", "ugh",
                "null", "Cleanup action starting"},
            {"2006-12-15 16:14:58.280", "main,5,main", "1111", "0", "ugh",
                "null", "Failed Statement is: select * from oops"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // And finally, do the usual checks for invalid uses.

        checkIllegalUses(st, "error_log_reader", "ErrorLogReader",
            vtiArg, "logtext");
    }

    /**
     * Tests to make sure that attempts to use the TABLE constructor
     * with things other than the VTI diagnostic table functions
     * do not work (with the exception of SELECT and VALUES queries,
     * which should work as normal).
     */
    public void testInvalidTableFunctions() throws Exception
    {
        // Sanity check: make sure SELECT and VALUES clauses still  work.
        
        Statement st = createStatement();
        st.executeUpdate("set schema APP");

        ResultSet rs = st.executeQuery(
            "select * from table (select * from t1) x");
        
        String [] expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"1", "one"},
            {"2", "two"},
            {"4", "four"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from table (values (1, 2), (2, 3)) x");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2"},
            {"2", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Use of TABLE constructor with regular tables should fail.

        assertStatementError("42X01", st, "select * from table (app.t1) x");
        assertStatementError("42Y03", st, "select * from table (app.t1()) x");

        // Use of TABLE constructor with VTI tables (as opposed to VTI
        // table *functions*) should fail.

        assertStatementError("42X01", st,
            "select * from table (syscs_diag.lock_table) x");

        assertStatementError("42Y03", st,
            "select * from table (syscs_diag.lock_table()) x");

        assertStatementError("42X01", st,
            "select * from table (syscs_diag.transaction_table) x");

        assertStatementError("42Y03", st,
            "select * from table (syscs_diag.transaction_table()) x");

        assertStatementError("42X01", st,
            "select * from table (syscs_diag.statement_cache) x");

        assertStatementError("42Y03", st,
            "select * from table (syscs_diag.statement_cache()) x");

        assertStatementError("42X01", st,
            "select * from table (syscs_diag.error_messages) x");

        assertStatementError("42Y03", st,
            "select * from table (syscs_diag.error_messages()) x");

        // Clean-up.
        getConnection().rollback();
        st.close();
    }

    /* All statements in this method should fail because a VTI table-
     * mapping that takes arguments can only be used as part of the TABLE 
     * constructor.  Any other uses of, or attempts to modify, such a
     * VTI table should throw an appropriate error.
     */
    private void checkIllegalUses(Statement st, String vtiTableName,
        String vtiMethodName, String args, String colName) throws SQLException
    {
        assertStatementError("42X05", st,
            "select * from syscs_diag." + vtiTableName);
        
        assertStatementError("42X01", st,
            "select * from syscs_diag." + vtiTableName + args + " x");
        
        assertStatementError("42X01", st,
            "select * from (syscs_diag." + vtiTableName + args + ") x");
        
        assertStatementError("42Y55", st,
            "drop table syscs_diag." + vtiTableName);
        
        assertStatementError("42X01", st,
            "drop table syscs_diag." + vtiTableName + args);
        
        assertStatementError("42X62", st,
            "drop function syscs_diag." + vtiTableName);
        
        assertStatementError("42X01", st,
            "drop function syscs_diag." + vtiTableName + args);
        
        assertStatementError("42X62", st,
            "alter table syscs_diag." + vtiTableName + " add column bad int");
        
        assertStatementError("42X01", st,
            "alter table syscs_diag." + vtiTableName + args
            + " add column bad int");
        
        assertStatementError("42X05", st,
            "update syscs_diag." + vtiTableName
            + " set " + colName + " = NULL");
        
        assertStatementError("42X01", st,
            "update syscs_diag." + vtiTableName + args + " set "
            + colName + "  = NULL");
        
        assertStatementError("42X05", st,
            "delete from syscs_diag." + vtiTableName + " where 1 = 1");
        
        assertStatementError("42X01", st,
            "delete from syscs_diag." + vtiTableName + args + " where 1 = 1");
        
        assertStatementError("42X05", st,
            "insert into syscs_diag." + vtiTableName + " values('bad')");
        
        assertStatementError("42X05", st,
            "insert into syscs_diag." + vtiTableName + " (" + colName
            + ") values('bad')");
        
        assertStatementError("42X01", st,
            "insert into syscs_diag." + vtiTableName + args
            + " values('bad')");
        
        CallableStatement cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, 1)");
        cSt.setString(1, "SYSCS_DIAG");
        cSt.setString(2, vtiTableName.toUpperCase());

        assertStatementError("42X62", cSt);
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, 1, 1, 1)");
        cSt.setString(1, "SYSCS_DIAG");
        cSt.setString(2, vtiTableName.toUpperCase());
        assertStatementError("42X05", cSt);

        assertStatementError("42X08", st,
            "update new org.apache.derby.diag." + vtiMethodName + args
            + " set " + colName + " = NULL");
        
        assertStatementError("42X08", st,
            "delete from new org.apache.derby.diag." + vtiMethodName + args
            + " where 1 = 0");

        // Simple check to verify same restrictions hold true if current
        // schema is "SYSCS_DIAG".

        st.execute("set schema syscs_diag");
        assertStatementError("42X01", st,
            "select * from " + vtiTableName + args + " x");
        
        assertStatementError("42X01", st,
            "select * from (" + vtiTableName + args + ") x");

        st.execute("set schema app");
    }
}
