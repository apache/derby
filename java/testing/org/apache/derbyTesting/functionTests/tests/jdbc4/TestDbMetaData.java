/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc.TestDbMetaData

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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of database metadata for new methods in JDBC 40.
 */
public class TestDbMetaData extends BaseJDBCTestCase {

    private DatabaseMetaData meta;

    public TestDbMetaData(String name) {
        super(name);
    }

    protected void setUp() throws SQLException {
        meta = getConnection().getMetaData();
    }

    protected void tearDown() throws Exception {
        meta = null;
        super.tearDown();
    }

    private static void createFunctions(Statement s) throws SQLException {
        // Create some functions in the default schema (app) to make
        // the output from getFunctions() and getFunctionColumns
        // more interesting
        s.execute("CREATE FUNCTION DUMMY1 ( X SMALLINT ) RETURNS SMALLINT "+
                  "PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL "+
                  "NAME 'java.some.func'");
        s.execute("CREATE FUNCTION DUMMY2 ( X INTEGER, Y SMALLINT ) RETURNS"+
                  " INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA "+
                  "EXTERNAL NAME 'java.some.func'");
        s.execute("CREATE FUNCTION DUMMY3 ( X VARCHAR(16), Y INTEGER ) "+
                  "RETURNS VARCHAR(16) PARAMETER STYLE JAVA NO SQL LANGUAGE"+
                  " JAVA EXTERNAL NAME 'java.some.func'");
        s.execute("CREATE FUNCTION DUMMY4 ( X VARCHAR(128), Y INTEGER ) "+
                  "RETURNS INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE "+
                  "JAVA EXTERNAL NAME 'java.some.func'");
    }

    private static Test baseSuite(String name) {
        TestSuite testSuite = new TestSuite(name);
        testSuite.addTestSuite(TestDbMetaData.class);
        return new CleanDatabaseTestSetup(testSuite) {
                protected void decorateSQL(Statement s) throws SQLException {
                    createFunctions(s);
                }
            };
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("TestDbMetaData suite");
        suite.addTest(baseSuite("TestDbMetaData:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("TestDbMetaData:client")));
        return suite;
    }

    public void testSupportsStoredFunctionsUsingCallSyntax()
            throws SQLException {
        assertTrue(meta.supportsStoredFunctionsUsingCallSyntax());
    }

    public void testAutoCommitFailureClosesAllResultSets() throws SQLException {
        assertFalse(meta.autoCommitFailureClosesAllResultSets());
    }

    public void testGetClientInfoProperties() throws SQLException {
        ResultSet rs = meta.getClientInfoProperties();
        JDBC.assertColumnNames(rs, new String[] {
            "NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION" });
        JDBC.assertDrainResults(rs, 0);
    }

    /**
     * Since JDBC40Translation cannot be accessed in queries in
     * metadata.properties, the query has to use
     * DatabaseMetaData.procedureNullable. Hence it is necessary
     * to verify that that value of
     * DatabaseMetaData.functionNullable is the same.
     */
    public void testFunctionNullable() {
        assertEquals(DatabaseMetaData.procedureNullable,
                     DatabaseMetaData.functionNullable);
    }

    /** Check that the column names are as expected from getFunctions(). */
    private void assertGetFunctionsRs(ResultSet rs) throws SQLException {
        JDBC.assertColumnNames(rs, new String[] {
            "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
            "SPECIFIC_NAME" });
    }
    
    private static final GeneratedId GENERIC_NAME = new GeneratedId();

    /** Expected rows from getFunctions() when all functions match. */
    private static final Object[][] ALL_FUNCTIONS = {
        { null, "APP", "DUMMY1", "java.some.func", GENERIC_NAME },
        { null, "APP", "DUMMY2", "java.some.func", GENERIC_NAME },
        { null, "APP", "DUMMY3", "java.some.func", GENERIC_NAME },
        { null, "APP", "DUMMY4", "java.some.func", GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_CHECK_TABLE",
          "org.apache.derby.catalog.SystemProcedures.SYSCS_CHECK_TABLE",
          GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_DATABASE_PROPERTY",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_DATABASE_PROPERTY", GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_RUNTIMESTATISTICS",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_RUNTIMESTATISTICS", GENERIC_NAME },
          { null, "SYSCS_UTIL", "SYSCS_GET_USER_ACCESS",
              "org.apache.derby.catalog.SystemProcedures." +
              "SYSCS_GET_USER_ACCESS", GENERIC_NAME },
        { null, "SYSIBM", "BLOBCREATELOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBCREATELOCATOR", GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETBYTES",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETBYTES", GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETLENGTH",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETLENGTH", GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETPOSITIONFROMBYTES",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETPOSITIONFROMBYTES", GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETPOSITIONFROMLOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETPOSITIONFROMLOCATOR", GENERIC_NAME },
        { null, "SYSIBM", "CLOBCREATELOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBCREATELOCATOR", GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETLENGTH",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETLENGTH", GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETPOSITIONFROMLOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETPOSITIONFROMLOCATOR", GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETPOSITIONFROMSTRING",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETPOSITIONFROMSTRING", GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETSUBSTRING",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETSUBSTRING", GENERIC_NAME },

    };

    public void testGetFunctionsNullNullNull() throws SQLException {
        // Any function in any schema in any catalog
        ResultSet rs = meta.getFunctions(null, null, null);
        assertGetFunctionsRs(rs);
        JDBC.assertFullResultSet(rs, ALL_FUNCTIONS, false);
    }

    public void testGetFunctionsDummySchema() throws SQLException {
        // Any function in any schema in "Dummy
        // Catalog". Same as above since the catalog
        // argument is ignored (is always null)
        ResultSet rs = meta.getFunctions("Dummy Catalog", null, null);
        assertGetFunctionsRs(rs);
        JDBC.assertFullResultSet(rs, ALL_FUNCTIONS, false);
    }

    public void testGetFunctionsFromSysSchemas() throws SQLException {
        
        getSysFunctions();
        // Any function in a schema starting with "SYS"
        ResultSet rs = meta.getFunctions(null, "SYS%", null);
        assertGetFunctionsRs(rs);
        JDBC.assertFullResultSet(rs, getSysFunctions(), false);
    }
    
    /**
     * From the list of all functions extract the ones in schemas
     * starting with SYS.
      */
    private static Object[][] getSysFunctions()
    {
        int n = 0;
        for (int i = 0; i < ALL_FUNCTIONS.length; i++)
        {
            String schema = (String) ALL_FUNCTIONS[i][1];
            if (schema.startsWith("SYS"))
                n++;
        }
        
        Object[][] sysFunctions = new Object[n][];
        n = 0;
        for (int i = 0; i < ALL_FUNCTIONS.length; i++)
        {
            String schema = (String) ALL_FUNCTIONS[i][1];
            if (schema.startsWith("SYS"))
                sysFunctions[n++] = ALL_FUNCTIONS[i];
        }        
               
        return sysFunctions;
    }
    
    /**
     * From the list of all functions extract the ones with GET in the name.
     * This assumes this test does not create functions with GET.
      */
    private static Object[][] getGetFunctions()
    {
        int n = 0;
        for (int i = 0; i < ALL_FUNCTIONS.length; i++)
        {
            String name = (String) ALL_FUNCTIONS[i][2];
            if (name.indexOf("GET") != -1)
                n++;
        }
        
        Object[][] getFunctions = new Object[n][];
        n = 0;
        for (int i = 0; i < ALL_FUNCTIONS.length; i++)
        {
            String name = (String) ALL_FUNCTIONS[i][2];
            if (name.indexOf("GET") != -1)
                getFunctions[n++] = ALL_FUNCTIONS[i];
        }        
               
        return getFunctions;
    }
    

    public void testGetFunctionsContainingGET() throws SQLException {
        // All functions containing "GET" in any schema 
        // (and any catalog)
        ResultSet rs = meta.getFunctions(null, null, "%GET%");
        assertGetFunctionsRs(rs);
        JDBC.assertFullResultSet(rs, getGetFunctions(), false);
    }

    public void testGetFunctionsNoSchemaNoCatalog() throws SQLException {
        // Any function that belongs to NO schema and 
        // NO catalog (none)
        ResultSet rs = meta.getFunctions("", "", null);
        assertGetFunctionsRs(rs);
        JDBC.assertDrainResults(rs, 0);
    }

    /** Check that the column names are as expected from
     * getFunctionColumns(). */
    private void assertGetFunctionColumnsRs(ResultSet rs) throws SQLException {
        JDBC.assertColumnNames(rs, new String[] {
            "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
            "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
            "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME",
            "METHOD_ID", "PARAMETER_ID"
        });
    }

    public void testGetFunctionColumnsStartingWithDUMMY() throws SQLException {
		// Test getFunctionColumns
        // Dump parameters for all functions beginning with DUMMY
        ResultSet rs = meta.getFunctionColumns(null, null, "DUMMY%", null);
        assertGetFunctionColumnsRs(rs);
        Object[][] expectedRows = {
            { null, "APP", "DUMMY1", "", new Integer(4), new Integer(5),
              "SMALLINT", new Integer(5), new Integer(2), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(0),
              "YES", GENERIC_NAME, new Integer(1), new Integer(-1) },
            { null, "APP", "DUMMY1", "X", new Integer(1), new Integer(5),
              "SMALLINT", new Integer(5), new Integer(2), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(1),
              "YES", GENERIC_NAME, new Integer(1), new Integer(0) },
            { null, "APP", "DUMMY2", "", new Integer(4), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(0),
              "YES", GENERIC_NAME, new Integer(2), new Integer(-1) },
            { null, "APP", "DUMMY2", "X", new Integer(1), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(1),
              "YES", GENERIC_NAME, new Integer(2), new Integer(0) },
            { null, "APP", "DUMMY2", "Y", new Integer(1), new Integer(5),
              "SMALLINT", new Integer(5), new Integer(2), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(2),
              "YES", GENERIC_NAME, new Integer(2), new Integer(1) },
            { null, "APP", "DUMMY3", "", new Integer(4), new Integer(12),
              "VARCHAR", new Integer(16), new Integer(32), null, null,
              new Integer(1), null, 32, new Integer(0), "YES",
              GENERIC_NAME, new Integer(2), new Integer(-1) },
            { null, "APP", "DUMMY3", "X", new Integer(1), new Integer(12),
              "VARCHAR", new Integer(16), new Integer(32), null, null,
              new Integer(1), null, 32, new Integer(1), "YES",
              GENERIC_NAME, new Integer(2), new Integer(0) },
            { null, "APP", "DUMMY3", "Y", new Integer(1), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(2),
              "YES", GENERIC_NAME, new Integer(2), new Integer(1) },
            { null, "APP", "DUMMY4", "", new Integer(4), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(0),
              "YES", GENERIC_NAME, new Integer(2), new Integer(-1) },
            { null, "APP", "DUMMY4", "X", new Integer(1), new Integer(12),
              "VARCHAR", new Integer(128), new Integer(256),
              null, null, new Integer(1), null, 256, new Integer(1), "YES",
              GENERIC_NAME,
              new Integer(2), new Integer(0) },
            { null, "APP", "DUMMY4", "Y", new Integer(1), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(2),
              "YES", GENERIC_NAME, new Integer(2), new Integer(1) },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    public void testGetFunctionColumnsForDummyFunctions() throws SQLException {
		// Dump return value for all DUMMY functions
        ResultSet rs = meta.getFunctionColumns(null, null, "DUMMY%", "");
        assertGetFunctionColumnsRs(rs);
        Object[][] expectedRows = {
            { null, "APP", "DUMMY1", "", new Integer(4), new Integer(5),
              "SMALLINT", new Integer(5), new Integer(2), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(0),
              "YES", GENERIC_NAME, new Integer(1), new Integer(-1) },
            { null, "APP", "DUMMY2", "", new Integer(4), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(0),
              "YES", GENERIC_NAME, new Integer(2), new Integer(-1) },
            { null, "APP", "DUMMY3", "", new Integer(4), new Integer(12),
              "VARCHAR", new Integer(16), new Integer(32),
              null, null, new Integer(1), null, 32, new Integer(0), "YES",
              GENERIC_NAME,
              new Integer(2), new Integer(-1) },
            { null, "APP", "DUMMY4", "", new Integer(4), new Integer(4),
              "INTEGER", new Integer(10), new Integer(4), new Integer(0),
              new Integer(10), new Integer(1), null, null, new Integer(0),
              "YES", GENERIC_NAME, new Integer(2), new Integer(-1) },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    /** Check that the column names are as expected from getSchemas(). */
    private void assertGetSchemasRs(ResultSet rs) throws SQLException {
        JDBC.assertColumnNames(rs, new String[] {
            "TABLE_SCHEM", "TABLE_CATALOG" });
    }

    public void testGetSchemasNullNull() throws SQLException {
        // Test the new getSchemas() with no schema qualifiers
        ResultSet rs = meta.getSchemas(null, null);
        assertGetSchemasRs(rs);
        Object[][] expectedRows = {
            { "APP", null },
            { "NULLID", null },
            { "SQLJ", null },
            { "SYS", null },
            { "SYSCAT", null },
            { "SYSCS_DIAG", null },
            { "SYSCS_UTIL", null },
            { "SYSFUN", null },
            { "SYSIBM", null },
            { "SYSPROC", null },
            { "SYSSTAT", null },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    public void testGetSchemasStartingWithSYS() throws SQLException {
        // Test the new getSchemas() with a schema wildcard qualifier
        ResultSet rs = meta.getSchemas(null, "SYS%");
        assertGetSchemasRs(rs);
        Object[][] expectedRows = {
            { "SYS", null },
            { "SYSCAT", null },
            { "SYSCS_DIAG", null },
            { "SYSCS_UTIL", null },
            { "SYSFUN", null },
            { "SYSIBM", null },
            { "SYSPROC", null },
            { "SYSSTAT", null },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    public void testGetSchemasMatchingAPP() throws SQLException {
        // Test the new getSchemas() with an exact match
        ResultSet rs = meta.getSchemas(null, "APP");
        assertGetSchemasRs(rs);
        Object[][] expectedRows = {
            { "APP", null },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    public void testGetSchemasMatchingBLAH() throws SQLException {
        // Make sure that getSchemas() returns an empty result
        // set when a schema is passed with no match
        ResultSet rs = meta.getSchemas(null, "BLAH");
        assertGetSchemasRs(rs);
        JDBC.assertDrainResults(rs, 0);
    }

    /**
     * Test supportsStoredFunctionsUsingCallSyntax() by checking
     * whether calling a stored procedure using the escape syntax
     * succeeds.
     *
     * @exception SQLException if an unexpected database error occurs
     */
    public void testStoredProcEscapeSyntax() throws SQLException {
        getConnection().setAutoCommit(false);
        String call = "{CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)}";
        Statement stmt = createStatement();

        boolean success;
        try {
            stmt.execute(call);
            success = true;
        } catch (SQLException e) {
            success = false;
        }

        assertEquals("supportsStoredFunctionsUsingCallSyntax() returned " +
                     "value which doesn't match actual behaviour.",
                     success, meta.supportsStoredFunctionsUsingCallSyntax());

        stmt.close();
    }

    /**
     * Test autoCommitFailureClosesAllResultSets() by checking whether
     * a failure in auto-commit mode will close all result sets, even
     * holdable ones.
     *
     * @exception SQLException if an unexpected database error occurs
     */
    public void testAutoCommitFailure() throws SQLException {
        Connection con = getConnection();
        con.setAutoCommit(true);

        Statement s1 =
            con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY,
                                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        ResultSet resultSet = s1.executeQuery("VALUES (1, 2), (3, 4)");

        Statement s2 = con.createStatement();
        try {
            String query =
                "SELECT dummy, nonexistent, phony FROM imaginarytable34521";
            s2.execute(query);
            fail("Query didn't fail.");
        } catch (SQLException e) {
            // should fail, but we don't care how
        }

        assertEquals("autoCommitFailureClosesAllResultSets() returned value " +
                     "which doesn't match actual behaviour.",
                     resultSet.isClosed(),
                     meta.autoCommitFailureClosesAllResultSets());

        resultSet.close();
        s1.close();
        s2.close();
    }

    public void testIsWrapperForPositive() throws SQLException {
        assertTrue("DatabaseMetaData should be wrapper for itself.",
                   meta.isWrapperFor(DatabaseMetaData.class));
    }

    public void testIsWrapperForNegative() throws SQLException {
        assertFalse("DatabaseMetaData should not wrap PreparedStatement.",
                    meta.isWrapperFor(PreparedStatement.class));
    }

    public void testGetWrapperPositive() throws SQLException {
        DatabaseMetaData dmd = meta.unwrap(DatabaseMetaData.class);
        assertSame("Unwrap should return same object.", meta, dmd);
    }

    public void testGetWrapperNegative() {
        try {
            PreparedStatement ps = meta.unwrap(PreparedStatement.class);
            fail("Unwrap should not return PreparedStatement.");
        } catch (SQLException e) {
            assertSQLState(SQLStateConstants.UNABLE_TO_UNWRAP, e);
        }
    }

    /**
     * Helper class whose <code>equals()</code> method returns
     * <code>true</code> for all strings on this format: SQL061021105830900
     */
    private static class GeneratedId {
        public boolean equals(Object o) {
            return o instanceof String &&
                ((String) o).matches("SQL[0-9]{15}");
        }
        public String toString() {
            return "xxxxGENERATED-IDxxxx";
        }
    }
}
