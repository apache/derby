/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestDbMetaData

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

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of database metadata for new methods in JDBC 40.
 */
public class TestDbMetaData extends BaseJDBCTestCase {

    private static  final   Integer FUNCTION_NO_TABLE_VALUE = DatabaseMetaData.functionNoTable;
    
    private DatabaseMetaData meta;

    public TestDbMetaData(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1989
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        s.execute("CREATE FUNCTION DUMMY5 ( X BOOLEAN ) "+
                  "RETURNS BOOLEAN PARAMETER STYLE JAVA NO SQL LANGUAGE "+
                  "JAVA EXTERNAL NAME 'java.some.func'");
    }

    private static Test baseSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite testSuite = new BaseTestSuite(name);
        testSuite.addTestSuite(TestDbMetaData.class);
        return new CleanDatabaseTestSetup(testSuite) {
                protected void decorateSQL(Statement s) throws SQLException {
                    createFunctions(s);
                }
            };
    }

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("TestDbMetaData suite");
//IC see: https://issues.apache.org/jira/browse/DERBY-2023
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3422
        if (usingEmbedded()) {
            assertTrue(meta.autoCommitFailureClosesAllResultSets());
        } else {
            assertFalse(meta.autoCommitFailureClosesAllResultSets());
        }
    }

    public void testGetClientInfoProperties() throws SQLException {
        ResultSet rs = meta.getClientInfoProperties();
        JDBC.assertDatabaseMetaDataColumns(rs, null, new String[] {
            "NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION" });
        JDBC.assertEmpty(rs);
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
        int[] ColTypes = new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.VARCHAR };
        JDBC.assertDatabaseMetaDataColumns(rs, ColTypes, new String[] {
            "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
//IC see: https://issues.apache.org/jira/browse/DERBY-2983
            "FUNCTION_TYPE", "SPECIFIC_NAME" });
    }
    
    private static final JDBC.GeneratedId GENERIC_NAME = new JDBC.GeneratedId();

    /** Expected rows from getFunctions() when all functions match. */
    private static final Object[][] ALL_FUNCTIONS = {
        { null, "APP", "DUMMY1", "java.some.func", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "APP", "DUMMY2", "java.some.func", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "APP", "DUMMY3", "java.some.func", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "APP", "DUMMY4", "java.some.func", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        { null, "APP", "DUMMY5", "java.some.func", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_CHECK_TABLE",
          "org.apache.derby.catalog.SystemProcedures.SYSCS_CHECK_TABLE",
          FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_DATABASE_NAME",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_DATABASE_NAME", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_DATABASE_PROPERTY",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_DATABASE_PROPERTY", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_RUNTIMESTATISTICS",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_RUNTIMESTATISTICS", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_USER_ACCESS",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_USER_ACCESS", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_XPLAIN_MODE",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_XPLAIN_MODE", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_GET_XPLAIN_SCHEMA",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_GET_XPLAIN_SCHEMA", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_PEEK_AT_IDENTITY",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_PEEK_AT_IDENTITY", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSCS_UTIL", "SYSCS_PEEK_AT_SEQUENCE",
          "org.apache.derby.catalog.SystemProcedures." +
          "SYSCS_PEEK_AT_SEQUENCE", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "BLOBCREATELOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBCREATELOCATOR", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETBYTES",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETBYTES", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETLENGTH",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETLENGTH", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETPOSITIONFROMBYTES",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETPOSITIONFROMBYTES", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "BLOBGETPOSITIONFROMLOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "BLOBGETPOSITIONFROMLOCATOR", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "CLOBCREATELOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBCREATELOCATOR", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETLENGTH",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETLENGTH", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETPOSITIONFROMLOCATOR",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETPOSITIONFROMLOCATOR", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETPOSITIONFROMSTRING",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETPOSITIONFROMSTRING", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },
        { null, "SYSIBM", "CLOBGETSUBSTRING",
          "org.apache.derby.impl.jdbc.LOBStoredProcedure." +
          "CLOBGETSUBSTRING", FUNCTION_NO_TABLE_VALUE, GENERIC_NAME },

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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2735
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2744
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        JDBC.assertEmpty(rs);
    }

    /** Check that the column names are as expected from
     * getFunctionColumns(). */
    private void assertGetFunctionColumnsRs(ResultSet rs) throws SQLException {
        JDBC.assertDatabaseMetaDataColumns(rs, null, new String[] {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            { null, "APP", "DUMMY1", "", 4, 5,
              "SMALLINT", 5, 2, 0,
              10, 1, null, null, 0,
              "YES", GENERIC_NAME, 1, -1 },
            { null, "APP", "DUMMY1", "X", 1, 5,
              "SMALLINT", 5, 2, 0,
              10, 1, null, null, 1,
              "YES", GENERIC_NAME, 1, 0 },
            { null, "APP", "DUMMY2", "", 4, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 0,
              "YES", GENERIC_NAME, 2, -1 },
            { null, "APP", "DUMMY2", "X", 1, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 1,
              "YES", GENERIC_NAME, 2, 0 },
            { null, "APP", "DUMMY2", "Y", 1, 5,
              "SMALLINT", 5, 2, 0,
              10, 1, null, null, 2,
              "YES", GENERIC_NAME, 2, 1 },
            { null, "APP", "DUMMY3", "", 4, 12,
              "VARCHAR", 16, 32, null, null,
              1, null, 32, 0, "YES",
              GENERIC_NAME, 2, -1 },
            { null, "APP", "DUMMY3", "X", 1, 12,
              "VARCHAR", 16, 32, null, null,
              1, null, 32, 1, "YES",
              GENERIC_NAME, 2, 0 },
            { null, "APP", "DUMMY3", "Y", 1, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 2,
              "YES", GENERIC_NAME, 2, 1 },
            { null, "APP", "DUMMY4", "", 4, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 0,
              "YES", GENERIC_NAME, 2, -1 },
            { null, "APP", "DUMMY4", "X", 1, 12,
              "VARCHAR", 128, 256,
              null, null, 1, null, 256, 1, "YES",
              GENERIC_NAME,
              2, 0 },
            { null, "APP", "DUMMY4", "Y", 1, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 2,
              "YES", GENERIC_NAME, 2, 1 },
            { null, "APP", "DUMMY5", "", 4, 16,
              "BOOLEAN", 1, 1, null,
              null, 1, null, null, 0,
              "YES", GENERIC_NAME, 1, -1 },
            { null, "APP", "DUMMY5", "X", 1, 16,
              "BOOLEAN", 1, 1, null,
              null, 1, null, null, 1,
              "YES", GENERIC_NAME, 1, 0 },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    public void testGetFunctionColumnsForDummyFunctions() throws SQLException {
		// Dump return value for all DUMMY functions
        ResultSet rs = meta.getFunctionColumns(null, null, "DUMMY%", "");
        assertGetFunctionColumnsRs(rs);
        Object[][] expectedRows = {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            { null, "APP", "DUMMY1", "", 4, 5,
              "SMALLINT", 5, 2, 0,
              10, 1, null, null, 0,
              "YES", GENERIC_NAME, 1, -1 },
            { null, "APP", "DUMMY2", "", 4, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 0,
              "YES", GENERIC_NAME, 2, -1 },
            { null, "APP", "DUMMY3", "", 4, 12,
              "VARCHAR", 16, 32,
              null, null, 1, null, 32, 0, "YES",
              GENERIC_NAME,
              2, -1 },
            { null, "APP", "DUMMY4", "", 4, 4,
              "INTEGER", 10, 4, 0,
              10, 1, null, null, 0,
              "YES", GENERIC_NAME, 2, -1 },
            { null, "APP", "DUMMY5", "", 4, 16,
              "BOOLEAN", 1, 1, null,
              null, 1, null, null, 0,
              "YES", GENERIC_NAME, 1, -1 },
        };
        JDBC.assertFullResultSet(rs, expectedRows, false);
    }

    /** Check that the column names are as expected from getSchemas(). */
    private void assertGetSchemasRs(ResultSet rs) throws SQLException {
        JDBC.assertDatabaseMetaDataColumns(rs, null, new String[] {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2744
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2744
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        JDBC.assertEmpty(rs);
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

//IC see: https://issues.apache.org/jira/browse/DERBY-1989
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

        // IMPORTANT: use auto-commit
        getConnection().setAutoCommit(true);
//IC see: https://issues.apache.org/jira/browse/DERBY-3422

        ResultSet[] rss = new ResultSet[2];
        // Use different statements so that both result sets are kept open
        rss[0] = createStatement().executeQuery("VALUES 1, 2, 3, 4");
        rss[1] = createStatement().executeQuery(
                "SELECT * FROM SYSIBM.SYSDUMMY1");

        // We want to test holdable result sets
        for (ResultSet rs : rss) {
            assertEquals("ResultSet should be holdable",
                         ResultSet.HOLD_CURSORS_OVER_COMMIT,
                         rs.getHoldability());
        }

        try {
            String query =
                "SELECT dummy, nonexistent, phony FROM imaginarytable34521";
            // Create a new statement so we don't close any of the open result
            // sets by re-executing a statement.
            createStatement().execute(query);
            fail("Query didn't fail.");
        } catch (SQLException e) {
            // should fail, but we don't care how
        }

        int closedResultSets = 0;
        for (ResultSet rs : rss) {
            // check if an operation fails with "ResultSet is closed"
            try {
                rs.next();
                // OK, didn't fail, ResultSet wasn't closed
            } catch (SQLException sqle) {
                assertSQLState("XCL16", sqle);
                // DERBY-4767, sample verification test for operation in XCL16 message.
                assertTrue(sqle.getMessage().indexOf("next") > 0);
                // OK, ResultSet is closed, increase counter
                closedResultSets++;
            }
        }

        boolean allResultSetsWereClosed = (closedResultSets == rss.length);

        assertEquals("autoCommitFailureClosesAllResultSets() returned value " +
                     "which doesn't match actual behaviour.",
                     allResultSetsWereClosed,
                     meta.autoCommitFailureClosesAllResultSets());

        for (ResultSet rs : rss) {
            rs.close();
        }
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

}
