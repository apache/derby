/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest

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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Locale;
import java.util.StringTokenizer;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the DatabaseMetaData api.
 * <P>
 * For the methods that return a ResultSet to determine the
 * attributes of SQL objects (e.g. getTables) two methods
 * are provided. A non-modify and a modify one.
 * 
 * <BR>
 * The non-modify method tests that the getXXX method call works.
 * This can be used by other tests where issues have been seen
 * with database meta data, such as upgrade and read-only databases.
 * The non-modify means that the test method does not change the database
 * in order to test the return of the getXXX method is correct.
 * <BR>
 * The modify versions test
 * Work in progress.
 * Methods left to test from JDBC 3
 * 
 *  getBestRowIdentifier
 *  getColumnPrivileges
 *  getColumns
 *  getCrossReference
 *  getExportedKeys
 *  getImportedKeys
 *  getIndexInfo
 *  getPrimaryKeys
 *  getProcedureColumns
 *  getProcedures
 *  getTablePrivileges
 *  getTables (modify)
 *  getTypeInfo
 *  <P>
 *  This test is also called from the upgrade tests to test that
 *  metadata continues to work at various points in the upgrade.
 */
public class DatabaseMetaDataTest extends BaseJDBCTestCase {
    /*
    ** Escaped function testing
    */
    private static final String[][] NUMERIC_FUNCTIONS =
    {
        // Section C.1 JDBC 3.0 spec.
        { "ABS", "-25.67" },
        { "ACOS", "0.0707" },
        { "ASIN", "0.997" },
        { "ATAN", "14.10" },
        { "ATAN2", "0.56", "1.2" },
        { "CEILING", "3.45" },
        { "COS", "1.2" },
        { "COT", "3.4" },
        { "DEGREES", "2.1" },
        { "EXP", "2.3" },
        { "FLOOR", "3.22" },
        { "LOG", "34.1" },
        { "LOG10", "18.7" },
        { "MOD", "124", "7" },
        { "PI" },
        { "POWER", "2", "3" },
        { "RADIANS", "54" },
        { "RAND", "17" }, 
        { "ROUND", "345.345", "1" }, 
        { "SIGN", "-34" },
        { "SIN", "0.32" },
        { "SQRT", "6.22" },
        { "TAN", "0.57", },
        { "TRUNCATE", "345.395", "1" }
    };
    
    private static final String[][] TIMEDATE_FUNCTIONS =
    {   
        // Section C.3 JDBC 3.0 spec.
        { "CURDATE" },
        { "CURTIME" },
        { "DAYNAME", "{d '1995-12-19'h}" },
        { "DAYOFMONTH", "{d '1995-12-19'}" },
        { "DAYOFWEEK", "{d '1995-12-19'}" },
        { "DAYOFYEAR", "{d '1995-12-19'}" },
        { "HOUR", "{t '16:13:03'}" },
        { "MINUTE", "{t '16:13:03'}" },
        { "MONTH", "{d '1995-12-19'}" },
        { "MONTHNAME", "{d '1995-12-19'}" },
        { "NOW" },
        { "QUARTER", "{d '1995-12-19'}" },
        { "SECOND", "{t '16:13:03'}" },
        { "TIMESTAMPADD", "SQL_TSI_DAY", "7", "{ts '1995-12-19 12:15:54'}" },
        { "TIMESTAMPDIFF", "SQL_TSI_DAY", "{ts '1995-12-19 12:15:54'}", "{ts '1997-11-02 00:15:23'}" },
        { "WEEK", "{d '1995-12-19'}" },
        { "YEAR", "{d '1995-12-19'}" },
        
    };

    private static final String[][] SYSTEM_FUNCTIONS =
    {   
        // Section C.4 JDBC 3.0 spec.
        { "DATABASE" },
        { "IFNULL", "'this'", "'that'" },
        { "USER"},
        };  
    
    private static final String[][] STRING_FUNCTIONS =
    {   
        // Section C.2 JDBC 3.0 spec.
        { "ASCII" , "'Yellow'" },
        { "CHAR", "65" },
        { "CONCAT", "'hello'", "'there'" },
        { "DIFFERENCE", "'Pires'", "'Piers'" },
        { "INSERT", "'Bill Clinton'", "4", "'William'" },
        { "LCASE", "'Fernando Alonso'" },
        { "LEFT", "'Bonjour'", "3" },
        { "LENGTH", "'four    '" } ,
        { "LOCATE", "'jour'", "'Bonjour'" },
        { "LTRIM", "'   left trim   '"},
        { "REPEAT", "'echo'", "3" },
        { "REPLACE", "'to be or not to be'", "'be'", "'England'" },
        { "RTRIM", "'  right trim   '"},
        { "SOUNDEX", "'Derby'" },
        { "SPACE", "12"},
        { "SUBSTRING", "'Ruby the Rubicon Jeep'", "10", "7", },
        { "UCASE", "'Fernando Alonso'" }
        };
    
    /**
     * Did the test modifiy the database.
     */
    private boolean modifiedDatabase;

    public DatabaseMetaDataTest(String name) {
        super(name);
    }
    
    protected void tearDown() throws Exception
    {
        if (modifiedDatabase)
        {
            Connection conn = getConnection();
            conn.setAutoCommit(false);

            DatabaseMetaData dmd = getDMD();
            for (int i = 0; i < IDS.length; i++)
                JDBC.dropSchema(dmd, getStoredIdentifier(IDS[i]));
  
            commit();
        }
        super.tearDown();
    }
    
    public static Test suite() {
        return TestConfiguration.defaultSuite(DatabaseMetaDataTest.class);
     }
    
    private final DatabaseMetaData getDMD() throws SQLException
    {
        return getConnection().getMetaData();
    }

    /**
     * Test the methods that indicate if a feature
     * is supported or not. Methods start with
     * 'support'. See secton 7.3 in JDBC 3.0 specification.
     * 
     * @throws SQLException 
     *
     */
    public void testDetermineFeatureSupport() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertTrue(dmd.supportsAlterTableWithAddColumn());
        assertTrue(dmd.supportsAlterTableWithDropColumn());
        
        // Bug DERBY-2243 - return value is indicating support
        // level of the SQL engine, so should be consistent.
        if (usingEmbedded())
            assertFalse(dmd.supportsANSI92EntryLevelSQL());
        else
            assertTrue(dmd.supportsANSI92EntryLevelSQL());
              
        assertFalse(dmd.supportsANSI92FullSQL());
        assertFalse(dmd.supportsANSI92IntermediateSQL());
        
        assertTrue(dmd.supportsBatchUpdates());
        
        assertFalse(dmd.supportsCatalogsInDataManipulation());
        assertFalse(dmd.supportsCatalogsInIndexDefinitions());
        assertFalse(dmd.supportsCatalogsInPrivilegeDefinitions());
        assertFalse(dmd.supportsCatalogsInProcedureCalls());
        assertFalse(dmd.supportsCatalogsInTableDefinitions());
        
        assertTrue(dmd.supportsColumnAliasing());
        
        // Bug DERBY-462 should return false.
        assertTrue(dmd.supportsConvert());
        // Simple check since convert is not supported.
        // A comprehensive test should be added when convert
        // is supported, though most likely in a test class
        // specific to convert.
        assertFalse(dmd.supportsConvert(Types.INTEGER, Types.SMALLINT));
        
        assertFalse(dmd.supportsCoreSQLGrammar());
        assertTrue(dmd.supportsCorrelatedSubqueries());
        
        assertTrue(dmd.supportsDataDefinitionAndDataManipulationTransactions());
        assertFalse(dmd.supportsDataManipulationTransactionsOnly());
        assertTrue(dmd.supportsDifferentTableCorrelationNames());
        
        // Bug DERBY-2244, order by with expressions was added by DERBY-134
        assertFalse(dmd.supportsExpressionsInOrderBy());
        
        assertFalse(dmd.supportsExtendedSQLGrammar());
        assertFalse(dmd.supportsFullOuterJoins());
        assertFalse(dmd.supportsGetGeneratedKeys());
        
        assertTrue(dmd.supportsGroupBy());
        assertTrue(dmd.supportsGroupByBeyondSelect());
        assertTrue(dmd.supportsGroupByUnrelated());
        
        assertFalse(dmd.supportsIntegrityEnhancementFacility());
        assertTrue(dmd.supportsLikeEscapeClause());
        assertTrue(dmd.supportsLimitedOuterJoins());
        assertTrue(dmd.supportsMinimumSQLGrammar());
        
        assertFalse(dmd.supportsMixedCaseIdentifiers());
        assertTrue(dmd.supportsMixedCaseQuotedIdentifiers());
        
        assertTrue(dmd.supportsMultipleOpenResults());
        assertTrue(dmd.supportsMultipleResultSets());
        assertTrue(dmd.supportsMultipleTransactions());
        
        assertFalse(dmd.supportsNamedParameters());
        assertTrue(dmd.supportsNonNullableColumns());
        
        // Open cursors are not supported across global
        // (XA) transactions so the driver returns false.
        assertFalse(dmd.supportsOpenCursorsAcrossCommit());
        
        assertFalse(dmd.supportsOpenCursorsAcrossRollback());
        assertTrue(dmd.supportsOpenStatementsAcrossCommit());
        
        assertFalse(dmd.supportsOpenStatementsAcrossRollback());
        
        assertFalse(dmd.supportsOrderByUnrelated());
        
        assertTrue(dmd.supportsOuterJoins());
        
        assertTrue(dmd.supportsPositionedDelete());
        assertTrue(dmd.supportsPositionedUpdate());
        
        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));

        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));

        assertFalse(dmd.supportsResultSetConcurrency(
             ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertFalse(dmd.supportsResultSetConcurrency(
             ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));

        assertTrue(dmd.supportsResultSetHoldability(
                ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertTrue(dmd.supportsResultSetHoldability(
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
        
        assertTrue(dmd.supportsResultSetType(
                ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.supportsResultSetType(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.supportsResultSetType(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        
        assertTrue(dmd.supportsSavepoints());
        assertTrue(dmd.supportsSchemasInDataManipulation());
        assertTrue(dmd.supportsSchemasInIndexDefinitions());
        assertTrue(dmd.supportsSchemasInPrivilegeDefinitions());
        assertTrue(dmd.supportsSchemasInProcedureCalls());
        assertTrue(dmd.supportsSchemasInTableDefinitions());
        assertTrue(dmd.supportsSelectForUpdate());
        
        assertFalse(dmd.supportsStatementPooling());
        assertTrue(dmd.supportsStoredProcedures());
        assertTrue(dmd.supportsSubqueriesInComparisons());
        assertTrue(dmd.supportsSubqueriesInExists());
        assertTrue(dmd.supportsSubqueriesInIns());
        assertTrue(dmd.supportsSubqueriesInQuantifieds());
        assertTrue(dmd.supportsTableCorrelationNames());
        
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_READ_COMMITTED));
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_READ_UNCOMMITTED));
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_REPEATABLE_READ));
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_SERIALIZABLE));
        
        assertTrue(dmd.supportsTransactions());
        assertTrue(dmd.supportsUnion());
        assertTrue(dmd.supportsUnionAll());         
    }
    
    /**
     * Test group of methods provides the limits imposed by a given data source
     *  Methods start with
     * 'getMax'. See section 7.4 in JDBC 3.0 specification.
     * 
     * Note a return of zero from one of these functions means
     * no limit or limit not known.
     * 
     * @throws SQLException 
     *
     */
    public void testDataSourceLimits() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertEquals(0, dmd.getMaxBinaryLiteralLength());
        
        // Catalogs not supported.
        assertEquals(0, dmd.getMaxCatalogNameLength());
        
        assertEquals(0, dmd.getMaxCharLiteralLength());
        
        assertEquals(128, dmd.getMaxColumnNameLength());
        assertEquals(0, dmd.getMaxColumnsInGroupBy());
        assertEquals(0, dmd.getMaxColumnsInIndex());
        assertEquals(0, dmd.getMaxColumnsInOrderBy());
        assertEquals(0, dmd.getMaxColumnsInSelect());
        assertEquals(0, dmd.getMaxColumnsInTable());
        assertEquals(0, dmd.getMaxConnections());
        assertEquals(128, dmd.getMaxCursorNameLength());
        assertEquals(0, dmd.getMaxIndexLength());
        assertEquals(128, dmd.getMaxProcedureNameLength());
        assertEquals(0, dmd.getMaxRowSize());
        assertEquals(128, dmd.getMaxSchemaNameLength());
        assertEquals(0, dmd.getMaxStatementLength());
        assertEquals(0, dmd.getMaxStatements());
        assertEquals(128, dmd.getMaxTableNameLength());
        assertEquals(0, dmd.getMaxTablesInSelect());
        assertEquals(30, dmd.getMaxUserNameLength());
    }
    
    public void testMiscellaneous() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertTrue(dmd.allProceduresAreCallable());
        assertTrue(dmd.allTablesAreSelectable());
        assertFalse(dmd.dataDefinitionCausesTransactionCommit());
        assertFalse(dmd.dataDefinitionIgnoredInTransactions());
        
        assertFalse(dmd.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        
        assertTrue(dmd.doesMaxRowSizeIncludeBlobs());
        
        // Catalogs not supported, so empty string returned for separator.
        assertEquals("", dmd.getCatalogSeparator());
        assertEquals("CATALOG", dmd.getCatalogTerm());
        
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
                dmd.getDefaultTransactionIsolation());
        
        assertEquals("", dmd.getExtraNameCharacters());
        assertEquals("\"", dmd.getIdentifierQuoteString());
        
        assertEquals("PROCEDURE", dmd.getProcedureTerm());
        
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT,
                dmd.getResultSetHoldability());
        
        assertEquals("SCHEMA", dmd.getSchemaTerm());
        
        assertEquals("", dmd.getSearchStringEscape());
        
        assertEquals(DatabaseMetaData.sqlStateSQL99,
                dmd.getSQLStateType());
        
        assertFalse(dmd.isCatalogAtStart()); 
        
        assertFalse(dmd.locatorsUpdateCopy());
        
        assertTrue(dmd.usesLocalFilePerTable());
        assertTrue(dmd.usesLocalFiles());
    }
    
    /**
     * Methods that describe the version of the
     * driver and database.
     */
    public void testVersionInfo() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        int databaseMajor = dmd.getDatabaseMajorVersion();
        int databaseMinor = dmd.getDatabaseMinorVersion();
        
        int driverMajor = dmd.getDriverMajorVersion();
        int driverMinor = dmd.getDriverMinorVersion();
        
        String databaseVersion = dmd.getDatabaseProductVersion();
        String driverVersion = dmd.getDriverVersion();
        
        if (usingEmbedded())
        {
            // Database *is* the driver.
            
            assertEquals("Embedded Major version ",
                    databaseMajor, driverMajor);
            assertEquals("Embedded Minor version ",
                    databaseMinor, driverMinor);
            
            assertEquals("Embedded version",
                    databaseVersion, driverVersion);
        }
        
        assertEquals("Apache Derby", dmd.getDatabaseProductName());
        
        String driverName = dmd.getDriverName();
        if (usingEmbedded())
        {
            assertEquals("Apache Derby Embedded JDBC Driver",
                    driverName);
        }
        else if (usingDerbyNetClient())
        {
            assertEquals("Apache Derby Network Client JDBC Driver",
                    driverName);
        }

        int jdbcMajor = dmd.getJDBCMajorVersion();
        int jdbcMinor = dmd.getJDBCMinorVersion();
        
        int expectedJDBCMajor = -1;
        if (JDBC.vmSupportsJDBC4())
        {
            expectedJDBCMajor = 4;
        }
        else if (JDBC.vmSupportsJDBC3())
        {
            expectedJDBCMajor = 3;
        }
        else if (JDBC.vmSupportsJSR169())
        {
            // Not sure what is the correct output for JSR 169
            expectedJDBCMajor = -1;
        }
         
        if (expectedJDBCMajor != -1)
        {
            assertEquals("JDBC Major version",
                    expectedJDBCMajor, jdbcMajor);
            assertEquals("JDBC Minor version", 0, jdbcMinor);
        }
    }
    
    /**
     * getURL() method. Note that this method
     * is the only JDBC 3 DatabaseMetaData method
     * that is dropped in JSR169.
     */
    public void testGetURL() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        String url;
        try {
            url = dmd.getURL();
        } catch (NoSuchMethodError e) {
            // JSR 169 - method must not be there!
            assertTrue("getURL not supported", JDBC.vmSupportsJSR169());
            assertFalse("getURL not supported", JDBC.vmSupportsJDBC2());
            return;
        }
        
        assertFalse("getURL is supported!", JDBC.vmSupportsJSR169());
        assertTrue("getURL is supported!", JDBC.vmSupportsJDBC2());
        
        assertEquals("getURL match",
                getTestConfiguration().getJDBCUrl(),
                url);              
    }
    
    /**
     * Derby stores unquoted identifiers as upper
     * case and quoted ones as mixed case.
     * They are always compared case sensitive.
     */
    public void testIdentifierStorage() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertFalse(dmd.storesLowerCaseIdentifiers());
        assertFalse(dmd.storesLowerCaseQuotedIdentifiers());
        assertFalse(dmd.storesMixedCaseIdentifiers());
        
        assertTrue(dmd.storesMixedCaseQuotedIdentifiers());
        assertTrue(dmd.storesUpperCaseIdentifiers());
        assertFalse(dmd.storesUpperCaseQuotedIdentifiers());
    }
    
    /**
     * methods that return information about handling NULL.
     */
    public void testNullInfo() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertTrue(dmd.nullPlusNonNullIsNull());
        assertFalse(dmd.nullsAreSortedAtEnd());
        assertFalse(dmd.nullsAreSortedAtStart());
        assertTrue(dmd.nullsAreSortedHigh());
        assertFalse(dmd.nullsAreSortedLow());
    }
    
    /**
     * Method getSQLKeywords, returns list of SQL keywords
     * that are not defined by SQL92.
     */
    public void testSQLKeywords() throws SQLException
    {
        String keywords = getDMD().getSQLKeywords();
        
        assertNotNull(keywords);
        
        //TODO: more testing but not sure what!     
    }
    
    /**
     * Methods that return information specific to
     * the current connection.
     */
    public void testConnectionSpecific() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertSame(getConnection(), dmd.getConnection());
        assertEquals(getTestConfiguration().getUserName(),
                dmd.getUserName());
        assertEquals(getConnection().isReadOnly(), dmd.isReadOnly());
    }
    
    /*
    ** DatabaseMetaData calls that return ResultSets.
    */
    
    /**
     * Test methods that describe attributes of SQL Objects
     * that are not supported by derby. In each case the
     * metadata should return an empty ResultSet of the
     * correct shape.
     * TODO: types and column names of the ResultSets
     */
    public void testUnimplementedSQLObjectAttributes() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        ResultSet rs;
        
        rs = dmd.getAttributes(null,null,null,null);
        assertMetaDataResultSet(rs, null, null);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getCatalogs();
        checkCatalogsShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getSuperTables(null,null,null);
        assertMetaDataResultSet(rs, null, null);
        JDBC.assertEmpty(rs);

        rs = dmd.getSuperTypes(null,null,null);
        assertMetaDataResultSet(rs, null, null);
        JDBC.assertEmpty(rs);

        rs = dmd.getUDTs(null,null,null,null);
        assertMetaDataResultSet(rs, null, null);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getVersionColumns(null,null, "No_such_table");
        checkVersionColumnsShape(rs);
        JDBC.assertEmpty(rs);
    }
    
    /**
     * Six cominations of valid identifiers with mixed
     * case, to see how the various pattern matching
     * and returned values handle them.
     * This test only creates objects in these schemas.
     */
    private static final String[] IDS =
    {
            "one_dmd_test",
            "TWO_dmd_test",
            "ThReE_dmd_test",
            "\"four_dmd_test\"",
            "\"FIVE_dmd_test\"",
            "\"sIx_dmd_test\"" 
    };
    
    /**
     * All the builtin schemas.
     */
    private static final String[] BUILTIN_SCHEMAS = {
            "APP", "NULLID", "SQLJ", "SYS", "SYSCAT", "SYSCS_DIAG",
            "SYSCS_UTIL", "SYSFUN", "SYSIBM", "SYSPROC", "SYSSTAT"};
    
    public static String getStoredIdentifier(String sqlIdentifier)
    {
        if (sqlIdentifier.charAt(0) == '"')
            return sqlIdentifier.substring(1, sqlIdentifier.length() - 1);
        else
            return sqlIdentifier.toUpperCase(Locale.ENGLISH);       
    }

    /**
     * Test getSchemas() without modifying the database.
     * 
     * @throws SQLException
     */
    public void testGetSchemasNoModify() throws SQLException {
        
        DatabaseMetaData dmd = getDMD();
         
        ResultSet rs = dmd.getSchemas();
        checkSchemas(rs, new String[0]);
    }
    
    /**
     * Test getSchemas().
     * 
     * @throws SQLException
     */
    public void testGetSchemasModify() throws SQLException {
        // Set to cleanup on teardown.
        modifiedDatabase = true;
        
        DatabaseMetaData dmd = getDMD();
        
        Statement s = createStatement();
        for (int i = 0; i < IDS.length; i++)
           s.executeUpdate("CREATE SCHEMA " + IDS[i]);
        s.close();
                 
        ResultSet rs = dmd.getSchemas();
        checkSchemas(rs, IDS);
    }
    /**
     * Check the returned information from a getSchemas().
     * The passed in String[] expected is a list of the
     * schemas expected to be present in the returned
     * set. The returned set may contain additional
     * schemas which will be ignored, thus this test
     * can be used regardless of the database state.
     * The builtin schemas are automatically checked
     * and must not be part of the passed in list.
     */
    public static void checkSchemas(ResultSet rs,
            String[] userExpected) throws SQLException
    {
        checkSchemasShape(rs);
        
        // Add in the system schemas
        String[] expected =
            new String[BUILTIN_SCHEMAS.length + userExpected.length];
        
        System.arraycopy(BUILTIN_SCHEMAS, 0,
                expected, 0, BUILTIN_SCHEMAS.length);
        System.arraycopy(userExpected, 0,
                expected, BUILTIN_SCHEMAS.length, userExpected.length);
                
        // Remove any quotes from user schemas and upper case
        // those without quotes.
        for (int i = BUILTIN_SCHEMAS.length; i < expected.length; i++)
        {          
            expected[i] = getStoredIdentifier(expected[i]);
        }
              
        //output is ordered by TABLE_SCHEM
        Arrays.sort(expected);
                   
        int nextMatch = 0;
 
        while (rs.next()) {
            String schema = rs.getString("TABLE_SCHEM");
            assertNotNull(schema);
            
            // Catalogs not supported
            assertNull(rs.getString("TABLE_CATALOG"));
                        
            if (nextMatch < expected.length)
            {
                if (expected[nextMatch].equals(schema))
                    nextMatch++;
            }
        }
        rs.close();
        assertEquals("Schemas missing ", expected.length, nextMatch);
    }

    /**
     * Check the shape of the ResultSet from any
     * getSchemas call.
     */
    private static void checkSchemasShape(ResultSet rs) throws SQLException
    {
        assertMetaDataResultSet(rs,
          new String[] {
          "TABLE_SCHEM", "TABLE_CATALOG"
         },
         new int[] {
          Types.VARCHAR, Types.VARCHAR
         }
        );        
    }
    
    /**
     * Test getTables() without modifying the database.
     * 
     * @throws SQLException
     */
    public void testGetTablesNoModify() throws SQLException {
        
        DatabaseMetaData dmd = getDMD();
        
        ResultSet rs;
        
        rs = dmd.getTables(null, null, null, null);
        checkTablesShape(rs);
        int allTableCount = JDBC.assertDrainResults(rs);
        assertTrue("getTables() on all was empty!", allTableCount > 0);
        
        rs = dmd.getTables("%", "%", "%", null);
        checkTablesShape(rs);
        assertEquals("Different counts from getTables",
                allTableCount, JDBC.assertDrainResults(rs));
        
        rs = dmd.getTables(null, "NO_such_schema", null, null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getTables(null, "SQLJ", null, null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getTables(null, "SQLJ", "%", null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getTables(null, "SYS", "No_such_table", null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        String[] userTableOnly = new String[] {"TABLE"};

        // no user tables in SYS
        rs = dmd.getTables(null, "SYS", null, userTableOnly);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getTables(null, "SYS", "%", userTableOnly);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        String[] systemTableOnly = new String[] {"SYSTEM_TABLE"};
        
        rs = dmd.getTables(null, "SYS", null, systemTableOnly);
        checkTablesShape(rs);
        int systemTableCount = JDBC.assertDrainResults(rs);
        assertTrue("getTables() on system tables was empty!", systemTableCount > 0);
        
        rs = dmd.getTables(null, "SYS", "%", systemTableOnly);
        checkTablesShape(rs);
        assertEquals(systemTableCount, JDBC.assertDrainResults(rs));

        String[] viewOnly = new String[] {"VIEW"};
        rs = dmd.getTables(null, "SYS", null, viewOnly);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getTables(null, "SYS", "%", viewOnly);
        JDBC.assertEmpty(rs);
        
        String[] allTables = {"SYNONYM","SYSTEM TABLE","TABLE","VIEW"};
        rs = dmd.getTables(null, null, null, allTables);
        checkTablesShape(rs);
        assertEquals("Different counts from getTables",
                allTableCount, JDBC.assertDrainResults(rs));
        rs = dmd.getTables("%", "%", "%", allTables);
        checkTablesShape(rs);
        assertEquals("Different counts from getTables",
                allTableCount, JDBC.assertDrainResults(rs));
        
    }
    
    /**
     * Test getTableTypes()
     */
    public void testTableTypes() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        ResultSet rs = dmd.getTableTypes();
        assertMetaDataResultSet(rs,
                new String[] {
                "TABLE_TYPE"
               },
               new int[] {
                Types.VARCHAR
               }
              );
        
        JDBC.assertFullResultSet(rs, new String[][]
          {
            {"SYNONYM"},{"SYSTEM TABLE"},{"TABLE"},{"VIEW"},               
          }, true);
        rs.close();
    }
    
    /**
     * Test getTypeInfo
     * @throws SQLException 
     */
    public void testGetTypeInfo() throws SQLException
    {
        // Client returns BOOLEAN type from the engine as SMALLINT
        int BOOLEAN = Types.BOOLEAN;      
        if (usingDerbyNetClient())
            BOOLEAN = Types.SMALLINT;
        
        ResultSet rs = getDMD().getTypeInfo();
        assertMetaDataResultSet(rs,
          new String[] {
            "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
            "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE",
            
            "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
            "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
            
            "MINIMUM_SCALE", "MAXIMUM_SCALE",
            "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
            
            "NUM_PREC_RADIX"          
          },
          new int[] {
            Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, BOOLEAN,
            
            Types.SMALLINT, BOOLEAN, BOOLEAN,
            BOOLEAN, Types.VARCHAR,
            
            Types.SMALLINT, Types.SMALLINT,
            Types.INTEGER, Types.INTEGER,
            
            Types.INTEGER
          }
        );
        
        int[] supportedTypes = new int[] {
          Types.BIGINT, Types.BINARY, Types.BLOB,
          Types.CHAR, Types.CHAR, Types.CLOB, Types.DATE,
          Types.DECIMAL, Types.DOUBLE, Types.FLOAT,
          Types.INTEGER, Types.LONGVARBINARY, Types.LONGVARCHAR, Types.LONGVARCHAR,
          Types.NUMERIC, Types.REAL, Types.SMALLINT,
          Types.TIME, Types.TIMESTAMP,  Types.VARBINARY,
          Types.VARCHAR, Types.VARCHAR 
        };
        
        // Rows are returned from getTypeInfo in order of
        // "DATA_TYPE" (which is a constant from java.sql.Types)
        Arrays.sort(supportedTypes);
        
        int offset = 0;
        while (rs.next()) {
            // TYPE_NAME (column 1)
            String typeName = rs.getString("TYPE_NAME");
            assertNotNull(typeName);
            
            // DATA_TYPE (column 2)
            int type = rs.getInt("DATA_TYPE");
            assertFalse(rs.wasNull());
            if (supportedTypes[offset] != type)
            {
                fail("Unexpected type " + typeName);
            }
            else
            {
                offset++;
            }
            
            // National types not supported, ignore them
            // DERBY-2258
            if (typeName.indexOf("NATIONAL") != -1)
                continue;
            if (typeName.indexOf("NVARCHAR") != -1)
                continue;
            
            // PRECISION (column 3)
            int precision = -1;
            switch (type)
            {
            case Types.BINARY:
            case Types.CHAR:
                precision = 254;
                break;
            case Types.BLOB:
            case Types.CLOB:
                precision = Integer.MAX_VALUE;
                break;
            
            case Types.DATE:
                precision = 10;
                break;
            case Types.TIME:
                precision = 8;
                break;
            case Types.TIMESTAMP:
                precision = 26;
                break;
                                
            case Types.DECIMAL:
            case Types.NUMERIC:
                precision = 31;
                break;
            case Types.DOUBLE:
            case Types.FLOAT:
                precision = 52;
                break;
            case Types.REAL:
                precision = 23;
                break;
                
            case Types.BIGINT:
                precision = 19;
                break;              
            case Types.INTEGER:
                precision = 10;
                break;
            case Types.SMALLINT:
                precision = 5;
                break;
                
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
                precision = 32700;
                break;
                        
            case Types.VARBINARY:
                precision = 32762; // BUG DERBY-2260
                break;

            case Types.VARCHAR:
                precision = 32672;
                break;
            }
            assertEquals("PRECISION " + typeName,
                    precision, rs.getInt("PRECISION"));
            assertFalse(rs.wasNull());
            
            // LITERAL_PREFIX (column 4)
            // LITERAL_SUFFIX (column 5)
            
            // CREATE_PARAMS (column 6)
            String createParams;
            switch (type)
            {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.BLOB:
            case Types.CLOB:
            case Types.BINARY:
            case Types.VARBINARY:
                createParams = "length";
                break;
                
            case Types.DECIMAL:
            case Types.NUMERIC:
                createParams = "precision,scale";
                break;
                
            case Types.FLOAT:
                createParams = "precision";
                break;
                
            default:
                createParams = null;
                break;
            }
            assertEquals("CREATE_PARAMS " + typeName,
                    createParams, rs.getString("CREATE_PARAMS"));
                     
            
            // NULLABLE (column 7) - all types are nullable in Derby
            assertEquals("NULLABLE " + typeName,
                    DatabaseMetaData.typeNullable, rs.getInt("NULLABLE"));
            assertFalse(rs.wasNull());
            
            // CASE_SENSITIVE (column 8)
            
            // SEARCHABLE (column 9) - most types searchable
            {
            int searchable;
            switch (type)
            {
            case Types.LONGVARBINARY:
                searchable = DatabaseMetaData.typePredBasic; // BUG DERBY-2259
                break;
            case Types.LONGVARCHAR:
                searchable = DatabaseMetaData.typeSearchable; // BUG DERBY-2259
                break;
                
            case Types.BLOB:
            case Types.CLOB:
                searchable = // DatabaseMetaData.typePredNone;
                    DatabaseMetaData.typePredChar; // BUG DERBY-2259
                break;
            case Types.CHAR:
            case Types.VARCHAR:
                searchable = DatabaseMetaData.typeSearchable;
                break;
            default:
                searchable = DatabaseMetaData.typePredBasic;
                break;  
            }
            assertEquals("SEARCHABLE " + typeName,
                    searchable, rs.getInt("SEARCHABLE"));
            }
            
            // UNSIGNED_ATTRIBUTE (column 10)
            //assertFalse("UNSIGNED_ATTRIBUTE " + typeName,
            //        rs.getBoolean("UNSIGNED_ATTRIBUTE"));
            
            
            // FIXED_PREC_SCALE (column 11)
            boolean fixedScale = type == Types.DECIMAL || type == Types.NUMERIC;
            assertEquals("FIXED_PREC_SCALE " + typeName,
                    fixedScale, rs.getBoolean("FIXED_PREC_SCALE"));
            assertFalse(rs.wasNull());
            
            // AUTO_INCREMENT (column 12)
            boolean autoIncrement;
            switch (type)
            {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
                autoIncrement = true;
                break;
            default:
                autoIncrement = false;
                break;
            }
            assertEquals("AUTO_INCREMENT " + typeName,
                    autoIncrement, rs.getBoolean("AUTO_INCREMENT"));
            
            // LOCAL_TYPE_NAME (column 13) always the same as TYPE_NAME
            assertEquals("LOCAL_TYPE_NAME " + typeName,
                    typeName, rs.getString("LOCAL_TYPE_NAME"));
            
            
            int maxScale;
            boolean hasScale = true;
            switch (type)
            {
            case Types.DECIMAL:
            case Types.NUMERIC:
                maxScale = 32767; // 31; BUG DERBY-2262
                break;
            case Types.TIMESTAMP:
                maxScale = 6;
                break;
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.DATE:
            case Types.TIME:
                maxScale = 0;
                break;
            default:
                maxScale = 0;
                hasScale = false;
                break;
            }
            
            // MINIMUM_SCALE (column 14)
            assertEquals("MINIMUM_SCALE " + typeName,
                    0, rs.getInt("MINIMUM_SCALE"));
            assertEquals("MINIMUM_SCALE (wasNull) " + typeName,
                    !hasScale, rs.wasNull());
            
            // MAXIMUM_SCALE (column 15)
            assertEquals("MAXIMUM_SCALE " + typeName,
                    maxScale, rs.getInt("MAXIMUM_SCALE"));            
            assertEquals("MAXIMUM_SCALE (wasNull)" + typeName,
                    !hasScale, rs.wasNull());

            
            // SQL_DATA_TYPE (column 16) - Unused
            assertEquals("SQL_DATA_TYPE " + typeName,
                    0, rs.getInt("SQL_DATA_TYPE"));
            assertTrue(rs.wasNull());
            
            // SQL_DATETIME_SUB (column 17) - Unused
            assertEquals("SQL_DATETIME_SUB " + typeName,
                    0, rs.getInt("SQL_DATETIME_SUB"));
            assertTrue(rs.wasNull());

            // NUM_PREC_RADIX (column 18)
            
        }
        
        rs.close();
    }
    
    /**
     * Check the shape of the ResultSet from any getTables call.
     */
    private void checkTablesShape(ResultSet rs) throws SQLException
    {
        assertMetaDataResultSet(rs,
          new String[] {
          "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
          "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
          "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
         },
         new int[] {
          Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
          Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
          Types.VARCHAR, Types.VARCHAR
         }
        );        
    }
    
    /**
     * Check the shape of the ResultSet from any getCatlogs call.
     */
    private void checkCatalogsShape(ResultSet rs) throws SQLException
    {
        assertMetaDataResultSet(rs,
          new String[] {
          "TABLE_CAT"
         },
         new int[] {
          Types.CHAR
         }
        );        
    }
    
    /**
     * Check the shape of the ResultSet from any
     * getVersionColumns call.
     */
    private static void checkVersionColumnsShape(ResultSet rs) throws SQLException
    {
        assertMetaDataResultSet(rs,
          new String[] {
          "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
          "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"
         },
         new int[] {
          Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
          Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.SMALLINT
         }
        );        
    }
    
    public static void assertMetaDataResultSet(ResultSet rs,
            String[] columnNames, int[] columnTypes) throws SQLException
    {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        
        if (columnNames != null)
            JDBC.assertColumnNames(rs, columnNames);
        if (columnTypes != null)
            JDBC.assertColumnTypes(rs, columnTypes);
    }
    
    /*
    ** Set of escaped functions.
    */
    
    /**
     * JDBC escaped numeric functions - JDBC 3.0 C.1
     * @throws SQLException
     */
    public void testNumericFunctions() throws SQLException
    {
        escapedFunctions(NUMERIC_FUNCTIONS,
                getDMD().getNumericFunctions());
    }
    /**
     * JDBC escaped string functions - JDBC 3.0 C.2
     * @throws SQLException
     */
    public void testStringFunctions() throws SQLException
    {
        escapedFunctions(STRING_FUNCTIONS,
                getDMD().getStringFunctions());
    }    
    /**
     * JDBC escaped date time functions - JDBC 3.0 C.3
     * @throws SQLException
     */
    public void testTimeDataFunctions() throws SQLException
    {
        escapedFunctions(TIMEDATE_FUNCTIONS,
                getDMD().getTimeDateFunctions());
    }    
    /**
     * JDBC escaped system functions - JDBC 3.0 C.4
     * @throws SQLException
     */
    public void testSystemFunctions() throws SQLException
    {
        escapedFunctions(SYSTEM_FUNCTIONS,
                getDMD().getSystemFunctions());
    }           
    
    /**
     * Check that the list of escaped functions provided by
     * the driver is a strict subet of the specified set,
     * the list does not contain duplicates, all the functions
     * listed can be executed and that if a function is not
     * in the list but is specified it cannot be executed.
     */
    private void escapedFunctions(String[][] specList, String metaDataList)
    throws SQLException
    {
        boolean[] seenFunction = new boolean[specList.length];
        
        StringTokenizer st = new StringTokenizer(metaDataList, ",");
        while (st.hasMoreTokens())
        {
            String function = st.nextToken();
            
            // find this function in the list
            boolean isSpecFunction = false;
            for (int f = 0; f < specList.length; f++)
            {
                String[] specDetails = specList[f];
                if (function.equals(specDetails[0]))
                {
                    // Matched spec.
                    if (seenFunction[f])
                        fail("Function in list twice: " + function);
                    seenFunction[f] = true;
                    isSpecFunction = true;
                    executeEscaped(specDetails);
                    break;
                }
            }
            
            if (!isSpecFunction)
            {
               fail("Non-JDBC spec function in list: " + function);
            }
        }
        
        // Now see if any speced functions are not in the metadata list
        for (int f = 0; f < specList.length; f++)
        {
            if (seenFunction[f])
                continue;
            String[] specDetails = specList[f];
            
            // bug DERBY-723 CHAR maps to wrong function
            if ("CHAR".equals(specDetails[0]))
                continue;
            try {
                executeEscaped(specDetails);
                fail("function works but not declared in list: " + specDetails[0]);
            } catch (SQLException e) {
                assertSQLState("42X01", e);
            }
        }
    }
    
    /**
     * Test we can execute a function listed as a supported
     * JDBC escaped function. We don't care about the actual
     * return value, that should be tested elsewhere in
     * the specific test of a function.
     */
    private void executeEscaped(String[] specDetails)
        throws SQLException
    {
        
        String sql = "VALUES { fn " + specDetails[0] + "(";
        
        for (int p = 0; p < specDetails.length - 1; p++)
        {
            if (p != 0)
                sql = sql + ", ";
            
            sql = sql + specDetails[p + 1];
        }
        
        sql = sql + ") }";       
        
        PreparedStatement ps = prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        JDBC.assertDrainResults(rs);
        rs.close();
        ps.close();
    }
}
