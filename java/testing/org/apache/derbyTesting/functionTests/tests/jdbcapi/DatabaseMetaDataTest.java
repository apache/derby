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
            CleanDatabaseTestSetup.cleanDatabase(conn);
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
        
        rs = dmd.getVersionColumns(null,null,
                usingDerbyNetClient() ? "%" : null);
        checkVersionColumnsShape(rs);
        JDBC.assertEmpty(rs);
    }
    
    /**
     * Six cominations of valid identifiers with mixed
     * case, to see how the various pattern matching
     * and returned values handle them.
     */
    public final String[] IDS =
    {
            "one",
            "TWO",
            "ThReE",
            "\"four\"",
            "\"FIVE\"",
            "\"sIx\"" 
    };
    
    /**
     * Test getSchemas() without modifying the database.
     * 
     * @throws SQLException
     */
    public void testGetSchemasNoModify() throws SQLException {
        
        DatabaseMetaData dmd = getDMD();
         
        ResultSet rs = dmd.getSchemas();
        checkSchemasShape(rs);
        int schemaCount = JDBC.assertDrainResults(rs);
        // There are 11 builtin schemas including APP
        assertTrue("too few schemas", schemaCount >= 11);
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
        checkSchemas(rs, new String[]
         { "APP", "FIVE", "NULLID", "ONE",
           "SQLJ", "SYS", "SYSCAT", "SYSCS_DIAG",
           "SYSCS_UTIL", "SYSFUN", "SYSIBM", "SYSPROC", "SYSSTAT",
           "THREE", "TWO", "four", "sIx"});
    }
    /**
     * Check the returned information from a getSchemas().
     * The passed in String[] expected is a list of the
     * schemas expected to be returned in order.
     */
    public static void checkSchemas(ResultSet rs,
            String[] expected) throws SQLException
    {
        // Since catalog is always NULL expand into the
        // two value set expected by assertFullResultSet.
        String[][] expected2cols = new String[expected.length][];
        for (int i = 0; i < expected.length; i++)
        {
            expected2cols[i] = new String[] {expected[i], null};
        }
        
        checkSchemasShape(rs);
        JDBC.assertFullResultSet(rs, expected2cols, true);
        rs.close();
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
    
   /* 
    # TABLE_CAT String => table catalog (may be null)
    # TABLE_SCHEM String => table schema (may be null)
    # TABLE_NAME String => table name
    # TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
    # REMARKS String => explanatory comment on the table
    # TYPE_CAT String => the types catalog (may be null)
    # TYPE_SCHEM String => the types schema (may be null)
    # TYPE_NAME String => type name (may be null)
    # SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
    # REF_GENERATION
*/
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
