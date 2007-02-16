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

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derby.shared.common.reference.JDBC40Translation;

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
 *  getColumns (filtering)
 *  getCrossReference
 *  getExportedKeys
 *  getImportedKeys
 *  getIndexInfo
 *  getPrimaryKeys
 *  getProcedureColumns
 *  getProcedures
 *  getTablePrivileges
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
        
	/* DERBY-2243 Derby does support ANSI 92 standards
	* and this behaviour is now consistant across drivers
	*/
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
        
	/* DERBY-2244 Derby does support Order By clause
	* thus the changing the assert condition to TRUE
	*/
	assertTrue(dmd.supportsExpressionsInOrderBy());
        
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
    
    /**
     * This is not a test of Derby but JDBC constants for meta data
     * that this test depends on.
     * The constants for nullability are the same but let's check to make sure.
     *
     */
    public void testConstants()
    {
      assertEquals(DatabaseMetaData.columnNoNulls, ResultSetMetaData.columnNoNulls);
      assertEquals(DatabaseMetaData.columnNullable, ResultSetMetaData.columnNullable);
      assertEquals(DatabaseMetaData.columnNullableUnknown, ResultSetMetaData.columnNullableUnknown);
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
        assertMetaDataResultSet(rs, null, null, null);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getCatalogs();
        checkCatalogsShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getSuperTables(null,null,null);
        assertMetaDataResultSet(rs, null, null, null);
        JDBC.assertEmpty(rs);

        rs = dmd.getSuperTypes(null,null,null);
        assertMetaDataResultSet(rs, null, null, null);
        JDBC.assertEmpty(rs);

        rs = dmd.getUDTs(null,null,null,null);
        assertMetaDataResultSet(rs, null, null, null);
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
        createSchemasForTests();
        DatabaseMetaData dmd = getDMD();
        ResultSet rs = dmd.getSchemas();
        checkSchemas(rs, IDS);
    }
    
    private void createSchemasForTests() throws SQLException
    {
        // Set to cleanup on teardown.
        modifiedDatabase = true;

        Statement s = createStatement();
        for (int i = 0; i < IDS.length; i++)
           s.executeUpdate("CREATE SCHEMA " + IDS[i]);
        s.close();
        
        commit();
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
        , new boolean[] {false, true}
        );        
    }
    
    /**
     * Execute dmd.getTables() but perform additional checking
     * of the ODBC variant.
     * @throws IOException 
     */
    private ResultSet getDMDTables(DatabaseMetaData dmd,
            String catalog, String schema, String table, String[] tableTypes)
        throws SQLException, IOException
    {
        checkGetTablesODBC(catalog, schema, table, tableTypes);
        return dmd.getTables(catalog, schema, table, tableTypes);
    }
    
    /**
     * Test getTables() without modifying the database.
     * 
     * @throws SQLException
     * @throws IOException 
     */
    public void testGetTablesNoModify() throws SQLException, IOException {
        
        DatabaseMetaData dmd = getDMD();
        
        ResultSet rs;
        
        rs = getDMDTables(dmd, null, null, null, null);
        checkTablesShape(rs);
        int allTableCount = JDBC.assertDrainResults(rs);
        assertTrue("getTables() on all was empty!", allTableCount > 0);
        
        rs = getDMDTables(dmd, "%", "%", "%", null);
        checkTablesShape(rs);
        assertEquals("Different counts from getTables",
                allTableCount, JDBC.assertDrainResults(rs));
        
        rs = getDMDTables(dmd, null, "NO_such_schema", null, null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = getDMDTables(dmd, null, "SQLJ", null, null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = getDMDTables(dmd, null, "SQLJ", "%", null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = getDMDTables(dmd, null, "SYS", "No_such_table", null);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        String[] userTableOnly = new String[] {"TABLE"};

        // no user tables in SYS
        rs = getDMDTables(dmd, null, "SYS", null, userTableOnly);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = getDMDTables(dmd, null, "SYS", "%", userTableOnly);
        checkTablesShape(rs);
        JDBC.assertEmpty(rs);
        
        String[] systemTableOnly = new String[] {"SYSTEM_TABLE"};
        
        rs = getDMDTables(dmd, null, "SYS", null, systemTableOnly);
        checkTablesShape(rs);
        int systemTableCount = JDBC.assertDrainResults(rs);
        assertTrue("getTables() on system tables was empty!", systemTableCount > 0);
        
        rs = getDMDTables(dmd, null, "SYS", "%", systemTableOnly);
        checkTablesShape(rs);
        assertEquals(systemTableCount, JDBC.assertDrainResults(rs));

        String[] viewOnly = new String[] {"VIEW"};
        rs = getDMDTables(dmd, null, "SYS", null, viewOnly);
        JDBC.assertEmpty(rs);
        
        rs = getDMDTables(dmd, null, "SYS", "%", viewOnly);
        JDBC.assertEmpty(rs);
        
        String[] allTables = {"SYNONYM","SYSTEM TABLE","TABLE","VIEW"};
        rs = getDMDTables(dmd, null, null, null, allTables);
        checkTablesShape(rs);
        assertEquals("Different counts from getTables",
                allTableCount, JDBC.assertDrainResults(rs));
        rs = getDMDTables(dmd, "%", "%", "%", allTables);
        checkTablesShape(rs);
        assertEquals("Different counts from getTables",
                allTableCount, JDBC.assertDrainResults(rs));
        
    }
    /**
     * Test getTables() with  modifying the database.
     * 
     * @throws SQLException
     * @throws IOException 
     */
    public void testGetTablesModify() throws SQLException, IOException {
                
        int totalTables = createTablesForTest(false);
        
        DatabaseMetaData dmd = getDMD();
        ResultSet rs;
        
        String[] userTableOnly = new String[] {"TABLE"};
        
        // Get the list of idenifiers from IDS as the database
        // would store them in the order required.
        
        String[] dbIDS = new String[IDS.length];
        // Remove any quotes from user schemas and upper case
        // those without quotes.
        for (int i = 0; i < IDS.length; i++)
        {          
            dbIDS[i] = getStoredIdentifier(IDS[i]);
        }
        Arrays.sort(dbIDS);     
               
        // Check the contents, ordered by TABLE_CAT, TABLE_SCHEMA, TABLE_NAME
        rs = getDMDTables(dmd, null, null, null, userTableOnly);
        checkTablesShape(rs);
        int rowPosition = 0;
        while (rs.next())
        {
            boolean ourTable;
            assertEquals("TABLE_CAT", "", rs.getString("TABLE_CAT"));
            
            String schema = rs.getString("TABLE_SCHEM");
            
            // See if the table is in one of the schemas we created.
            // If not we perform what checking we can.
            boolean ourSchema = Arrays.binarySearch(dbIDS, schema) >= 0;
            
            if (ourSchema) {        
                assertEquals("TABLE_SCHEM",
                    dbIDS[rowPosition/dbIDS.length], schema);
                assertEquals("TABLE_NAME",
                    dbIDS[rowPosition%dbIDS.length], rs.getString("TABLE_NAME"));
            }
            
            assertEquals("TABLE_TYPE", "TABLE", rs.getString("TABLE_TYPE"));
            
            assertEquals("REMARKS", "", rs.getString("REMARKS"));

            assertNull("TYPE_CAT", rs.getString("TYPE_CAT"));
            assertNull("TYPE_SCHEM", rs.getString("TYPE_SCHEM"));
            assertNull("TYPE_NAME", rs.getString("TYPE_NAME"));
            assertNull("SELF_REFERENCING_COL_NAME", rs.getString("SELF_REFERENCING_COL_NAME"));
            assertNull("REF_GENERATION", rs.getString("REF_GENERATION"));
            
            if (ourSchema)
                rowPosition++;
         }
         rs.close();
         assertEquals("getTables count for all user tables",
               totalTables, rowPosition);
       
         Random rand = new Random();
        
         // Test using schema pattern with a pattern unique to
         // a single schema.
         for (int i = 0; i < dbIDS.length; i++)
         {
            String schema = dbIDS[i];
            int pc = rand.nextInt(6);
            String schemaPattern = schema.substring(0, pc + 2) + "%";
            
            rs = getDMDTables(dmd, null, schemaPattern, null, userTableOnly);
            checkTablesShape(rs);
            rowPosition = 0;
            while (rs.next())
            {
                assertEquals("TABLE_SCHEM",
                        schema, rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE_NAME",
                        dbIDS[rowPosition%dbIDS.length], rs.getString("TABLE_NAME"));
                assertEquals("TABLE_TYPE", "TABLE", rs.getString("TABLE_TYPE"));
                rowPosition++;
            }
            rs.close();
            assertEquals("getTables count schema pattern",
                    dbIDS.length, rowPosition);
         }
         
         // Test using table pattern with a pattern unique to
         // a single table per schema.
         for (int i = 0; i < dbIDS.length; i++)
         {
            String table = dbIDS[i];
            int pc = rand.nextInt(6);
            String tablePattern = table.substring(0, pc + 2) + "%";
            
            rs = getDMDTables(dmd, null, null, tablePattern, userTableOnly);
            checkTablesShape(rs);
            rowPosition = 0;
            while (rs.next())
            {
                assertEquals("TABLE_SCHEM",
                        dbIDS[rowPosition%dbIDS.length], rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE_TYPE", "TABLE", rs.getString("TABLE_TYPE"));
                assertEquals("TABLE_NAME",
                        table, rs.getString("TABLE_NAME"));
                rowPosition++;
            }
            rs.close();
            assertEquals("getTables count schema pattern",
                    dbIDS.length, rowPosition);
         }        
    }
    
  
    /**
     * Execute and check the ODBC variant of getTables which
     * uses a procedure to provide the same information to ODBC clients.
     * @throws IOException 
     */
    private void checkGetTablesODBC(String catalog, String schema,
            String table, String[] tableTypes) throws SQLException, IOException
    {
        String tableTypesAsString = null;
        if (tableTypes != null) {
            int count = tableTypes.length;
            StringBuffer sb = new StringBuffer();
           for (int i = 0; i < count; i++) {
               if (i > 0)
                    sb.append(",");
            sb.append(tableTypes[i]);
           }
           tableTypesAsString = sb.toString();
        }

        CallableStatement cs = prepareCall(
            "CALL SYSIBM.SQLTABLES(?, ?, ?, ?, 'DATATYPE=''ODBC''')");
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.setString(4, tableTypesAsString);
        
        cs.execute();
        ResultSet odbcrs = cs.getResultSet();
        assertNotNull(odbcrs);
        
        // Returned ResultSet will have the same shape as
        // DatabaseMetaData.getTables() even though ODBC
        // only defines the first five columns.
        checkTablesShape(odbcrs);
        
        // Expect the contents of JDBC and ODBC metadata to be the same.
        ResultSet dmdrs = getDMD().getTables(catalog, schema, table, tableTypes);
        JDBC.assertSameContents(odbcrs, dmdrs);
        
        cs.close();
    }   

    /**
     * Create a set of tables using the identifiers in IDS.
     * For each identifier in IDS a schema is created.
     * For each identifier in IDS create a table in every schema just created.
     * Each table has five columns with names using the identifiers from IDS
     * suffixed with _N where N is the column number in the table. The base
     * name for each column is round-robined from the set of IDS.
     * The type of each column is round-robined from the set of supported
     * types returned by getSQLTypes.
     * 
     * <BR>
     * skipXML can be set to true to create tables without any XML
     * columns. This is useful for getColumns() testing where
     * the fixture compares the output of DatabaseMetaData to
     * ResultSetMetaData by a SELCT * from the table. However
     * for XML columns they cannot be returned through JDBC yet.
     * 
     * @param skipXML true if tables with the XML column should not
     * be created.
     * @throws SQLException
     */
    private int createTablesForTest(boolean skipXML) throws SQLException
    {
        getConnection().setAutoCommit(false);
        List types = getSQLTypes(getConnection());
        if (skipXML)
            types.remove("XML");
            
        int typeCount = types.size();
               
        createSchemasForTests();
        
        Statement s = createStatement();
        
        int columnCounter = 0;
        
        for (int sid = 0; sid < IDS.length; sid++) {
            for (int tid = 0; tid < IDS.length; tid++)
            {
                StringBuffer sb = new StringBuffer();
                sb.append("CREATE TABLE ");
                sb.append(IDS[sid]);
                sb.append('.');
                sb.append(IDS[tid]);
                sb.append(" (");
                
                // Five columns per table
                for (int c = 1; c <= 5; c++) {
                    String colName = IDS[columnCounter % IDS.length];
                    boolean delimited = colName.charAt(colName.length() - 1) == '"';
                    if (delimited)
                        colName = colName.substring(0, colName.length() - 1);
                    sb.append(colName);  
                    sb.append('_');
                    sb.append(c); // append the column number
                    if (delimited)
                        sb.append('"');
                    sb.append(' ');
                    sb.append(types.get(columnCounter++ % typeCount));
                    if (c < 5)
                        sb.append(", ");
                }
                
                sb.append(")");
                s.execute(sb.toString());
            }
        }
        
        s.close();
        
        commit();

        return IDS.length * IDS.length;
    }
    
    /**
     * Test getTableColumns().
     * Contents are compared to the ResultSetMetaData
     * for a SELECT * from the table. All columns in
     * all tables are checked.
     */
    public void testGetColumnsNoModify() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        ResultSet rs = dmd.getColumns(null, null, null, null);
        checkColumnsShape(rs);
        crossCheckGetColumnsAndResultSetMetaData(rs);
        
    }
    
    /**
     * Test getColumns() with  modifying the database.
     * 
     * @throws SQLException
     */
    public void testGetColumns() throws SQLException {
           
        // skip XML datatype as our cross check with
        // ResultSetMetaData will fail
        int totalTables = createTablesForTest(true);
        
        // First cross check all the columns in the database
        // with the ResultSetMetaData.
        testGetColumnsNoModify();
        
        // TODO: pattern matching on getColumns
    }
    
    /**
     * Compare a ResultSet from getColumns() with ResultSetMetaData
     * returned from a SELECT * against the table. This method
     * handles situations where a full set of the columns are in
     * the ResultSet.
     * The first action is to call rs.next().
     * The ResultSet will be closed by this method.
     * @param rs
     * @throws SQLException
     */
    private void crossCheckGetColumnsAndResultSetMetaData(ResultSet rs)
    throws SQLException
    {
        Statement s = createStatement();
        while (rs.next())
        {
            String schema = rs.getString("TABLE_SCHEM");
            String table = rs.getString("TABLE_NAME");
            
            ResultSet rst = s.executeQuery(
                "SELECT * FROM " + JDBC.escape(schema, table));
            ResultSetMetaData rsmdt = rst.getMetaData();
                     
            for (int col = 1; col <= rsmdt.getColumnCount() ; col++)
            {
                if (col != 1)
                     assertTrue(rs.next());
                
                assertEquals("ORDINAL_POSITION",
                            col, rs.getInt("ORDINAL_POSITION"));
                
                assertEquals("TABLE_CAT",
                        "", rs.getString("TABLE_CAT"));
                assertEquals("TABLE_SCHEM",
                        schema, rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE_NAME",
                        table, rs.getString("TABLE_NAME"));
                
                crossCheckGetColumnRowAndResultSetMetaData(rs, rsmdt);
                
            }
            rst.close();
            
            
        }
        rs.close();
        s.close();
    }
    
    /**
     * Cross check a single row from getColumns() with ResultSetMetaData
     * for a SELECT * from the same table.
     * @param rs ResultSet from getColumns already positioned on the row.
     * @param rsmdt ResultSetMetaData for the SELECT *
     * @throws SQLException
     */
    public static void crossCheckGetColumnRowAndResultSetMetaData(
            ResultSet rs, ResultSetMetaData rsmdt)
        throws SQLException
    {
        int col = rs.getInt("ORDINAL_POSITION");
        
        assertEquals("RSMD.getCatalogName",
                rsmdt.getCatalogName(col), rs.getString("TABLE_CAT"));
        assertEquals("RSMD.getSchemaName",
                rsmdt.getSchemaName(col), rs.getString("TABLE_SCHEM"));
        assertEquals("RSMD.getTableName",
                rsmdt.getTableName(col), rs.getString("TABLE_NAME"));
        
        assertEquals("COLUMN_NAME",
                rsmdt.getColumnName(col), rs.getString("COLUMN_NAME"));
        
        // DERBY-2285 BOOLEAN columns appear different on
        // network client.
        // DMD returns BOOLEAN
        // RSMD returns SMALLINT
        int dmdColumnType = rs.getInt("DATA_TYPE");
        if (dmdColumnType == Types.BOOLEAN && usingDerbyNetClient())
        {
            assertEquals("TYPE_NAME",
                    "BOOLEAN", rs.getString("TYPE_NAME"));
            assertEquals("TYPE_NAME",
                    "SMALLINT", rsmdt.getColumnTypeName(col));

            assertEquals("DATA_TYPE",
                    Types.SMALLINT, rsmdt.getColumnType(col));
        }
        else if (dmdColumnType == Types.JAVA_OBJECT && usingDerbyNetClient())
        {
            // DMD returns JAVA_OBJECT
            // RSMD returns LONGVARBINARY!                    
            assertEquals("DATA_TYPE",
                    Types.LONGVARBINARY, rsmdt.getColumnType(col));                   
        }
        else if (dmdColumnType == Types.VARBINARY && usingDerbyNetClient())
        {
            // DMD returns different type name to RSMD
            assertEquals("DATA_TYPE",
                    Types.VARBINARY, rsmdt.getColumnType(col));  
        }
        else if (dmdColumnType == Types.BINARY && usingDerbyNetClient())
        {
            // DMD returns different type name to RSMD
            assertEquals("DATA_TYPE",
                    Types.BINARY, rsmdt.getColumnType(col));                               
        }
        else
        {
            assertEquals("DATA_TYPE",
                rsmdt.getColumnType(col), rs.getInt("DATA_TYPE"));
            assertEquals("TYPE_NAME",
                rsmdt.getColumnTypeName(col), rs.getString("TYPE_NAME"));
        }
        
        /*
        if (dmdColumnType != Types.JAVA_OBJECT) {
        System.out.println("TYPE " + rs.getInt("DATA_TYPE"));
        System.out.println(JDBC.escape(schema, table) + " " + rs.getString("COLUMN_NAME"));
        assertEquals("COLUMN_SIZE",
                rsmdt.getPrecision(col), rs.getInt("COLUMN_SIZE"));
        }
        */
        
        // not used by JDBC spec
        assertEquals("BUFFER_LENGTH", 0, rs.getInt("BUFFER_LENGTH"));
        assertTrue("BUFFER_LENGTH", rs.wasNull());
        
        /*
        assertEquals("DECIMAL_DIGITS",
                rsmdt.getScale(col), rs.getInt("DECIMAL_DIGITS"));
        */
        
        // This assumes the constants defined by DMD and ResultSet
        // for nullability are equal. They are by inspection
        // and since they are static final and part of a defined
        // api by definition they cannot change. We also
        // check statically this is true in the testConstants fixture.
        assertEquals("NULLABLE",
                rsmdt.isNullable(col), rs.getInt("NULLABLE"));
        
        // REMARKS set to empty string by Derby
        assertEquals("REMARKS", "", rs.getString("REMARKS"));
        
        // COLUMN_DEF ??
       
        // both unused by JDBC spec
        assertEquals("SQL_DATA_TYPE", 0, rs.getInt("SQL_DATA_TYPE"));
        assertTrue(rs.wasNull());
        assertEquals("SQL_DATETIME_SUB", 0, rs.getInt("SQL_DATETIME_SUB"));
        assertTrue(rs.wasNull());
        
        // IS_NULLABLE
        switch (rsmdt.isNullable(col))
        {
        case ResultSetMetaData.columnNoNulls:
            assertEquals("IS_NULLABLE", "NO", rs.getString("IS_NULLABLE"));
            break;
        case ResultSetMetaData.columnNullable:
            assertEquals("IS_NULLABLE", "YES", rs.getString("IS_NULLABLE"));
            break;
        case ResultSetMetaData.columnNullableUnknown:
            assertEquals("IS_NULLABLE", "", rs.getString("IS_NULLABLE"));
            break;
        default:
            fail("invalid return from rsmdt.isNullable(col)");
        }
        
        // SCOPE not supported
        assertNull("SCOPE_CATLOG", rs.getString("SCOPE_CATLOG"));
        assertNull("SCOPE_SCHEMA", rs.getString("SCOPE_SCHEMA"));
        assertNull("SCOPE_TABLE", rs.getString("SCOPE_TABLE"));
        
        // DISTINCT not supported
        assertEquals("SOURCE_DATA_TYPE", 0, rs.getShort("SOURCE_DATA_TYPE"));
        assertTrue(rs.wasNull());
        
        // IS_AUTOINCREMENT added in JDBC 4.0
       assertEquals("IS_AUTOINCREMENT",
               rsmdt.isAutoIncrement(col) ? "YES" : "NO",
               rs.getString("IS_AUTOINCREMENT"));
       assertFalse(rs.wasNull());        
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
        , null
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
        
        String[] JDBC_COLUMN_NAMES = new String[] {
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
                "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE",
                
                "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
                "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
                
                "MINIMUM_SCALE", "MAXIMUM_SCALE",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                
                "NUM_PREC_RADIX"          
              };
        
        int[] JDBC_COLUMN_TYPES = new int[] {
                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, BOOLEAN,
                
                Types.SMALLINT, BOOLEAN, BOOLEAN,
                BOOLEAN, Types.VARCHAR,
                
                Types.SMALLINT, Types.SMALLINT,
                Types.INTEGER, Types.INTEGER,
                
                Types.INTEGER
              };
        
        boolean[] JDBC_COLUMN_NULLABILITY = {
                false, false, true, true,
                true, true, false, false,
                false, true, false,
                true, true,
                true, true,
                true, true,
                true 
        };
        
        // DERBY-2307 Nullablity is wrong for columns 1,7,9 (1-based)
        // Make a modified copy of JDBC_COLUMN_NULLABILITY
        // here to allow the test to pass. Left JDBC_COLUMN_NULLABILITY
        // as the expected versions as it is also used for the ODBC
        // checks below and has the correct values.
        boolean[] JDBC_COLUMN_NULLABILITY_DERBY_2307 = {
                true, false, true, true,
                true, true, true, false,
                true, true, false,
                true, true,
                true, true,
                true, true,
                true 
        };
        
        ResultSet rs = getDMD().getTypeInfo();
        assertMetaDataResultSet(rs, JDBC_COLUMN_NAMES, JDBC_COLUMN_TYPES
        , JDBC_COLUMN_NULLABILITY_DERBY_2307
        );

	/*
	 Derby-2258 Removed 3 data types which are not supported by Derby
	 and added XML data type which is supported by Derby
	*/
        int[] supportedTypes = new int[] {
          Types.BIGINT, Types.BINARY, Types.BLOB,
          Types.CHAR, Types.CLOB, Types.DATE,
          Types.DECIMAL, Types.DOUBLE, Types.FLOAT,
          Types.INTEGER, Types.LONGVARBINARY, Types.LONGVARCHAR,
          Types.NUMERIC, Types.REAL, Types.SMALLINT,
          Types.TIME, Types.TIMESTAMP,  Types.VARBINARY,
          Types.VARCHAR, JDBC.SQLXML
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
                        
	    /*
	     Derby-2260 Correcting the precision value for VARCHAR FOR BIT DATA
	     Thus this test also now expects the correct value i.e. 32672
	     Also adding precision check for SQLXML data type
	    */
            case Types.VARBINARY:
                precision = 32672;
                break;

            case Types.VARCHAR:
                precision = 32672;
                break;
	    case JDBC.SQLXML:
		precision = 0;
		break;
            }
            assertEquals("PRECISION " + typeName,
                    precision, rs.getInt("PRECISION"));

            /*
              Precision value is null for XML data type
            */
            if (typeName.equals("XML" ))
                assertTrue(rs.wasNull());
            else
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
	    /*
	     Derby-2259 Correcting the searchable value for 
	     LONGVARBINARY, LONGVARCHAR & BLOB data type
	     also adding SQLXML data type in the test.
	    */
            case Types.LONGVARBINARY:
                searchable = DatabaseMetaData.typePredNone;
                break;
            case Types.LONGVARCHAR:
                searchable = DatabaseMetaData.typePredChar;
                break;
                
            case Types.BLOB:
		searchable = DatabaseMetaData.typePredNone;
		break;
            case Types.CLOB:
		searchable = DatabaseMetaData.typePredChar;
                break;
            case Types.CHAR:
            case Types.VARCHAR:
                searchable = DatabaseMetaData.typeSearchable;
                break;
	    case JDBC.SQLXML:
		searchable = DatabaseMetaData.typePredNone;
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
        
        // Now check the ODBC version:
        
        // ODBC column names & types differ from JDBC slightly.
        // ODBC has one more column.
        String[] ODBC_COLUMN_NAMES = new String[19];
        System.arraycopy(JDBC_COLUMN_NAMES, 0, ODBC_COLUMN_NAMES, 0,
                JDBC_COLUMN_NAMES.length);
        ODBC_COLUMN_NAMES[2] = "COLUMN_SIZE";
        ODBC_COLUMN_NAMES[11] = "AUTO_UNIQUE_VAL";
        ODBC_COLUMN_NAMES[18] = "INTERVAL_PRECISION";
        
        int[] ODBC_COLUMN_TYPES = new int[ODBC_COLUMN_NAMES.length];
        System.arraycopy(JDBC_COLUMN_TYPES, 0, ODBC_COLUMN_TYPES, 0,
                JDBC_COLUMN_TYPES.length);
        
        ODBC_COLUMN_TYPES[1] = Types.SMALLINT; // DATA_TYPE
        ODBC_COLUMN_TYPES[7] = Types.SMALLINT; // CASE_SENSITIVE
        ODBC_COLUMN_TYPES[9] = Types.SMALLINT; // UNSIGNED_ATTRIBUTE
        ODBC_COLUMN_TYPES[10] = Types.SMALLINT; // FIXED_PREC_SCALE
        ODBC_COLUMN_TYPES[11] = Types.SMALLINT; // AUTO_UNIQUE_VAL
        ODBC_COLUMN_TYPES[15] = Types.SMALLINT; // SQL_DATA_TYPE
        ODBC_COLUMN_TYPES[16] = Types.SMALLINT; // SQL_DATETIME_SUB
        ODBC_COLUMN_TYPES[18] = Types.SMALLINT; // INTERVAL_PRECISION
        
        boolean[] ODBC_COLUMN_NULLABILITY = new boolean[ODBC_COLUMN_NAMES.length];
        System.arraycopy(JDBC_COLUMN_NULLABILITY, 0, ODBC_COLUMN_NULLABILITY, 0,
                JDBC_COLUMN_NULLABILITY.length);
        
        ODBC_COLUMN_NULLABILITY[15] = false; // // SQL_DATETIME_SUB (JDBC unused)
        ODBC_COLUMN_NULLABILITY[18] = true; // INTERVAL_PRECISION
                
        CallableStatement cs = prepareCall(
                "CALL SYSIBM.SQLGETTYPEINFO (0, 'DATATYPE=''ODBC''')");
        
        cs.execute();
        ResultSet odbcrs = cs.getResultSet();
        assertNotNull(odbcrs);
        
        assertMetaDataResultSet(odbcrs, ODBC_COLUMN_NAMES, ODBC_COLUMN_TYPES, null);
        
        odbcrs.close();
        cs.close();

    }
    
    /*
     * Check the shape of the ResultSet from any getColumns call.
     */
    private void checkColumnsShape(ResultSet rs) throws SQLException
    {
        assertMetaDataResultSet(rs,
                new String[] {
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA",
                "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT"
               },
               new int[] {
               Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
               Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
               Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
               Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
               Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
               Types.VARCHAR, Types.SMALLINT, Types.VARCHAR
               }
        , null
              );          
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
        , new boolean[] {
          true, false, false, true, // TABLE_SCHEM cannot be NULL in Derby
          true, true, true, true,
          true, true
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
        , new boolean[] {false}
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
        , null
        );        
    }
    
    public static void assertMetaDataResultSet(ResultSet rs,
            String[] columnNames, int[] columnTypes,
            boolean[] nullability) throws SQLException
    {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        //assertNull(rs.getStatement());
        
        if (columnNames != null)
            JDBC.assertColumnNames(rs, columnNames);
        if (columnTypes != null)
            JDBC.assertColumnTypes(rs, columnTypes);
        if (nullability != null)
            JDBC.assertNullability(rs, nullability);
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
    
    /**
     * Return a list of all valid supported datatypes as Strings
     * suitable for use in any SQL statement where a SQL type is
     * expected. For variable sixzed types the string will
     * have random valid length information. E.g. CHAR(37).
     */
    public static List getSQLTypes(Connection conn) throws SQLException
    {
        List list = new ArrayList();
        
        Random rand = new Random();
        
        ResultSet rs = conn.getMetaData().getTypeInfo();
        while (rs.next())
        {
            String typeName = rs.getString("TYPE_NAME");
            
            String createParams = rs.getString("CREATE_PARAMS");
            
            if (createParams == null) {
                // Type name stands by itself.
                list.add(typeName);
                continue;
            }
            
            if (createParams.indexOf("length") != -1)
            {
                int maxLength = rs.getInt("PRECISION");
                
                // nextInt returns a value between 0 and maxLength-1
                int length = rand.nextInt(maxLength) + 1;
                
                int paren = typeName.indexOf('(');
                if (paren == -1) {
                    list.add(typeName + "(" + length + ")");
                    
                } else {
                    StringBuffer sb = new StringBuffer();
                    sb.append(typeName.substring(0, paren+1));
                    sb.append(length);
                    sb.append(typeName.substring(paren+1));
                    list.add(sb.toString());
                }
                
                continue;
            }
        }
        
        return list;
    }
    
    /**
     * Given a valid SQL type return the corresponding
     * JDBC type identifier from java.sql.Types.
     * Will assert if the type is not known
     * (in future, currently just return Types.NULL).
     */
    public static int getJDBCType(String type)
    {
        if ("SMALLINT".equals(type))
            return Types.SMALLINT;
        if ("INTEGER".equals(type) || "INT".equals(type))
            return Types.INTEGER;
        if ("BIGINT".equals(type))
            return Types.BIGINT;
        
        if (type.equals("FLOAT") || type.startsWith("FLOAT("))
            return Types.FLOAT;
        if (type.equals("REAL"))
            return Types.REAL;

        if ("DOUBLE".equals(type) || "DOUBLE PRECISION".equals(type))
            return Types.INTEGER;
        
        if ("DATE".equals(type))
            return Types.DATE;
        if ("TIME".equals(type))
            return Types.TIME;
        if ("TIMESTAMP".equals(type))
            return Types.TIMESTAMP;
        
        if (type.equals("DECIMAL") || type.startsWith("DECIMAL("))
            return Types.DECIMAL;
        if (type.equals("NUMERIC") || type.startsWith("NUMERIC("))
            return Types.NUMERIC;
        
        if (type.endsWith("FOR BIT DATA")) {
           if (type.startsWith("CHAR"))
               return Types.BINARY;
           if (type.startsWith("CHARACTER"))
            return Types.BINARY; 
           
           if (type.startsWith("VARCHAR"))
               return Types.VARBINARY;
           if (type.startsWith("CHARACTER VARYING"))
               return Types.VARBINARY;
           if (type.startsWith("CHAR VARYING"))
               return Types.VARBINARY;           
        }
        
        if ("LONG VARCHAR".equals(type))
            return Types.LONGVARCHAR;
        if ("LONG VARCHAR FOR BIT DATA".equals(type))
            return Types.LONGVARBINARY;
        
        if (type.equals("CHAR") || type.startsWith("CHAR("))
            return Types.CHAR;
        if (type.equals("CHARACTER") || 
                type.startsWith("CHARACTER("))
            return Types.CHAR;
        
        if (type.equals("VARCHAR") || type.startsWith("VARCHAR("))
            return Types.VARCHAR;
        if (type.equals("CHARACTER VARYING") || 
                type.startsWith("CHARACTER VARYING("))
            return Types.VARCHAR;
        if (type.equals("CHAR VARYING") || 
                type.startsWith("CHAR VARYING("))
            return Types.VARCHAR;

        if (type.equals("BLOB") || type.startsWith("BLOB("))
            return Types.BLOB;
        if (type.equals("BINARY LARGE OBJECT") || 
                type.startsWith("BINARY LARGE OBJECT("))
            return Types.BLOB;
        
        if (type.equals("CLOB") || type.startsWith("CLOB("))
            return Types.CLOB;
        if (type.equals("CHARACTER LARGE OBJECT") || 
                type.startsWith("CHARACTER LARGE OBJECT("))
            return Types.CLOB;

        if ("XML".equals(type))
            return JDBC.SQLXML;
        
        fail("Unexpected SQL type: " + type);
        return Types.NULL;
    }
}
