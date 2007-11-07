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
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Method;
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
//import java.util.HashMap;
//import java.util.Iterator;
import java.util.List;
import java.util.Locale;
//import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
//import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
//import org.apache.derby.shared.common.reference.JDBC40Translation;

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
 * TODO: Methods left to test from JDBC 3
 * 
 *  getColumnPrivileges
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
    
    /**
     * Default suite for running this test.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("DatabaseMetaDataTest");
        suite.addTest(
            TestConfiguration.defaultSuite(DatabaseMetaDataTest.class));
        // Test for DERBY-2584 needs a fresh database to ensure that the
        // meta-data queries haven't already been compiled. No need to run the
        // test in client/server mode since it only tests the compilation of
        // meta-data queries.
        suite.addTest(
            TestConfiguration.singleUseDatabaseDecorator(
                // until DERBY-177 is fixed, set lock timeout to prevent the
                // test from waiting one minute
                DatabasePropertyTestSetup.setLockTimeouts(
                    new DatabaseMetaDataTest("initialCompilationTest"), 2, 4)));
        return suite;
    }
    
    /**
     * Return the identifiers used to create schemas,
     * tables etc. in the order the database stores them.
     */
    private String[] getSortedIdentifiers()
    {
        String[] dbIDS = new String[IDS.length];
        // Remove any quotes from user schemas and upper case
        // those without quotes.
        for (int i = 0; i < IDS.length; i++)
        {          
            dbIDS[i] = getStoredIdentifier(IDS[i]);
        }
        Arrays.sort(dbIDS); 
        
        return dbIDS;
    }
    
    private final DatabaseMetaData getDMD() throws SQLException
    {
        return getConnection().getMetaData();
    }

    /**
     * Tests that a meta-data query is compiled and stored correctly even when
     * there's a lock on the system tables (DERBY-2584). This test must run on
     * a fresh database (that is, <code>getIndexInfo</code> must not have been
     * prepared and stored in <code>SYS.SYSSTATEMENTS</code>).
     */
    public void initialCompilationTest() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(false);
        c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Statement s = createStatement();
        // First get shared row locks on the SYSSTATEMENTS table.
        JDBC.assertDrainResults(
            s.executeQuery("SELECT * FROM SYS.SYSSTATEMENTS"));
        s.close();
        // Execute getIndexInfo() for the first time. Because of the shared
        // locks on SYSSTATEMENTS, the query is compiled in the main
        // transaction.
        getDMD().getIndexInfo(null, null, "T", false, false).close();
        // Re-use the previously compiled query from disk. Fails with
        // ArrayIndexOutOfBoundsException before DERBY-2584.
        getDMD().getIndexInfo(null, null, "T", false, false).close();
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
        
        assertTrue(dmd.locatorsUpdateCopy());
        
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
     * correct shape, and with correct names, datatypes and 
     * nullability for the columns in the ResultSet. 
     * 
     */
    public void testUnimplementedSQLObjectAttributes() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        ResultSet rs;
        
        rs = dmd.getAttributes(null,null,null,null);
        
        String [] columnNames = {
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE",
                "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE", 
                "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", 
                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE"
        };
        int [] columnTypes = { 
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, 
                Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT            
        };
        // DERBY-3171; we get a different value back for nullability for
        // a number of the columns with networkserver/client vs. embedded 
        boolean nullval = true;
        if (usingDerbyNetClient())
            nullval = false;
        boolean [] nullability = {
                true, true, true, true, nullval, true, nullval,
                nullval, nullval, nullval, true, true, nullval, nullval,
                nullval, nullval, true, true, true, true, true
        };
            
        assertMetaDataResultSet(rs, columnNames, columnTypes, nullability);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getCatalogs();
        checkCatalogsShape(rs);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getSuperTables(null,null,null);
        columnNames = new String[] {
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"};
        columnTypes = new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        nullability = new boolean[] {
                true, true, true, true};
        assertMetaDataResultSet(rs, columnNames, columnTypes, nullability);
        JDBC.assertEmpty(rs);

        rs = dmd.getSuperTypes(null,null,null);
        columnNames = new String[] {
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT",
                "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"};
        columnTypes = new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR};
        nullability = new boolean[] {
                true, true, true, true, true, true};
        assertMetaDataResultSet(rs, columnNames, columnTypes, nullability);
        JDBC.assertEmpty(rs);

        rs = dmd.getUDTs(null,null,null,null);
        columnNames = new String[] {
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME",
                "DATA_TYPE", "REMARKS", "BASE_TYPE"};
        columnTypes = new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        nullability = new boolean[] {
                true, true, true, true};
        assertMetaDataResultSet(rs, null, null, null);
        JDBC.assertEmpty(rs);
        
        rs = dmd.getVersionColumns(null,null, "No_such_table");
        checkVersionColumnsShape(rs);
        JDBC.assertEmpty(rs);
    }
    
    /**
     * Six combinations of valid identifiers with mixed
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
    public void testGetSchemasReadOnly() throws SQLException {
        
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
    public void testGetTablesReadOnly() throws SQLException, IOException {
        
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
        String[] dbIDS = getSortedIdentifiers();    
               
        // Check the contents, ordered by TABLE_CAT, TABLE_SCHEMA, TABLE_NAME
        rs = getDMDTables(dmd, null, null, null, userTableOnly);
        checkTablesShape(rs);
        int rowPosition = 0;
        while (rs.next())
        {
            //boolean ourTable;
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
    public void testGetColumnsReadOnly() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        ResultSet rs = dmd.getColumns(null, null, null, null);
        checkColumnsShape(rs);
        crossCheckGetColumnsAndResultSetMetaData(rs, false);
        
    }
    
    /**
     * Test getColumns() with  modifying the database.
     * 
     * @throws SQLException
     */
    public void testGetColumnsModify() throws SQLException {
           
        // skip XML datatype as our cross check with
        // ResultSetMetaData will fail
        int totalTables = createTablesForTest(true);
        
        // First cross check all the columns in the database
        // with the ResultSetMetaData.
        testGetColumnsReadOnly();
        
        Random rand = new Random();
        String[] dbIDS = getSortedIdentifiers();
            
        
        DatabaseMetaData dmd = this.getDMD();
        
        
        for (int i = 1; i < 20; i++) {
            int seenColumnCount = 0;
            // These are the pattern matching parameters
            String schemaPattern = getPattern(rand, dbIDS);
            String tableNamePattern = getPattern(rand, dbIDS);
            String columnNamePattern = getPattern(rand, dbIDS);
        
            ResultSet rs = dmd.getColumns(null,
                schemaPattern, tableNamePattern, columnNamePattern);
            
            
            checkColumnsShape(rs);
            
            while (rs.next())
            {
                String schema = rs.getString("TABLE_SCHEM");
                String table = rs.getString("TABLE_NAME");
                String column = rs.getString("COLUMN_NAME");
                
                assertMatchesPattern(schemaPattern, schema);
                assertMatchesPattern(tableNamePattern, table);
                assertMatchesPattern(columnNamePattern, column);
                
                
                seenColumnCount++;
            }
            rs.close();
            
            // Re-run to check the correct data is returned
            // when filtering is enabled
            rs = dmd.getColumns(null,
                    schemaPattern, tableNamePattern, columnNamePattern);
            crossCheckGetColumnsAndResultSetMetaData(rs, true);
            
            // Now re-execute fetching all schemas, columns etc.
            // and see we can the same result when we "filter"
            // in the application
            int appColumnCount = 0;
            rs = dmd.getColumns(null,null, null, null);
            
            while (rs.next())
            {
                String schema = rs.getString("TABLE_SCHEM");
                String table = rs.getString("TABLE_NAME");
                String column = rs.getString("COLUMN_NAME");
                
                if (!doesMatch(schemaPattern, 0, schema, 0))
                    continue;               
                if (!doesMatch(tableNamePattern, 0, table, 0))
                    continue;
                if (!doesMatch(columnNamePattern, 0, column, 0))
                    continue;
                
                appColumnCount++;
            }
            rs.close();
            
            assertEquals("Mismatched column count on getColumns() filtering",
                    seenColumnCount, appColumnCount);    
        }       
    }
    
    
    private void assertMatchesPattern(String pattern, String result)
    {       
        if (!doesMatch(pattern, 0, result, 0))
        {
            fail("Bad pattern matching:" + pattern + 
                            " result:" + result);
        }

    }
     
    /**
     * See if a string matches the pattern as defined by
     * DatabaseMetaData. By passing in non-zero values
     * can check sub-sets of the pattern against the
     * sub strings of the result.
     * <BR>
     * _ matches a single character
     * <BR>
     * % matches zero or more characters
     * <BR>
     * Other characters match themselves.
     * @param pattern Pattern
     * @param pp Position in pattern to start the actual pattern from
     * @param result result string
     * @param rp position in result to starting checking
     * @return true if a match is found
     */
    private boolean doesMatch(String pattern, int pp,
            String result, int rp)
    {
        // Find a match
        for (;;)
        {
            if (pp == pattern.length() && rp == result.length())
                return true;
            
            // more characters to match in the result but
            // no more pattern.
            if (pp == pattern.length())
                return false;
            
            char pc = pattern.charAt(pp);
            if (pc == '_')
            {
                // need to match a single character but
                // exhausted result, so no match.
                if (rp == result.length())
                    return false;
                
                pp++;
                rp++;
            }
            else if (pc == '%')
            {
                // % at end, complete match regardless of
                // position of result since % matches zero or more.
                if (pp == pattern.length() - 1)
                {
                    return true;
                }
                
                // Brut force, we have a pattern like %X
                // and we are say in the third character of
                // abCdefgX
                // then start a 'CdefgX' and look for a match,
                // then 'defgX' etc.
                for (int sp = rp; sp < result.length(); sp++)
                {
                    if (doesMatch(pattern, pp+1, result, sp))
                    {
                        // Have a match for the pattern after the %
                        // which means we have a match for the pattern
                        // with the % since we can match 0 or mor characters
                        // with %.
                        return true;
                    }
                }
                
                // Could not match the pattern after the %
                return false;
          }
            else
            {
                // need to match a single character but
                // exhausted result, so no match.
                if (rp == result.length())
                    return false;
                
                // Single character, must match exactly.
                if (pc != result.charAt(rp))
                {
                    //Computer says no.
                    return false;
                }
                pp++;
                rp++;
            }
            
        }
        
    }
    
    private String getPattern(Random rand, String[] dbIDS)
    {
        int y = rand.nextInt(100);
        if (y < 10)
            return "%"; // All
        if (y < 30)
            return dbIDS[rand.nextInt(dbIDS.length)]; // exact match
        
        String base;
        if (y < 40)
        {
            // Base for some pattern that can never match
            base = "XxZZzXXZZZxxXxZz";
        }
        else
        {
            base = dbIDS[rand.nextInt(dbIDS.length)];
        }
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < base.length();)
        {
            int x = rand.nextInt(10);
            if (x < 5)
                x = 0; // bias towards keeping the characters.
            
            boolean inWild;
            if (sb.length() == 0)
                inWild = false;
            else
            {
                char last = sb.charAt(sb.length() - 1);
                inWild = last == '_' || last == '%';
            }

            if (x == 0)
            {
                // character from base
                sb.append(base.charAt(i++));
            }
            else if (x == 5)
            {
                i++;
               // single character match
                if (!inWild)
                    sb.append('_');

            }
            else
            {
                i += (x - 5);
                
                // replace a number of characters with %
                if (!inWild)
                    sb.append('%');
               
            }
        }
        
        // Some pattern involving 
        return sb.toString();
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
    private void crossCheckGetColumnsAndResultSetMetaData(ResultSet rs,
            boolean partial)
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
                if (!partial) {
                    if (col != 1)
                        assertTrue(rs.next());
                
                    assertEquals("ORDINAL_POSITION",
                            col, rs.getInt("ORDINAL_POSITION"));
                }
                
                assertEquals("TABLE_CAT",
                        "", rs.getString("TABLE_CAT"));
                assertEquals("TABLE_SCHEM",
                        schema, rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE_NAME",
                        table, rs.getString("TABLE_NAME"));
                
                crossCheckGetColumnRowAndResultSetMetaData(rs, rsmdt);
                if (partial)
                    break;
                
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
        else if (dmdColumnType == Types.NUMERIC && usingDerbyNetClient())
        {
            // DERBY-584 inconsistency in numeric & decimal
            assertEquals("DATA_TYPE",
                    Types.DECIMAL, rsmdt.getColumnType(col));
            
            assertEquals("TYPE_NAME",
                    "DECIMAL", rsmdt.getColumnTypeName(col));
                       
            assertEquals("TYPE_NAME",
                    "NUMERIC", rs.getString("TYPE_NAME"));
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
            maxScale = 31; // Max Scale for Decimal & Numeric is 31: Derby-2262
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
            
            if (createParams.indexOf("scale") != -1)
            {
                int maxPrecision = rs.getInt("PRECISION");
                StringBuffer sb = new StringBuffer();
                int precision = rand.nextInt(maxPrecision) + 1;
                sb.append(typeName);
                sb.append("(");
                sb.append(precision);
                // Most DECIMAL usage does have a scale
                // but randomly pick some that do not.
                if (rand.nextInt(100) < 95) {
                    sb.append(",");
                    sb.append(rand.nextInt(precision+1));
                }
                sb.append(")");
                list.add(sb.toString());
                continue;
            }
            
            if (createParams.indexOf("precision") != -1)
            {
                list.add(typeName);
                continue;
            }
            
            fail("unknown how to generate valid type for " + typeName
                    + " CREATE_PARAMS=" + createParams);
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
            return Types.DOUBLE;
        
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
    
    /**
     * Given a valid SQL type return the corresponding
     * precision/length for this specific value
     * if the type is variable, e.g. CHAR(5) will
     * return 5, but LONG VARCHAR will return 0.
     */
    public static int getPrecision(int jdbcType, String type)
    {
        switch (jdbcType)
        {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.CLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            int lp = type.indexOf('(');
            int rp = type.indexOf(')');
            int precision =
                Integer.valueOf(type.substring(lp+1, rp)).intValue();
            return precision;

        default:
            return 0;
        }
    }
 
    /**
     * Execute and check the ODBC variant of getImported/Exported keys, which
     * uses the SQLFOREIGNKEYS system procedure to provide the same information
     * to ODBC clients.  Note that for "correctness" we just compare the results
     * to those of the equivalent JDBC calls; this fixture assumes that the
     * the JDBC calls return correct results (testing of the JDBC results occurs
     * elsewhere, esp. jdbcapi/metadata_test.java).
     */
    public void testGetXXportedKeysODBC() throws SQLException, IOException
    {
        Statement st = createStatement();

        // Create some simple tables with primary/foreign keys.

        st.execute("create table pkt1 (i int not null, c char(1) not null)");
        st.execute("create table pkt2 (i int not null, c char(1) not null)");
        st.execute("create table pkt3 (i int not null, c char(1) not null)");

        st.execute("alter table pkt1 add constraint pk1 primary key (i)");
        st.execute("alter table pkt2 add constraint pk2 primary key (c)");
        st.execute("alter table pkt3 add constraint pk3 primary key (i, c)");

        st.execute("create table fkt1 (fi int, fc char(1), vc varchar(80))");
        st.execute("create table fkt2 (fi int, fc char(1), vc varchar(80))");

        st.execute("alter table fkt1 add constraint fk1 foreign key (fi) " +
            "references pkt1(i)");

        st.execute("alter table fkt1 add constraint fk2 foreign key (fc) " +
            "references pkt2(c)");

        st.execute("alter table fkt2 add constraint fk3 foreign key (fi, fc) " +
            "references pkt3(i, c)");

        /* Check for all arguments NULL; SQLFOREIGNKEYS allows this, though
         * there is no equivalent in JDBC.
         */
        checkODBCKeys(null, null, null, null, null, null);

        /* Run equivalent of getImportedKeys(), getExportedKeys(),
         * and getCrossReference for each of the primary/foreign
         * key pairs.
         */

        checkODBCKeys(null, null, null, null, null, "FKT1");
        checkODBCKeys(null, null, "PKT1", null, null, null);
        checkODBCKeys(null, null, "PKT1", null, null, "FKT1");

        checkODBCKeys(null, null, null, null, null, "FKT2");
        checkODBCKeys(null, null, "PKT2", null, null, null);
        checkODBCKeys(null, null, "PKT2", null, null, "FKT2");

        checkODBCKeys(null, null, null, null, null, "FKT3");
        checkODBCKeys(null, null, "PKT3", null, null, null);
        checkODBCKeys(null, null, "PKT3", null, null, "FKT3");

        // Reverse primary and foreign tables.

        checkODBCKeys(null, null, "FKT1", null, null, null);
        checkODBCKeys(null, null, null, null, null, "PKT3");
        checkODBCKeys(null, null, "FKT1", null, null, "PKT1");
        checkODBCKeys(null, null, "FKT2", null, null, "PKT2");
        checkODBCKeys(null, null, "FKT3", null, null, "PKT3");

        // Mix-and-match primary key tables and foreign key tables.

        checkODBCKeys(null, null, "PKT1", null, null, "FKT2");
        checkODBCKeys(null, null, "PKT1", null, null, "FKT3");
        checkODBCKeys(null, null, "PKT2", null, null, "FKT3");

        checkODBCKeys(null, null, "FKT1", null, null, "PKT2");
        checkODBCKeys(null, null, "FKT1", null, null, "PKT3");
        checkODBCKeys(null, null, "FKT2", null, null, "PKT3");

        // Cleanup.

        st.execute("drop table fkt1");
        st.execute("drop table fkt2");
        st.execute("drop table pkt1");
        st.execute("drop table pkt2");
        st.execute("drop table pkt3");
        st.close();
    }

    /**
     * Execute a call to the ODBC system procedure "SQLFOREIGNKEYS"
     * and verify the results by comparing them with the results of
     * an equivalent JDBC call (if one exists).
     */
    private void checkODBCKeys(String pCatalog, String pSchema,
        String pTable, String fCatalog, String fSchema, String fTable)
        throws SQLException, IOException
    {
        /* To mimic the behavior of the issue which prompted this test
         * (DERBY-2758) we only send the "ODBC" option; we do *not*
         * explicitly send the "IMPORTEDKEY=1" nor "EXPORTEDKEY=1"
         * options, as DB2 Runtime Client does not send those, either.
         * This effectively means that the SQLFOREIGNKEYS function
         * will always be mapped to getCrossReference() internally.
         * Since that worked fine prior to 10.3, we need to preserve
         * that behavior if we want to maintina backward compatibility.
         */
        CallableStatement cs = prepareCall(
            "CALL SYSIBM.SQLFOREIGNKEYS(?, ?, ?, ?, ?, ?, " +
            "'DATATYPE=''ODBC''')");

        cs.setString(1, pCatalog);
        cs.setString(2, pSchema);
        cs.setString(3, pTable);
        cs.setString(4, fCatalog);
        cs.setString(5, fSchema);
        cs.setString(6, fTable);
        
        cs.execute();
        ResultSet odbcrs = cs.getResultSet();
        assertNotNull(odbcrs);
        
        /* Returned ResultSet will have the same shape as
         * DatabaseMetaData.getImportedKeys()
         */
        checkODBCKeysShape(odbcrs);
        
        /* Expect the contents of JDBC and ODBC metadata to be the same,
         * except if both pTable and cTable are null.  In that case
         * ODBC treats everything as a wildcard (and so effectively
         * returns all foreign key columns), while JDBC throws
         * an error.
         */

        ResultSet dmdrs = null;
        if ((pTable != null) && (fTable == null))
            dmdrs = getDMD().getExportedKeys(pCatalog, pSchema, pTable);
        else if ((pTable == null) && (fTable != null))
            dmdrs = getDMD().getImportedKeys(fCatalog, fSchema, fTable);
        else if (pTable != null)
        {
            dmdrs = getDMD().getCrossReference(
                pCatalog, pSchema, pTable, fCatalog, fSchema, fTable);
        }
        else
        {
            /* Must be the case of pTable and fTable both null.  Check
             * results for ODBC (one row for each foreign key column)
             * and assert error for JDBC.
             */

            JDBC.assertFullResultSet(odbcrs,
                new String [][] {
                    {"","APP","PKT1","I","","APP","FKT1","FI",
                        "1","3","3","FK1","PK1","7"},
                    {"","APP","PKT2","C","","APP","FKT1","FC",
                        "1","3","3","FK2","PK2","7"},
                    {"","APP","PKT3","I","","APP","FKT2","FI",
                        "1","3","3","FK3","PK3","7"},
                    {"","APP","PKT3","C","","APP","FKT2","FC",
                        "2","3","3","FK3","PK3","7"}
                });

            try {

                getDMD().getCrossReference(
                    pCatalog, pSchema, pTable, fCatalog, fSchema, fTable);

                fail("Expected error from call to DMD.getCrossReference() " +
                    "with NULL primary and foreign key tables.");

            } catch (SQLException se) {

                /* Looks like embedded and client have different (but similar)
                 * errors for this...
                 */
                assertSQLState(usingEmbedded() ? "XJ103" : "XJ110", se);

            }
            
        }
                
        /* If both pTable and fTable are null then dmdrs will be null, as
         * well.  So nothing to compare in that case.
         */
        if (dmdrs != null)
        {
            // Next call closes both results sets as a side effect.
            JDBC.assertSameContents(odbcrs, dmdrs);
        }
        
        cs.close();
    }

    /**
     * Check the shape of the ResultSet from a call to the ODBC function
     * SQLForeignKeys.
     */
    private void checkODBCKeysShape(ResultSet rs) throws SQLException
    {
        assertMetaDataResultSet(rs,

            // ODBC and JDBC agree on column names and types.

            new String[] {
                "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
                "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME",
                "PK_NAME", "DEFERRABILITY"
            },

            new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT
            },

            // Nullability comes from ODBC spec, not JDBC.

            /* DERBY-2797: Nullability of columns in ODBC's SQLForeignKey
             * result set is incorrect.  Un-comment the correct boolean array
             * when DERBY-2797 has been fixed.
             */

            // incorrect
            new boolean[] {
                true, false, false, false,
                true, false, false, false,
                true, true, true, false,
                false, true
            }

            // correct
            /* new boolean[] {
                true, true, false, false,
                true, true, false, false,
                false, true, true, true,
                true, true
            } */

        );        
    }
    
    /**
     * Test getBestRowIdentifier
     * @throws SQLException 
     */
    public void testGetBestRowIdentifier() throws SQLException
    {
        Statement st = createStatement();

        // First, create the test tables and indexes/keys
        // Create 5 tables which have only one best row identifier
        st.execute("create table brit1 (i int not null primary key, j int)");
        st.execute("create table brit2 (i int not null unique, j int)");
        // adding not null unique to j - otherwise brit2 & brit3 would be same.
        st.execute("create table brit3 (i int not null unique, " +
                "j int not null unique)");
        st.execute("create table brit4 (i int, j int)");
        st.execute("create unique index brit4i on brit4(i)");
        st.execute("create table brit5 (i int, j int)");
        // following have more than one best row identifier
        st.execute("create table brit6 (i int not null unique, " +
                "j int not null primary key)");
        // PK preferred to unique index
        st.execute("create table brit7 (i int not null, " +
                "j int not null primary key)");
        st.execute("create unique index brit7i on brit7(i)");
        // unique con preferred to unique index
        st.execute("create table brit8 (i int not null, " +
                "j int not null unique)");
        st.execute("create unique index brit8i on brit8(i)");
        // non-unique index just ignored
        st.execute("create table brit9 (i int, j int)");
        st.execute("create index brit9i on brit9(i)");
        // fewer cols unique con still ignored over primary key
        st.execute("create table brit10 " +
                "(i int unique not null , j int not null, primary key (i,j))");
        // fewer cols unique index still ignored over primary key
        st.execute("create table brit11 (i int not null, j int not null, "
                + "primary key (i,j))");
        st.execute("create unique index brit11i on brit11(i)");
        // fewer cols unique index still ignored over unique con
        st.execute("create table brit12 (i int not null, j int not null, "
                + "unique (i,j))");
        st.execute("create unique index brit12i on brit12(i)");
        st.execute("create table brit13 (i int not null, j int not null, k "
                + "int, unique (i,j))");
        // fewest cols unique con is the one picked of several
        st.execute("create table brit14 (i int not null unique, j int not "
                + "null, k int, unique (i,j))");
        // fewest cols unique index is the one picked of several
        st.execute("create table brit15 (i int not null, j int not null, k int)");
        st.execute("create unique index brit15ij on brit15(i,j)");
        st.execute("create unique index brit15i on brit15(i)");
        st.execute("create table brit16 (i int not null primary key, j int)");
        // from old metadata test
        // DERBY-3180; if this table gets created here, running the entire test
        // twice with defaultSuite runs into into trouble.
        // Moving into separate fixture does not have this problem.
        st.execute("create table brit17 (i int not null default 10, " +
                "s smallint not null, c30 char(30) not null, " +
                "vc10 varchar(10) not null default 'asdf', " +
                "constraint PRIMKEY primary key(vc10, i), " +
                "constraint UNIQUEKEY unique(c30, s), ai bigint " +
                "generated always as identity " +
                "(start with -10, increment by 2001))");
        // Create another unique index on brit17
        st.execute("create unique index brit17i on brit17(s, i)");
        // Create a non-unique index on brit17
        st.execute("create index brit17ij on brit17(s)");

        getConnection().setAutoCommit(false);
        
        // except for the last table, the expected results are
        // column i, column j, or columns i and j.
        String [][] expRSI = {
                {"2", "I", "4", "INTEGER", "4", null, "10", "1"}};
        String [][] expRSJ = {
                {"2", "J", "4", "INTEGER", "4", null, "10", "1"}};
        String [][] expRSIJ = {
                {"2", "I", "4", "INTEGER", "4", null, "10", "1"},
                {"2", "J", "4", "INTEGER", "4", null, "10", "1"}};
        // not used unless DERBY-3182 is fixed
        // String [][] expRSK =
        //    {"2", "K", "4", "INTEGER", "4", null, "10", "1"},

        boolean [] nullability = {
                true, false, true, true, true, true, true, true};
        
        DatabaseMetaData dmd = getConnection().getMetaData();

        // result: column i
        ResultSet rs = dmd.getBestRowIdentifier(null,"APP","BRIT1",0,true);
        verifyBRIResults(rs, expRSI, nullability);

        // result: column i
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT2",0,true);
        verifyBRIResults(rs, expRSI, nullability);

        // result: column j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT3",0,true);
        verifyBRIResults(rs, expRSJ, nullability);
        
        // result: column i
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT4",0,true);
        verifyBRIResults(rs, expRSI, nullability);
        
        // result: columns i and j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT5",0,true);
        verifyBRIResults(rs, expRSIJ, nullability);

        // result: column j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT6",0,true);
        verifyBRIResults(rs, expRSJ, nullability);

        // result: column j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT7",0,true);
        verifyBRIResults(rs, expRSJ, nullability);
        
        // result: column j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT8",0,true);
        verifyBRIResults(rs, expRSJ, nullability);
        
        // result: columns i,j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT9",0,true);
        verifyBRIResults(rs, expRSIJ, nullability);
        
        // result: columns i,j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT10",0,true);
        verifyBRIResults(rs, expRSIJ, nullability);
        
        // result: columns i,j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT11",0,true);
        verifyBRIResults(rs, expRSIJ, nullability);
        
        // result: columns i,j
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT12",0,true);
        verifyBRIResults(rs, expRSIJ, nullability);
        
        // DERBY-3182: we aren't handling nullOk flag correctly we 
        // just drop nullable cols, we should skip an answer that 
        // has nullable cols in it instead and look for another one.

        // result: columns i, j (WRONG) the correct answer is k: 
        // the non-null columns of the table
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT13",0,false);
        verifyBRIResults(rs, expRSIJ, nullability);
        
        // result: columns i
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT14",0,true);
        verifyBRIResults(rs, expRSI, nullability);
        
        // result: columns i
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT15",0,true);
        verifyBRIResults(rs, expRSI, nullability);
        
        // we don't do anything with SCOPE except detect bad values
        // result: columns i
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT16",1,true);
        verifyBRIResults(rs, expRSI, nullability);
        // result: columns i
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT16",2,true);
        verifyBRIResults(rs, expRSI, nullability);
        // result: no rows
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT16",-1,true);
        // column nullability is opposite to with scope=1 or 2...DERBY-3181
        nullability = new boolean [] {
                false, true, false, true, false, false, false, false};
        assertBestRowIdentifierMetaDataResultSet(rs, nullability);
        JDBC.assertDrainResults(rs, 0);
        // result: no rows
        rs = dmd.getBestRowIdentifier(null,"APP","BRIT16",3,true);
        assertBestRowIdentifierMetaDataResultSet(rs, nullability);
        JDBC.assertDrainResults(rs, 0);
        // set back nullability
        nullability = new boolean[] {
                true, false, true, true, true, true, true, true};
        
        rs = dmd.getBestRowIdentifier(null, "APP","BRIT17",0,true);
        String [][] expRS = new String [][] {
                {"2", "I", "4", "INTEGER", "4", null, "10", "1"},
                {"2", "VC10", "12", "VARCHAR", "10", null, null, "1"}
        };
        verifyBRIResults(rs, expRS, nullability);
      
        // test DERBY-2610 for fun; can't pass in null table name
        try {
            rs = dmd.getBestRowIdentifier(null,"APP",null,3,true);
        } catch (SQLException sqle) {
            assertSQLState( "XJ103", sqle);
        }
        
        // check on systables
        rs = dmd.getBestRowIdentifier(null,"SYS","SYSTABLES",0,true);
        expRS = new String [][] {
                {"2", "TABLEID", "1", "CHAR", "36", null, null, "1"}
        };
        verifyBRIResults(rs, expRS, nullability);
        
        getConnection().setAutoCommit(true);
        
        st.execute("drop table brit1");
        st.execute("drop table brit2");
        st.execute("drop table brit3");
        st.execute("drop index brit4i");
        st.execute("drop table brit4");
        st.execute("drop table brit5");
        st.execute("drop table brit6");
        st.execute("drop index brit7i");
        st.execute("drop table brit7");
        st.execute("drop index brit8i");
        st.execute("drop table brit8");
        st.execute("drop index brit9i");
        st.execute("drop table brit9");
        st.execute("drop table brit10");
        st.execute("drop index brit11i");
        st.execute("drop table brit11");
        st.execute("drop index brit12i");
        st.execute("drop table brit12");
        st.execute("drop table brit13");
        st.execute("drop table brit14");
        st.execute("drop index brit15i");
        st.execute("drop index brit15ij");
        st.execute("drop table brit15");
        st.execute("drop table brit16");
        st.execute("drop index brit17i");
        st.execute("drop index brit17ij");
        st.execute("drop table brit17");

        st.close();
    }
    
    /**
     * helper method for test testGetBestRowIdentifier
     * @param rs - Resultset from dmd.getBestRowIdentifier
     * @param expRS - bidimensional String array with expected result row(s)
     * @param nullability - boolean array holding expected nullability
     *   values. This needs to be a paramter because of DERBY-3081
     * @throws SQLException 
     */
    public void verifyBRIResults(ResultSet rs, String[][] expRS,
            boolean[] nullability) throws SQLException {
        assertBestRowIdentifierMetaDataResultSet(rs, nullability);
        JDBC.assertFullResultSet(rs, expRS, true);
    }
    
    // helper method for testGetBestRowIdentifier
    public void assertBestRowIdentifierMetaDataResultSet(
            ResultSet rs, boolean[] nullability) throws SQLException {
        String[] columnNames = {
                "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", 
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", 
                "PSEUDO_COLUMN"};
        int[] columnTypes = {
                Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.SMALLINT};
        assertMetaDataResultSet(rs, columnNames, columnTypes, nullability);
    } 
}
