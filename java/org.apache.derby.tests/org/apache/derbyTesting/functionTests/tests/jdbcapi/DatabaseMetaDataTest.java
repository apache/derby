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
   See the License for the specific language go1481
   verning permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.tests.upgradeTests.Version;
import org.apache.derbyTesting.functionTests.util.Barrier;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the DatabaseMetaData api.
 * <P>
 * For a number of methods that return a ResultSet to determine the
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
    /**
     * The schema used by the test.
     * <p>
     * Added to avoid other tests interfering with our metadata queries.
     * Configured by running the test with a specific user, i.e. use
     * {@linkplain TestConfiguration#changeUserDecorator}.
     */
    private String schema;

    public DatabaseMetaDataTest(String name) {
        super(name);
    }
    
    /**
     * Set the schema name to be used by the test.
     * @throws java.lang.Exception
     */
    @Override
    protected void setUp()
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            throws Exception {
        // Currently there are no tests that depend on data created outside
        // of the test ficture itself. This means we can relax the retrictions 
        // on the user name, and thus schema; we don't need to keep it the same
        // over all test fixtures.
        schema = TestConfiguration.getCurrent().getUserName();
        // Only a prerequisite for one test, but enforce it here (fail-fast).
        assertTrue("schema name must be at least three characters long",
                schema.length() > 2);
    }

    @Override
    protected void tearDown() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        if (modifiedDatabase)
        {
            Connection conn = getConnection();
            conn.setAutoCommit(false);

//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
            DatabaseMetaData dmd = getDMD();
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            for (String IDS1 : IDS) {
                JDBC.dropSchema(dmd, getStoredIdentifier(IDS1));
            }
  
            commit();
        }
        super.tearDown();
    }
    
    /**
     * Default suite for running this test.
     * @return the suite
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("DatabaseMetaDataTest");
        suite.addTest(
            TestConfiguration.defaultSuite(DatabaseMetaDataTest.class));
//IC see: https://issues.apache.org/jira/browse/DERBY-3350
//IC see: https://issues.apache.org/jira/browse/DERBY-3171

        // Add some tests to be run with connection pooling enabled.
        suite.addTest(connectionPoolingSuite("embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                    connectionPoolingSuite("client")));

        // Test for DERBY-2584 needs a fresh database to ensure that the
        // meta-data queries haven't already been compiled. No need to run the
        // test in client/server mode since it only tests the compilation of
        // meta-data queries.
        suite.addTest(
            TestConfiguration.singleUseDatabaseDecorator(
                new DatabaseMetaDataTest("initialCompilationTest")));
//IC see: https://issues.apache.org/jira/browse/DERBY-3850
//IC see: https://issues.apache.org/jira/browse/DERBY-177
//IC see: https://issues.apache.org/jira/browse/DERBY-3693

        // The test for DERBY-4160 needs a fresh database to ensure that the
        // meta-data queries haven't already been compiled.
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(
                new DatabaseMetaDataTest("concurrentCompilationTest")));

        // Test for DERBY-3693 needs a fresh database to ensure that the size
        // of SYSTABLES is so small that creating a relatively small number of
        // tables will cause the query plan for getTables() to be invalidated.
        // Also, set a high lock timeout explicitly so that we can check that
        // an internal timeout followed by a retry didn't happen, and set
        // derby.language.stalePlanCheckInterval to a low value so that the
        // invalidation happens earlier.
        Properties props = new Properties();
        props.setProperty("derby.locks.waitTimeout", "90");
        props.setProperty("derby.language.stalePlanCheckInterval", "5");
        suite.addTest(
            TestConfiguration.singleUseDatabaseDecorator(
                new DatabasePropertyTestSetup(
                    new DatabaseMetaDataTest("recompileTimeoutTest"),
                    props, true)));

        return suite;
    }
    
    /**
     * Returns a suite of tests to be run with connection pooling enabled.
     *
     * @param jdbcClient name of the client being used (for verbosity only)
     * @return A suite of tests.
     */
    private static Test connectionPoolingSuite(String jdbcClient) {
        // Return an empty suite if running in JavaME environment.
//IC see: https://issues.apache.org/jira/browse/DERBY-3685
        if (JDBC.vmSupportsJSR169()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("Base connection pooling suite:DISABLED");
        }

        BaseTestSuite baseCpSuite =
            new BaseTestSuite("Base connection pooling suite");
        // Add the tests here.
        baseCpSuite.addTest(new DatabaseMetaDataTest("testConnectionSpecific"));
//IC see: https://issues.apache.org/jira/browse/DERBY-3431

        // Setup the two configurations; CPDS and XADS.
        BaseTestSuite fullCpSuite = new BaseTestSuite(
                "DatabaseMetaData with connection pooling:" + jdbcClient);
        BaseTestSuite cpSuite = new BaseTestSuite("ConnectionPoolDataSource");
        BaseTestSuite xaSuite = new BaseTestSuite("XADataSource");
        cpSuite.addTest(TestConfiguration.connectionCPDecorator(baseCpSuite));
        xaSuite.addTest(TestConfiguration.connectionXADecorator(baseCpSuite));
        fullCpSuite.addTest(cpSuite);
        fullCpSuite.addTest(xaSuite);

        return fullCpSuite;
    }

    /**
     * Return the identifiers used to create schemas,
     * tables etc. in the order the database stores them.
     * @return identifiers
     */
    private String[] getSortedIdentifiers()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
    
    private DatabaseMetaData getDMD() throws SQLException
    {
        return getConnection().getMetaData();
    }

    /**
     * Tests that a meta-data query is compiled and stored correctly even when
     * there's a lock on the system tables (DERBY-2584). This test must run on
     * a fresh database (that is, <code>getIndexInfo</code> must not have been
     * prepared and stored in <code>SYS.SYSSTATEMENTS</code>).
     *
     * @throws java.sql.SQLException
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
     * Test that a meta-data query is compiled and stored correctly even when
     * there's a lock conflict that causes the first attempt to store it to
     * stop midway (DERBY-4160). This test needs a fresh database so that the
     * meta-data calls are not already compiled.
     * @throws java.lang.Exception
     */
    public void concurrentCompilationTest() throws Exception {
        // Create a barrier that can be used to synchronize the two threads
        // so they perform the meta-data compilation at the same time.
        final Barrier barrier = new Barrier(2);

        // Create a thread thread that attempts to compile meta-data queries.
        final DatabaseMetaData dmd = getDMD();
        final Exception[] exception = new Exception[1];
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    concurrentCompilationTestHelper(barrier, dmd);
                } catch (Exception e) {
                    exception[0] = e;
                }
            }
        };
        th.start();

        // At the same time, in the main thread, attempt to compile the same
        // meta-data queries.
        Connection c2 = openDefaultConnection();
        concurrentCompilationTestHelper(barrier, c2.getMetaData());
        c2.close();

        // Wait until both threads are done.
        th.join();

        // Check if the helper thread got any exceptions.
        if (exception[0] != null) {
            fail("Exception in other thread", exception[0]);
        }

        // Finally, verify that the two meta-data methods used in the test
        // are working.
        testGetBestRowIdentifier();
        testGetIndexInfo();
    }

    private void concurrentCompilationTestHelper(
            Barrier barrier, DatabaseMetaData dmd) throws Exception {
        // Wait until the other thread is ready to start, so that the
        // compilation happens at the same time in both threads.
        barrier.await();

        // Often, but not always, the getIndexInfo() call would fail
        // in one of the threads with the following error message:
        // ERROR X0Y68: Column 'PARAM1' already exists.
        ResultSet rs1 = dmd.getBestRowIdentifier(null, null, "", 0, true);
        ResultSet rs2 = dmd.getIndexInfo(null, null, "", true, true);
        JDBC.assertDrainResults(rs1);
        JDBC.assertDrainResults(rs2);
    }

    /**
     * Tests that we don't get an internal timeout when a meta-data statement
     * is recompiled because the size of the tables it queries has changed
     * (DERBY-3693). The test must be run on a fresh database, to ensure that
     * SYSTABLES initially has a relatively small number of records. The lock
     * timeout must be high (more than 60 seconds) to enable us to see the
     * difference between an internal lock timeout and slow execution.
     * derby.language.stalePlanCheckInterval should be set to 5 (the lowest
     * possible value) so that we don't have to wait long for the query plan
     * to be invalidated.
     * @throws java.sql.SQLException
     */
    public void recompileTimeoutTest() throws SQLException {
        DatabaseMetaData dmd = getDMD();

        // Make sure getTables() is initially compiled while SYSTABLES is small
        JDBC.assertDrainResults(dmd.getTables(null, "%", "%", null));

        // Grow SYSTABLES
        Statement s = createStatement();
        for (int i = 0; i < 20; i++) {
            s.executeUpdate("create table t" + i + "(x int)");
        }

        // Execute getTables() derby.language.stalePlanCheckInterval times so
        // that its plan is invalidated. Before DERBY-3693 was fixed, the
        // recompilation after the invalidation would get an internal timeout
        // and take very long time to complete.
        for (int i = 0; i < 5; i++) {
            long time = System.currentTimeMillis();
            JDBC.assertDrainResults(dmd.getTables(null, "%", "T0", null));
            time = System.currentTimeMillis() - time;
            if (time > 60000) {
                fail("getTables() took a very long time, possibly because " +
                     "of an internal timeout. i=" + i + ", time=" + time);
            }
        }
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-462
        assertFalse(dmd.supportsConvert());
        // Simple check since convert is not supported.
        // A comprehensive test should be added when convert
        // is supported, though most likely in a test class
        // specific to convert.
        assertFalse(dmd.supportsConvert(Types.INTEGER, Types.SMALLINT));
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3146
        assertEquals(128, dmd.getMaxUserNameLength());
    }
    
    public void testMiscellaneous() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        DatabaseMetaData dmd = getDMD();
        
        assertTrue(dmd.allProceduresAreCallable());
        assertTrue(dmd.allTablesAreSelectable());
        assertFalse(dmd.dataDefinitionCausesTransactionCommit());
        assertFalse(dmd.dataDefinitionIgnoredInTransactions());
        
        // Derby does not yet implement scroll sensitive resultsets, so can't
        //   see changes for those; all *AreDetected and *AreVisible methods
        //   return false.
        // For Forward Only ResultSets, also see lang.UpdatableResultSetTest
        
        // *AreDetected; expect true for updates and deletes of 
        //   TYPE_SCROLL_INSENSITIVE, all others should be false
        assertFalse(dmd.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        assertFalse(dmd.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(dmd.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(dmd.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        
        // others*AreVisible
        // Since Derby materializes a forward only ResultSet incrementally, 
        //   it is possible to see changes for FORWARD_ONLY
        // Scroll insensitive ResultSet by their definition do not see changes
        //   made by others
        assertTrue(dmd.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(dmd.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertTrue(dmd.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(dmd.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertTrue(dmd.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(dmd.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        
        // Scroll insensitive ResultSets see updates, deletes, but not inserts
        assertFalse(dmd.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(dmd.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(dmd.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(dmd.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
                
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2789
        assertTrue(dmd.locatorsUpdateCopy());
        
        assertTrue(dmd.usesLocalFilePerTable());
        assertTrue(dmd.usesLocalFiles());
    }
    
    /**
     * Methods that describe the version of the
     * driver and database.
     *
     * @throws java.sql.SQLException
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
        int expectedJDBCMinor = -1;
        // java 8 - jdbc 4.2
//IC see: https://issues.apache.org/jira/browse/DERBY-6324
        if (JDBC.vmSupportsJDBC42())
        {
            expectedJDBCMajor = 4;
            expectedJDBCMinor = 2;
        }
        // java 7 - jdbc 4.1
        else if (JDBC.vmSupportsJDBC41())
        {
            expectedJDBCMajor = 4;
            expectedJDBCMinor = 1;
        }
        // java 6 - jdbc 4.0
        else if (JDBC.vmSupportsJDBC4())
        {
            expectedJDBCMajor = 4;
            expectedJDBCMinor = 0;
        }
         
        if (expectedJDBCMajor != -1)
        {
            assertEquals("JDBC Major version",
                    expectedJDBCMajor, jdbcMajor);
            assertEquals("JDBC Minor version", expectedJDBCMinor, jdbcMinor);
        }
    }
    
    /**
     * getURL() method. Note that this method
     * is the only JDBC 3 DatabaseMetaData method
     * that is dropped in JSR169.
     *
     * @throws java.sql.SQLException
     */
    public void testGetURL() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        DatabaseMetaData dmd = getDMD();
        
        String url;
        try {
            url = dmd.getURL();
        } catch (NoSuchMethodError e) {
            // JSR 169 - method must not be there!
            assertTrue("getURL not supported", JDBC.vmSupportsJSR169());
//IC see: https://issues.apache.org/jira/browse/DERBY-2024
            assertFalse("getURL not supported", JDBC.vmSupportsJDBC3());
            return;
        }
        
        assertFalse("getURL is supported!", JDBC.vmSupportsJSR169());
        assertTrue("getURL is supported!", JDBC.vmSupportsJDBC3());

        TestConfiguration config = getTestConfiguration();
        String expectedURL = config.getJDBCUrl();

        // DERBY-4886: Embedded returns the URL without connection attributes,
        // client returns the URL with connection attributes.
        if (usingDerbyNetClient()) {
            List<String> urlComponents = Arrays.asList(url.split(";"));
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

            // Only compare whatever comes before the first semi-colon with
            // the expected URL. Check connection attributes separately.
            url = urlComponents.get(0);

            // Put each actual connection attribute in a HashSet for easy
            // comparison.
            HashSet<String> attrs = new HashSet<String>(
                    urlComponents.subList(1, urlComponents.size()));

            // Put each expected connection attribute in a HashSet.
            HashSet<String> expectedAttrs = new HashSet<String>();
            Properties ca = config.getConnectionAttributes();
            for (String key : ca.stringPropertyNames()) {
                expectedAttrs.add(key + '=' + ca.getProperty(key));
            }

            // Verify that the actual connection attributes match the
            // expected attributes. Order is irrelevant.
            assertEquals("Connection attributes don't match",
                         expectedAttrs, attrs);
        }

        assertEquals("getURL match", expectedURL, url);
    }
    
    /**
     * Derby stores unquoted identifiers as upper
     * case and quoted ones as mixed case.
     * They are always compared case sensitive.
     *
     * @throws java.sql.SQLException
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
     *
     * @throws java.sql.SQLException
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
     *
     * @throws java.sql.SQLException
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
     *
     * @throws java.sql.SQLException
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
      assertEquals(DatabaseMetaData.columnNoNulls, ResultSetMetaData.columnNoNulls);
      assertEquals(DatabaseMetaData.columnNullable, ResultSetMetaData.columnNullable);
      assertEquals(DatabaseMetaData.columnNullableUnknown, ResultSetMetaData.columnNullableUnknown);
    }
    
    /*
    ** DatabaseMetaData calls that return ResultSets.
    */
    
    /**
     * Test UDT-related metadata methods.
     * 
     * @throws java.lang.Exception
     */
    public void testUDTs() throws Exception
    {
        //
        // We only run this test if the database version is at least 10.6.
        // Otherwise we can't create a UDT.
        //
        Version dataVersion = getDataVersion( getConnection() );
        if ( dataVersion.compareTo( new Version( 10, 6, 0, 0 ) ) < 0 ) { return; }
        
        DatabaseMetaData dmd = getDMD();

        createObjectsForUDTTests();

        ResultSet rs = dmd.getUDTs(null,null,null,null);
        String[] columnNames = new String[] {
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME",
                "DATA_TYPE", "REMARKS", "BASE_TYPE"};
        int[] columnTypes = new int[] {
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.LONGVARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.SMALLINT
        };
        boolean[] nullability = new boolean[] {
            true, true, false, false,
            false, true, true
        };
        assertMetaDataResultSet(rs, columnNames, columnTypes, nullability);

        String[][] expectedRows = new String[][]
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            {  null, schema, "PRICE", "org.apache.derbyTesting.functionTests.tests.lang.Price", "2000", null, null },
        };
        JDBC.assertFullResultSet( rs, expectedRows );

        // now try the test, specifying a specific type of UDT
        rs = dmd.getUDTs( null, null, null, new int[] { Types.JAVA_OBJECT } );
        JDBC.assertFullResultSet( rs, expectedRows );

        rs = dmd.getUDTs( null, null, null, new int[] { Types.DISTINCT, Types.JAVA_OBJECT } );
        JDBC.assertFullResultSet( rs, expectedRows );

        // no UDTs of these types
        rs = dmd.getUDTs( null, null, null, new int[] { Types.DISTINCT, Types.STRUCT } );
        JDBC.assertEmpty(rs);

        // try explicit schema and type name
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = dmd.getUDTs( null, schema, "PRICE",
                new int[] { Types.DISTINCT, Types.JAVA_OBJECT } );
        JDBC.assertFullResultSet( rs, expectedRows );

        rs = dmd.getUDTs( null, schema.substring(0, 2) + "%", "PRI%",
                new int[] { Types.DISTINCT, Types.JAVA_OBJECT } );
        JDBC.assertFullResultSet( rs, expectedRows );
        
        rs = dmd.getUDTs( null, "FOO", "PRICE", new int[] { Types.DISTINCT, Types.JAVA_OBJECT } );
        JDBC.assertEmpty(rs);

        // now make sure that getColumns() returns the right data
        rs = dmd.getColumns( null, schema, "ORDERS", null );
        expectedRows = new String[][]
        {
            {
                "", schema, "ORDERS", "TOTALPRICE",
                "2000", "\"" + schema + "\".\"PRICE\"", "-1", null,
                null, null, "1", "",
                null, null, null, null,
                "1", "YES", null, null,
                null, null, "NO", "NO", null
            },
        };
        JDBC.assertFullResultSet( rs, expectedRows );
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = dmd.getColumns( null, schema, "ORDERS", null );
        crossCheckGetColumnsAndResultSetMetaData( rs, false, 0 );

        dropObjectsForUDTTests();
    }

    /**
     * Create objects needed to test the UDT-related metadata calls
     *
     * @throws java.sql.SQLException
     */
    private void createObjectsForUDTTests() throws SQLException
    {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();

        s.execute( "create type price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java" );
        s.execute( "create table orders( totalPrice price )" );

        commit();
        getConnection().setAutoCommit(true);
    }

    /**
     * Drop the objects needed for testing the UDT-related metadata methods
     *
     * @throws java.sql.SQLException
     */
    private void dropObjectsForUDTTests() throws SQLException
    {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();

        s.execute( "drop table orders" );
        s.execute( "drop type price restrict" );

        commit();
        getConnection().setAutoCommit(true);
    }
    
    /**
     * Test methods that describe attributes of SQL Objects
     * that are not supported by derby. In each case the
     * metadata should return an empty ResultSet of the
     * correct shape, and with correct names, datatypes and 
     * nullability for the columns in the ResultSet. 
     * 
     * @throws java.sql.SQLException
     */
    public void testUnimplementedSQLObjectAttributes() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        ResultSet rs;
        
        rs = dmd.getAttributes(null,null,null,null);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3350
//IC see: https://issues.apache.org/jira/browse/DERBY-3171
                true, true, false, nullval, nullval, nullval, nullval,
                nullval, nullval, nullval, true, true, nullval, nullval,
                nullval, nullval, nullval, true, true, true, true
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3350
                true, true, false, false};
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3350
                true, true, false, true, true, false};
        assertMetaDataResultSet(rs, columnNames, columnTypes, nullability);
        JDBC.assertEmpty(rs);

//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        ResultSet rss[] = getVersionColumns(null,null, "No_such_table");
        checkVersionColumnsShape(rss);
        JDBC.assertEmpty(rss[0]);
        JDBC.assertEmpty(rss[1]);
        
        rs.close();
        rss[0].close();
        rss[1].close();
    }
    
    /**
     * Implement ODBC equivalent for getVersionColumns - SYSIBM.SQLCOLUMNS
     *
     * @param catalog
     * @param schema
     * @param table
     *
     * @return version
     * @throws java.sql.SQLException
     */
    public ResultSet getVersionColumnsODBC(
            String catalog, String schema, String table)
        throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLSPECIALCOLUMNS " +
            "(2, ?, ?, ?, 1, 1, 'DATATYPE=''ODBC''')");

        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getVersionColumns - calls 
     * dmd.getVersionColumns for the JDBC call, and getVersionColumnsODBC for
     * the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @return version
     * @throws SQLException 
     */
    private ResultSet[] getVersionColumns(
            String catalog, String schema, String table)
        throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getVersionColumns(catalog, schema, table);
        rss[1]= getVersionColumnsODBC(catalog, schema, table);
        return rss;        
    }

    /**
     * Six combinations of valid identifiers with mixed
     * case, to see how the various pattern matching
     * and returned values handle them.
     * This test only creates objects in these schemas.
     */
    private static final String[] IDS =
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        checkSchemas(rs, new String[0]);
    }
    
    /**
     * Test getSchemas().
     * 
     * @throws SQLException
     */
    public void testGetSchemasModify() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        createSchemasForTests();
        DatabaseMetaData dmd = getDMD();
        ResultSet rs = dmd.getSchemas();
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        checkSchemas(rs, IDS);
    }
    
    private void createSchemasForTests() throws SQLException
    {
        // Set to cleanup on teardown.
        modifiedDatabase = true;

        Statement s = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        for (String IDS1 : IDS) {
            s.executeUpdate("CREATE SCHEMA " + IDS1);
        }
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
     * @param rs
     * @param userExpected
     *
     * @throws java.sql.SQLException
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
     *
     * @param rs result set
     * @throws java.sql.SQLException
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        , new boolean[] {false, true}
        );        
    }
    
    /**
     * Execute dmd.getTables() but perform additional checking
     * of the ODBC variant.
     * @param dmd
     * @param catalog
     * @param schema
     * @param table
     * @param tableTypes
     * @return result set from query
     * @throws java.sql.SQLException
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
    public void testGetTablesModify() throws Exception {
                
        int totalTables = createTablesForTest(false);
        
        DatabaseMetaData dmd = getDMD();
        ResultSet rs;
        
        String[] userTableOnly = new String[] {"TABLE"};
        
        // Get the list of idenifiers from IDS as the database
        // would store them in the order required.      
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        String[] dbIDS = getSortedIdentifiers();    
               
        // Check the contents, ordered by TABLE_CAT, TABLE_SCHEMA, TABLE_NAME
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        rs = getDMDTables(dmd, null, null, null, userTableOnly);
        checkTablesShape(rs);
        int rowPosition = 0;
        while (rs.next())
        {
            //boolean ourTable;
            assertEquals("TABLE_CAT", "", rs.getString("TABLE_CAT"));
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            String schema_ = rs.getString("TABLE_SCHEM");
            
            // See if the table is in one of the schemas we created.
            // If not we perform what checking we can.
            boolean ourSchema = Arrays.binarySearch(dbIDS, schema_) >= 0;
            
            if (ourSchema) {        
                assertEquals("TABLE_SCHEM",
                    dbIDS[rowPosition/dbIDS.length], schema_);
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
            
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            if (ourSchema)
                rowPosition++;
         }
         rs.close();
         assertEquals("getTables count for all user tables",
               totalTables, rowPosition);
       
         Random rand = new Random();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        for (String schema_ : dbIDS) {
            int pc = rand.nextInt(6);
            String schemaPattern = schema_.substring(0, pc + 2) + "%";
            
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            rs = getDMDTables(dmd, null, schemaPattern, null, userTableOnly);
            checkTablesShape(rs);
            rowPosition = 0;
            while (rs.next())
            {
                assertEquals("TABLE_SCHEM",
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
                        schema_, rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE_NAME",
                        dbIDS[rowPosition%dbIDS.length], rs.getString("TABLE_NAME"));
                assertEquals("TABLE_TYPE", "TABLE", rs.getString("TABLE_TYPE"));
                rowPosition++;
            }
            rs.close();
            assertEquals("getTables count schema pattern",
                    dbIDS.length, rowPosition);
        }
         
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        for (String table : dbIDS) {
            int pc = rand.nextInt(6);
            String tablePattern = table.substring(0, pc + 2) + "%";
            
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
     * @param catalog
     * @param schema
     * @param table
     * @param tableTypes
     * @throws java.sql.SQLException
     * @throws IOException 
     */
    private void checkGetTablesODBC(String catalog, String schema,
            String table, String[] tableTypes) throws SQLException, IOException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        String tableTypesAsString = null;
        if (tableTypes != null) {
            int count = tableTypes.length;
            StringBuilder sb = new StringBuilder();
//IC see: https://issues.apache.org/jira/browse/DERBY-6623

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
     * @return total number of tables created
     * @throws SQLException
     */
    private int createTablesForTest(boolean skipXML) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        getConnection().setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        List<String> types = getSQLTypes(getConnection());
        if (skipXML)
            types.remove("XML");
            
        //
        // The BOOLEAN datatype is only allowed in databases
        // at level 10.7 or higher.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        Version dataVersion = getDataVersion( getConnection() );
        if ( dataVersion.compareTo( new Version( 10, 7, 0, 0 ) ) < 0 )
        {
            types.remove("BOOLEAN");
        }
        
        int typeCount = types.size();
               
        createSchemasForTests();
        
        Statement s = createStatement();
        
        int columnCounter = 0;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        for (String IDS1 : IDS) {
            for (String IDS2 : IDS) {
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ");
                sb.append(IDS1);
                sb.append('.');
                sb.append(IDS2);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242

        return IDS.length * IDS.length;
    }
    
    /**
     * Test getTableColumns().
     * Contents are compared to the ResultSetMetaData
     * for a SELECT * from the table. All columns in
     * all tables are checked.
     *
     * @throws java.lang.Exception
     */
    public void testGetColumnsReadOnly() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        ResultSet[] rs = getColumns(null, null, null, null);
        for ( int j =0 ; j<2 ; j++) {
            checkColumnsShape(rs[j], j);
            crossCheckGetColumnsAndResultSetMetaData(rs[j], false, j);
        }
    }
    
    /**
     * Test getColumns() with  modifying the database.
     * 
     * @throws SQLException
     */
    public void testGetColumnsModify() throws Exception {
           
        // skip XML datatype as our cross check with
        // ResultSetMetaData will fail
        int totalTables = createTablesForTest(true);
        
        // First cross check all the columns in the database
        // with the ResultSetMetaData.
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        testGetColumnsReadOnly();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        Random rand = new Random();
        String[] dbIDS = getSortedIdentifiers();
            
        for (int i = 1; i < 20; i++) {
            int seenColumnCount = 0;
            // These are the pattern matching parameters
            String schemaPattern = getPattern(rand, dbIDS);
            String tableNamePattern = getPattern(rand, dbIDS);
            String columnNamePattern = getPattern(rand, dbIDS);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            ResultSet[] rs = getColumns(null,
                schemaPattern, tableNamePattern, columnNamePattern);
            
            for (int j=0  ; j<2 ; j++) {
                checkColumnsShape(rs[j], j);

                while (rs[j].next())
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
                    String schema_ = rs[j].getString("TABLE_SCHEM");
                    String table = rs[j].getString("TABLE_NAME");
                    String column = rs[j].getString("COLUMN_NAME");

                    assertMatchesPattern(schemaPattern, schema_);
                    assertMatchesPattern(tableNamePattern, table);
                    assertMatchesPattern(columnNamePattern, column);

                    seenColumnCount++;
                }
                rs[j].close();
            }
            
            // Re-run to check the correct data is returned
            // when filtering is enabled
            rs = getColumns(null,
                    schemaPattern, tableNamePattern, columnNamePattern);
            for (int j=0  ; j<2 ; j++) {
                crossCheckGetColumnsAndResultSetMetaData(rs[j], true, j);
            }
            
            // Now re-execute fetching all schemas, columns etc.
            // and see we can the same result when we "filter"
            // in the application
            rs = getColumns(null,null, null, null);
            
            int appColumnCount = 0;
            for (int j=0  ; j<2 ; j++) {
                while (rs[j].next())
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
                    String schema_ = rs[j].getString("TABLE_SCHEM");
                    String table = rs[j].getString("TABLE_NAME");
                    String column = rs[j].getString("COLUMN_NAME");

                    if (!doesMatch(schemaPattern, 0, schema_, 0))
                        continue;               
                    if (!doesMatch(tableNamePattern, 0, table, 0))
                        continue;
                    if (!doesMatch(columnNamePattern, 0, column, 0))
                        continue;

                    appColumnCount++;
                }
                rs[j].close();
            }
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        StringBuilder sb = new StringBuilder();
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
     * @param rs resultset to crossCheck
     * @param partial used to indicate if ordinal position should get checked
     * @param odbc - flag to indicate if this was a resultset obtained
     *    from a JDBC (0) or ODBC (1) call.
     * @throws SQLException
     */
    private void crossCheckGetColumnsAndResultSetMetaData(ResultSet rs,
            boolean partial, int odbc)
    throws Exception
    {
        Statement s = createStatement();
        while (rs.next())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            String schema_ = rs.getString("TABLE_SCHEM");
            String table = rs.getString("TABLE_NAME");
            
            ResultSet rst = s.executeQuery(
                "SELECT * FROM " + JDBC.escape(schema_, table));
            ResultSetMetaData rsmdt = rst.getMetaData();

                     
            for (int col = 1; col <= rsmdt.getColumnCount() ; col++)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
                if (!partial) {
                    if (col != 1)
                        assertTrue(rs.next());
                
                    assertEquals("ORDINAL_POSITION",
                            col, rs.getInt("ORDINAL_POSITION"));
                }
                
                assertEquals("TABLE_CAT",
                        "", rs.getString("TABLE_CAT"));
                assertEquals("TABLE_SCHEM",
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
                        schema_, rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE_NAME",
                        table, rs.getString("TABLE_NAME"));
                
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
                crossCheckGetColumnRowAndResultSetMetaData(rs, rsmdt, odbc);
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
     * @param odbc 0 for JDBC call, 1 for ODBC. Needed to allow for difference
     *    in using BUFFER_LENGTH (ODBC) or no(JDBC).
     * @throws SQLException
     */
    public void crossCheckGetColumnRowAndResultSetMetaData(
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            ResultSet rs, ResultSetMetaData rsmdt, int odbc)
        throws Exception
    {
        int col = rs.getInt("ORDINAL_POSITION");
        Version dataVersion = getDataVersion( getConnection() );

        String catalogName = rs.getString("TABLE_CAT");
        String schemaName = rs.getString("TABLE_SCHEM");
        String tableName = rs.getString("TABLE_NAME");

        // Check that the catalog/schema/table names reported by the
        // ResultSetMetaData are correct. Note that for views, RSMD will
        // return data for the underlying table, not for the view itself.
        // Therefore, skip the check for views.
        ResultSet views =
            rs.getStatement().getConnection().getMetaData().getTables(
                    catalogName, schemaName, tableName, JDBC.GET_TABLES_VIEW);
        boolean isView = JDBC.assertDrainResults(views) > 0;

        if (!isView) {
            assertEquals("RSMD.getCatalogName",
                         catalogName, rsmdt.getCatalogName(col));
            assertEquals("RSMD.getSchemaName",
                         schemaName, rsmdt.getSchemaName(col));
            assertEquals("RSMD.getTableName",
                         tableName, rsmdt.getTableName(col));
        }
        
        assertEquals("COLUMN_NAME",
                rsmdt.getColumnName(col), rs.getString("COLUMN_NAME"));
        
        // DERBY-2285 BOOLEAN columns appear different on
        // network client.
        // DMD returns BOOLEAN
        // RSMD returns SMALLINT
        int dmdColumnType = rs.getInt("DATA_TYPE");
        if (dmdColumnType == Types.JAVA_OBJECT && usingDerbyNetClient()
                 &&  ( dataVersion.compareTo( new Version( 10, 6, 0, 0 ) ) < 0 ) )
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
        
        // not used by JDBC spec, but by ODBC
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        if (odbc == 0)
        {
            assertEquals("BUFFER_LENGTH", 0, rs.getInt("BUFFER_LENGTH"));
            assertTrue("BUFFER_LENGTH", rs.wasNull());
        }
        else
        {
            if (col == 0)
                assertEquals("BUFFER_LENGTH", 0, rs.getInt("BUFFER_LENGTH"));
            else
                assertTrue(rs.getInt("BUFFER_LENGTH") != 0);
        }
        
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
       
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        if (odbc == 0)
        {
            // both unused by JDBC spec
            assertEquals("SQL_DATA_TYPE", 0, rs.getInt("SQL_DATA_TYPE"));
            assertTrue(rs.wasNull());
            assertEquals("SQL_DATETIME_SUB", 0, rs.getInt("SQL_DATETIME_SUB"));
            assertTrue(rs.wasNull());
        }
        else {
            // ODBC uses formula:
            // DATA_TYPE = 10 * SQL_DATA_TYPE + SQL_DATETIME_SUB,
            // e.g SQL_TIME_STAMP = 10 * SQL_DATETIME + SQL_CODE_TIMESTAMP
            //                 93 = 10 * 9            + 3
            if (dmdColumnType == 91)
            {
                assertTrue(rs.getInt("SQL_DATA_TYPE")== 9);
                assertTrue(rs.getInt("SQL_DATETIME_SUB")== 1);
            }
            else if (dmdColumnType == 92)
            {
                assertTrue(rs.getInt("SQL_DATA_TYPE")== 9);
                assertTrue(rs.getInt("SQL_DATETIME_SUB")== 2);
            }
            else if (dmdColumnType == 93)
            {
                assertTrue(rs.getInt("SQL_DATA_TYPE")== 9);
                assertTrue(rs.getInt("SQL_DATETIME_SUB")== 3);
            }
            else
                assertTrue(rs.getInt("SQL_DATA_TYPE") == dmdColumnType);
        }
        
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
        assertNull("SCOPE_CATALOG", rs.getString("SCOPE_CATALOG"));
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
     * Implement ODBC equivalent for getColumns - SYSIBM.SQLCOLUMNS
     *
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @param columnNamePattern
     *
     * @return result set of query
     * @throws java.sql.SQLException
     */
    private ResultSet getColumnsODBC(
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern)
        throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLCOLUMNS(" +
                "?, ?, ?, ?, 'DATATYPE=''ODBC''')");

        cs.setString(1, catalog);
        cs.setString(2, schemaPattern);
        cs.setString(3, tableNamePattern);
        cs.setString(4, columnNamePattern);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getColumns - calls dmd.getColumns for
     * the JDBC call, and getColumnsODBC for the ODBC procedure
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @param columnNamePattern
     * @return Array of the two result sets of columns
     * @throws SQLException 
     */
    private ResultSet[] getColumns(
            String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern)
        throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getColumns(catalog, schemaPattern, tableNamePattern,
            columnNamePattern);
        rss[1]= getColumnsODBC(catalog, schemaPattern, tableNamePattern,
                columnNamePattern);
        return rss;        
    }
    
    /**
     * Test getTableTypes()
     *
     * @throws java.sql.SQLException
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
     *
     * @throws java.lang.Exception
     */
    public void testGetTypeInfo() throws Exception
    {
        // Client returns BOOLEAN type from the engine as SMALLINT
        int BOOLEAN = Types.BOOLEAN;      
        
        String[] JDBC_COLUMN_NAMES = new String[] {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
        
        // DERBY-2307 Nullablity is wrong for column 1 (1-based)
        // Modify JDBC_COLUMN_NULLABILITY to reflect current reality
        JDBC_COLUMN_NULLABILITY[1 - 1] = true;
      
        ResultSet rs = getDMD().getTypeInfo();
        assertMetaDataResultSet(rs, JDBC_COLUMN_NAMES, JDBC_COLUMN_TYPES
        , JDBC_COLUMN_NULLABILITY
        );

	/*
//IC see: https://issues.apache.org/jira/browse/DERBY-2258
//IC see: https://issues.apache.org/jira/browse/DERBY-2259
//IC see: https://issues.apache.org/jira/browse/DERBY-2260
//IC see: https://issues.apache.org/jira/browse/DERBY-2245
	 Derby-2258 Removed 3 data types which are not supported by Derby
	 and added XML data type which is supported by Derby
	*/
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        List<Integer> supportedTypes = new ArrayList<Integer>(Arrays.asList(
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
          Types.BIGINT, Types.BINARY, Types.BLOB, Types.BOOLEAN,
          Types.CHAR, Types.CLOB, Types.DATE,
          Types.DECIMAL, Types.DOUBLE, Types.FLOAT,
          Types.INTEGER, Types.LONGVARBINARY, Types.LONGVARCHAR,
          Types.NUMERIC, Types.REAL, Types.SMALLINT,
          Types.TIME, Types.TIMESTAMP,  Types.VARBINARY,
          Types.VARCHAR, JDBC.SQLXML, Types.JAVA_OBJECT
        ));
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        Version dataVersion = getDataVersion(getConnection());
        boolean booleanSupported =
                dataVersion.compareTo(new Version(10, 7, 0, 0)) >= 0;
        
        // DERBY-4946: Boolean isn't supported if DB is soft-upgraded from
        // pre-10.7 version
        if (!booleanSupported) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            supportedTypes.remove(Integer.valueOf(Types.BOOLEAN));
        }

        // Rows are returned from getTypeInfo in order of
        // "DATA_TYPE" (which is a constant from java.sql.Types)
        Collections.sort(supportedTypes);
        
        int offset = 0;
        while (rs.next()) {
            // TYPE_NAME (column 1)
            String typeName = rs.getString("TYPE_NAME");
            assertNotNull(typeName);
            
            // DATA_TYPE (column 2)
            int type = rs.getInt("DATA_TYPE");
            assertFalse(rs.wasNull());
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            if (!supportedTypes.get(offset).equals(type))
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
            case Types.BOOLEAN:
                precision = 1;
                break;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4614
                precision = 29;
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
	    case Types.JAVA_OBJECT:
	    case JDBC.SQLXML:
		precision = 0;
		break;
            }
            assertEquals("PRECISION " + typeName,
                    precision, rs.getInt("PRECISION"));

            /*
              Precision value is null for XML and OBJECT data type
            */
            if ( (typeName.equals("XML" )) || (typeName.equals("OBJECT" )) )
            { assertTrue(rs.wasNull()); }
            else
            { assertFalse(rs.wasNull()); }

            
            // LITERAL_PREFIX (column 4)
            // LITERAL_SUFFIX (column 5)
            
            // CREATE_PARAMS (column 6)
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2258
//IC see: https://issues.apache.org/jira/browse/DERBY-2259
//IC see: https://issues.apache.org/jira/browse/DERBY-2260
//IC see: https://issues.apache.org/jira/browse/DERBY-2245
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2262
        case Types.DECIMAL:
        case Types.NUMERIC:
            maxScale = 31; // Max Scale for Decimal & Numeric is 31: Derby-2262
            break;
            case Types.TIMESTAMP:
//IC see: https://issues.apache.org/jira/browse/DERBY-4614
                maxScale = 9;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
        
        // ODBC_COLUMN_NULLABILTY is the same as JDBC except for:
        // column 16 - SQL_DATA_TYPE is NULL in JDBC but a valid non-null value in ODBC
        // column 19 -  INTERVAL_PRECISION (extra column comapred to JDBC)
        boolean[] ODBC_COLUMN_NULLABILITY = {
//IC see: https://issues.apache.org/jira/browse/DERBY-3400
                true, false, true, true,
//IC see: https://issues.apache.org/jira/browse/DERBY-3350
                true, true, false, false,
                false, true, false,
                true, true,
                true, true,
                false, true,
                true,
                true 
        };

        CallableStatement cs = prepareCall(
                "CALL SYSIBM.SQLGETTYPEINFO (0, 'DATATYPE=''ODBC''')");
        
        cs.execute();
        ResultSet odbcrs = cs.getResultSet();
        assertNotNull(odbcrs);
        
        assertMetaDataResultSet(odbcrs, ODBC_COLUMN_NAMES, ODBC_COLUMN_TYPES,
        		ODBC_COLUMN_NULLABILITY);
        
        odbcrs.close();
        cs.close();

    }
    
    /*
     * Check the shape of the ResultSet from any getColumns call.
     */
    private void checkColumnsShape(ResultSet rs, int odbc) throws SQLException
    {
        int[] columnTypes = new int[] {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR
                };
        boolean[]   nullability =
        {
            false,   false,  false,  false,
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
            true,  true,   true,  true,
            true,   true,   true,  false,
            true,   true,   true,   true,
            false,  false,  true,   true,
            true,   true,   false,  false,
            true
        };
        if (odbc == 1)
        {
            columnTypes[4] = Types.SMALLINT;
            columnTypes[8] = Types.SMALLINT;
            columnTypes[9] = Types.SMALLINT;
            columnTypes[10] = Types.SMALLINT;
            columnTypes[13] = Types.SMALLINT;
            columnTypes[14] = Types.SMALLINT;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        assertMetaDataResultSet
            (
             rs,
             new String[]
             {
                 "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                 "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                 "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                 "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                 "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                 "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN",
                 "SCOPE_CATLOG"
             },
             columnTypes,
             nullability
             );          
    }

    /**
     * Check the shape of the ResultSet from any getTables call.
     * Note nullability of TABLE_CAT is not nullable for Derby
     * even though it doesn't support catalogs because the
     * SQL query returns a constant (empty string) for
     * a table's catalog.
     * @param rs
     * @throws java.sql.SQLException
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
          false, false, false, true, // TABLE_SCHEM cannot be NULL in Derby
          false, true, true, true,
          true, true
        }
        );        
    }
    
    /**
     * Check the shape of the ResultSet from any getCatlogs call.
     * @param rs
     * @throws java.sql.SQLException
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        , new boolean[] {false}
        );        
    }
    
    /**
     * Check the shape of the ResultSet from any
     * getVersionColumns call.
     * @param rs
     * @throws java.sql.SQLException
     */
    private static void checkVersionColumnsShape(ResultSet[] rs) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        String [] columnNames = new String[] {
                "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"
               };
        int[] columnTypes = new int[] {
                Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.SMALLINT};
        assertMetaDataResultSet(rs[0], columnNames, columnTypes, null);
        columnTypes[2] = Types.SMALLINT;
        assertMetaDataResultSet(rs[1], columnNames, columnTypes, null);        
    }
    
    public static void assertMetaDataResultSet(ResultSet rs,
            String[] columnNames, int[] columnTypes,
            boolean[] nullability) throws SQLException
    {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        //assertNull(rs.getStatement());
        
        if (columnNames != null)
            JDBC.assertDatabaseMetaDataColumns(rs, 
                    nullability, columnTypes, columnNames);
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
     *
     * @param specList
     * @param metaDataList
     *
     * @throws java.sql.SQLException
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
     *
     * @param specDetails
     *
     * @throws java.sql.SQLException
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
     * expected. For variable sized types the string will
     * have random valid length information. E.g. CHAR(37).
     * @param conn current connection
     * @return list of types
     * @throws java.sql.SQLException
     */
    public static List<String> getSQLTypes(Connection conn) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        List<String> list = new ArrayList<String>();
        
        Random rand = new Random();
        
        ResultSet rs = conn.getMetaData().getTypeInfo();
        while (rs.next())
        {
            String typeName = rs.getString("TYPE_NAME");

            // skip the OBJECT type because this does not correspond to an
            // actual data type but merely flags the fact that the database
            // supports the creation of user defined types
            if ( "OBJECT".equals( typeName ) ) { continue; }
            
            String createParams = rs.getString("CREATE_PARAMS");
            
            if (createParams == null) {
                // Type name stands by itself.
                list.add(typeName);
                continue;
            }
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            if (createParams.contains("length"))
            {
                int maxLength = rs.getInt("PRECISION");
                
                // nextInt returns a value between 0 and maxLength-1
                int length = rand.nextInt(maxLength) + 1;
                
                int paren = typeName.indexOf('(');
                if (paren == -1) {
                    list.add(typeName + "(" + length + ")");
                    
                } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
                    StringBuilder sb = new StringBuilder();
                    sb.append(typeName.substring(0, paren+1));
                    sb.append(length);
                    sb.append(typeName.substring(paren+1));
                    list.add(sb.toString());
                }
                
                continue;
            }
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            if (createParams.contains("scale"))
            {
                int maxPrecision = rs.getInt("PRECISION");
                StringBuilder sb = new StringBuilder();
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
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            if (createParams.contains("precision"))
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
     *
     * @param type
     * @return JDBC type identifier (java.sql.Types.*)
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        if (type.equals("BOOLEAN"))
            return Types.BOOLEAN;
        
        fail("Unexpected SQL type: " + type);
        return Types.NULL;
    }
    
    /**
     * Given a valid SQL type return the corresponding
     * precision/length for this specific value
     * if the type is variable, e.g. CHAR(5) will
     * return 5, but LONG VARCHAR will return 0.
     *
     * @param jdbcType
     * @param type
     *
     * @return precision or length
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
                Integer.parseInt(type.substring(lp+1, rp));
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
     * elsewhere, see fixtures testGetXXportedKeys()
     *
     * @throws java.sql.SQLException
     * @throws java.io.IOException
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
        checkODBCKeys(null, schema, null, null, null, null);
//IC see: https://issues.apache.org/jira/browse/DERBY-5764

        /* Run equivalent of getImportedKeys(), getExportedKeys(),
         * and getCrossReference for each of the primary/foreign
         * key pairs.
         */

        checkODBCKeys(null, schema, null, null, null, "FKT1");
        checkODBCKeys(null, schema, "PKT1", null, null, null);
        checkODBCKeys(null, schema, "PKT1", null, null, "FKT1");

        checkODBCKeys(null, schema, null, null, null, "FKT2");
        checkODBCKeys(null, schema, "PKT2", null, null, null);
        checkODBCKeys(null, schema, "PKT2", null, null, "FKT2");

        checkODBCKeys(null, schema, null, null, null, "FKT3");
        checkODBCKeys(null, schema, "PKT3", null, null, null);
        checkODBCKeys(null, schema, "PKT3", null, null, "FKT3");

        // Reverse primary and foreign tables.

        checkODBCKeys(null, schema, "FKT1", null, null, null);
        checkODBCKeys(null, schema, null, null, null, "PKT3");
        checkODBCKeys(null, schema, "FKT1", null, null, "PKT1");
        checkODBCKeys(null, schema, "FKT2", null, null, "PKT2");
        checkODBCKeys(null, schema, "FKT3", null, null, "PKT3");

        // Mix-and-match primary key tables and foreign key tables.

        checkODBCKeys(null, schema, "PKT1", null, null, "FKT2");
        checkODBCKeys(null, schema, "PKT1", null, null, "FKT3");
        checkODBCKeys(null, schema, "PKT2", null, null, "FKT3");

        checkODBCKeys(null, schema, "FKT1", null, null, "PKT2");
        checkODBCKeys(null, schema, "FKT1", null, null, "PKT3");
        checkODBCKeys(null, schema, "FKT2", null, null, "PKT3");

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
     *
     * @param pCatalog
     * @param pSchema
     * @param pTable
     * @param fCatalog
     * @param fSchema
     * @param fTable
     *
     * @throws java.sql.SQLException
     * @throws java.io.IOException
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
         * that behavior if we want to maintain backward compatibility.
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                    {"",schema,"PKT1","I","",schema,"FKT1","FI",
                        "1","3","3","FK1","PK1","7"},
                    {"",schema,"PKT2","C","",schema,"FKT1","FC",
                        "1","3","3","FK2","PK2","7"},
                    {"",schema,"PKT3","I","",schema,"FKT2","FI",
                        "1","3","3","FK3","PK3","7"},
                    {"",schema,"PKT3","C","",schema,"FKT2","FC",
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
     *
     * @param rs
     * @throws java.sql.SQLException
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
                false, false, false, false,
                false, false, false, false,
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
        st.execute("create table brit13 (i int not null, j int)");
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
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
        
        // result: column i
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        ResultSet[] rs = getBestRowIdentifier(null,schema,"BRIT1",
        		DatabaseMetaData.bestRowTemporary, true);
        verifyBRIResults(rs, expRSI);

        // result: column i
        rs = getBestRowIdentifier(null,schema,"BRIT2",
        		DatabaseMetaData.bestRowTemporary, true);
        verifyBRIResults(rs, expRSI);

        // result: column i (j is equally good but its index descriptor
        // sorts after i: DERBY-6623)
        rs = getBestRowIdentifier(null,schema,"BRIT3",
        		DatabaseMetaData.bestRowTemporary,true);
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        verifyBRIResults(rs, expRSI);
        // result: column i
        rs = getBestRowIdentifier(null,schema,"BRIT4",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSI);
        
        // result: columns i and j
        rs = getBestRowIdentifier(null,schema,"BRIT5",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSIJ);

        // result: column j
        rs = getBestRowIdentifier(null,schema,"BRIT6",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSJ);

        // result: column j
        rs = getBestRowIdentifier(null,schema,"BRIT7",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSJ);
        
        // result: column j
        rs = getBestRowIdentifier(null,schema,"BRIT8",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSJ);
        
        // result: columns i,j
        rs = getBestRowIdentifier(null,schema,"BRIT9",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSIJ);
        
        // result: columns i,j
        rs = getBestRowIdentifier(null,schema,"BRIT10",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSIJ);
        
        // result: columns i,j
        rs = getBestRowIdentifier(null,schema,"BRIT11",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSIJ);
        
        // result: columns i,j
        rs = getBestRowIdentifier(null,schema,"BRIT12",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSIJ);
        
        // Verify nullOK flags makes a difference. See also DERBY-3182
        // result: column i, should've ignored null column
        rs = getBestRowIdentifier(null,schema,"BRIT13",
        		DatabaseMetaData.bestRowTemporary,false);
        verifyBRIResults(rs, expRSI);
        // result: columns i, j
        rs = getBestRowIdentifier(null,schema,"BRIT13",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSIJ);
        
        // result: columns i
        rs = getBestRowIdentifier(null,schema,"BRIT14",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSI);
        
        // result: columns i
        rs = getBestRowIdentifier(null,schema,"BRIT15",
        		DatabaseMetaData.bestRowTemporary,true);
        verifyBRIResults(rs, expRSI);
        
        // we don't do anything with SCOPE except detect bad values
        // result: columns i
        rs = getBestRowIdentifier(null,schema,"BRIT16",
        		DatabaseMetaData.bestRowTransaction,true);
        verifyBRIResults(rs, expRSI);
        // result: columns i
        rs = getBestRowIdentifier(null,schema,"BRIT16",
        		DatabaseMetaData.bestRowSession,true);
        verifyBRIResults(rs, expRSI);
        // result: exception (invalid scope -1)
        try {
            rs = getBestRowIdentifier(null,schema,"BRIT16",-1,true);
        } catch (SQLException sqle) {
            assertSQLState( "42XAT", sqle);
        }
        
        // result: exception (invalid scope 3)
        try {
            rs = getBestRowIdentifier(null,schema,"BRIT16",3,true);
        } catch (SQLException sqle) {
            assertSQLState( "42XAT", sqle);
        }
        
        rs = getBestRowIdentifier(null, schema,"BRIT17",
        		DatabaseMetaData.bestRowTemporary,true);
        String [][] expRS = new String [][] {
                {"2", "I", "4", "INTEGER", "4", null, "10", "1"},
                {"2", "VC10", "12", "VARCHAR", "10", null, null, "1"}
        };
//IC see: https://issues.apache.org/jira/browse/DERBY-6968
        JDBC.assertUnorderedResultSet(rs[0], expRS, true);
        // set buffer_length expected for ODBC; for most of the simple 
        // tables/rows in our test it's "4" so set in verifyBRIResults
        expRS[0][5] = "4";
        expRS[1][5] = "20";
        JDBC.assertUnorderedResultSet(rs[1], expRS, true);
        
        // test DERBY-2610 for fun; can't pass in null table name      
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            getBestRowIdentifier(null,schema,null,
            		DatabaseMetaData.bestRowTemporary,true);
        } catch (SQLException sqle) {
            assertSQLState( "XJ103", sqle);
        }
        
        // check on systables
        rs = getBestRowIdentifier(null,"SYS","SYSTABLES",
        		DatabaseMetaData.bestRowTemporary,true);
        expRS = new String [][] {
                {"2", "TABLEID", "1", "CHAR", "36", null, null, "1"}
        };
//IC see: https://issues.apache.org/jira/browse/DERBY-6968
        JDBC.assertUnorderedResultSet(rs[0], expRS, true);
        // set buffer_length expected for ODBC
        expRS[0][5] = "72";
        JDBC.assertUnorderedResultSet(rs[1], expRS, true);
        
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
     * Helper method for testing getBestRowIdentifier - calls the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param table
     * @param scope
     * @param nullable
     * @return the result set of the query
     *
     * @throws SQLException 
     */
    private ResultSet getBestRowIdentifierODBC(String catalog, String schema, 
        String table, int scope, boolean nullable) throws SQLException 
    {
        CallableStatement cs = prepareCall(
            "CALL SYSIBM.SQLSPECIALCOLUMNS(1, ?, ?, ?, ?, ?, " +
        "'DATATYPE=''ODBC''')");
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.setInt(4, scope);
        cs.setBoolean(5, nullable);

        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getBestRowIdentifier - calls 
     * dmd.getBestRowIdentifier for the JDBC call, and getBestRowIdentifierODBC
     *
     * for the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @param scope
     * @param nullable
     * @return array of two result sets corresponding to JDBC and ODBC
     *
     * @throws SQLException 
     */
    private ResultSet[] getBestRowIdentifier(String catalog, String schema, String table,
        int scope, boolean nullable) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getBestRowIdentifier(catalog, schema, table, scope, nullable);
        rss[1]= getBestRowIdentifierODBC(catalog, schema, table, scope, nullable);
        
        String[] columnNames = {
                "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", 
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", 
                "PSEUDO_COLUMN"};
        int[] columnTypes = {
                Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.SMALLINT};
        int[] odbcColumnTypes = {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
                Types.SMALLINT, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.SMALLINT};
        boolean [] nullability = {
                true, false, true, true, true, true, true, true};
        
        // column nullability is opposite to with scope=1 or 2...DERBY-3181
        // When running the ODBC version, the datatype returned for
        // column 3 is SMALLINT, vs. INTEGER when scope=1 or 2...
        // So, in this case, the columnTypes are the same for ODBC and JDBC,
        // but with calls with a valid scope, they are different.

        if (scope != DatabaseMetaData.bestRowTemporary &&
        		scope != DatabaseMetaData.bestRowTransaction &&
        		scope != DatabaseMetaData.bestRowSession)
        {
        	nullability = new boolean [] {
                    false, false, false, false, false, false, false, false};
        	
        	odbcColumnTypes = columnTypes;
        }
       
        assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
        assertMetaDataResultSet(rss[1], columnNames, odbcColumnTypes, nullability);
              
        return rss;        
    }

    /**
     * helper method for test testGetBestRowIdentifier
     * @param rss - ResultSet array from getBestRowIdentifier;
     *     rss[0] will have the JDBC result, rss[1] the ODBC result
     * @param expRS - bi-dimensional String array with expected result row(s)
     * @throws SQLException 
     */
    public void verifyBRIResults(ResultSet[] rss, String[][] expRS) throws SQLException {      
        JDBC.assertFullResultSet(rss[0], expRS, true);
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        for (String[] expRS1 : expRS) {
            expRS1[5] = "4";
        }
        JDBC.assertFullResultSet(rss[1], expRS, true);
        for (String[] expRS1 : expRS) {
            expRS1[5] = null;
        }
    }
    
    /**
     * Test getGetColumnPrivileges; does not modify database
     * For further testing see test lang.grantRevokeTest
     * @throws SQLException 
     */
    public void testGetColumnPrivileges() throws SQLException
    {       
        // unlike for instance getTables() and getUDTs trying to call
        // getColumnPrivileges with all nulls gets stopped because 
        // the spec indicates it takes a table name, not just a pattern
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            getColumnPrivileges(null,null,null,null);
            fail ("expected error XJ103");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        
        ResultSet[] rs = getColumnPrivileges(null,null,"",null);
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
        
        rs = getColumnPrivileges("","","","");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
        
        rs = getColumnPrivileges("%","%","%","%");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

        // we didn't (can't) grant any privileges to the systabels, so no row
        rs = getColumnPrivileges(null,"SYS","SYSTABLES","TABLEID");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

    }

    /**
     * Helper method for testing getColumnPrivileges - calls the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @param columnNamePattern
     * @return the result set of the query
     * @throws SQLException 
     */
    private ResultSet getColumnPrivilegesODBC(String catalog, String schema, 
        String table, String columnNamePattern) throws SQLException 
    {
        CallableStatement cs = prepareCall(
            "CALL SYSIBM.SQLCOLPRIVILEGES(?, ?, ?, ?, 'DATATYPE=''ODBC''')");
        
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.setString(4, columnNamePattern);

        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getColumnPrivileges - calls dmd.getIndexInfo for the 
     * JDBC call, and getColumnPrivilegesODBC for the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param table
     * @param columnNamePattern
     * @return array of two result sets corresponding to JDBC and ODBC
     *
     * @throws SQLException 
     */
    private ResultSet[] getColumnPrivileges(String catalog, String schema, String table,
        String columnNamePattern) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        rss[1]= getColumnPrivilegesODBC(catalog, schema, table, columnNamePattern);
        
        String [] columnNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME",
                "COLUMN_NAME","GRANTOR","GRANTEE","PRIVILEGE","IS_GRANTABLE"};
        int [] columnTypes = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR};
        boolean [] nullability = {false,false,false,false,false,false,false,false};

        assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
        assertMetaDataResultSet(rss[1], columnNames, columnTypes, nullability);
                       
        return rss;        
    }
    
    /**
     * Test getGetTablePrivileges; does not modify database
     * For further testing see test lang.grantRevokeTest
     * @throws SQLException 
     */
    public void testGetTablePrivileges() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        ResultSet rs[] = getTablePrivileges(null,null,null);
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
        
        rs = getTablePrivileges("","","");
       JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
        
        rs = getTablePrivileges("%","%","%");
         JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

        // we didn't (can't) grant any privileges to the systabels, so no row
        rs = getTablePrivileges(null,"SYS","SYSTABLES");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
    }
    
    /**
     * Helper method for testing getTablePrivileges - calls the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param tableNamePattern
     * @return the result set of the query
     *
     * @throws SQLException 
     */
    private ResultSet getTablePrivilegesODBC(String catalog, String schema, 
        String tableNamePattern) throws SQLException 
    {
        CallableStatement cs = prepareCall(
            "CALL SYSIBM.SQLTABLEPRIVILEGES(?, ?, ?, 'DATATYPE=''ODBC''')");
        
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, tableNamePattern);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getTablePrivileges - calls dmd.getIndexInfo for the 
     * JDBC call, and getTablePrivilegesODBC for the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param tableNamePattern
     * @return array of two result sets corresponding to JDBC and ODBC
     *
     * @throws SQLException 
     */
    private ResultSet[] getTablePrivileges(
        String catalog, String schema, String tableNamePattern) 
    throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getTablePrivileges(catalog, schema, tableNamePattern);
        rss[1]= getTablePrivilegesODBC(catalog, schema, tableNamePattern);
        
        String [] columnNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME",
                "GRANTOR","GRANTEE","PRIVILEGE","IS_GRANTABLE"};
        int [] columnTypes = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR};
        boolean [] nullability = {false,false,false,false,false,false,false};
        
        assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
        assertMetaDataResultSet(rss[1], columnNames, columnTypes, nullability);
            
        return rss;        
    }

    /**
     * Test getIndexInfo; does not modify database
     * @throws SQLException 
     */
    public void testGetIndexInfo() throws SQLException
    {
        
        // unlike for instance getTables() and getUDTs trying to call
        // getIndexInfo with all nulls gets stopped because 
        // the spec indicates it takes a table name, not just a pattern
        try {
            getIndexInfo(null,null,null,true,true);
            fail ("expected error XJ103");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        
        // do a call which selects unique indexes only
        ResultSet rss[] = getIndexInfo("","SYS","SYSCOLUMNS",true,false);
        String[][] expRS = {
            {"","SYS","SYSCOLUMNS","false","","SYSCOLUMNS_INDEX1","3","1",
                "REFERENCEID","A",null,null,null},
            {"","SYS","SYSCOLUMNS","false","","SYSCOLUMNS_INDEX1","3","2",
                "COLUMNNAME","A",null,null,null}};
        assertFullResultSet(rss, expRS, true);
        
        // same table, but select all indexes (unique=false)
        // note, that true for approximate does nothing in Derby
        rss = getIndexInfo("","SYS","SYSCOLUMNS",false,false);

        expRS = new String[][] {
            {"","SYS","SYSCOLUMNS","false","","SYSCOLUMNS_INDEX1","3","1",
                "REFERENCEID","A",null,null,null},
            {"","SYS","SYSCOLUMNS","false","","SYSCOLUMNS_INDEX1","3","2",
                "COLUMNNAME","A",null,null,null},
            {"","SYS","SYSCOLUMNS","true","","SYSCOLUMNS_INDEX2","3","1",
                 "COLUMNDEFAULTID","A",null,null,null}};
        assertFullResultSet(rss, expRS, true);
        
        rss = getIndexInfo("","SYS","SYSTABLES",true,false);

        expRS = new String[][] {
            {"","SYS","SYSTABLES","false","","SYSTABLES_INDEX1","3","1",
                "TABLENAME","A",null,null,null},
            {"","SYS","SYSTABLES","false","","SYSTABLES_INDEX1","3","2",
                "SCHEMAID","A",null,null,null},
            {"","SYS","SYSTABLES","false","","SYSTABLES_INDEX2","3","1",
                "TABLEID","A",null,null,null}};
        assertFullResultSet(rss, expRS, true);
        
        // should return no rows
        rss = getIndexInfo("","SYS","SYSSTABLES",true,false);

        JDBC.assertEmpty(rss[0]);
        JDBC.assertEmpty(rss[1]);

    }
    
    /**
     * Test getIndexInfo further; does modify database
     * @throws SQLException 
     */
    public void testMoreGetIndexInfo() throws SQLException
    {
        // test to see that we are correctly returning D for ASC_OR_DESC.
        // As Derby only supports tableIndexHashed Type, and 
        // CARDINALITY, PAGES, nor FILTER_CONDITION get set, no further
        // tests seem necessary.
        Statement st = createStatement();

        // First, create the test table and indexes/keys
        st.execute("create table iit (i int not null, j int)");
        st.execute("create unique index iii on iit(i asc, j desc)");
        DatabaseMetaData dmd = getDMD();
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        ResultSet rs = dmd.getIndexInfo("",schema,"IIT",false,false);
        boolean more = rs.next();

//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        if (more) {
            assertEquals("A",rs.getString(10));
        }

        more = rs.next();

        if (more) {
            assertEquals("D",rs.getString(10));
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = getIndexInfoODBC("",schema,"IIT",false,false);
        more = rs.next();

        if (more) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            assertEquals("A",rs.getString(10));
        }

        more = rs.next();

        if (more) {
            assertEquals("D",rs.getString(10));
        }

        st.execute("drop index iii");
        st.execute("drop table iit");

        st.close();
    }

    /**
     * Helper method for testing getIndexInfo - calls the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param table
     * @param approximate
     * @param unique
     * @return the result set of the query
     *
     * @throws SQLException 
     */
    private ResultSet getIndexInfoODBC(String catalog, String schema, 
        String table, boolean unique, boolean approximate) throws SQLException 
    {
        CallableStatement cs = prepareCall(
                "CALL SYSIBM.SQLSTATISTICS(?, ?, ?, ?, ?, " +
                "'DATATYPE=''ODBC''')");

        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        // the unique parameter needs to be flopped...See the call
        // in SystemProcedures.
        cs.setBoolean(4, unique? false : true);
        cs.setBoolean(5, approximate);

        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getIndexInfo - calls dmd.getIndexInfo for the 
     * JDBC call, and getIndexInfoODBC for the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @param unique
     * @param approximate
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     * @throws SQLException 
     */
    private ResultSet[] getIndexInfo(String catalog, String schema, String table,
        boolean unique, boolean approximate) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getIndexInfo(catalog, schema, table, unique, approximate);
        rss[1]= getIndexInfoODBC(catalog, schema, table, unique, approximate);
        
        String [] columnNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME",
                "NON_UNIQUE","INDEX_QUALIFIER","INDEX_NAME","TYPE",
                "ORDINAL_POSITION","COLUMN_NAME","ASC_OR_DESC","CARDINALITY",
                "PAGES","FILTER_CONDITION"};
        int [] columnTypes = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.BOOLEAN,Types.VARCHAR,Types.VARCHAR,Types.SMALLINT,
                // ASC_OR_DESC is Types.CHAR rather than VARCHAR...
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
                Types.SMALLINT,Types.VARCHAR,Types.CHAR,Types.BIGINT,
                Types.BIGINT,Types.VARCHAR};
        
        boolean [] nullability = {false,false,false,
            false,false,true,true,true,false,false,true,true,true};
        
        // JDBC result set
        assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
        
        // Change shape for ODBC.
        columnTypes[4 - 1] = Types.SMALLINT; // types.boolean is not supported with ODBC

        // ODBC result set
        assertMetaDataResultSet(rss[1], columnNames, columnTypes, nullability);
       
        return rss;        
    }
    
    /**
     * Helper method - unravels a ResultSet array created e.g.
     * with this.getIndexInfo, i.e. Resultset[0] has the JDBC resultset
     * and ResultSet[1] the ODBC resultset
     *
     * @param rs
     * @param expRS
     * @param trim
     *
     * @throws SQLException 
     */
    private void assertFullResultSet(
        ResultSet rs[], String[][] expRS, boolean trim) throws SQLException
    {
        JDBC.assertFullResultSet(rs[0], expRS, trim);
        JDBC.assertFullResultSet(rs[1], expRS, trim);
    }

    /**
     * Helper method - unravels a ResultSet array created e.g.
     * with this.getIndexInfo, i.e. Resultset[0] has the JDBC resultset
     * and ResultSet[1] the ODBC resultset
     *
     * @param rs
     * @param expRS
     * @param trim
     *
     * @throws SQLException
     */
    private void assertFullUnorderedResultSet(
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        ResultSet rs[], String[][] expRS, boolean trim) throws SQLException
    {
        JDBC.assertUnorderedResultSet(rs[0], expRS, trim);
        JDBC.assertUnorderedResultSet(rs[1], expRS, trim);
    }

    /**
     * Create the tables for get*Keys tests
     * @throws SQLException 
     */
    private void createObjectsForKeysTests() throws SQLException
    {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table kt1 (" +
                "i int not null default 10, " +
                "s smallint not null, " +
                "c30 char(30) not null, " +
                "vc10 varchar(10) not null default 'asdf', " +
                "constraint PRIMKEY primary key(vc10, i), " +
                "constraint UNIQUEKEY unique(c30, s), " + 
                "ai bigint generated always as identity " +
                    "(start with -10, increment by 2001))");

        // Create another unique index on kt1
        s.execute("create unique index u1 on kt1(s, i)");
        // Create a non-unique index on kt1
        s.execute("create index u2 on kt1(s)");
        // Create a view on key table 1
        s.execute("create view kv as select * from kt1");

        // Create a foreign key
        s.execute("create table reftab (vc10 varchar(10), i int, " +
                  "s smallint, c30 char(30), " +
                  "s2 smallint, c302 char(30), " +
                  "dprim decimal(5,1) not null, dfor decimal(5,1) not null, "+
                  "constraint PKEY_REFTAB primary key (dprim), " + 
                  "constraint FKEYSELF " +
                      "foreign key (dfor) references reftab, "+
                  "constraint FKEY1 " +
                      "foreign key(vc10, i) references kt1, " + 
                  "constraint FKEY2 " +
                      "foreign key(c30, s2) references kt1 (c30, s), "+
                  "constraint FKEY3 " +
                      "foreign key(c30, s) references kt1 (c30, s))");

        s.execute("create table reftab2 (t2_vc10 varchar(10), t2_i int, " +
                  "constraint T2_FKEY1 " +
                      "foreign key(t2_vc10, t2_i) references kt1)");
        commit();
        getConnection().setAutoCommit(true);
    }
    
    /**
     * Drop the database objects for get*Keys tests
     * @throws SQLException 
     */
    private void dropObjectsForKeysTests() throws SQLException
    {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        s.execute("drop table reftab2");
        s.execute("drop table reftab");
        commit();
        s.execute("drop view kv");
        s.execute("drop index u2");
        s.execute("drop index u1");
        s.execute("drop table kt1");
        commit();
        getConnection().setAutoCommit(true);        
    }

    /**
     * Test getPrimaryKeys; does modify database
     * @throws SQLException 
     */
    public void testGetPrimaryKeys() throws SQLException
    {
        String[][] expRS = new String[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                {"",schema,"KT1","I","2","PRIMKEY"},
                {"",schema,"KT1","VC10","1","PRIMKEY"}};
                       
        createObjectsForKeysTests();
        
        // try with valid search criteria
        // although, % may not actually be appropriate?
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        ResultSet rs[] = getPrimaryKeys("", "%", "KT1");
        assertFullResultSet(rs, expRS, true);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = getPrimaryKeys(null, schema, "KT1");
        assertFullResultSet(rs, expRS, true);

        rs = getPrimaryKeys(null, null, "KT1");
        assertFullResultSet(rs, expRS, true);

        rs = getPrimaryKeys(null, "", "KT1");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

        // tablename may not be null
        DatabaseMetaData dmd = getDMD();
        try {
            rs[0] = dmd.getPrimaryKeys(null, null, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        try {
            rs[1] = getPrimaryKeysODBC(null, null, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        
        // DERBY-2610, tablename must be given as stored - % means no rows
        rs = getPrimaryKeys(null, null, "%");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
        
        dropObjectsForKeysTests();
    }

    /**
     * Helper method for testing getPrimaryKeys - calls the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param table
     * @return result set of the query
     * @throws SQLException 
     */
    private ResultSet getPrimaryKeysODBC(
            String catalog, String schema, String table) throws SQLException 
    {
        CallableStatement cs = prepareCall(
                "CALL SYSIBM.SQLPRIMARYKEYS(?, ?, ?, 'DATATYPE=''ODBC''')");
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getPrimaryKeys - calls dmd.getPrimaryKeys for
     * the JDBC call, and getPrimaryKeysODBC for the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     * @throws SQLException 
     */
    private ResultSet[] getPrimaryKeys(
            String catalog, String schema, String table) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getPrimaryKeys(catalog, schema, table);
        rss[1]= getPrimaryKeysODBC(catalog, schema, table);
        
        String [] columnNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME",
                "COLUMN_NAME","KEY_SEQ","PK_NAME"};
        int [] columnTypes = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.SMALLINT,Types.VARCHAR};
        boolean [] nullability = {false,false,false,false,true,false};
        assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
        assertMetaDataResultSet(rss[1], columnNames, columnTypes, nullability);
           
        return rss;        
    }

    /**
     * Test getImportedKeys, getExportedKeys, getCrossReference; modifies db
     * @throws SQLException 
     */
    public void testGetXXportedKeys() throws SQLException
    {
        // getExportedKeys


        String[][] expRS1 = new String[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            {"",schema,"KT1","VC10","",schema,"REFTAB","VC10","1","3","3","FKEY1","PRIMKEY","7"},
            {"",schema,"KT1","I","",schema,"REFTAB","I","2","3","3","FKEY1","PRIMKEY","7"},
            {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY3","UNIQUEKEY","7"},
            {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY2","UNIQUEKEY","7"},
            {"",schema,"KT1","S","",schema,"REFTAB","S","2","3","3","FKEY3","UNIQUEKEY","7"},
            {"",schema,"KT1","S","",schema,"REFTAB","S2","2","3","3","FKEY2","UNIQUEKEY","7"},
            {"",schema,"REFTAB","DPRIM","",schema,"REFTAB","DFOR","1","3","3","FKEYSELF","PKEY_REFTAB","7"}};
        String[][] expRS2 = new String[][] {
            {"",schema,"KT1","VC10","",schema,"REFTAB2","T2_VC10","1","3","3","T2_FKEY1","PRIMKEY","7"},
            {"",schema,"KT1","I","",schema,"REFTAB2","T2_I","2","3","3","T2_FKEY1","PRIMKEY","7"}};

        createObjectsForKeysTests();
        
        // try with valid search criteria
        // although, % may not actually be appropriate?
        ResultSet rs[] = getImportedKeys("", "%", "REFTAB");
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        assertFullUnorderedResultSet(rs, expRS1, true);
        rs = getImportedKeys("", "%", "REFTAB2");
        assertFullResultSet(rs, expRS2, true);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = getImportedKeys(null, schema, "REFTAB");
        assertFullUnorderedResultSet(rs, expRS1, true);
        rs = getImportedKeys(null, schema, "REFTAB2");
        assertFullResultSet(rs, expRS2, true);

        rs = getImportedKeys(null, null, "REFTAB");
        assertFullUnorderedResultSet(rs, expRS1, true);

        rs = getImportedKeys(null, "", "REFTAB");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

        // tablename may not be null
        DatabaseMetaData dmd = getDMD();
        try {
            rs[0] = dmd.getImportedKeys(null, null, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        try {
            rs[1] = getImportedKeysODBC(null, null, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        
        // DERBY-2610, tablename must be given as stored - % means no rows
        rs = getImportedKeys(null, null, "%");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

        // getExportedKeys
        expRS1 = new String[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                {"",schema,"KT1","VC10","",schema,"REFTAB","VC10","1","3","3","FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","I","",schema,"REFTAB","I","2","3","3","FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY2","UNIQUEKEY","7"},
                {"",schema,"KT1","S","",schema,"REFTAB","S2","2","3","3","FKEY2","UNIQUEKEY","7"},
                {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY3","UNIQUEKEY","7"},
                {"",schema,"KT1","S","",schema,"REFTAB","S","2","3","3","FKEY3","UNIQUEKEY","7"},
                {"",schema,"KT1","VC10","",schema,"REFTAB2","T2_VC10","1","3","3","T2_FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","I","",schema,"REFTAB2","T2_I","2","3","3","T2_FKEY1","PRIMKEY","7"}};
        expRS2 = new String[][] {
                {"",schema,"REFTAB","DPRIM","",schema,"REFTAB","DFOR","1","3","3","FKEYSELF","PKEY_REFTAB","7"}};

        rs = getExportedKeys("", "%", "KT1");
        assertFullResultSet(rs, expRS1, true);
        rs = getExportedKeys("", "%", "REFTAB");
        assertFullResultSet(rs, expRS2, true);
        
        rs = getExportedKeys(null, schema, "KT1");
        assertFullResultSet(rs, expRS1, true);
        rs = getExportedKeys(null, schema, "REFTAB");
       assertFullResultSet(rs, expRS2, true);

//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = getExportedKeys(null, null, "KT1");
        assertFullResultSet(rs, expRS1, true);

        rs = getExportedKeys(null, "", "KT1");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);

        // tablename may not be null
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            rs[0] = dmd.getExportedKeys(null, schema, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        try {
            rs[1] = getExportedKeysODBC(null, schema, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            assertSQLState("XJ103", sqle);
        }
        
        // DERBY-2610, tablename must be given as stored - % means no rows
        rs = getExportedKeys(null, null, "%");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertEmpty(rs[1]);
        
        // getCrossReference
        expRS1 = new String[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                {"",schema,"KT1","VC10","",schema,"REFTAB","VC10","1","3","3","FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","I","",schema,"REFTAB","I","2","3","3","FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY2","UNIQUEKEY","7"},
                {"",schema,"KT1","S","",schema,"REFTAB","S2","2","3","3","FKEY2","UNIQUEKEY","7"},
                {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY3","UNIQUEKEY","7"},
                {"",schema,"KT1","S","",schema,"REFTAB","S","2","3","3","FKEY3","UNIQUEKEY","7"}};
        expRS2 = new String[][] {
                {"",schema,"REFTAB","DPRIM","",schema,"REFTAB","DFOR","1","3","3","FKEYSELF","PKEY_REFTAB","7"}};

        // try with valid search criteria
        rs = getCrossReference("", schema, "KT1", "", schema, "REFTAB");
        assertFullResultSet(rs, expRS1, true);
        
        rs = getCrossReference("", schema, "REFTAB", "", schema, "REFTAB");
        assertFullResultSet(rs, expRS2, true);

        rs = getCrossReference("", schema, "KT1", "", schema, "REFTAB");
        assertFullResultSet(rs, expRS1, true);

        rs = getCrossReference("", schema, "REFTAB", "", schema, "REFTAB");
        assertFullResultSet(rs, expRS2, true);

        rs = getCrossReference(null, schema, "KT1", null, schema, "REFTAB");
        assertFullResultSet(rs, expRS1, true);

        rs = getCrossReference(null, schema, "REFTAB", null, schema, "REFTAB");
        assertFullResultSet(rs, expRS2, true);

        // DERBY-2758; query should return a different value for odbc vs. jdbc
        // only experiment jdbc here, odbc is handled elsewhere.
        rs = getCrossReference(null, schema, "%", null, schema, "%");
        JDBC.assertEmpty(rs[0]);
        String[][] expRS = new String[][] {
                {"",schema,"KT1","VC10","",schema,"REFTAB","VC10","1","3","3","FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","I","",schema,"REFTAB","I","2","3","3","FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY2","UNIQUEKEY","7"},
                {"",schema,"KT1","S","",schema,"REFTAB","S2","2","3","3","FKEY2","UNIQUEKEY","7"},
                {"",schema,"KT1","C30","",schema,"REFTAB","C30","1","3","3","FKEY3","UNIQUEKEY","7"},
                {"",schema,"KT1","S","",schema,"REFTAB","S","2","3","3","FKEY3","UNIQUEKEY","7"},
                {"",schema,"REFTAB","DPRIM","",schema,"REFTAB","DFOR","1","3","3","FKEYSELF","PKEY_REFTAB","7"},
                {"",schema,"KT1","VC10","",schema,"REFTAB2","T2_VC10","1","3","3","T2_FKEY1","PRIMKEY","7"},
                {"",schema,"KT1","I","",schema,"REFTAB2","T2_I","2","3","3","T2_FKEY1","PRIMKEY","7"}};
        JDBC.assertFullResultSet(rs[1], expRS, true);
        // Test also with null for schema.
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = getCrossReference(null, null, "%", null, null, "%");
        JDBC.assertEmpty(rs[0]);
        JDBC.assertResultSetContains(rs[1], expRS);
        
        // tablename may not be null
        try {
            rs[0] = dmd.getCrossReference(null, null, null, null, null, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            if (usingDerbyNetClient())
                assertSQLState("XJ110", sqle);
            else
                assertSQLState("XJ103", sqle);
        }
        // Note: With ODBC, this does *not* give an error. 
        // If that changes, uncomment the fail.
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            rs[1] = getCrossReferenceODBC(null, schema, null, null, schema, null);
        //    fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            if (usingDerbyNetClient())
                assertSQLState("XJ110", sqle);
            else
                assertSQLState("XJ103", sqle);
        }
        // tablename may not be null
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            rs[0] = dmd.getCrossReference(null, null, "", null, null, null);
            fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            if (usingDerbyNetClient())
                assertSQLState("XJ111", sqle);
            else
                assertSQLState("XJ103", sqle);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            rs[1] = getCrossReferenceODBC(null, schema, "", null, schema, null);
            //fail ("table name may not be null, should've given error");
        } catch (SQLException sqle) {
            if (usingDerbyNetClient())
                assertSQLState("XJ111", sqle);
            else
                assertSQLState("XJ103", sqle);
        }        
        
        // DERBY-2610, tablename must be given as stored - % means no rows
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        rs = getCrossReference(null, schema, "%", null, schema, "%");
        JDBC.assertEmpty(rs[0]);
        // But it *is* allowed with ODBC, see DERBY-2758
        JDBC.assertFullResultSet(rs[1], expRS, true);
        
        rs[0].close();
        rs[1].close();
        
        dropObjectsForKeysTests();
    }
    
    /**
     * Helper method for testing getImportedKeys - calls the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param table
     * @return result set of the query
     *
     * @throws SQLException 
     */
    private ResultSet getImportedKeysODBC(
            String catalog, String schema, String table) throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLFOREIGNKEYS(" +
            "null, null, null, ?, ?, ?, 'IMPORTEDKEY=1;DATATYPE=''ODBC''')");
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getImportedKeys - calls dmd.getImportedKeys for
     * the JDBC call, and getImportedKeysODBC for the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     * @throws SQLException 
     */
    private ResultSet[] getImportedKeys(
            String catalog, String schema, String table) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getImportedKeys(catalog, schema, table);
        rss[1]= getImportedKeysODBC(catalog, schema, table);
        
        assertGetImportedAndExportedKeysShape(rss);
        return rss;        
    }
    
    /**
     * Assert the shape of the ResultSets for getImportedKeys,
     * getExportedKeys and getCrossReference.
     * @param rss ResultSets from JDBC and ODBC calls.
     * @throws SQLException
     */
    private void assertGetImportedAndExportedKeysShape(ResultSet[] rss)
        throws SQLException
    {
        String [] columnNames = {
                "PKTABLE_CAT","PKTABLE_SCHEM","PKTABLE_NAME","PKCOLUMN_NAME",
                "FKTABLE_CAT","FKTABLE_SCHEM","FKTABLE_NAME","FKCOLUMN_NAME",
                "KEY_SEQ","UPDATE_RULE","DELETE_RULE",
                "FK_NAME","PK_NAME","DEFERRABILITY"};
            int [] columnTypes = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.SMALLINT,Types.SMALLINT,Types.SMALLINT,
                Types.VARCHAR,Types.VARCHAR,Types.SMALLINT};
            boolean [] nullability = {false,false,false,false,
                false,false,false,false,true,true,true,false,false,true};
            
         assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
         assertMetaDataResultSet(rss[1], columnNames, columnTypes, nullability);   
    }

    /**
     * Helper method for testing getExportedKeys - calls the ODBC procedure
     *
     * @param catalog
     * @param schema
     * @param table
     * @return the result set of the query
     *
     * @throws SQLException 
     */
    private ResultSet getExportedKeysODBC(
            String catalog, String schema, String table) throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLFOREIGNKEYS(" +
            "?, ?, ?, null, null, null, 'EXPORTEDKEY=1;DATATYPE=''ODBC''')");
        cs.setString(1, catalog);
        cs.setString(2, schema);
        cs.setString(3, table);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getExportedKeys - calls dmd.getExportedKeys for
     * the JDBC call, and getExportedKeysODBC for the ODBC procedure
     * @param catalog
     * @param schema
     * @param table
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     * @throws SQLException 
     */
    private ResultSet[] getExportedKeys(
            String catalog, String schema, String table) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getExportedKeys(catalog, schema, table);
        rss[1]= getExportedKeysODBC(catalog, schema, table);
        
        assertGetImportedAndExportedKeysShape(rss);
        
        return rss;        
    }

    /**
     * Helper method for testing getCrossReference - calls the ODBC procedure
     * @param parentcatalog
     * @param parentschema
     * @param parenttable
     * @param foreigncatalog
     * @param foreignschema
     * @param foreigntable
     * @return the result set of the query
     * @throws SQLException 
     */
    private ResultSet getCrossReferenceODBC(
            String parentcatalog, String parentschema, String parenttable,
            String foreigncatalog, String foreignschema, String foreigntable)
        throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLFOREIGNKEYS(" +
                "?, ?, ?, ?, ?, ?, 'DATATYPE=''ODBC''')");

        cs.setString(1, parentcatalog);
        cs.setString(2, parentschema);
        cs.setString(3, parenttable);
        cs.setString(4, foreigncatalog);
        cs.setString(5, foreignschema);
        cs.setString(6, foreigntable);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getCrossReference - calls dmd.getCrossReference for
     * the JDBC call, and getCrossReferenceODBC for the ODBC procedure
     *
     * @param parentcatalog
     * @param parentschema
     * @param parenttable
     * @param foreigncatalog
     * @param foreignschema
     * @param foreigntable
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     *
     * @throws SQLException 
     */
    private ResultSet[] getCrossReference(
            String parentcatalog, String parentschema, String parenttable,
            String foreigncatalog, String foreignschema, String foreigntable)
        throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getCrossReference(parentcatalog, parentschema, parenttable,
            foreigncatalog, foreignschema, foreigntable);
        rss[1]= getCrossReferenceODBC(parentcatalog, parentschema, parenttable,
            foreigncatalog, foreignschema, foreigntable);
        
        assertGetImportedAndExportedKeysShape(rss);
        return rss;        
    }
    
    /**
     * Test referential action values; modifies database
     * @throws SQLException 
     */
    public void testReferentialAction() throws SQLException
    {
        Statement s = createStatement();

        getConnection().setAutoCommit(false);        
        // First, create the test table and indexes/keys
        // note: apparently we have no test for setdefault.
        s.execute("create table refaction1(a int not null primary key)");
        s.execute("create table refactnone(a int references refaction1(a))");
        s.execute("create table refactrestrict(a int references refaction1(a) on delete restrict)");
        s.execute("create table refactnoaction(a int references refaction1(a) on delete no action)");
        s.execute("create table refactcascade(a int references refaction1(a) on delete cascade)");
        s.execute("create table refactsetnull(a int references refaction1(a) on delete set null)");
        s.execute("create table refactupdrestrict(a int references refaction1(a) on update restrict)");
        s.execute("create table refactupdnoaction(a int references refaction1(a) on update no action)");
        
        short restrict = DatabaseMetaData.importedKeyRestrict;
        short no_action = DatabaseMetaData.importedKeyNoAction;
        short cascade = DatabaseMetaData.importedKeyCascade;
        short setnull = DatabaseMetaData.importedKeySetNull;
        short setdefault = DatabaseMetaData.importedKeySetDefault;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        ResultSet rs[] = getCrossReference("",schema,"REFACTION1","",schema,"REFACTNONE");
        verifyReferentialAction(rs, new short[] {no_action, no_action});
        rs = getCrossReference("",schema,"REFACTION1","",schema,"REFACTRESTRICT");
        verifyReferentialAction(rs, new short[] {no_action, restrict});
        rs = getCrossReference("",schema,"REFACTION1","",schema,"REFACTNOACTION");
        verifyReferentialAction(rs, new short[] {no_action, no_action});
        rs = getCrossReference("",schema,"REFACTION1","",schema,"REFACTCASCADE");
        verifyReferentialAction(rs, new short[] {no_action, cascade});
        rs = getCrossReference("",schema,"REFACTION1","",schema,"REFACTSETNULL");
        verifyReferentialAction(rs, new short[] {no_action, setnull});
        rs = getCrossReference("",schema,"REFACTION1","",schema,"REFACTUPDRESTRICT");
        verifyReferentialAction(rs, new short[] {restrict, no_action});
        rs = getCrossReference("",schema,"REFACTION1","",schema,"REFACTUPDNOACTION");
        verifyReferentialAction(rs, new short[] {no_action, no_action});

        rs = getImportedKeys(null, schema, "REFACTNONE");
        verifyReferentialAction(rs, new short[] {no_action, no_action});
        rs = getImportedKeys(null, schema, "REFACTRESTRICT");
        verifyReferentialAction(rs, new short[] {restrict, restrict}); 
        rs = getImportedKeys(null, schema, "REFACTNOACTION");
        verifyReferentialAction(rs, new short[] {no_action, no_action});
        rs = getImportedKeys(null, schema, "REFACTCASCADE");
        verifyReferentialAction(rs, new short[] {no_action, cascade});
        rs = getImportedKeys(null, schema, "REFACTSETNULL");
        verifyReferentialAction(rs, new short[] {no_action, setnull});
        rs = getImportedKeys(null, schema, "REFACTUPDRESTRICT");
        verifyReferentialAction(rs, new short[] {no_action, no_action});
        rs = getImportedKeys(null, schema, "REFACTUPDNOACTION");
        verifyReferentialAction(rs, new short[] {no_action, no_action});

        rs = getExportedKeys(null, schema, "REFACTION1");
        short [][] expkeyresults = {
                {no_action, cascade},
                {no_action, no_action},
                {no_action, no_action},
                {no_action, restrict},
                {no_action, setnull},
                {no_action, no_action},
                {restrict, no_action}};
        for (int i = 0 ; i < 6 ; i++)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
            rs[0].next();
            assertEquals(expkeyresults[i][0], rs[0].getShort(10));
            assertEquals(expkeyresults[i][1], rs[0].getShort(11));
        }
        for (int i = 0 ; i < 6 ; i++)
        {
            rs[1].next();
            assertEquals(expkeyresults[i][0], rs[1].getShort(10));
            assertEquals(expkeyresults[i][1], rs[1].getShort(11));
        }

        s.execute("drop table refactnone");
        s.execute("drop table refactupdrestrict");
        s.execute("drop table refactupdnoaction");
        s.execute("drop table refactrestrict");
        s.execute("drop table refactnoaction");
        s.execute("drop table refactcascade");
        s.execute("drop table refactsetnull");
        s.execute("drop table refaction1");
        commit();
        
        rs[0].close();
        rs[1].close();
        s.close();
        
        getConnection().setAutoCommit(true);        
    }    
    
    // helper method for Referential Action test; verifies result
    // of various calls.
    // rs[0] is for JDBC calls, rs[1] for ODBC
    public void verifyReferentialAction(ResultSet[] rs, short[] expRes) 
        throws SQLException {
        rs[0].next();
        assertEquals(expRes[0], rs[0].getShort(10));
        assertEquals(expRes[1], rs[0].getShort(11));
        rs[1].next();
        assertEquals(expRes[0], rs[1].getShort(10));
        assertEquals(expRes[1], rs[1].getShort(11));
    }
    
    
    /**
     * Test DatabaseMetaData.getProcedures and .getProcedureColumns,
     * Both for JDBC and ODBC.
     * Further testing of these methods is done in lang/LangProcedureTest
     *
     * 
     * @throws Exception
     */
    // Possible TODO: 
    //   rewrite data portion of this test to compare results from 
    //   metadata with sys.sys* query results (leave shape check in place)
    public void testGetProceduresGetProcColumns() throws Exception {

        boolean supportsBoolean = true;
        Version dataVersion = getDataVersion( getConnection() );
        if ( dataVersion.compareTo( new Version( 10, 7, 0, 0 ) ) < 0 ) { supportsBoolean = false; }

        Statement s = createStatement();
        getConnection().setAutoCommit(false);
        
        s.execute("create procedure GETPCTEST1 (" +
                // for creating, the procedure's params do not need to exactly match the method's
                "out outb VARCHAR(3), a VARCHAR(3), b NUMERIC, c SMALLINT, " +
                "e SMALLINT, f INTEGER, g BIGINT, h FLOAT, i DOUBLE PRECISION, " +
                "k DATE, l TIME, T TIMESTAMP )"+
                "language java external name " +
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-514
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc'" +
        " parameter style java"); 
        s.execute("create procedure GETPCTEST2 (pa INTEGER, pb BIGINT)"+
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc'" +
        " parameter style java"); 
        s.execute("create procedure GETPCTEST3A (STRING1 VARCHAR(5), out STRING2 VARCHAR(5))"+
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc'" +
        " parameter style java"); 
        s.execute("create procedure GETPCTEST3B (in STRING3 VARCHAR(5), inout STRING4 VARCHAR(5))"+
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc'" +
        " parameter style java"); 
        s.execute("create procedure GETPCTEST4A()  "+
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc4a'"+
        " parameter style java"); 
        s.execute("create procedure GETPCTEST4B() "+
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc4b'" +
        " parameter style java"); 
        s.execute("create procedure GETPCTEST4Bx(out retparam INTEGER) "+
                "language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.getpc4b'" +
        " parameter style java");
//IC see: https://issues.apache.org/jira/browse/DERBY-4659

        if ( supportsBoolean )
        {
            s.execute("create procedure GETPCTEST5(in inarg boolean, out outarg boolean, inout inoutarg boolean) "+
                      "language java external name " +
                      "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.foo'" +
                      " parameter style java");
        }
        
        JDBC.GeneratedId genid = new JDBC.GeneratedId();

        String classname = getClass().getName();
        String getpc = classname + ".getpc";
        String getpc4a = classname + ".getpc4a";
        String getpc4b = classname + ".getpc4b";
        String foo = classname + ".foo";

        ResultSet rs[] = getProcedures(null, "%", "GETPCTEST%");
        Object[][] expRS = new Object[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                {"",schema,"GETPCTEST1",null,null,null,getpc,i(1),genid},
                {"",schema,"GETPCTEST2",null,null,null,getpc,i(1),genid},
                {"",schema,"GETPCTEST3A",null,null,null,getpc,i(1),genid},
                {"",schema,"GETPCTEST3B",null,null,null,getpc,i(1),genid},
                {"",schema,"GETPCTEST4A",null,null,null,getpc4a,i(1),genid},
                {"",schema,"GETPCTEST4B",null,null,null,getpc4b,i(1),genid},
                {"",schema,"GETPCTEST4BX",null,null,null,getpc4b,i(1),genid},
        };

        if ( supportsBoolean )
        {
            expRS = appendArray
                (
                 expRS,
                 new Object[][]
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                     {"",schema,"GETPCTEST5",null,null,null,foo,i(1),genid},
                 }
                 );
        }

        // Check the JDBC variant of getProcedures().
        JDBC.assertFullResultSet(rs[0], expRS, false);

        // Check the ODBC variant of getProcedures(). It's identical to the
        // JDBC variant, except that it lacks the last column.
        for (int i = 0; i < expRS.length; i++) {
            Object[] jdbcRow = expRS[i];
            Object[] odbcRow = new Object[jdbcRow.length - 1];
            System.arraycopy(jdbcRow, 0, odbcRow, 0, odbcRow.length);
            expRS[i] = odbcRow;
        }
        JDBC.assertFullResultSet(rs[1], expRS, false);

        rs = getProcedureColumns(null, "%", "GETPCTEST%", "%");

        expRS = new Object[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                {null,schema,"GETPCTEST1","OUTB",i(4),i(12),"VARCHAR",i(3),i(6),null,null,i(1),null,null,i(12),null,i(6),i(1),"YES",genid,i(12),i(0)},
                {null,schema,"GETPCTEST1","A",i(1),i(12),"VARCHAR",i(3),i(6),null,null,i(1),null,null,i(12),null,i(6),i(2),"YES",genid,i(12),i(1)},
                {null,schema,"GETPCTEST1","B",i(1),i(2),"NUMERIC",i(5),i(14),i(0),i(10),i(1),null,null,i(2),null,null,i(3),"YES",genid,i(12),i(2)},
                {null,schema,"GETPCTEST1","C",i(1),i(5),"SMALLINT",i(5),i(2),i(0),i(10),i(1),null,null,i(5),null,null,i(4),"YES",genid,i(12),i(3)},
                {null,schema,"GETPCTEST1","E",i(1),i(5),"SMALLINT",i(5),i(2),i(0),i(10),i(1),null,null,i(5),null,null,i(5),"YES",genid,i(12),i(4)},
                {null,schema,"GETPCTEST1","F",i(1),i(4),"INTEGER",i(10),i(4),i(0),i(10),i(1),null,null,i(4),null,null,i(6),"YES",genid,i(12),i(5)},
                {null,schema,"GETPCTEST1","G",i(1),i(-5),"BIGINT",i(19),i(40),i(0),i(10),i(1),null,null,i(-5),null,null,i(7),"YES",genid,i(12),i(6)},
                {null,schema,"GETPCTEST1","H",i(1),i(8),"DOUBLE",i(52),i(8),null,i(2),i(1),null,null,i(8),null,null,i(8),"YES",genid,i(12),i(7)},
                {null,schema,"GETPCTEST1","I",i(1),i(8),"DOUBLE",i(52),i(8),null,i(2),i(1),null,null,i(8),null,null,i(9),"YES",genid,i(12),i(8)},
                {null,schema,"GETPCTEST1","K",i(1),i(91),"DATE",i(10),i(6),i(0),i(10),i(1),null,null,i(9),i(1),null,i(10),"YES",genid,i(12),i(9)},
                {null,schema,"GETPCTEST1","L",i(1),i(92),"TIME",i(8),i(6),i(0),i(10),i(1),null,null,i(9),i(2),null,i(11),"YES",genid,i(12),i(10)},
                {null,schema,"GETPCTEST1","T",i(1),i(93),"TIMESTAMP",i(29),i(16),i(9),i(10),i(1),null,null,i(9),i(3),null,i(12),"YES",genid,i(12),i(11)},
                {null,schema,"GETPCTEST2","PA",i(1),i(4),"INTEGER",i(10),i(4),i(0),i(10),i(1),null,null,i(4),null,null,i(1),"YES",genid,i(2),i(0)},
                {null,schema,"GETPCTEST2","PB",i(1),i(-5),"BIGINT",i(19),i(40),i(0),i(10),i(1),null,null,i(-5),null,null,i(2),"YES",genid,i(2),i(1)},
                {null,schema,"GETPCTEST3A","STRING1",i(1),i(12),"VARCHAR",i(5),i(10),null,null,i(1),null,null,i(12),null,i(10),i(1),"YES",genid,i(2),i(0)},
                {null,schema,"GETPCTEST3A","STRING2",i(4),i(12),"VARCHAR",i(5),i(10),null,null,i(1),null,null,i(12),null,i(10),i(2),"YES",genid,i(2),i(1)},
                {null,schema,"GETPCTEST3B","STRING3",i(1),i(12),"VARCHAR",i(5),i(10),null,null,i(1),null,null,i(12),null,i(10),i(1),"YES",genid,i(2),i(0)},
                {null,schema,"GETPCTEST3B","STRING4",i(2),i(12),"VARCHAR",i(5),i(10),null,null,i(1),null,null,i(12),null,i(10),i(2),"YES",genid,i(2),i(1)},
                {null,schema,"GETPCTEST4BX","RETPARAM",i(4),i(4),"INTEGER",i(10),i(4),i(0),i(10),i(1),null,null,i(4),null,null,i(1),"YES",genid,i(1),i(0)},
        };
        if ( supportsBoolean )
        {
            expRS = appendArray
                (
                 expRS,
                 new Object[][]
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                     {null,schema,"GETPCTEST5","INARG",i(1),i(16),"BOOLEAN",i(1),i(1),null,null,i(1),null,null,i(16),null,null,i(1),"YES",genid,i(3),i(0)},
                     {null,schema,"GETPCTEST5","OUTARG",i(4),i(16),"BOOLEAN",i(1),i(1),null,null,i(1),null,null,i(16),null,null,i(2),"YES",genid,i(3),i(1)},
                     {null,schema,"GETPCTEST5","INOUTARG",i(2),i(16),"BOOLEAN",i(1),i(1),null,null,i(1),null,null,i(16),null,null,i(3),"YES",genid,i(3),i(2)},
                 }
                 );
        }

        Object[][] jdbcExpRS = new Object[expRS.length][];
        Object[][] odbcExpRS = new Object[expRS.length][];
        for (int i = 0; i < jdbcExpRS.length; i++) {
            Object[] row = expRS[i];

            // JDBC variant has always null in column #15 (SQL_DATA_TYPE) and
            // column #16 (SQL_DATETIME_SUB)
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            Object[] jdbcRow = row.clone();
            jdbcRow[14] = jdbcRow[15] = null;

            // ODBC variant lacks column #20 (SPECIFIC_NAME)...
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            ArrayList<Object> odbcRow =
                    new ArrayList<Object>(Arrays.asList(row));
            odbcRow.remove(19);
            // ... and it is a bit different in the datetime rows
            if (i == 9) {
                odbcRow.set(9, null);
            }
            if (i >= 9 && i <= 11) {
                odbcRow.set(10, i(2));
            }

            jdbcExpRS[i] = jdbcRow;
            odbcExpRS[i] = odbcRow.toArray();
        }

        JDBC.assertFullResultSet(rs[0], jdbcExpRS, false);
        JDBC.assertFullResultSet(rs[1], odbcExpRS, false);

//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        if ( supportsBoolean ) { s.execute("drop procedure GETPCTEST5"); }
        s.execute("drop procedure GETPCTEST4Bx");
        s.execute("drop procedure GETPCTEST4B");
        s.execute("drop procedure GETPCTEST4A");
        s.execute("drop procedure GETPCTEST3B");
        s.execute("drop procedure GETPCTEST3A");
        s.execute("drop procedure GETPCTEST2");
        s.execute("drop procedure GETPCTEST1");
        commit();
    }

    /**
     * Append one two-dimensional array to another.
     * @param target
     * @param suffix
     * @return appended array
     */
    private Object[][] appendArray(Object[][] target, Object[][] suffix)
    {
        int targetLength = target.length;
        int suffixLength = suffix.length;
        int resultLength = targetLength + suffixLength;

//IC see: https://issues.apache.org/jira/browse/DERBY-4933
        Object[][] result = new Object[resultLength][];
        System.arraycopy(target, 0, result, 0, targetLength);
        System.arraycopy(suffix, 0, result, targetLength, suffixLength);

        println( "Appended array" );

        return result;
    }

    /**
     * Helper method for testing getProcedures - calls the ODBC procedure
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @return the result set of the query
     * @throws SQLException 
     */
    private ResultSet getProceduresODBC(
            String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLPROCEDURES(" +
                "?, ?, ?, 'DATATYPE=''ODBC''')");
            cs.setString(1, catalog);
            cs.setString(2, schemaPattern);
            cs.setString(3, procedureNamePattern);
            cs.execute();
            return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getProcedures - calls dmd.getProcedures for
     * the JDBC call, and getProceduresODBC for the ODBC procedure
     *
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     *
     * @throws SQLException 
     */
    private ResultSet[] getProcedures(
            String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException 
    {
        ResultSet[] rs = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rs[0]= dmd.getProcedures(catalog, schemaPattern, procedureNamePattern);
        rs[1]= getProceduresODBC(catalog, schemaPattern, procedureNamePattern);
        
        String[] columnNames = new String[] {
                "PROCEDURE_CAT","PROCEDURE_SCHEM","PROCEDURE_NAME",
                "RESERVED1","RESERVED2","RESERVED3",
                "REMARKS","PROCEDURE_TYPE","SPECIFIC_NAME"};

        int[] columnTypes = new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.SMALLINT,
                Types.VARCHAR};

        boolean[] nullability = new boolean[] {
                false, false, false, true, true, true, true, true, false};
        
        // JDBC result set
        assertMetaDataResultSet(rs[0], columnNames, columnTypes, nullability);
        
        // change the shape for ODBC - one less column, no SPECIFIC_NAME (9)
        
        String[] odbcColumnNames = new String[columnNames.length - 1];
        System.arraycopy(columnNames, 0, odbcColumnNames, 0, odbcColumnNames.length);
        int[] odbcColumnTypes = new int[columnTypes.length - 1];
        System.arraycopy(columnTypes, 0, odbcColumnTypes, 0, odbcColumnTypes.length);
        boolean[] odbcNullability = new boolean[nullability.length - 1];
        System.arraycopy(nullability, 0, odbcNullability, 0, odbcNullability.length);
        
        // Change column names
        odbcColumnNames[4 - 1] = "NUM_INPUT_PARAMS";
        odbcColumnNames[5 - 1] = "NUM_OUTPUT_PARAMS";
        odbcColumnNames[6 - 1] = "NUM_RESULT_SETS";
              
        // ODBC result set.
        assertMetaDataResultSet(
                rs[1], odbcColumnNames, odbcColumnTypes, odbcNullability);

        return rs;        
    }

    /**
     * Helper method for testing getProcedureColumns - calls the ODBC procedure
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @param columnNamePattern
     * @return the result set of the query
     * @throws SQLException 
     */
    private ResultSet getProcedureColumnsODBC(String catalog, 
            String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException 
    {
        CallableStatement cs = prepareCall("CALL SYSIBM.SQLPROCEDURECOLS(" +
                "?, ?, ?, ?, 'DATATYPE=''ODBC''')");

        cs.setString(1, catalog);
        cs.setString(2, schemaPattern);
        cs.setString(3, procedureNamePattern);
        cs.setString(4, columnNamePattern);
        cs.execute();
        return cs.getResultSet();
    }
    
    /**
     * Helper method for testing getProcedureColumns - calls 
     * dmd.getProcedureColumns for the JDBC call, and 
     * getProcedureColumnssODBC for the ODBC procedure
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @param columnNamePattern
     * @return an array of two result sets corresponding to JDBC and ODBC
     *         respectively
     * @throws SQLException 
     */
    private ResultSet[] getProcedureColumns(String catalog, 
            String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException 
    {
        ResultSet[] rss = new ResultSet[2]; 
        DatabaseMetaData dmd = getDMD();
        rss[0]= dmd.getProcedureColumns(catalog, schemaPattern, 
                procedureNamePattern, columnNamePattern);
        rss[1]= getProcedureColumnsODBC(catalog, schemaPattern,
                procedureNamePattern, columnNamePattern);
        
        String[] columnNames = new String[] {
                "PROCEDURE_CAT","PROCEDURE_SCHEM","PROCEDURE_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
                "PRECISION", "LENGTH", "SCALE",
                "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME" //};
                // interesting, we seem to have two extra columns vs the API
                ,"METHOD_ID", "PARAMETER_ID"};

        int[] columnTypes = new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.INTEGER, Types.VARCHAR, Types.INTEGER,
                Types.INTEGER, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR //};
                , Types.SMALLINT, Types.SMALLINT};

        boolean[] nullability = new boolean[] {
//IC see: https://issues.apache.org/jira/browse/DERBY-2775
//IC see: https://issues.apache.org/jira/browse/DERBY-3346
//IC see: https://issues.apache.org/jira/browse/DERBY-3342
                true, false, false, false, false, false, false, false, false, true,
                true, false, true, true, true, true, true, false, false, false//};
                , false, false};

        
        // JDBC result set
        assertMetaDataResultSet(rss[0], columnNames, columnTypes, nullability);
        
        // Change expected shape for ODBC
        
        // One less column for ODBC, (20) SPECIFIC_NAME is missing.
        
        String[] odbcColumnNames = new String[columnNames.length - 1];
        System.arraycopy(columnNames, 0, odbcColumnNames, 0, 19);
        System.arraycopy(columnNames, 21 - 1, odbcColumnNames, 20 - 1, 2);
        
        int[] odbcColumnTypes = new int[columnTypes.length - 1];
        System.arraycopy(columnTypes, 0, odbcColumnTypes, 0, 19);
        System.arraycopy(columnTypes, 21 - 1, odbcColumnTypes, 20 - 1, 2);
        
        //      SQL_DATA_TYPE NULL in JDBC, valid type in ODBC.
        // otherwise the same as JDBC
        boolean[] odbcNullability = new boolean[] {
//IC see: https://issues.apache.org/jira/browse/DERBY-3400
                true, false, false, false, false, false, false, false, false, true,
                true, false, true, true, false, true, true, false, false, false
                , false};

        
        // And change some column names.
        odbcColumnNames[8 - 1] = "COLUMN_SIZE";
        odbcColumnNames[9 - 1] = "BUFFER_LENGTH";
        odbcColumnNames[10 - 1] = "DECIMAL_DIGITS";
        odbcColumnNames[11 - 1] = "NUM_PREC_RADIX";
        
        // And some column types.
        odbcColumnTypes[6 - 1] = Types.SMALLINT;
        odbcColumnTypes[15 - 1] = Types.SMALLINT;
        odbcColumnTypes[16 - 1] = Types.SMALLINT;
             
        // odbc result set
        assertMetaDataResultSet(
            rss[1], odbcColumnNames, odbcColumnTypes, odbcNullability);
        
        return rss;        
    }

    /**
     * Test DatabaseMetaData.getFunctionColumns()
     *
     * @throws java.lang.Exception
     */
    public void testGetFunctionColumns() throws Exception
    {
        // this method is supported in database meta data only from 10.2 onward

        boolean supportsBoolean = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        Version dataVersion = getDataVersion( getConnection() );
        if ( dataVersion.compareTo( new Version( 10, 7, 0, 0 ) ) < 0 ) { supportsBoolean = false; }
        
        if ( dataVersion.compareTo( new Version( 10, 2, 0, 0 ) ) < 0 ) { return; }

        DatabaseMetaData dmd = getDMD();
        Statement s = createStatement();
        getConnection().setAutoCommit(false);
        
        s.execute("create function f_gfc_1 (" +
                "a VARCHAR(3), b NUMERIC, c SMALLINT, " +
                "e CHAR(3), f INTEGER, g BIGINT, h FLOAT, i DOUBLE PRECISION, " +
                "k DATE, l TIME, T TIMESTAMP ) returns int "+
                "language java external name " +
                "'org.apache.derbyTesting.BlahBlah.blah'" +
        " parameter style java"); 

        if ( supportsBoolean )
        {
            s.execute("create function f_gfc_2 ( a boolean) returns boolean "+
                      "language java external name " +
                      "'org.apache.derbyTesting.functionTests.BlahBlah.blah'" +
                      " parameter style java");
        }

        // We have to use reflection to get the getFunctionColumns() method.
        // That is because we compile this test to run on old versions of the
        // vm whose DatabaseMetaData doesn't include this method, even though
        // our actual drivers do.
        Method gfcMethod = dmd.getClass().getMethod
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
            ( "getFunctionColumns", new Class<?>[] {
                String.class, String.class, String.class, String.class, } );
        
        ResultSet rs = (ResultSet) gfcMethod.invoke(
                dmd, (Object[])new String[] { null, "%", "F_GFC_%", "%" } );

        JDBC.GeneratedId genid = new JDBC.GeneratedId();

        Object[][] expRS = new Object[][] {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            {null,schema,"F_GFC_1","",i(4),i(4),"INTEGER",i(10),i(4),i(0),i(10),i(1),null,null,i(0),"YES",genid,i(11),i(-1)},
            {null,schema,"F_GFC_1","A",i(1),i(12),"VARCHAR",i(3),i(6),null,null,i(1),null,i(6),i(1),"YES",genid,i(11),i(0)},
            {null,schema,"F_GFC_1","B",i(1),i(2),"NUMERIC",i(5),i(14),i(0),i(10),i(1),null,null,i(2),"YES",genid,i(11),i(1)},
            {null,schema,"F_GFC_1","C",i(1),i(5),"SMALLINT",i(5),i(2),i(0),i(10),i(1),null,null,i(3),"YES",genid,i(11),i(2)},
            {null,schema,"F_GFC_1","E",i(1),i(1),"CHAR",i(3),i(6),null,null,i(1),null,i(6),i(4),"YES",genid,i(11),i(3)},
            {null,schema,"F_GFC_1","F",i(1),i(4),"INTEGER",i(10),i(4),i(0),i(10),i(1),null,null,i(5),"YES",genid,i(11),i(4)},
            {null,schema,"F_GFC_1","G",i(1),i(-5),"BIGINT",i(19),i(40),i(0),i(10),i(1),null,null,i(6),"YES",genid,i(11),i(5)},
            {null,schema,"F_GFC_1","H",i(1),i(8),"DOUBLE",i(52),i(8),null,i(2),i(1),null,null,i(7),"YES",genid,i(11),i(6)},
            {null,schema,"F_GFC_1","I",i(1),i(8),"DOUBLE",i(52),i(8),null,i(2),i(1),null,null,i(8),"YES",genid,i(11),i(7)},
            {null,schema,"F_GFC_1","K",i(1),i(91),"DATE",i(10),i(6),i(0),i(10),i(1),null,null,i(9),"YES",genid,i(11),i(8)},
            {null,schema,"F_GFC_1","L",i(1),i(92),"TIME",i(8),i(6),i(0),i(10),i(1),null,null,i(10),"YES",genid,i(11),i(9)},
            {null,schema,"F_GFC_1","T",i(1),i(93),"TIMESTAMP",i(29),i(16),i(9),i(10),i(1),null,null,i(11),"YES",genid,i(11),i(10)},
        };

        if ( supportsBoolean )
        {
            expRS = appendArray
                (
                 expRS,
                 new Object[][]
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                     {null,schema,"F_GFC_2","",i(4),i(16),"BOOLEAN",i(1),i(1),null,null,i(1),null,null,i(0),"YES",genid,i(1),i(-1)},
                     {null,schema,"F_GFC_2","A",i(1),i(16),"BOOLEAN",i(1),i(1),null,null,i(1),null,null,i(1),"YES",genid,i(1),i(0)},
                 }
                 );
        }

        JDBC.assertFullResultSet(rs, expRS, false);

        if ( supportsBoolean ) { s.execute("drop function f_gfc_2"); }
        s.execute("drop function f_gfc_1");
        commit();
    }

    /**
     * Convert an {@code int} to a {@code java.lang.Integer}.
     * @param i
     * @return an Integer
     */
    private static Integer i(int i) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        return i;
    }

    /**
     * Test methods added by JDBC 4.1
     *
     * @throws java.lang.Exception
     */
    public void test_jdbc4_1() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        Version dataVersion = getDataVersion( getConnection() );
        if ( dataVersion.compareTo( new Version( 10, 8, 0, 0 ) ) < 0 ) { return; }

        Statement s = createStatement();
        DatabaseMetaData dmd = getDMD();
        println( "Testing JDBC 4.1 methods on a " + dmd.getClass().getName() );
        Wrapper41DBMD wrapper = new Wrapper41DBMD( dmd );

        //
        // generatedKeyAlwaysReturned()
        //
        assertEquals( true, wrapper.generatedKeyAlwaysReturned() );

        //
        // getPseudoColumns()
        //
        ResultSet   rs = wrapper.getPseudoColumns( null, null, null, null );
        assertMetaDataResultSet
            (
             rs,
             new String[]
             {
                 "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                 "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                 "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE",
             },
             new int[]
             {
                 Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                 Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                 Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
             },
             new boolean[]
             {
                 true,  true,   false,  false,
                 false, false,  true,   true,
                 false, true,   true,   false,
             }
             );
        JDBC.assertFullResultSet( rs, new String[][] {} );

        //
        // IS_GENERATEDCOLUMN added to ResultSet returned by getColumns()
        //
        s.execute( "create table t_jdbc41( a int, b int, c generated always as ( -a ) )" );
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        ResultSet   rs2 = dmd.getColumns( null, schema, "T_JDBC41", null );
        String[][] expectedRows = new String[][]
        {
            {
                "", schema, "T_JDBC41", "A",
                "4", "INTEGER", "10", null,
                "0", "10", "1", "",
                null, null, null, null,
                "1", "YES", null, null,
                null, null, "NO", "NO",
                null
            },
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                "", schema, "T_JDBC41", "B",
                "4", "INTEGER", "10", null,
                "0", "10", "1", "",
                null, null, null, null,
                "2", "YES", null, null,
                null, null, "NO", "NO",
                null
            },
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
                "", schema, "T_JDBC41", "C",
                "4", "INTEGER", "10", null,
                "0", "10", "1", "",
                "GENERATED ALWAYS AS ( -a )", null, null, null,
                "3", "YES", null, null,
                null, null, "NO", "YES",
                null
            },
        };
        JDBC.assertFullResultSet( rs2, expectedRows );


        s.execute( "drop table t_jdbc41" );
    }
    
    /**
     * Test methods added by JDBC 4.2
     *
     * @throws java.lang.Exception
     */
    public void test_jdbc4_2() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        Version dataVersion = getDataVersion( getConnection() );
        if ( dataVersion.compareTo( new Version( 10, 10, 0, 0 ) ) < 0 ) { return; }

        Statement s = createStatement();
        DatabaseMetaData dmd = getDMD();
        println( "Testing JDBC 4.2 methods on a " + dmd.getClass().getName() );
        Wrapper42DBMD wrapper = new Wrapper42DBMD( dmd );

        //
        // getMaxLogicalLobSize()
        //
        assertEquals( 0L, wrapper.getMaxLogicalLobSize() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6000

        //
        // supportsRefCursors()
        //
        assertEquals( false, wrapper.supportsRefCursors() );
    }

    public void testBugFixes() throws SQLException {
        
        Statement s = createStatement();
        getConnection().setAutoCommit(false);        
        DatabaseMetaData dmd = getDMD();

        // test DERBY-655, DERBY-1343
        // If a table has duplicate backing index, then it will share the 
        // physical conglomerate with the existing index, but the duplicate
        // indexes should have their own unique logical congomerates 
        // associated with them. That way, it will be possible to 
        // distinguish the 2 indexes in SYSCONGLOMERATES from each other.
        s.execute("CREATE TABLE Derby655t1(c11_ID BIGINT NOT NULL)");
        s.execute("CREATE TABLE Derby655t2 (c21_ID BIGINT NOT NULL primary key)");
        s.execute("ALTER TABLE Derby655t1 ADD CONSTRAINT F_12 Foreign Key (c11_ID) REFERENCES Derby655t2 (c21_ID) ON DELETE CASCADE ON UPDATE NO ACTION");
        s.execute("CREATE TABLE Derby655t3(c31_ID BIGINT NOT NULL primary key)");
        s.execute("ALTER TABLE Derby655t2 ADD CONSTRAINT F_443 Foreign Key (c21_ID) REFERENCES Derby655t3(c31_ID) ON DELETE CASCADE ON UPDATE NO ACTION");

//IC see: https://issues.apache.org/jira/browse/DERBY-5764
        ResultSet rs = dmd.getImportedKeys("", schema, "DERBY655T1");
        JDBC.assertDrainResults(rs, 1);
        
        s.execute("drop table Derby655t1");
        s.execute("drop table Derby655t2");
        s.execute("drop table Derby655t3");

        // This checks for a bug where you get incorrect behavior on a nested connection.
        // if you do not get an error, the bug does not occur.          
        if(JDBC.vmSupportsJDBC3()){
            s.execute("create procedure isReadO() language java external name " +
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
//IC see: https://issues.apache.org/jira/browse/DERBY-514
                    "'org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest.isro'" +
            " parameter style java"); 
            s.execute("call isReadO()");
        }
    }
    /**
     * method used in testBugFixes, for testing nexted connection metadata
     * @throws SQLException
     */
    public static void isro() throws SQLException {
        DriverManager.getConnection(
        "jdbc:default:connection").getMetaData().isReadOnly();
    }

    /**
     * getColumns() used to fail with a truncation error if an auto-increment
     * column had a start value or an increment that was very large (that is,
     * when its CHAR representation exceeded 12 characters). DERBY-5274.
     *
     * @throws java.sql.SQLException
     */
    public void testGetColumns_DERBY5274() throws SQLException {
        // Disable auto-commit to allow easy cleanup.
        setAutoCommit(false);

        Statement s = createStatement();

        // Create a test table with an identity column whose start value and
        // increment are very large.
        final long bignum = 648518346341351400L;
        s.execute("create table derby5274(x bigint not null " +
                  "generated always as identity (" +
                  "start with " + bignum + ", increment by " + bignum + "))");

        // Expected values for various columns in the meta-data.
        String[][] expected = {
//IC see: https://issues.apache.org/jira/browse/DERBY-5764
            {"TABLE_SCHEM", schema},
            {"TABLE_NAME", "DERBY5274"},
            {"COLUMN_NAME", "X"},
            {"COLUMN_DEF",
                "AUTOINCREMENT: start " + bignum + " increment " + bignum},
            {"IS_NULLABLE", "NO"},
        };

        // Get meta-data for the column in the test table. This used to fail
        // with a truncation error before DERBY-5274.
        ResultSet rs = getDMD().getColumns(null, null, "DERBY5274", null);

        // Verify that the returned meta-data looks right.
        assertTrue("No columns found", rs.next());
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        for (String[] expected1 : expected) {
            String label = expected1[0];
            String expectedVal = expected1[1];
            assertEquals(label, expectedVal, rs.getString(label));
        }

        // There's only one column in the test table, so there should be no
        // more rows in the meta-data.
        JDBC.assertEmpty(rs);

        // Clean up.
        rollback();
    }
    
    /**
     * Dummy method to test getProcedureColumns
     *
     * @param a
     * @param b
     * @param k
     * @param c
     * @param e
     * @param d
     * @param f
     * @param g
     * @param j
     * @param i
     * @param l
     * @param h
     * @param T
     * @return the {@code j} byte array
     */
    public static byte[] getpc(
//IC see: https://issues.apache.org/jira/browse/DERBY-6623
        String a, BigDecimal b, short c, byte d, short e,
        int f, long g, float h, double i, byte[] j, Date k, Time l, Timestamp T)
    {
        return j;
    }

    /**
     * Overload getpc to further test getProcedureColumns
     *
     * @param a
     * @param b
     */
    public static void getpc(int a, long[] b)
    {
    }

    /**
     * Overload getpc to further test getProcedureColumns
     * private method shouldn't be returned with alias, ok with procedure
     *
     * @param a
     * @param b
     */
    private static void getpc(int a, long b)
    {
    }

    /** 
     * instance method for getProcedureColumns testing
     * with method alias, this should not be returned by getProcedureColumns
     * but DB2 returns this with a java procedure
     * @param a
     * @param b
     */ 
    public void getpc(String a, String b) {
    }

    /**
     * this method should notbe seen by getProcedureColumns as
     * it has no parameters and no return value.
     */
    public static void getpc4a() {
    }

    /**
     * Check a method with no parameters and a return value works
     * for getProcedureColumns
     *
     * @return the int 4
     */
    public static int getpc4b() {
        return 4;
    }

    /**
     * Reading of DatabaseMetaData obtained earlier, after a connection
     * is closed.
     *
     * @throws java.sql.SQLException
     */
    public void testDMDconnClosed() throws SQLException {
        ResultSet rs_ = getConnection().getMetaData().
        getTables("%","%","%",null); // should work
        getConnection().close();
        try {
            //should throw exception since the connection is closed
            rs_.next();
            fail("No Exception throw when getting metadata.");
        } catch(SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }
    }

    /**
     * <p>
     * Get the version of the data in the database.
     * </p>
     * @param conn the current connection
     * @return data base version
     * @throws java.lang.Exception
     */
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    private Version getDataVersion( Connection conn )
        throws Exception
    {
        PreparedStatement ps = null;
        ResultSet               rs = null;

        try {
            ps = conn.prepareStatement( "values syscs_util.syscs_get_database_property('DataDictionaryVersion')" );
            rs = ps.executeQuery();

            rs.next();

            String retval = rs.getString( 1 );
            int  dotIdx = retval.indexOf( '.' );
            int major = Integer.parseInt( retval.substring( 0, dotIdx ) );
            int minor = Integer.parseInt( retval.substring( dotIdx + 1, retval.length() ) );

            return new Version( major, minor, 0, 0 );
        }
        catch (Exception se)
        {
            printStackTrace( se );
            return null;
        }
        finally
        {
            if ( rs != null ) { rs.close(); }
            if ( ps != null ) { ps.close(); }
        }
    }
}
