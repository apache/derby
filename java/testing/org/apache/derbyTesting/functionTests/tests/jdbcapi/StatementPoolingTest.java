/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.StatementPoolingTest

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * A set of tests specifically targeted at connections that support statement
 * pooling.
 */
public class StatementPoolingTest
    extends BaseJDBCTestCase {

    public StatementPoolingTest(String name) {
        super(name);
    }

    /**
     * Tests that the statement cache is able to throw out prepared statements
     * when it reaches maximum capacity.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testCacheOverflow()
            throws SQLException {
        final int stmtCount = 150;
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpDs, "maxStatements", new Integer(11));
        J2EEDataSource.setBeanProperty(cpDs, "createDatabase", "create");
        PooledConnection pc = cpDs.getPooledConnection();
        Connection con = pc.getConnection();
        for (int i=0; i < stmtCount; i++) {
            // Yes, the "values + i" is intended here.
            PreparedStatement pStmt = con.prepareStatement("values " + i);
            ResultSet rs = pStmt.executeQuery();
            JDBC.assertSingleValueResultSet(rs, Integer.toString(i));
            pStmt.close();
        }
        con.close();
        pc.close();
    }

    /**
     * Verifies that statement pooling is enabled by checking the names of the
     * implementation classes in Derby.
     *
     * @throws SQLException if creating the JDBC objects fail
     */
    public void testPoolingEnabledByCheckingImplementationDetails()
            throws SQLException {
        final String conClass = "CachingLogicalConnection";
        final String psClass = "LogicalPreparedStatement";
        final String csClass = "LogicalCallableStatement";
        Connection con = getConnection();
        assertClassName(con, conClass);
        assertClassName(con.prepareStatement("values 1"), psClass);
        assertClassName(con.prepareStatement("values 1",
                                             Statement.RETURN_GENERATED_KEYS),
                        psClass);
        assertClassName(con.prepareStatement("values 1",
                                             Statement.NO_GENERATED_KEYS),
                        psClass);
        assertClassName(con.prepareStatement("values 1",
                                             ResultSet.TYPE_FORWARD_ONLY,
                                             ResultSet.CONCUR_READ_ONLY),
                        psClass);
        assertClassName(con.prepareStatement("values 1",
                                             ResultSet.TYPE_SCROLL_INSENSITIVE,
                                             ResultSet.CONCUR_UPDATABLE),
                        psClass);
        assertClassName(con.prepareStatement("values 1",
                                             (String[])null),
                        psClass);
        assertClassName(con.prepareStatement("values 1",
                                             new String[] {}),
                        psClass);
        assertClassName(con.prepareCall("values 1"), csClass);
        assertClassName(con.prepareCall("values 1",
                                        ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY),
                        csClass);
        assertClassName(con.prepareCall("values 1",
                                        ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY,
                                        ResultSet.CLOSE_CURSORS_AT_COMMIT),
                        csClass);
    }

    /**
     * Assert that the name of the class of the object is what is expected.
     * <p>
     * The assert does not consider package names, only the name passed in as
     * {@code expectedName} and the passed in name concatenated with "40".
     *
     * @param obj object to check
     * @param expectedName the expected name of the class
     * @throws AssertionFailedError if the class name is not as expected
     */
    private static void assertClassName(Object obj, String expectedName) {
        assertNotNull("The expected name cannot be <null>", expectedName);
        assertNotNull("The object cannot be <null>", obj);
        String[] names = obj.getClass().getName().split("\\.");
        String simpleName = names[names.length -1];
        if (!simpleName.equals(expectedName)) {
            if (!simpleName.equals(expectedName + "40")) {
                fail("Expected class name " + expectedName + " or " +
                        expectedName + "40, got " + simpleName);
            }
        }
    }

    /**
     * This test merley checks that creating a logical prepared statement does
     * not fail.
     *
     * @throws SQLException if creating the prepared statement fails
     */
    public void testPrepareStatementPath()
            throws SQLException {
        PreparedStatement ps = prepareStatement("values 9708");
        ps.close();
    }

    /**
     * This test merley checks that creating a logical callable statement does
     * not fail.
     *
     * @throws SQLException if creating the callable statement fails
     */
    public void testPrepareCallPath()
            throws SQLException {
        CallableStatement cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        cs.close();
    }

    /**
     * This test merley checks that creating a logical callable statement, which
     * is not really a call, does not fail.
     *
     * @throws SQLException if creating the callable statement fails
     */
    public void testPrepareCallWithNoCallPath()
            throws SQLException {
        CallableStatement cs = prepareCall("values 1");
        cs.close();
    }

    /**
     * Tests that closing the prepared statement also closes the result set.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testClosingPSClosesRS()
            throws SQLException {
        PreparedStatement ps = prepareStatement("values 99");
        ResultSet rs = ps.executeQuery();
        ps.close();
        try {
            rs.next();
            fail("Result set should have been closed");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }
    }

    /**
     * Tests that the connection holdability is reset, when it is first
     * modified, the connection closed and a new logical connection obtained.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testHoldabilityIsResetExplicitClose()
            throws SQLException {
        doTestHoldabilityIsReset(true);
    }

    /**
     * Tests that the connection holdability is reset, when it is first
     * modified, and a new logical connection obtained without first explicitly
     * closing the previous one.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testHoldabilityIsResetNoExplicitClose()
            throws SQLException {
        doTestHoldabilityIsReset(false);
    }

    /**
     * Test sequence for testing if the connection holdability is reset.
     *
     * @param closeConnection determines if the logical connection is
     *      explicitly closed before a new one is obtained
     * @throws SQLException if something goes wrong...
     */
    private void doTestHoldabilityIsReset(final boolean closeConnection)
            throws SQLException {
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpDs, "maxStatements", new Integer(7));
        J2EEDataSource.setBeanProperty(cpDs, "createDatabase", "create");
        PooledConnection pc = cpDs.getPooledConnection();
        // Keep track of our own connection, the framework currently creates
        // a new pooled connection and then obtains a connection from that.
        // Statement pooling only works within a single pooled connection.
        Connection con = pc.getConnection();
        assertEquals("Unexpected default holdability",
                ResultSet.HOLD_CURSORS_OVER_COMMIT, con.getHoldability());
        con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals("Holdability not updated",
                ResultSet.CLOSE_CURSORS_AT_COMMIT, con.getHoldability());
        if (closeConnection) {
            con.close();
        }
        con = pc.getConnection();
        assertEquals("Holdability not reset",
                ResultSet.HOLD_CURSORS_OVER_COMMIT, con.getHoldability());
        pc.close();
    }

    public void testIsolationLevelIsResetExplicitCloseQuery()
            throws SQLException {
        doTestIsolationLevelIsReset(true, true);
    }

    public void testIsolationLevelIsResetExplicitCloseNoQuery()
            throws SQLException {
        doTestIsolationLevelIsReset(true, false);
    }

    public void testIsolationLevelIsResetNoExplicitCloseNoQuery()
            throws SQLException {
        doTestIsolationLevelIsReset(false, false);
    }

    public void testIsolationLevelIsResetNoExplicitCloseQuery()
            throws SQLException {
        doTestIsolationLevelIsReset(false, true);
    }

    /**
     * Tests if the connection isolation level is reset when a new connection
     * is obtained.
     * <p>
     * The two arguments are introduced to test different scenarios; explicit
     * and implicit connection closing, and session data caching (piggybacked
     * information).
     *
     * @param closeConnection tells if the connection is explicitly closed
     *      before a new one is obtained
     * @param executeQuery tells if a query is executed on the connection before
     *      a new connection is obtained.
     * @throws SQLException if something goes wrong...
     */
    private void doTestIsolationLevelIsReset(final boolean closeConnection,
                                             final boolean executeQuery)
            throws SQLException {
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpDs, "maxStatements", new Integer(7));
        J2EEDataSource.setBeanProperty(cpDs, "createDatabase", "create");
        PooledConnection pc = cpDs.getPooledConnection();
        // Keep track of our own connection, the framework currently creates
        // a new pooled connection and then obtains a connection from that.
        // Statement pooling only works within a single pooled connection.
        Connection con = pc.getConnection();
        assertEquals("Unexpected default isolation level",
                Connection.TRANSACTION_READ_COMMITTED,
                con.getTransactionIsolation());
        con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        assertEquals("Isolation level not updated",
                Connection.TRANSACTION_REPEATABLE_READ,
                con.getTransactionIsolation());
        if (executeQuery) {
            PreparedStatement ps = con.prepareStatement("values 2");
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "2");
            ps.close();
        }
        if (closeConnection) {
            con.close();
        }
        con = pc.getConnection();
        assertEquals("Isolation level not reset",
                Connection.TRANSACTION_READ_COMMITTED,
                con.getTransactionIsolation());
        pc.close();
    }

    /**
     * Tests that closing the connection closes the logical prepared statement,
     * but not the physical statement.
     * <p>
     * Since there are not public interface methods to test this, the approcah
     * taken will be this:
     * <ol> <li>Create a new table</li>
     *      <li>Prepare a statement selecting from the table.</li>
     *      <li>Close the statement, putting it into the cache.</li>
     *      <li>Delete the table.</li>
     *      <li>Prepare the selecting statement again.</li>
     *      <li>Execute the statement.</li>
     * </ol>
     * If the physical statement was closed when closing the caching logical
     * connection, the prepare will fail. If it was left open, the prepare will
     * succeed because the statement is fetched from the cache, but the
     * execution will fail becuase the table no longer exists.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testCachingLogicalConnectionCloseLeavesStatementsOpen()
            throws SQLException {
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpDs, "maxStatements", new Integer(7));
        J2EEDataSource.setBeanProperty(cpDs, "createDatabase", "create");
        PooledConnection pc = cpDs.getPooledConnection();
        // Keep track of our own connection, the framework currently creates
        // a new pooled connection and then obtains a connection from that.
        // Statement pooling only works within a single pooled connection.
        Connection con = pc.getConnection();
        con.setAutoCommit(false);
        Statement stmt = createStatement();
        stmt.executeUpdate("create table clcclso (id int)");
        PreparedStatement ps = con.prepareStatement("select * from clcclso");
        commit();
        con.close();
        try {
            ps.execute();
        } catch (SQLException sqle) {
            // Already closed.
            assertSQLState("XJ012", sqle);
        }
        stmt = createStatement();
        stmt.executeUpdate("drop table clcclso");
        commit();
        // If an exception is thrown here, statement pooling is disabled or not
        // working correctly.
        con = pc.getConnection();
        ps = con.prepareStatement("select * from clcclso");
        try {
            ps.execute();
            fail("Execution should have failed");
        } catch (SQLException sqle) {
            assertSQLState("42X05", sqle);
        }
        ps.close();
        // Make sure the connection is still valid.
        ps = con.prepareStatement("values 976");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "976");
        ps.close();
        con.close();
    }

    /**
     * Checks if a reset of one statement affects other open statement on the
     * connection.
     *
     * @throws SQLException if something goes wrong...
     */
    public void resTestCloseDoesNotAffectOtherStatement()
            throws SQLException {
        final String sql = "select * from stmtpooltest where val > 0 and val " +
                "<= 7 order by val";
        PreparedStatement psForward = prepareStatement(sql);
        ResultSet rsForward = psForward.executeQuery();
        assertTrue(rsForward.next());
        assertEquals("1", rsForward.getString(1));
        assertTrue(rsForward.next());
        assertEquals("2", rsForward.getString(1));
        PreparedStatement psScrollable = prepareStatement(sql,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rsScrollable = psScrollable.executeQuery();
        // Read seven rows from the scrollable rs, position at last row.
        for (int val=1; val <= 7; val++) {
            assertTrue(rsScrollable.next());
            assertEquals(val, rsScrollable.getInt(1));
        }

        // Create a statement, then close it.
        PreparedStatement psToClose = prepareStatement(
                "select val from stmtpooltest where val = 5");
        JDBC.assertSingleValueResultSet(psToClose.executeQuery(), "5");
        psToClose.close();
        assertTrue(rsForward.next());
        assertEquals("3", rsForward.getString(1));
        assertTrue(rsScrollable.first());
        assertEquals("1", rsScrollable.getString(1));
        // Should fetch a cached statement.
        psToClose = prepareStatement(
                "select val from stmtpooltest where val = 5");
        JDBC.assertSingleValueResultSet(psToClose.executeQuery(), "5");
        psToClose.close();
        assertTrue(rsScrollable.last());
        assertEquals("7", rsScrollable.getString(1));
        assertFalse(rsScrollable.next());
        rsScrollable.close();
        assertTrue(rsForward.next());
        assertEquals("4", rsForward.getString(1));
        rsForward.close();
    }

    /**
     * Tests that closing a logical prepared statement referring a non-existing
     * table works.
     * <p>
     * In this test, the prepared statement that will be made invalid by the
     * delete is held open by the user.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testDeleteReferringTableWhenOpen()
            throws SQLException {
        getConnection().setAutoCommit(false);
        // Create a table, insert a row, then create a statement selecting it.
        Statement stmt = createStatement();
        stmt.executeUpdate("create table testdeletewhenopen (id int)");
        stmt.executeUpdate("insert into testdeletewhenopen values 1");
        PreparedStatement ps = prepareStatement(
                "select * from testdeletewhenopen");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        // Now delete the table and logically close the prepared statement.
        stmt.executeUpdate("drop table testdeletewhenopen");
        stmt.close();
        ps.close();
        // If running without statement pooling, you will get exception here.
        ps = prepareStatement("select * from testdeletewhenopen");
        // If we get this far, there is a big change we have fetched an
        // invalid statement from the cache, but we won't get the exception
        // until we try to execute it.
        try {
            ps.executeQuery();
            fail("Prepared statement not valid, referring non-existing table");
        } catch (SQLException sqle) {
            assertSQLState("42X05", sqle);
        }
    }

    /**
     * Tests that closing a logical prepared statement referring a non-existing
     * table works.
     * <p>
     * In this test, the prepared statement that will be made invalid by the
     * delete is in the statement cache when the delete happens.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testDeleteReferringTableWhenInCache()
            throws SQLException {
        getConnection().setAutoCommit(false);
        // Create a table, insert a row, then create a statement selecting it.
        Statement stmt = createStatement();
        stmt.executeUpdate("create table testdeletewhenopen (id int)");
        stmt.executeUpdate("insert into testdeletewhenopen values 1");
        PreparedStatement ps = prepareStatement(
                "select * from testdeletewhenopen");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        // Put the statement into the cache.
        ps.close();
        // Now delete the table and fetch the cached prepared statement.
        stmt.executeUpdate("drop table testdeletewhenopen");
        stmt.close();
        // If running without statement pooling, you will get exception here.
        ps = prepareStatement("select * from testdeletewhenopen");
        // If we get this far, there is a big change we have fetched an
        // invalid statement from the cache, but we won't get the exception
        // until we try to execute it.
        try {
            ps.executeQuery();
            fail("Prepared statement not valid, referring non-existing table");
        } catch (SQLException sqle) {
            assertSQLState("42X05", sqle);
        }
    }

    public void resTestCloseCursorsAtCommit()
            throws SQLException {
        doTestResultSetCloseForHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public void resTestHoldCursorsOverCommit()
            throws SQLException {
        doTestResultSetCloseForHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Tests that a temporary table crated in one logical connection is gone
     * in the next logical connection.
     *
     * @throws SQLException if the test fails for some reason
     */
    public void testTemporaryTablesAreDeletedInNewLogicalConnection()
            throws SQLException {
        ConnectionPoolDataSource cpds =
                J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(cpds, "maxStatements", new Integer(3));
        J2EEDataSource.setBeanProperty(cpds, "createDatabase", "create");
        PooledConnection pc = cpds.getPooledConnection();
        Connection lcOne = pc.getConnection();

        // Create the first logical connection and the temporary table.
        Statement stmt = lcOne.createStatement();
        stmt.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE cpds_temp_table " +
                "(id int) ON COMMIT PRESERVE ROWS NOT LOGGED");
        // The temporary table is created in SESSION.
        JDBC.assertEmpty(
                stmt.executeQuery("select * from SESSION.cpds_temp_table"));
        stmt.executeUpdate("insert into SESSION.cpds_temp_table values 1");
        lcOne.commit();
        lcOne.close();

        // Create the second logical connection and try to query the temp table.
        Connection lcTwo = pc.getConnection();
        stmt = lcTwo.createStatement();
        try {
            stmt.executeQuery("select * from SESSION.cpds_temp_table");
            fail("Temporary table still existing in new logical connection.");
        } catch (SQLException sqle) {
            // Expect syntax error.
            assertSQLState("42X05", sqle);
        }
        lcTwo.rollback();
        lcTwo.close();
        pc.close();
    }

    /**
     * Tests if the holdability settings is taking effect, and also that the
     * result set is closed when the connection is closed.
     *
     * @param holdability result set holdability as specfied by
     *      {@link java.sql.ResultSet}
     * @throws SQLException if something goes wrong...
     */
    private void doTestResultSetCloseForHoldability(int holdability)
            throws SQLException {
        getConnection().setAutoCommit(false);
        PreparedStatement ps = prepareStatement(
                "select * from stmtpooltest order by val",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                holdability);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        commit();
        if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        getConnection().close();
        try {
            rs.next();
            fail("Should have thrown exception");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }
    }

    /**
     * Checks if closing the logical connection closes the logical statement.
     *
     * @throws SQLException if something goes wrong...
     */
    public void resTestLogicalConnectionCloseInvalidatesLogicalStatement()
            throws SQLException {
        Connection con = getConnection();
        PreparedStatement ps =
                con.prepareStatement("select * from stmtpooltest");
        // Don't execute the statement.
        assertNotNull(ps.getMetaData());
        con.close();
        try {
            ps.getMetaData();
            fail("Logical statement should be closed and throw exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }
        con = getConnection();
        ps = con.prepareStatement("select * from stmtpooltest order by val");
        // Execute the statement this time.
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertNotNull(ps.getMetaData());
        rs.close();
        con.close();
        try {
            ps.getMetaData();
            fail("Logical statement should be closed and throw exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }
    }

    /**
     * Tests that nothing is committed on the connection when autocommit is
     * disabled.
     *
     * @throws SQLException if something goes wrong...
     */
    public void resTestNoCommitOnReuse()
            throws SQLException {
        // Make sure the table is empty.
        cleanTableExceptedToBeEmpty();

        // Start test
        final String sql = "insert into stmtpooldata (val) values ?";
        getConnection().setAutoCommit(false);
        PreparedStatement ps = prepareStatement(sql);
        ps.setInt(1, 68);
        assertEquals(1, ps.executeUpdate());
        ps.close();
        ps = prepareStatement(sql);
        ps.setInt(1, 77);
        assertEquals(1, ps.executeUpdate());
        Statement stmt = createStatement();
        ResultSet rs =stmt.executeQuery(
                "select val from stmtpooldata order by val");
        JDBC.assertFullResultSet(rs, new String[][] {{"68"},{"77"}});
        rollback();
        rs = stmt.executeQuery("select val from stmtpooldata order by val");
        JDBC.assertEmpty(rs);
    }

    /**
     * Tests that autocommit is working.
     *
     * @throws SQLException if something goes wrong...
     */
    public void resTestCommitOnReuse()
            throws SQLException {
        // Make sure the table is empty.
        cleanTableExceptedToBeEmpty();

        // Start test
        final String sql = "insert into stmtpooldata (val) values ?";
        getConnection().setAutoCommit(true);
        PreparedStatement ps = prepareStatement(sql);
        ps.setInt(1, 68);
        assertEquals(1, ps.executeUpdate());
        ps.close();
        ps = prepareStatement(sql);
        ps.setInt(1, 77);
        assertEquals(1, ps.executeUpdate());
        Statement stmt = createStatement();
        ResultSet rs =stmt.executeQuery(
                "select val from stmtpooldata order by val");
        JDBC.assertFullResultSet(rs, new String[][] {{"68"},{"77"}});
        rollback();
        rs = stmt.executeQuery("select val from stmtpooldata order by val");
        JDBC.assertFullResultSet(rs, new String[][] {{"68"},{"77"}});
        // Clean up
        assertEquals(2, stmt.executeUpdate("delete from stmtpooldata"));
    }

    /**
     * Tests that nothing is committed on the connection when autocommit is
     * disabled.
     *
     * @throws SQLException if something goes wrong...
     */
    public void resTestNoDataCommittedOnInvalidTransactionState()
            throws SQLException {
        // Make sure the table is empty.
        cleanTableExceptedToBeEmpty();

        // Start test
        final String sql = "insert into stmtpooldata (val) values ?";
        getConnection().setAutoCommit(false);
        PreparedStatement ps = prepareStatement(sql);
        ps.setInt(1, 68);
        assertEquals(1, ps.executeUpdate());
        ps.close();
        ps = prepareStatement(sql);
        ps.setInt(1, 77);
        assertEquals(1, ps.executeUpdate());
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select val from stmtpooldata order by val");
        JDBC.assertFullResultSet(rs, new String[][] {{"68"},{"77"}});
        try {
            getConnection().close();
            // We should not get here, but let's see what has happened.
            // Possible symptoms:
            //   - lock timeout: connection resources has not been freed.
            //   - no rows: rollback was issued.
            //   - two rows: commit was issued.
            stmt = createStatement();
            rs = stmt.executeQuery("select val from stmtpooldata order by val");
            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            fail("Connection should not be allowed to close. Rows in table: " +
                    rows);
        } catch (SQLException sqle) {
            assertSQLState("25001", sqle);
            rollback();
        }
        stmt = createStatement();
        rs = stmt.executeQuery("select val from stmtpooldata order by val");
        JDBC.assertEmpty(rs);
    }

    /**
     * Deletes row from a test table that is expected to be empty.
     *
     * @throws SQLException if a database operation fails
     */
    private void cleanTableExceptedToBeEmpty()
            throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select * from stmtpooldata");
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }
        rs.close();
        // Delete rows if any, and print a warning if verbosity is on.
        if (rowCount > 0) {
            println("Expected empty table, got " + rowCount + " rows.");
            assertEquals(rowCount,
                    stmt.executeUpdate("delete from stmtpooldata"));
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("StatementPoolingTest suite");
        TestSuite baseSuite = new TestSuite(StatementPoolingTest.class);
        // Statement pooling is not yet enabled for XA.
        //suite.addTest(TestConfiguration.connectionXADecorator(baseSuite));
        suite.addTest(TestConfiguration.connectionCPDecorator(baseSuite));

        // Add tests that require data from the database.
        TestSuite reqDataSuite = new TestSuite("Requires data suite");
        reqDataSuite.addTest(new StatementPoolingTest(
                "resTestCloseDoesNotAffectOtherStatement"));
        reqDataSuite.addTest(new StatementPoolingTest(
                "resTestLogicalConnectionCloseInvalidatesLogicalStatement"));
        reqDataSuite.addTest(new StatementPoolingTest(
                "resTestHoldCursorsOverCommit"));
        reqDataSuite.addTest(new StatementPoolingTest(
                "resTestCloseCursorsAtCommit"));
        reqDataSuite.addTest(new StatementPoolingTest(
                "resTestNoCommitOnReuse"));
        reqDataSuite.addTest(new StatementPoolingTest(
                "resTestCommitOnReuse"));
        // This test fails, DERBY-3319 is probably the cause.
        //reqDataSuite.addTest(new StatementPoolingTest(
        //        "resTestNoDataCommittedOnInvalidTransactionState"));
        suite.addTest(TestConfiguration.connectionCPDecorator(
                new BaseJDBCTestSetup(reqDataSuite) {
                public void setUp() throws Exception {
                    // Generate some data we can use in the tests.
                    Statement stmt = getConnection().createStatement();
                    try {
                        stmt.executeUpdate("drop table stmtpooltest");
                    } catch (SQLException sqle) {
                        assertSQLState("42Y55", sqle);
                    }
                    stmt.executeUpdate("create table stmtpooltest (" +
                            "id int generated always as identity," +
                            "val int)");
                    PreparedStatement ps = getConnection().prepareStatement(
                            "insert into stmtpooltest values (DEFAULT, ?)");
                    // Insert data with val in range [1,7].
                    for (int val=1; val <= 7; val++) {
                        ps.setInt(1, val);
                        ps.addBatch();
                    }
                    ps.executeBatch();

                    try {
                        stmt.executeUpdate("drop table stmtpooldata");
                    } catch (SQLException sqle) {
                        assertSQLState("42Y55", sqle);
                    }
                    stmt.executeUpdate("create table stmtpooldata (" +
                            "id int generated always as identity," +
                            "val int)");
                    // Leave this table empty.
                }
            }));
        return TestConfiguration.clientServerDecorator(suite);
    }
}
