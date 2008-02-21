/*

   Derby - Class org.apache.derby.client.am.LogicalStatementEntityTest

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
package org.apache.derby.client.am;

import java.sql.SQLException;

import junit.framework.Test;

import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.client.am.stmtcache.StatementKeyFactory;

import org.apache.derby.jdbc.ClientDriver;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for the handling of logical prepared statements.
 */
public class LogicalStatementEntityTest
    extends BaseJDBCTestCase {

    public LogicalStatementEntityTest(String name) {
        super(name);
    }

    /**
     * Verifies that the logical statement representing a prepared statement
     * behaves correctly when it has been closed.
     *
     * @throws SQLException if a JDBC operation fails
     */
    public void testCloseBehaviorExternalPs()
            throws SQLException {
        final String sql = "values 7";
        final String schema = "APP";
        java.sql.PreparedStatement ps = prepareStatement(sql);
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, schema, getConnection().getHoldability());
        JDBCStatementCache cache = new JDBCStatementCache(10);
        LogicalStatementEntity logic =
                new LogicalStatementEntityClass(ps, stmtKey, cache);
        assertSame(ps, logic.getPhysPs());
        assertFalse(logic.isLogicalEntityClosed());
        logic.close();
        assertTrue(logic.isLogicalEntityClosed());
        logic.close();
        logic.close();
        assertTrue(logic.isLogicalEntityClosed());
        try {
            logic.getPhysPs();
            fail("Should have thrown exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }
    }

    /**
     * Verifies that the logical statement representing a callable statement
     * behaves correctly when it has been closed.
     *
     * @throws SQLException if a JDBC operation fails
     */
    public void testCloseBehaviorExternalCs()
            throws SQLException {
        final String sql = "values 3";
        final String schema = "APP";
        java.sql.CallableStatement cs = prepareCall(sql);
        StatementKey stmtKey = StatementKeyFactory.newCallable(
                sql, schema, getConnection().getHoldability());
        JDBCStatementCache cache = new JDBCStatementCache(10);
        LogicalStatementEntity logic =
                new LogicalStatementEntityClass(cs, stmtKey, cache);
        assertSame(cs, logic.getPhysCs());
        assertFalse(logic.isLogicalEntityClosed());
        logic.close();
        assertTrue(logic.isLogicalEntityClosed());
        logic.close();
        logic.close();
        assertTrue(logic.isLogicalEntityClosed());
        try {
            logic.getPhysCs();
            fail("Should have thrown exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }
    }

    /**
     * Tests that a statement equal to one in the cache is not cached when
     * closing the logical statement, and that the physical statement is closed.
     *
     * @throws SQLException if a JDBC operation fails
     */
    public void testCloseOnDuplicateStatement()
            throws SQLException {
        // Initial setup.
        final String sql = "values 7";
        final String schema = "APP";
        java.sql.PreparedStatement ps = prepareStatement(sql);
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, schema, getConnection().getHoldability());
        JDBCStatementCache cache = new JDBCStatementCache(10);
        LogicalStatementEntity logic =
                new LogicalStatementEntityClass(ps, stmtKey, cache);
        assertSame(ps, logic.getPhysPs());
        assertFalse(logic.isLogicalEntityClosed());

        // Put a statement into the cache.
        assertTrue(cache.cacheStatement(stmtKey, ps));
        // Create a second statement, equal to the first.
        java.sql.PreparedStatement psDupe = prepareStatement(sql);
        LogicalStatementEntity logicDupe =
                new LogicalStatementEntityClass(psDupe, stmtKey, cache);
        // When we ask the logical entity to close the statement now, the
        // underlying physical prepared statement should actually be closed.
        logicDupe.close();
        assertTrue(logicDupe.isLogicalEntityClosed());
        // Since we are possibly running in pre-JDBC 4, try do do something to
        // provoke exception.
        try {
            psDupe.execute();
            fail("Statement should have been closed and throw an exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }

        // The cached statement should still be open.
        java.sql.PreparedStatement psCached = cache.getCached(stmtKey);
        assertSame(ps, psCached);
        java.sql.ResultSet rs = psCached.executeQuery();
        JDBC.assertSingleValueResultSet(rs, "7");
    }

    /**
     * Asserts that closing the logical statement and caching the physical one
     * does close the logical one but not the physical one.
     *
     * @throws SQLException if a JDBC operation fails
     */
    public void testCloseWhenStatementShallBeCached()
            throws SQLException {
        // Initial setup.
        final String sql = "values 9";
        final String schema = "APP";
        java.sql.PreparedStatement ps = prepareStatement(sql);
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, schema, getConnection().getHoldability());
        JDBCStatementCache cache = new JDBCStatementCache(10);
        LogicalStatementEntity logic =
                new LogicalStatementEntityClass(ps, stmtKey, cache);
        assertSame(ps, logic.getPhysPs());
        assertFalse(logic.isLogicalEntityClosed());

        // Close the statement, it should go into the cache.
        logic.close();
        assertTrue(logic.isLogicalEntityClosed());
        // Use the physical statement.
        java.sql.ResultSet rs = ps.executeQuery();
        JDBC.assertSingleValueResultSet(rs, "9");
        // Get the statement from the cache.
        assertSame(ps, cache.getCached(stmtKey));
    }

    /**
     * Tries to execute a method on a logical statement when the underlying
     * physical statement has been closed without the logical connection
     * knowing.
     *
     * @throws SQLException if something goes wrong...
     */
    public void testClosedUnderlyingStatement()
            throws SQLException {
        // Initial setup.
        final String sql = "values 19";
        final String schema = "APP";
        java.sql.PreparedStatement ps = prepareStatement(sql);
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, schema, getConnection().getHoldability());
        JDBCStatementCache cache = new JDBCStatementCache(10);
        LogicalStatementEntity logic =
                new LogicalStatementEntityClass(ps, stmtKey, cache);
        assertSame(ps, logic.getPhysPs());
        assertFalse(logic.isLogicalEntityClosed());
        java.sql.PreparedStatement logicalPs = ClientDriver.getFactory().
                newLogicalPreparedStatement(ps, stmtKey, cache);
        assertNotNull(logicalPs.getMetaData());
        ps.close();
        try {
            logicalPs.getMetaData();
            fail("Getting meta data on a closed connection should fail");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }
        logicalPs.close();
    }

    /**
     * Tests that the cache throws out the least frequently used statement when
     * it reaches its maximum capacity, and that the thrown out statement is
     * closed in the process.
     * <p>
     * Note: This test assumes things about the replacement policy.
     *
     * @throws SQLException if a JDBC operation fails
     */
    public void testEvictionFromCache()
            throws SQLException {
        // Initial setup.
        JDBCStatementCache cache = new JDBCStatementCache(2);
        final String schema = "APP";
        java.sql.PreparedStatement ps1 = prepareStatement("values 1");
        java.sql.PreparedStatement ps2 = prepareStatement("values 2");
        java.sql.PreparedStatement ps3 = prepareStatement("values 3");
        StatementKey stmtKey1 = StatementKeyFactory.newPrepared(
                "values 1", schema, getConnection().getHoldability());
        StatementKey stmtKey2 = StatementKeyFactory.newPrepared(
                "values 2", schema, getConnection().getHoldability());
        StatementKey stmtKey3 = StatementKeyFactory.newPrepared(
                "values 3", schema, getConnection().getHoldability());
        LogicalStatementEntity logic1 =
                new LogicalStatementEntityClass(ps1, stmtKey1, cache);
        LogicalStatementEntity logic2 =
                new LogicalStatementEntityClass(ps2, stmtKey2, cache);
        LogicalStatementEntity logic3 =
                new LogicalStatementEntityClass(ps3, stmtKey3, cache);

        // Close the two first logical statements, putting them into the cache.
        logic1.close();
        logic2.close();
        // Assert both of the statements are open.
        JDBC.assertSingleValueResultSet(ps1.executeQuery(), "1");
        JDBC.assertSingleValueResultSet(ps2.executeQuery(), "2");
        // Close the third statement. It should be cached, but since the cache
        // will exceed its maximum capacity, the first statement will be thrown
        // out and it should be closed in the process.
        logic3.close();
        JDBC.assertSingleValueResultSet(ps3.executeQuery(), "3");
        assertNull("ps1 still in the cache", cache.getCached(stmtKey1));
        try {
            ps1.executeQuery();
            fail("ps1 should have been closed by the cache");
        } catch (SQLException sqle) {
            assertSQLState("XJ012", sqle);
        }
        // Make sure the right statements are returned from the cache.
        assertSame(ps2, cache.getCached(stmtKey2));
        assertSame(ps3, cache.getCached(stmtKey3));
    }

    /**
     * Returns a suite of tests running in a client-server environment.
     *
     * @return A test suite.
     */
    public static Test suite() {
        return TestConfiguration.clientServerSuite(
                LogicalStatementEntityTest.class);
    }

    /**
     * Class used to represent a logical statement.
     */
    private static class LogicalStatementEntityClass
            extends LogicalStatementEntity {

        /**
         * Constructor creating an object handling closing of a logical
         * prepared / callable statement.
         *
         * @param ps underlying physical prepared / callable statement
         */
        public LogicalStatementEntityClass(java.sql.PreparedStatement ps,
                                           StatementKey key,
                                           JDBCStatementCache cache) {
            super(ps, key, cache);
        }
    }
}
