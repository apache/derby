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

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for the handling of logical statements.
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
        java.sql.PreparedStatement ps = prepareStatement(sql);
        JDBCStatementCache cache = new JDBCStatementCache(10);
        insertStatementIntoCache(cache, ps, sql);
        LogicalStatementEntity logic =
                createLogicalStatementEntity(sql, false, cache);
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
        java.sql.CallableStatement cs = prepareCall(sql);
        JDBCStatementCache cache = new JDBCStatementCache(10);
        insertStatementIntoCache(cache, cs, sql);
        LogicalStatementEntity logic =
                createLogicalStatementEntity(sql, true, cache);
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
        java.sql.PreparedStatement ps = prepareStatement(sql);
        JDBCStatementCache cache = new JDBCStatementCache(10);
        StatementKey stmtKey = insertStatementIntoCache(cache, ps, sql);
        LogicalStatementEntity logic =
                createLogicalStatementEntity(sql, false, cache);
        assertSame(ps, logic.getPhysPs());
        assertFalse(logic.isLogicalEntityClosed());

        // Put a statement into the cache.
        //assertTrue(cache.cacheStatement(stmtKey, ps));
        // Create a second statement, equal to the first.
        java.sql.PreparedStatement psDupe = prepareStatement(sql);
        insertStatementIntoCache(cache, psDupe, sql);
        LogicalStatementEntity logicDupe =
                createLogicalStatementEntity(sql, false, cache);
        // Close the first logical entry, to put the physical statement back
        // into the cache.
        logic.close();
        // When we ask the logical entity to close the statement now, the
        // underlying physical prepared statement should actually be closed.
        logicDupe.close();
        assertTrue(logicDupe.isLogicalEntityClosed());
        // Since we are possibly running in a pre-JDBC 4 environment, try do do
        // something to provoke an exception.
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
        java.sql.PreparedStatement ps = prepareStatement(sql);
        JDBCStatementCache cache = new JDBCStatementCache(10);
        StatementKey stmtKey = insertStatementIntoCache(cache, ps, sql);
        LogicalStatementEntity logic =
                createLogicalStatementEntity(sql, false, cache);
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
        java.sql.PreparedStatement ps = prepareStatement(sql);
        JDBCStatementCache cache = new JDBCStatementCache(10);
        insertStatementIntoCache(cache, ps, sql);
        LogicalStatementEntity logic =
                createLogicalStatementEntity(sql, false, cache);
        assertSame(ps, logic.getPhysPs());
        assertFalse(logic.isLogicalEntityClosed());
        java.sql.PreparedStatement logicalPs =
                (java.sql.PreparedStatement)logic;
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
        final String sql1 = "values 1";
        final String sql2 = "values 2";
        final String sql3 = "values 3";
        // Create three physical prepares statements.
        java.sql.PreparedStatement ps1 = prepareStatement(sql1);
        java.sql.PreparedStatement ps2 = prepareStatement(sql2);
        java.sql.PreparedStatement ps3 = prepareStatement(sql3);
        // Insert the two first physical statements, the get logical wrappers.
        StatementKey stmtKey1 = insertStatementIntoCache(cache, ps1, sql1);
        StatementKey stmtKey2 = insertStatementIntoCache(cache, ps2, sql2);
        LogicalStatementEntity logic1 =
                createLogicalStatementEntity(sql1, false, cache);
        LogicalStatementEntity logic2 =
                createLogicalStatementEntity(sql2, false, cache);
        // Insert the last physical statement and get the logical wrapper.
        StatementKey stmtKey3 = insertStatementIntoCache(cache, ps3, sql3);
        LogicalStatementEntity logic3 =
                createLogicalStatementEntity(sql3, false, cache);
        assertSame(ps1, logic1.getPhysPs());
        assertSame(ps2, logic2.getPhysPs());
        assertSame(ps3, logic3.getPhysPs());

        // Close two first logical statements, putting them back into the cache.
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
     * Creates a logical statement entity.
     * <p>
     * The entity represents a prepared statement.
     *
     * @param sql the SQL text
     * @param isCallable whether the entity is a callable statement or not
     * @param cache the statement cache to interact with.
     * @return A logical statement entity.
     * @throws SQLException if creating the entity fails
     */
    private LogicalStatementEntity createLogicalStatementEntity(
                                        String sql,
                                        boolean isCallable,
                                        JDBCStatementCache cache)
            throws SQLException {
        StatementCacheInteractor cacheInteractor =
            new StatementCacheInteractor(
                cache,
                ((org.apache.derby.client.am.ClientConnection)getConnection()));
        LogicalStatementEntity entity;
        if (isCallable) {
            entity = (LogicalStatementEntity)cacheInteractor.prepareCall(sql);
        } else {
            entity =(LogicalStatementEntity)
                    cacheInteractor.prepareStatement(sql);
        }
        return entity;
    }

    /**
     * Insers the statement into the cache.
     *
     * @param cache the to insert into
     * @param ps the statement to insert
     * @param sql the SQL text of the statement
     * @return The key the statement was inserted with.
     *
     * @throws SQLException if getting the connection holdability fails
     */
    private StatementKey insertStatementIntoCache(
                            JDBCStatementCache cache,
                            java.sql.PreparedStatement ps,
                            String sql) throws SQLException {
        StatementKey key;
        if (ps instanceof java.sql.CallableStatement) {
            key = StatementKeyFactory.newCallable(sql, "APP",
                    getConnection().getHoldability());
        } else {
            key = StatementKeyFactory.newPrepared(sql, "APP",
                    getConnection().getHoldability());
        }
        assertTrue(cache.cacheStatement(key, ps));
        return key;
    }
}
