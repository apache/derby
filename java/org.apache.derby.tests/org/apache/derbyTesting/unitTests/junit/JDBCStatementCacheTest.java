/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.JDBCStatementCacheTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.client.am.stmtcache.StatementKeyFactory;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests basic operation of the JDBC prepared statement object cache and the
 * keys used to operate on it.
 */
public class JDBCStatementCacheTest
        extends BaseJDBCTestCase {

    public JDBCStatementCacheTest(String name) {
        super(name);
    }

    /**
     * Make sure a negative or zero max size is not allowed, as this will in
     * effect be no caching but with an overhead.
     * <p>
     * The overhead would come from always throwing out the newly inserted
     * element.
     */
    public void testCreateCacheWithZeroOrNegativeMaxSize() {
        try {
            new JDBCStatementCache(-10);
            fail("Negative max size should not be allowed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
        try {
            new JDBCStatementCache(0);
            fail("Zero max size should not be allowed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
    }

    /**
     * Test basic insertion into the cache.
     * 
     * @throws SQLException if obtaining a PreparedStatement fails
     */
    public void testBasicInsertion()
            throws SQLException {
        String sql = "values 1";
        JDBCStatementCache cache = new JDBCStatementCache(10);
        PreparedStatement ps = prepareStatement(sql);
        StatementKey key = StatementKeyFactory.newPrepared(sql, "app", 1);
        assertTrue(cache.cacheStatement(key, ps));
        assertEquals(ps, cache.getCached(key));
    }

    /**
     * Test insertion of a duplicate key.
     * 
     * @throws SQLException if obtaining a PreparedStatement fails
     */
    public void testBasicDuplicateKeyInsertion()
            throws SQLException {
        String sql = "values 1";
        JDBCStatementCache cache = new JDBCStatementCache(10);
        PreparedStatement ps = prepareStatement(sql);
        StatementKey key = StatementKeyFactory.newPrepared(sql, "app", 1);
        assertTrue(cache.cacheStatement(key, ps));
        // Duplicates shall not be inserted.
        assertFalse(cache.cacheStatement(key, ps));
        assertEquals(ps, cache.getCached(key));
    }

    /**
     * Make sure requesting a cached callable statement does not return a
     * <code>PreparedStatement</code> object.
     * 
     * @throws SQLException if creating database resources fail
     */
    public void testBasicCallableVsPrepared()
            throws SQLException {
        String sql = "values 7";
        String schema = "MYAPP";
        int rsh = 1;
        JDBCStatementCache cache = new JDBCStatementCache(10);
        PreparedStatement ps = prepareStatement(sql);
        StatementKey key = StatementKeyFactory.newPrepared(sql, schema, rsh);
        assertTrue(cache.cacheStatement(key, ps));
        StatementKey callKey =
                StatementKeyFactory.newCallable(sql, schema, rsh);
        assertNotSame(ps, cache.getCached(callKey));
        CallableStatement cs = prepareCall(sql);
        // No entry should exists yet.
        assertNull(cache.getCached(callKey));
        // New callable statements should be inserted.
        assertTrue(cache.cacheStatement(callKey, cs));
        // Make sure we get the same object back.
        assertSame(cs, cache.getCached(callKey));
        // Make sure we don't get a callable when we ask for a prepared.
        assertNotSame(cs, cache.getCached(key));
    }

    /**
     * Returns the appropriate tests.
     * <p>
     * Run only client/server, because the code being tested does not live
     * in the embedded driver (yet).
     * 
     * @return A suite of tests (may be empty).
     */
    public static Test suite() {
        // Run only client/server, because the code being tested does not live
        // in the embedded driver (yet).
        return TestConfiguration.clientServerSuite(
                JDBCStatementCacheTest.class);
    }
}
