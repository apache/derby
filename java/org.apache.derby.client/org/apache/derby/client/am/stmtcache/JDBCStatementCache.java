/*

   Derby - Class org.apache.derby.client.am.stmtcache.JDBCStatementCache

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

package org.apache.derby.client.am.stmtcache;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A cache for JDBC statement objects.
 * <p>
 * The entries in the cache contains objects implementing the
 * <code>java.sql.PreparedStatement</code> interface, and they are inserted with
 * a key object implementing the interface <code>StatementKey</code>. The cached
 * objects can be either <code>java.sql.PreparedStatement</code> or
 * <code>java.sql.CallableStatement</code>. These two should be separated by
 * using different types of keys.
 * <p>
 * The cache only contains free statement objects, and on a successful request
 * for a cached statement the statement is removed from the cache. The cache is
 * not intended to hold duplicate statements. The physical prepared statement
 * should be (re-)inserted into the cache when <code>close</code> is called on
 * the logical prepared statement using it.
 * <p>
 * There is a maximum number of cached statements associated with the cache.
 * If this number is exceeded, the oldest entry will be thrown out. One can
 * always throw out an entry, because the fact that it is in the cache means it
 * is free and not in use.
 */
//@ThreadSafe
public final class JDBCStatementCache {

    /** Structure holding the cached prepared statement objects. */
    //@GuardedBy("this");
    private final
        LinkedHashMap<StatementKey, PreparedStatement> statements;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    /**
     * Creates a new, empty JDBC statement cache.
     *
     * @param maxSize maximum number of statements in the cache
     *
     * @throws IllegalArgumentException if <code>maxSize</code> is less than one
     */
    public JDBCStatementCache(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be positive: " +
                    maxSize);
        }
        this.statements = new BoundedLinkedHashMap(maxSize);
    }

    /**
     * Retrieves a cached prepared statement if one exists.
     *
     * @param statementKey key for the prepared statement to look up
     * @return A cached statement if one exists, <code>null</code> otherwise.
     */
    public synchronized PreparedStatement getCached(
            StatementKey statementKey) {
        if (SanityManager.DEBUG) {
            // Getting a null here indicates a programming error, but does not
            // cause Derby to fail.
            SanityManager.ASSERT(statementKey != null,
                                 "statementKey is not supposed to be null");
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return statements.remove(statementKey);
    }

    /**
     * Cache the prepared statement if it does not already exist.
     *
     * @param statementKey key to insert prepared statement with
     * @param ps prepared statement to cache
     * @return <code>true</code> if added to the cache, <code>false</code> if
     *      not.
     */
    public synchronized boolean cacheStatement(
                                        StatementKey statementKey,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                        PreparedStatement ps) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(statementKey != null,
                                 "statementKey is not supposed to be null");
            SanityManager.ASSERT(ps != null,
                                 "ps is not supposed to be null");
        }
        final boolean alreadyCached = this.statements.containsKey(statementKey);
        if (!alreadyCached) {
            statements.put(statementKey, ps);
        }
        return !alreadyCached;
    }

    /**
     * A {@link LinkedHashMap} with an upper bound on the number of entries.
     * <p>
     * If the maximum size is exceeded, the oldest entry is automatically
     * removed after the new entry has been inserted.
     */
    //@NotThreadSafe
    private static class BoundedLinkedHashMap
            extends LinkedHashMap<StatementKey, PreparedStatement> {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

        /** Maximum number of entries. */
        private final int maxSize;

        /**
         * Creates a bounded {@link LinkedHashMap} with the specified maximum
         * size.
         * <p>
         * Iteration is by insertion-order.
         *
         * @param maxCapacity maximum size of the map
         */
        public BoundedLinkedHashMap(int maxCapacity) {
            super();
            this.maxSize = maxCapacity;
        }

        /**
         * Tells if an entry should be removed from the map.
         * <p>
         * If the cache has exceeded its maximum size, the oldest element will
         * be marked for removal. The oldest element will be removed after the
         * new element has been inserted.
         *
         * @param eldest the element picked out for removal
         * @return <code>true</code> if the element is to be removed,
         *      <code>false</code> if not.
         */
        protected boolean removeEldestEntry(
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                Map.Entry<StatementKey, PreparedStatement> eldest) {
            final boolean remove = size() > maxSize;
//IC see: https://issues.apache.org/jira/browse/DERBY-3324
            if (remove && eldest != null) {
                try {
                    eldest.getValue().close();
                } catch (SQLException ex) {
                    // Ignore this exception in insane mode, throw an assertion
                    // error if a sane build is run.
                    if (SanityManager.DEBUG) {
                        SanityManager.THROWASSERT("Failed to close prepared " +
                                "statement marked for cache removal", ex);
                    }
                }
            }
            return remove;
        }
    } // End inner class BoundedLinkedHashMap
} // End JDBCStatementCache
