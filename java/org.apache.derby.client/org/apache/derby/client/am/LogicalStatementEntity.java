/*

   Derby - Class org.apache.derby.client.am.LogicalStatementEntity

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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Common class interacting with the JDBC statement cache for logical prepared
 * statements and logical callable statements.
 * <p>
 * Note that {@link #getPhysPs} and {@link #getPhysCs} takes care of checking if
 * the logical statement has been closed. The physical statement will take care
 * of validating itself.
 * <p>
 * Beside from the above, special treatment of logical entities happens
 * on close. This is the point where cache interaction takes place, and also
 * where the appropriate methods are called on the physical statement to perform
 * the necessary clean up for later reuse.
 * <p>
 * A note regarding the thread safety of this class, is that access to
 * {@code physicalPs} and {@code physicalCs} is guarded by the instance of this
 * class, but it is assumed that operation on/within the physical statement is
 * synchronized in the physical statement itself .
 */
//@ThreadSafe
abstract class LogicalStatementEntity
        implements Statement {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    /**
     * Tells if we're holding a callable statement or not.
     * <p>
     * Used for sanity checking.
     */
    private final boolean hasCallableStmt;
    /**
     * Associated physical prepared statement.
     * <p>
     * If this is {@code null}, the logical entity is closed.
     */
    //@GuardedBy("this")
    private PreparedStatement physicalPs;
    /**
     * Associated physical callable statement, if any.
     * <p>
     * This is a convenience reference, to avoid having to cast on every
     * invocation of {@link #getPhysCs} if the logical entity represents a
     * callable statement.
     */
    //@GuardedBy("this)
    private CallableStatement physicalCs;
    /** The owner of this logical entity. */
    private StatementCacheInteractor owner;
    /** The key for the associated statement. */
    private final StatementKey stmtKey;
    /** Cache for physical statements. */
    //@GuardedBy("this)
    private final JDBCStatementCache cache;

    /**
     * Create a logical entity for a {@link java.sql.PreparedStatement}.
     *
     * @param physicalPs a physical {@link java.sql.PreparedStatement}
     * @param stmtKey cache key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    protected LogicalStatementEntity(PreparedStatement physicalPs,
                                     StatementKey stmtKey,
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                                     StatementCacheInteractor cacheInteractor) {
        if (cacheInteractor.getCache() == null) {
            // Internal check, failure indicates programming error.
            // No need to localize error message.
            throw new IllegalArgumentException(
                    "statement cache reference cannot be <null>");
        }
        this.stmtKey = stmtKey;
        this.cache = cacheInteractor.getCache();
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
        this.owner = cacheInteractor;
        this.physicalPs = physicalPs;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        if (physicalPs instanceof CallableStatement) {
            this.hasCallableStmt = true;
            this.physicalCs = (CallableStatement)physicalPs;
        } else {
            this.hasCallableStmt = false;
            this.physicalCs = null;
        }
        ((ClientPreparedStatement)physicalPs).setOwner(this);
    }

    /**
     * Returns the associated physical prepared statement.
     *
     * @return A prepared statement.
     * @throws SQLException if the logical statement has been closed
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized PreparedStatement getPhysPs()
            throws SQLException {
        if (physicalPs == null) {
            throw (new SqlException(null,
                new ClientMessageId(SQLState.ALREADY_CLOSED),
                                    "PreparedStatement")).getSQLException();
        }
        return physicalPs;
    }

    /**
     * Returns the associated physical callable statement.
     *
     * @return A callable statement.
     * @throws SQLException if the logical statement has been closed
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized CallableStatement getPhysCs()
            throws SQLException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(hasCallableStmt,
                    "called getPhysCs(), but created with PreparedStatement");
        }
        if (physicalCs == null) {
            throw (new SqlException(null,
                new ClientMessageId(SQLState.ALREADY_CLOSED),
                                    "CallableStatement")).getSQLException();
        }
        return physicalCs;
    }

    /**
     * Returns the associated physical statement.
     *
     * @return A statement.
     * @throws SQLException if the logical statement has been closed
     */
    private synchronized Statement getPhysStmt()
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
            throws SQLException
    {
        if ( hasCallableStmt ) { return getPhysCs(); }
        else { return getPhysPs(); }
    }

    /**
     * Close the logical statement.
     *
     * @throws SQLException if closing the statement fails
     */
    public synchronized void close() throws SQLException {
        if (physicalPs != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            final ClientPreparedStatement temporaryPsRef =
                    (ClientPreparedStatement)physicalPs;
            // Nullify both references.
            physicalPs = null;
            physicalCs = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-3457
            this.owner.markClosed(this);
            // Nullify the reference, since the entity object might stick around
            // for a while.
            this.owner = null;
            // Reset the owner of the physical statement.
            temporaryPsRef.setOwner(null);
            // NOTE: Accessing ps state directly below only to avoid tracing.
            // If the underlying statement has become closed, don't cache it.
//IC see: https://issues.apache.org/jira/browse/DERBY-4843
            if (!temporaryPsRef.openOnClient_) {
                return;
            }
            // If the poolable hint is false, don't cache it.
            if (!temporaryPsRef.isPoolable) {
                temporaryPsRef.close();
                return;
            }

            // Reset the statement for reuse.
            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-3441
                temporaryPsRef.resetForReuse();
            } catch (SqlException sqle) {
                // Get a wrapper and throw it.
                throw sqle.getSQLException();
            }

            // Try to insert the statement into the cache.
            if (!cache.cacheStatement(stmtKey, temporaryPsRef)) {
                // Statement was already in the cache, discard this one.
                temporaryPsRef.close();
            }
        }
    }

    /**
     * Tells if the logical entity is closed.
     * <p>
     * If this method is used to avoid the possibility of raising an exception
     * because the logical statement has been closed and then invoke a method on
     * the physical statement, one must synchronize on this instance in the
     * calling code.
     *
     * @return {@code true} if closed, {@code false} if open.
     */
    synchronized boolean isLogicalEntityClosed() {
        return (physicalPs == null);
    }
    
    // JDBC 4.0 java.sql.Wrapper interface methods

    /**
     * Check whether this instance wraps an object that implements the interface
     * specified by {@code iface}.
     *
     * @param iface a class defining an interface
     * @return {@code true} if this instance implements {@code iface}, or
     * {@code false} otherwise
     * @throws SQLException if an error occurs while determining if this
     * instance implements {@code iface}
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5872
        getPhysStmt(); // Just to check that the statement is not closed.
        return iface.isInstance(this);
    }

    /**
     * Returns an instance of the specified interface if this instance is
     * a wrapper for the interface.
     *
     * @param  iface a class defining an interface
     * @return an object that implements the interface
     * @throws SQLException if no object is found that implements the
     * interface
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        if (((ClientStatement) getPhysStmt()).isClosed()) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.ALREADY_CLOSED),
                hasCallableStmt ? "CallableStatement" : "PreparedStatement")
                    .getSQLException();
        }

        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP), iface)
                    .getSQLException();
        }
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  void    closeOnCompletion() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        ((ClientStatement) getPhysStmt()).closeOnCompletion();
    }

    public  boolean isCloseOnCompletion() throws SQLException
    {
        return ((ClientStatement) getPhysStmt()).isCloseOnCompletion();
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.2 IN JAVA 8
    //
    ////////////////////////////////////////////////////////////////////

    public  long[] executeLargeBatch() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        return ((ClientStatement) getPhysStmt()).executeLargeBatch();
    }
    public  long executeLargeUpdate( String sql ) throws SQLException
    {
        return ((ClientStatement) getPhysStmt()).executeLargeUpdate( sql );
    }
    public  long executeLargeUpdate( String sql, int autoGeneratedKeys) throws SQLException
    {
        return ((ClientStatement)getPhysStmt()).
            executeLargeUpdate(sql, autoGeneratedKeys);
    }
    public  long executeLargeUpdate( String sql, int[] columnIndexes ) throws SQLException
    {
        return ((ClientStatement)getPhysStmt()).
            executeLargeUpdate(sql, columnIndexes);
    }
    public  long executeLargeUpdate( String sql, String[] columnNames ) throws SQLException
    {
        return ((ClientStatement)getPhysStmt()).
            executeLargeUpdate(sql, columnNames);
    }
    public  long getLargeUpdateCount() throws SQLException
    {
        return ((ClientStatement) getPhysStmt()).getLargeUpdateCount();
    }
    public  long getLargeMaxRows() throws SQLException
    {
        return ((ClientStatement) getPhysStmt()).getLargeMaxRows();
    }
    public  void    setLargeMaxRows(long maxRows) throws SQLException
    {
        ((ClientStatement) getPhysStmt()).setLargeMaxRows( maxRows );
    }

}
