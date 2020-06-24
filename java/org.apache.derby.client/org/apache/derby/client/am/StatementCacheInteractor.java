/*

Derby - Class org.apache.derby.client.am.StatementCacheInteractor

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
import java.util.ArrayList;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.client.am.stmtcache.StatementKeyFactory;
import org.apache.derby.client.ClientAutoloadedDriver;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Utility class encapsulating the logic for interacting with the JDBC statement
 * cache when creating new logical statements.
 * <p>
 * This class was introduced to share code between the pre-JDBC 4 and the JDBC
 * 4+ versions of the JDBC classes.
 * <p>
 * The pattern for the {@code prepareX} methods is:
 * <ol> <li>Generate a key for the statement to create.</li>
 *      <li>Consult cache to see if an existing statement can be used.</li>
 *      <li>Create new statement on physical connection if necessary.</li>
 *      <li>Return reference to existing or newly created statement.</li>
 * </ol>
 */
public final class StatementCacheInteractor {

    /** Statement cache for the associated physical connection. */
    private final JDBCStatementCache cache;
    /**
     * The underlying physical connection.
     * <p>
     * Note that it is the responsibility of the logical statement assoiciated
     * with this cache interactor to ensure the interactor methods are not
     * invoked if the logical statement has been closed.
     */
    private final ClientConnection physicalConnection;
    /** List of open logical statements created by this cache interactor. */
    //@GuardedBy("this")
    private final ArrayList<LogicalStatementEntity> openLogicalStatements =
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            new ArrayList<LogicalStatementEntity>();
    /**
     * Tells if this interactor is in the process of shutting down.
     * <p>
     * If this is true, it means that the logical connection is being closed.
     */
    private boolean connCloseInProgress = false;

    /**
     * Creates a new JDBC statement cache interactor.
     *
     * @param cache statement cache
     * @param physicalConnection associated physical connection
     */
    StatementCacheInteractor(JDBCStatementCache cache,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                             ClientConnection physicalConnection) {
        this.cache = cache;
        this.physicalConnection = physicalConnection;
    }

    /**
     * @see java.sql.Connection#prepareStatement(String)
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized PreparedStatement prepareStatement(String sql)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(),
                physicalConnection.holdability());
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(sql);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String,int,int)
     */
    synchronized PreparedStatement prepareStatement(
                                                String sql,
                                                int resultSetType,
                                                int resultSetConcurrency)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, physicalConnection.holdability());
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(
                    sql, resultSetType, resultSetConcurrency);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String,int,int,int)
     */
    synchronized PreparedStatement prepareStatement(
                                                String sql,
                                                int resultSetType,
                                                int resultSetConcurrency,
                                                int resultSetHoldability)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, resultSetHoldability);

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(
                sql, resultSetType,resultSetConcurrency, resultSetHoldability);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String,int)
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized PreparedStatement prepareStatement(
                                                String sql,
                                                int autoGeneratedKeys)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(),
                physicalConnection.getHoldability(), autoGeneratedKeys);
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(sql, autoGeneratedKeys);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareCall(String)
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized CallableStatement prepareCall(String sql)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newCallable(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(),
                physicalConnection.holdability());
        CallableStatement cs =
            (CallableStatement)cache.getCached(stmtKey);

        if (cs == null) {
            cs = physicalConnection.prepareCall(sql);
        }
        return createLogicalCallableStatement(cs, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareCall(String,int,int)
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized CallableStatement prepareCall(String sql,
                                                      int resultSetType,
                                                      int resultSetConcurrency)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newCallable(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, physicalConnection.holdability());
        CallableStatement cs =
            (CallableStatement)cache.getCached(stmtKey);

        if (cs == null) {
            cs = physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        }
        return createLogicalCallableStatement(cs, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareCall(String,int,int,int)
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized CallableStatement prepareCall(String sql,
                                                      int resultSetType,
                                                      int resultSetConcurrency,
                                                      int resultSetHoldability)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newCallable(
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, resultSetHoldability);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        CallableStatement cs =
            (CallableStatement)cache.getCached(stmtKey);

        if (cs == null) {
            cs = physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
        return createLogicalCallableStatement(cs, stmtKey);
    }

    /**
     * Closes all open logical statements created by this cache interactor.
     * <p>
     * A cache interactor is bound to a single (caching) logical connection.
     * @throws SQLException if closing an open logical connection fails
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized void closeOpenLogicalStatements()
//IC see: https://issues.apache.org/jira/browse/DERBY-3457
            throws SQLException {
        // Transist to closing state, to avoid changing the list of open
        // statements as we work our way through the list.
        this.connCloseInProgress = true;
        // Iterate through the list and close the logical statements.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (LogicalStatementEntity logicalStatement : openLogicalStatements) {
            logicalStatement.close();
        }
        // Clear the list for good measure.
        this.openLogicalStatements.clear();
    }

    /**
     * Designates the specified logical statement as closed.
     *
     * @param logicalStmt the logical statement being closed
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized void markClosed(LogicalStatementEntity logicalStmt) {
        // If we are not in the process of shutting down the logical connection,
        // remove the notifying statement from the list of open statements.
        if (!connCloseInProgress) {
            boolean removed = this.openLogicalStatements.remove(logicalStmt);
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(removed,
                    "Tried to remove unregistered logical statement: " +
                    logicalStmt);
            }
        }
    }

    /**
     * Creates a logical prepared statement.
     *
     * @param ps the underlying physical prepared statement
     * @param stmtKey the statement key for the physical statement
     * @return A logical prepared statement.
     * @throws SQLException if creating a logical prepared statement fails
     */
    private PreparedStatement createLogicalPreparedStatement(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            PreparedStatement ps,
            StatementKey stmtKey) throws SQLException {

        LogicalPreparedStatement logicalPs =
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                ClientAutoloadedDriver.getFactory().newLogicalPreparedStatement(
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                                                    ps, stmtKey, this);
        this.openLogicalStatements.add(logicalPs);
        return logicalPs;
    }

    /**
     * Creates a logical callable statement.
     *
     * @param cs the underlying physical callable statement
     * @param stmtKey the statement key for the physical statement
     * @return A logical callable statement.
     * @throws SQLException if creating a logical callable statement fails
     */
    private CallableStatement createLogicalCallableStatement(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            CallableStatement cs,
            StatementKey stmtKey) throws SQLException {

        LogicalCallableStatement logicalCs =
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                ClientAutoloadedDriver.getFactory().newLogicalCallableStatement(
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                                                    cs, stmtKey, this);
        this.openLogicalStatements.add(logicalCs);
        return logicalCs;
    }

    /**
     * Returns the associated statement cache.
     *
     * @return A statement cache.
     */
    JDBCStatementCache getCache() {
        return this.cache;
    }
}
