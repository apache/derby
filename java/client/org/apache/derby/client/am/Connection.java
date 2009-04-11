/*

   Derby - Class org.apache.derby.client.am.Connection

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

import org.apache.derby.jdbc.ClientBaseDataSource;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.shared.common.reference.SQLState;

import java.sql.SQLException;
import org.apache.derby.shared.common.sanity.SanityManager;

public abstract class Connection implements java.sql.Connection,
        ConnectionCallbackInterface {
    //---------------------navigational members-----------------------------------


    public Agent agent_;

    public DatabaseMetaData databaseMetaData_;
    // DERBY-210 -  WeakHashMap is used to store references to objects to avoid
    // memory leaks. When there are no other references to the keys in a 
    // WeakHashMap, they will get removed from the map and can thus get 
    // garbage-collected. They do not have to wait till the Connection object 
    // is collected.
        
    // In Connection.markStatementsClosed() method, this list is traversed to get a
    // list of open statements, which are marked closed and removed from the list.
    final java.util.WeakHashMap openStatements_ = new java.util.WeakHashMap();

    // Some statuses of DERBY objects may be invalid on server
    // after both commit and rollback. For example,
    // (1) prepared statements need to be re-prepared
    //     after both commit and rollback
    // (2) result set will be unpositioned on server after both commit and rollback.
    // If they depend on both commit and rollback, they need to get on CommitAndRollbackListeners_.
    final java.util.WeakHashMap CommitAndRollbackListeners_ = new java.util.WeakHashMap();
    private SqlWarning warnings_ = null;
    
    //Constant representing an invalid locator value
    private static final int INVALID_LOCATOR = -1;

    // ------------------------properties set for life of connection--------------

    // See ClientDataSource pre-connect settings
    protected final String user_;
    public boolean retrieveMessageText_;
    protected boolean jdbcReadOnly_;
    /**
     * Holdabilty for created statements.
     * Only access through the holdability method
     * to ensure the correct value is returned for an
     * XA connection.
     */
    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    
    public String databaseName_;

    // Holds the Product-Specific Identifier which specifies
    // the product release level of a DDM Server.
    // The max length is 8.
    public String productID_;

    // Used to get the public key and encrypt password and/or userid
    protected EncryptionManager encryptionManager_;

    // used to set transaction isolation level
    private Statement setTransactionIsolationStmt = null;
    
    // used to get transaction isolation level
    private Statement getTransactionIsolationStmt = null;
    
    // ------------------------dynamic properties---------------------------------

    protected boolean open_ = true;
    private boolean availableForReuse_ = false;

    /**
     * Constant indicating that isolation_ has not been updated through
     * piggy-backing, (or that the previously stored value was invalidated,
     * e.g. by an XA state change).
     */
    private static final int TRANSACTION_UNKNOWN = -1;
    /**
     * Cached copy of the isolation level. Kept in sync with server through
     * piggy-backing.
     */
    private int isolation_ = TRANSACTION_UNKNOWN;

    /**
     * Cached copy of the schema name. Updated through piggy-backing and used
     * to implement statement caching.
     */
    private String currentSchemaName_ = null;

    public boolean autoCommit_ = true;
    protected boolean inUnitOfWork_ = false; // This means a transaction is in progress.

    private boolean accumulated440ForMessageProcFailure_ = false;
    private boolean accumulated444ForMessageProcFailure_ = false;

    private int transactionID_ = 0;

    //---------------------XA-----------------------------------------------------

    protected boolean isXAConnection_ = false; // Indicates an XA connection

    // XA States
    // The client needs to keep track of the connection's transaction branch association
    // per table 2.6 in the XA+ specification in order to determine if commits should flow in
    // autocommit mode.  There is no need to keep track of suspended transactions separately from
    // XA_TO_NOT_ASSOCIATED.
    // 
    /**
     * <code>XA_T0_NOT_ASSOCIATED</code>
     * This connection is not currently associated with an XA transaction
     * In this state commits will flow in autocommit mode.
     */
    public static final int XA_T0_NOT_ASSOCIATED = 0;   
    
    /**
     * <code>XA_T1_ASSOCIATED</code>
     * In this state commits will not flow in autocommit mode.
     */
    public static final int XA_T1_ASSOCIATED = 1;  
    
    //TODO: Remove XA_RECOVER entirely once indoubtlist is gone.  
    //public static final int XA_RECOVER = 14;


    private int xaState_ = XA_T0_NOT_ASSOCIATED;

    // XA Host Type
    public int xaHostVersion_ = 0;

    public int loginTimeout_;
    public org.apache.derby.jdbc.ClientBaseDataSource dataSource_;
    public String serverNameIP_;
    public int portNumber_;
    public int clientSSLMode_ = ClientBaseDataSource.SSL_OFF;

    java.util.Hashtable clientCursorNameCache_ = new java.util.Hashtable();
    public int commBufferSize_ = 32767;

    // indicates if a deferred reset connection is required
    public boolean resetConnectionAtFirstSql_ = false;

    //---------------------constructors/finalizer---------------------------------

    // For jdbc 2 connections
    protected Connection(org.apache.derby.client.am.LogWriter logWriter,
                         String user,
                         String password,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource) 
                                                           throws SqlException {
        this.user_ = user;
        initConnection(logWriter, dataSource);
    }

    protected Connection(org.apache.derby.client.am.LogWriter logWriter,
                         String user,
                         String password,
                         boolean isXAConn,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource) 
                                                           throws SqlException {
        this.user_ = user;
        isXAConnection_ = isXAConn;
        initConnection(logWriter, dataSource);
    }

    // For jdbc 2 connections
    protected void initConnection(org.apache.derby.client.am.LogWriter logWriter,
                                  org.apache.derby.jdbc.ClientBaseDataSource
                                            dataSource) throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectEntry(dataSource);
        }

        // Extract common properties.
        // Derby-409 fix - Append connectionAttributes only if it is non-null. 
        // DERBY-1130 - Append connectionAttributes only if database name is
        // non-null. This will prevent use of database name set using 
        // "setConnectionAttributes" method.  
        databaseName_ = dataSource.getDatabaseName();
        String connAtrrs = dataSource.getConnectionAttributes();
        if (dataSource.getCreateDatabase() != null) // can be "create" or null
        {
            if (connAtrrs == null)
                connAtrrs = "create=true";
            else
                connAtrrs = connAtrrs + ";create=true";
        }
        if (dataSource.getShutdownDatabase() != null) // "shutdown" or null
        {
            if (connAtrrs == null)
                connAtrrs = "shutdown=true";
            else
                connAtrrs = connAtrrs + ";shutdown=true";
        }
        if(databaseName_ != null && connAtrrs != null)
            databaseName_ = databaseName_ + ";" + connAtrrs;

        retrieveMessageText_ = dataSource.getRetrieveMessageText();

        loginTimeout_ = dataSource.getLoginTimeout();
        dataSource_ = dataSource;

        serverNameIP_ = dataSource.getServerName();
        portNumber_ = dataSource.getPortNumber();

        clientSSLMode_ = 
            ClientBaseDataSource.getSSLModeFromString(dataSource.getSsl());

        agent_ = newAgent_(logWriter,
                loginTimeout_,
                serverNameIP_,
                portNumber_,
                clientSSLMode_);
    }

    // For jdbc 2 connections
    protected Connection(org.apache.derby.client.am.LogWriter logWriter,
                         boolean isXAConn,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource) 
                                                            throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectEntry(dataSource);
        }
        isXAConnection_ = isXAConn;

        user_ = ClientDataSource.propertyDefault_user;

        // Extract common properties.
        databaseName_ = dataSource.getDatabaseName();
        retrieveMessageText_ = dataSource.getRetrieveMessageText();

        loginTimeout_ = dataSource.getLoginTimeout();
        dataSource_ = dataSource;

        serverNameIP_ = dataSource.getServerName();
        portNumber_ = dataSource.getPortNumber();

        clientSSLMode_ = 
            ClientBaseDataSource.getSSLModeFromString(dataSource.getSsl());

        agent_ = newAgent_(logWriter,
                loginTimeout_,
                serverNameIP_,
                portNumber_,
                clientSSLMode_);
    }

    // This is a callback method, called by subsystem - NetConnection
    protected void resetConnection(LogWriter logWriter)
            throws SqlException {
        // Transaction isolation level is handled in completeReset.
        // clearWarningsX() will re-initialize the following properties
        clearWarningsX();

        // property encryptionManager_
        // if needed this will later be initialized by NET calls to initializePublicKeyForEncryption()
        encryptionManager_ = null;

        // DERBY-3723: Reset schema to user name.
        currentSchemaName_ = this.user_;
        autoCommit_ = true;
        inUnitOfWork_ = false;
        holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;

        this.agent_.resetAgent(
                this, logWriter, loginTimeout_, serverNameIP_, portNumber_);
    }

    // For jdbc 1 connections
    protected Connection(LogWriter logWriter,
                         int driverManagerLoginTimeout,
                         String serverName,
                         int portNumber,
                         String databaseName,
                         java.util.Properties properties) throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectEntry(serverName, portNumber, databaseName, properties);
        }

        databaseName_ = databaseName;

        // Extract common properties.
        user_ = ClientDataSource.getUser(properties);
        retrieveMessageText_ = ClientDataSource.getRetrieveMessageText(properties);

        loginTimeout_ = driverManagerLoginTimeout;
        serverNameIP_ = serverName;
        portNumber_ = portNumber;
        clientSSLMode_ = ClientDataSource.getClientSSLMode(properties);

        agent_ = newAgent_(logWriter,
                loginTimeout_,
                serverNameIP_,
                portNumber_,
                clientSSLMode_);
    }

    // Users are advised to call the method close() on Statement and Connection objects when they are done with them.
    // However, some users will forget, and some code may get killed before it can close these objects.
    // Therefore, if JDBC drivers have state associated with JDBC objects that need to get
    // explicitly cleared up, they should provide finalize methods to take care of them.
    // The garbage collector will call these finalize methods when the objects are found to be garbage,
    // and this will give the driver a chance to close (or otherwise clean up) the objects.
    // Note, however, that there is no guarantee that the garbage collector will ever run.
    // If that is the case, the finalizers will not be called.
    protected void finalize() throws java.lang.Throwable {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "finalize");
        }

        // finalize() differs from close() in that it will not throw an
        // exception if a transaction is in progress.
        // finalize() also differs from close() in that it will not drive
        // an auto-commit before disconnecting.
        //
        // If a transaction is in progress, a close() request will throw an SqlException.
        // However, if a connection with an incomplete transaction is finalized,
        // or is abruptly terminated by application exit,
        // the normal rollback semantics imposed by the DERBY server are adopted.
        // So we just pull the plug and let the server handle this default semantic.

        if (!open_) {
            return;
        }
        agent_.disconnectEvent();
        super.finalize();
    }

    // ---------------------------jdbc 1------------------------------------------

    synchronized public java.sql.Statement createStatement() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "createStatement");
            }
            Statement s = createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability());
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "createStatement", s);
            }
            return s;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareStatement", sql);
            }
            PreparedStatement ps = prepareStatementX(sql,
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY,
                    holdability(),
                    java.sql.Statement.NO_GENERATED_KEYS,
                    null, null);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareStatement", ps);
            }
            return ps;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // For internal use only.  Use by updatable result set code.
    synchronized public PreparedStatement preparePositionedUpdateStatement(String sql, Section querySection) throws SqlException {
        checkForClosedConnection();
        // create a net material prepared statement.
        PreparedStatement preparedStatement = newPositionedUpdatePreparedStatement_(sql, querySection);
        preparedStatement.flowPrepareDescribeInputOutput();
        // The positioned update statement is not added to the list of open statements,
        // because this would cause a java.util.ConcurrentModificationException when
        // iterating thru the list of open statements to call completeRollback().
        // An updatable result set is marked closed on a call to completeRollback(),
        // and would therefore need to close the positioned update statement associated with the result set which would cause
        // it to be removed from the open statements list. Resulting in concurrent modification
        // on the open statements list.
        // Notice that ordinary Statement.closeX() is never called on the positioned update statement,
        // rather markClosed() is called to avoid trying to remove the statement from the openStatements_ list.
        return preparedStatement;
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareCall", sql);
            }
            CallableStatement cs = prepareCallX(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability());
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareCall", cs);
            }
            return cs;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized PreparedStatement prepareDynamicCatalogQuery(String sql) throws SqlException {
        PreparedStatement ps = newPreparedStatement_(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability(), java.sql.Statement.NO_GENERATED_KEYS, null,
                null);
        ps.isCatalogQuery_ = true;
        ps.prepare();
        openStatements_.put(ps, null);
        return ps;
    }

    public String nativeSQL(String sql) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "nativeSQL", sql);
            }
            String nativeSql = nativeSQLX(sql);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "nativeSQL", nativeSql);
            }
            return nativeSql;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }

    }

    synchronized public String nativeSQLX(String sql) throws SqlException {
        checkForClosedConnection();
        if (sql == null) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId (SQLState.NULL_SQL_TEXT));
        }

        // Derby can handle the escape syntax directly so only needs escape
        // processing for { ? = CALL  ....}
        String trimSql = sql.trim();
        if (trimSql.startsWith("{")) {
            if (trimSql.lastIndexOf("}") >= 0) {
                return trimSql.substring(1, trimSql.lastIndexOf("}"));
            }
        }

        return trimSql;
    }

    // Driver-specific determination if local COMMIT/ROLLBACK is allowed;
    // primary usage is distinction between local and global trans. envs.;
    protected abstract boolean allowLocalCommitRollback_() throws org.apache.derby.client.am.SqlException;

    synchronized public void setAutoCommit(boolean autoCommit) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setAutoCommit", autoCommit);
            }
            checkForClosedConnection();

            if (! allowLocalCommitRollback_()) {
                if (autoCommit) { // can't toggle to autocommit mode when between xars.start() and xars.end()
                    throw new SqlException(agent_.logWriter_,
                            new ClientMessageId (SQLState.DRDA_NO_AUTOCOMMIT_UNDER_XA));                            
                }
            } else {
                if (autoCommit == autoCommit_) {
                    return; // don't flow a commit if nothing changed.
                }
                if (inUnitOfWork_) {
                    flowCommit(); // we are not between xars.start() and xars.end(), can flow commit
                }
            }
            autoCommit_ = autoCommit;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean getAutoCommit() throws SQLException {
        try
        {
            checkForClosedConnection();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getAutoCommit", autoCommit_);
            }
            if (! allowLocalCommitRollback_()) { // autoCommit is always false between xars.start() and xars.end()
                return false;
            }
            return autoCommit_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public void commit() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "commit");
            }
            checkForClosedConnection();

            // the following XA State check must be in commit instead of commitX since
            // external application call commit, the SqlException should be thrown
            // only if an external application calls commit during a Global Transaction,
            // internal code will call commitX which will ignore the commit request
            // while in a Global transaction
            checkForInvalidXAStateOnCommitOrRollback();
            flowCommit();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void checkForInvalidXAStateOnCommitOrRollback() throws SqlException {
        if (! allowLocalCommitRollback_()) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.DRDA_INVALID_XA_STATE_ON_COMMIT_OR_ROLLBACK));
        }
    }
    public void flowCommit() throws SqlException {
        // Per JDBC specification (see javadoc for Connection.commit()):
        //   "This method should be used only when auto-commit mode has been disabled."
        // However, some applications do this anyway, it is harmless, so
        // if they ask to commit, we could go ahead and flow a commit.
        // But note that rollback() is less harmless, rollback() shouldn't be used in auto-commit mode.
        // This behavior is subject to further review.

        //   if (!this.inUnitOfWork)
        //     return;
        // We won't try to be "too smart", if the user requests a commit, we'll flow a commit,
        // regardless of whether or not we're in a unit of work or in auto-commit mode.
        //
        if (isXAConnection_) {
            agent_.beginWriteChainOutsideUOW();
            writeCommit();
            agent_.flowOutsideUOW();
            readCommit(); // This will invoke the commitEvent() callback from the material layer.
            agent_.endReadChain();
        } else {
            agent_.beginWriteChain(null);
            writeCommit();
            agent_.flow(null);
            readCommit(); // This will invoke the commitEvent() callback from the material layer.
            agent_.endReadChain();
        }

    }

    // precondition: autoCommit_ is true
    public boolean flowAutoCommit() throws SqlException {
        if (willAutoCommitGenerateFlow()) {
            flowCommit();
            return true;
        }
        return false;
    }

    public boolean willAutoCommitGenerateFlow() throws org.apache.derby.client.am.SqlException {
        if (!autoCommit_) {
            return false;
        }
        if (! allowLocalCommitRollback_()) {
            return false;
        }
        return true;
    }

    // precondition: autoCommit_ is true
    void writeAutoCommit() throws SqlException {
        if (willAutoCommitGenerateFlow()) {
            writeCommit();
        }
    }

    public void writeCommit() throws SqlException {
        if (isXAConnection_) {
            writeXACommit_ ();
        } else {
            writeLocalCommit_();
        }
    }

    // precondition: autoCommit_ is true
    void readAutoCommit() throws SqlException {
        if (willAutoCommitGenerateFlow()) {
            readCommit();
        }
    }

    public void readCommit() throws SqlException {
        if (isXAConnection_) {
            readXACommit_ ();
        } else {
            readLocalCommit_();
        }
    }

    synchronized public void rollback() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "rollback");
            }
            
            checkForClosedConnection();
            checkForInvalidXAStateOnCommitOrRollback();

            flowRollback();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Even if we're not in a transaction, all open result sets will be closed.
    // So we could probably just return if we're not in a transaction
    // using the following code:
    //     if (!this.inUnitOfWork)
    //       return;
    // But we'll just play it safe, and blindly flow the rollback.
    // We won't try to be "too smart", if the user requests a rollback, we'll flow a rollback,
    // regardless of whether or not we're in a unit of work or in auto-commit mode.
    //
    // Per JDBC specification (see javadoc for Connection.rollback()):
    //   "This method should be used only when auto-commit mode has been disabled."
    // However, rather than trying to be too smart, we'll just flow the rollback anyway
    // before throwing an exception.
    // As a side-effect of invoking rollback() in auto-commit mode,
    // we'll close all open result sets on this connection in the rollbackEvent().
    //
    protected void flowRollback() throws SqlException {
        if (isXAConnection_) {
            agent_.beginWriteChainOutsideUOW();
            writeRollback();
            agent_.flowOutsideUOW();
            readRollback(); // This method will invoke the rollbackEvent() callback from the material layer.
            agent_.endReadChain();
        } else {
            agent_.beginWriteChain(null);
            writeRollback();
            agent_.flow(null);
            readRollback(); // This method will invoke the rollbackEvent() callback from the material layer.
            agent_.endReadChain();
        }
    }

    public void writeRollback() throws SqlException {
        if (isXAConnection_) {
            writeXARollback_ ();
        } else {
            writeLocalRollback_();
        }
    }

    public void readRollback() throws SqlException {
        if (isXAConnection_) {
            readLocalXARollback_();
        } else {
            readLocalRollback_();
        }
    }

    synchronized public void close() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "close");
        }
        closeX();
    }

    /**
     * Check if the transaction is in progress and the connection cannot be
     * closed.
     *
     * @throws SqlException if the connection cannot be closed because the
     * transaction is active
     */
    void checkForTransactionInProgress() throws SqlException {
        // The following precondition matches CLI semantics, see SQLDisconnect()
        if (transactionInProgress()) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId (SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION));                   
        }
    }

    /**
     * Check if there are uncommitted operations in the current transaction
     * that prevent us from closing the connection.
     *
     * @return {@code true} if the connection cannot be closed due to
     * uncommitted operations in the transaction
     */
    public boolean transactionInProgress() {
        return inUnitOfWork_ && !allowCloseInUOW_();
    }

    // This is a no-op if the connection is already closed.
    synchronized public void closeX() throws SQLException {
        if (!open_) {
            return;
        }
        closeResourcesX();
    }

    // Close physical socket or attachment even if connection is marked close.
    // Used by ClientPooledConnection.close().
    synchronized public void closeResources() throws SQLException {
        if (open_ || (!open_ && availableForReuse_)) {
            availableForReuse_ = false;
            closeResourcesX();
        }
    }

    private void closeResourcesX() throws SQLException {
        try
        {
            checkForTransactionInProgress();
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
        
        resetConnectionAtFirstSql_ = false; // unset indicator of deferred reset
        SQLException accumulatedExceptions = null;
        if (setTransactionIsolationStmt != null) {
            try {
                setTransactionIsolationStmt.close();
            } catch (SQLException se) {
                accumulatedExceptions = se;
            }
        }
        setTransactionIsolationStmt = null;
        if (getTransactionIsolationStmt != null) {
            try {
                getTransactionIsolationStmt.close();
            } catch (SQLException se) {
                accumulatedExceptions = Utils.accumulateSQLException(
                        se, accumulatedExceptions);
            }
        }
        getTransactionIsolationStmt = null;
        try {
            flowClose();
        } catch (SqlException e) {
            accumulatedExceptions =
                    Utils.accumulateSQLException(
                        e.getSQLException(), accumulatedExceptions);
        }

        markClosed(false);
        try {
            agent_.close();
        } catch (SqlException e) {
            throw Utils.accumulateSQLException(e.getSQLException(), 
                accumulatedExceptions);
        }
    }

    protected abstract boolean isGlobalPending_();

    // Just like closeX except the socket is not pulled.
    // Physical resources are not closed.
    synchronized public void closeForReuse(boolean statementPooling)
            throws SqlException {
        if (!open_) {
            return;
        }
        resetConnectionAtFirstSql_ = false; // unset indicator of deferred reset
        SqlException accumulatedExceptions = null;
        try {
            flowClose();
        } catch (SqlException e) {
            accumulatedExceptions = e;
        }
        if (open_) {
            markClosedForReuse(statementPooling);
        }
        if (accumulatedExceptions != null) {
            throw accumulatedExceptions;
        }
    }

    private void flowClose() throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        if (doCloseStatementsOnClose_()) {
            writeCloseStatements();
        }
        if (autoCommit_) {
            writeAutoCommit();
        }
        agent_.flowOutsideUOW();
        if (doCloseStatementsOnClose_()) {
            readCloseStatements();
        }
        if (autoCommit_) {
            readAutoCommit();
        }
        agent_.endReadChain();
    }

    protected abstract void markClosed_();

    public void markClosed(boolean statementPooling) // called by LogicalConnection.close()
    {
        open_ = false;
        inUnitOfWork_ = false;
        if (!statementPooling) {
            markStatementsClosed();
        }
        CommitAndRollbackListeners_.clear();
        markClosed_();
    }


    private void markClosedForReuse(boolean statementPooling) {
        availableForReuse_ = true;
        markClosed(statementPooling);
    }

    private void markStatementsClosed() {
    	java.util.Set keySet = openStatements_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            Statement stmt = (Statement) i.next();
            stmt.markClosed();
            i.remove();
        }
    }

    private void writeCloseStatements() throws SqlException {
    	java.util.Set keySet = openStatements_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            ((Statement) i.next()).writeClose(false);  // false means don't permit auto-commits
        }
    }

    private void readCloseStatements() throws SqlException {
    	java.util.Set keySet = openStatements_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            ((Statement) i.next()).readClose(false);  // false means don't permit auto-commits
        }
    }

   /**
    * 	Return true if the physical connection is still open.
    * 	Might be logically closed but available for reuse.
    *   @return true if physical connection still open
    */
    public boolean isPhysicalConnClosed() {
    return !open_ && !availableForReuse_; 
   }

   
    public boolean isClosed() {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "isClosed", !open_);
        }
        return !open_;
    }

   
    public boolean isClosedX() {
        return !open_;
    }

    private static String DERBY_TRANSACTION_REPEATABLE_READ = "RS";
    private static String DERBY_TRANSACTION_SERIALIZABLE = "RR";
    private static String DERBY_TRANSACTION_READ_COMMITTED = "CS";
    private static String DERBY_TRANSACTION_READ_UNCOMMITTED = "UR";

    synchronized public void setTransactionIsolation(int level) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTransactionIsolation", level);
        }
        try {
            // Per jdbc spec (see java.sql.Connection.close() javadoc).
            checkForClosedConnection();
            setTransactionIsolationX(level);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Set the transaction isolation level as specified.
     * <p>
     * If this method is called during a transaction, the result is
     * implementation-defined.
     * <p>
     * Information about Derby specific isolation level handling:
     * <ul> <li>REPEATABLE_READ = JDBC: TRANSACTION_SERIALIZABLE, DERBY: RR,
     *          PROTOCOL: repeatable read</li>
     *      <li>READ_STABILITY = JDBC: TRANSACTION_REPEATABLE_READ, DERBY: RS,
     *          PROTOCOL: All</li>
     *      <li>CURSOR_STABILITY = JDBC: TRANSACTION_READ_COMMITTED, DERBY: CS,
     *          PROTOCOL: Cursor stability</li>
     *      <li>UNCOMMITTED_READ = JDBC: TRANSACTION_READ_UNCOMMITTED,
     *          DERBY: UR, PROTOCOL: Change</li>
     *      <li>NO_COMMIT = JDBC: TRANSACTION_NONE, DERBY: NC, PROTOCOL:
     *          No commit</li>
     * </ul>
     */
    //@GuardedBy("this")
    private void setTransactionIsolationX(int level)
            throws SqlException {
        String levelString = null;
        switch (level) {
        case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
            levelString = DERBY_TRANSACTION_REPEATABLE_READ;
            break;
        case java.sql.Connection.TRANSACTION_READ_COMMITTED:
            levelString = DERBY_TRANSACTION_READ_COMMITTED;
            break;
        case java.sql.Connection.TRANSACTION_SERIALIZABLE:
            levelString = DERBY_TRANSACTION_SERIALIZABLE;
            break;
        case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
            levelString = DERBY_TRANSACTION_READ_UNCOMMITTED;
            break;
            // Per javadoc:
            //   Note that Connection.TRANSACTION_NONE cannot be used because it
            //   specifies that transactions are not supported.
        case java.sql.Connection.TRANSACTION_NONE:
        default:
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId (SQLState.UNIMPLEMENTED_ISOLATION_LEVEL),
                new Integer(level));
        }
        if (setTransactionIsolationStmt == null  ||
                !(setTransactionIsolationStmt.openOnClient_ &&
                        setTransactionIsolationStmt.openOnServer_)) {
            setTransactionIsolationStmt =
                    createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY,
                            holdability());
        }

        try {
            setTransactionIsolationStmt.executeUpdate(
                "SET CURRENT ISOLATION = " + levelString);
        } catch (SQLException sqle) {
            throw new SqlException(sqle);
        }

        // The server has now implicitly committed the
        // transaction so we have to clean up locally.
        completeLocalCommit();

        if (SanityManager.DEBUG && supportsSessionDataCaching()) {
            SanityManager.ASSERT(isolation_ == level);
        }
    }

    /**
     * Finds out if the underlaying database connection supports session data
     * caching.
     * @return true if sessionData is supported
     */
    protected abstract boolean supportsSessionDataCaching();

    /**
     * Checks whether the server supports locators for large objects.
     * Note that even though the server supports LOB locators, the database
     * that is accessed through the server may not have the necessary stored
     * procedures to support LOB locators (e.g., because the database is
     * soft upgraded from an earlier version).
     *
     * @return {@code true} if LOB locators are supported.
     */
    protected abstract boolean serverSupportsLocators();

    public int getTransactionIsolation() throws SQLException {
    	
    	// Store the current auto-commit value and use it to restore 
    	// at the end of this method.
    	boolean currentAutoCommit = autoCommit_;
    	java.sql.ResultSet rs = null;
    	
        try
        {
            checkForClosedConnection();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTransactionIsolation", isolation_);
            }
            
            if (isolation_ != TRANSACTION_UNKNOWN) {
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(supportsSessionDataCaching(),
                            "Cannot return cached isolation when caching is " +
                            "not supported!");
                }
                return isolation_;
            }

            // Set auto-commit to false when executing the statement as we do not want to
            // cause an auto-commit from getTransactionIsolation() method. 
            autoCommit_ = false;
            
            // DERBY-1148 - Client reports wrong isolation level. We need to get the isolation
            // level from the server. 'isolation_' maintained in the client's connection object
            // can be out of sync with the real isolation when in an XA transaction. This can 
            // also happen when isolation is set using SQL instead of JDBC. So we try to get the
            // value from the server by calling the "current isolation" function. If we fail to 
            // get the value, return the value stored in the client's connection object.
            // DERBY-3192 - Cache session data in the client driver allows
            // the re-introduction of isolation level caching. Changes to the
            // isolation level triggered from SQL are now handled by
            // piggybacking the modified isolation level on messages going
            // back to the client.
            // The XA-problem is handled by letting XA state changes set the
            // cached isolation level to TRANSACTION_UNKNOWN which will trigger
            // a refresh from the server.
            if (getTransactionIsolationStmt == null  || 
            		!(getTransactionIsolationStmt.openOnClient_ &&
            				getTransactionIsolationStmt.openOnServer_)) {
                getTransactionIsolationStmt =
                        createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                java.sql.ResultSet.CONCUR_READ_ONLY,
                                holdability());
            }
            
            boolean savedInUnitOfWork = inUnitOfWork_;
            rs = getTransactionIsolationStmt.executeQuery("values current isolation");
            rs.next();
            String isolationStr = rs.getString(1);

            int isolation = translateIsolation(isolationStr);
            if (isolation_ == TRANSACTION_UNKNOWN &&
                    supportsSessionDataCaching()) {
                // isolation_ will be TRANSACTION_UNKNOWN if the connection has
                // been reset on
                // the client. The server will not observe a
                // change in isolation level so no information is
                // piggy-backed. Update the cached value here, rather than
                // waiting for the isolation to change on the server.
                isolation_ = isolation;
            }
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!supportsSessionDataCaching() ||
                        (isolation_ == isolation),
                        "Cached isolation_ not updated, (isolation_="+
                        isolation_+")!=(isolation="+isolation+")");
                SanityManager.ASSERT(supportsSessionDataCaching() ||
                        (isolation_ == TRANSACTION_UNKNOWN),
                        "isolation_ modified when caching is not supported");
            }
            rs.close();	
            // So... of we did not have an active transaction before
            // the query, we pretend to still not have an open
            // transaction. The result set is closed, so this should
            // not be problematic. DERBY-2084
            inUnitOfWork_ = savedInUnitOfWork;

            return isolation;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
        finally {
        	// Restore auto-commit value
        	autoCommit_ = currentAutoCommit;
        	if(rs != null)
        		rs.close();
        }
    }
  
    /**
     * Returns the ID of the active transaction for this connection.
     * @return the ID of the active transaction
     */
    public int getTransactionID(){
        return transactionID_;
    }

    /**
     * Returns the current schema (the schema that would be used for
     * compilation. This is not part of the java.sql.Connection interface, and
     * is only intended for use with statement caching.
     * @return the name of the current schema
     * @throws java.sql.SQLException
     */
    public String getCurrentSchemaName() throws SQLException {
        try {
            checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        if (currentSchemaName_ == null) {
            if (agent_.loggingEnabled()) {
               agent_.logWriter_.traceEntry(this,
                  "getCurrentSchemaName() executes query");
            }
            java.sql.Statement s = createStatement();
            java.sql.ResultSet rs = s.executeQuery("VALUES CURRENT SCHEMA");
            rs.next();
            String schema = rs.getString(1);
            rs.close();
            s.close();
            return schema;
        }
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(supportsSessionDataCaching(),
                    "A cached schema name ("+currentSchemaName_+
                    ") is not expected when session data caching is not" +
                    "supported");
        }
        return currentSchemaName_;
    }

    /**
     * Translates the isolation level from a SQL string to the JDBC int value
     *  
     * @param isolationStr SQL isolation string
     * @return isolation level as a JDBC integer value 
     */
    private int translateIsolation(String isolationStr) {
    	if(isolationStr.compareTo(DERBY_TRANSACTION_REPEATABLE_READ) == 0)
    		return java.sql.Connection.TRANSACTION_REPEATABLE_READ;
    	else if (isolationStr.compareTo(DERBY_TRANSACTION_SERIALIZABLE) == 0)
    		return java.sql.Connection.TRANSACTION_SERIALIZABLE;
    	else if (isolationStr.compareTo(DERBY_TRANSACTION_READ_COMMITTED) == 0)
    		return java.sql.Connection.TRANSACTION_READ_COMMITTED;
    	else if (isolationStr.compareTo(DERBY_TRANSACTION_READ_UNCOMMITTED) == 0)
    		return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
    	else 
    		return java.sql.Connection.TRANSACTION_NONE;
    }

    public java.sql.SQLWarning getWarnings() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getWarnings", warnings_);
        }
        try {
            checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        return warnings_ == null ? null : warnings_.getSQLWarning();
    }

    synchronized public void clearWarnings() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "clearWarnings");
            }
            checkForClosedConnection();
            clearWarningsX();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // An untraced version of clearWarnings()
    public void clearWarningsX() throws SqlException {
        warnings_ = null;
        accumulated440ForMessageProcFailure_ = false;
        accumulated444ForMessageProcFailure_ = false;
    }

    //======================================================================
    // Advanced features:

    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
        try
        {
            checkForClosedConnection();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getMetaData", databaseMetaData_);
            }
            return databaseMetaData_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public void setReadOnly(boolean readOnly) throws SQLException {
        try
        {
            // This is a hint to the driver only, so this request is silently ignored.
            // PROTOCOL can only flow a set-read-only before the connection is established.
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setReadOnly", readOnly);
            }
            checkForClosedConnection();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean isReadOnly() throws SQLException {
        try
        {
            checkForClosedConnection();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "isReadOnly", jdbcReadOnly_);
            }
            return false;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public void setCatalog(String catalog) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setCatalog", catalog);
            }
            checkForClosedConnection();
            // Per jdbc spec: if the driver does not support catalogs, it will silently ignore this request.
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public String getCatalog() throws SQLException {
        try
        {
            checkForClosedConnection();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getCatalog", (String) null);
            }
            return null;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //--------------------------JDBC 2.0-----------------------------

    synchronized public java.sql.Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "createStatement", resultSetType, resultSetConcurrency);
            }
            Statement s = createStatementX(resultSetType, resultSetConcurrency, holdability());
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "createStatement", s);
            }
            return s;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql,
                                                                    int resultSetType,
                                                                    int resultSetConcurrency) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareStatement", sql, resultSetType, resultSetConcurrency);
            }
            PreparedStatement ps = prepareStatementX(sql,
                    resultSetType,
                    resultSetConcurrency,
                    holdability(),
                    java.sql.Statement.NO_GENERATED_KEYS,
                    null, null);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareStatement", ps);
            }
            return ps;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql,
                                                               int resultSetType,
                                                               int resultSetConcurrency) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareCall", sql, resultSetType, resultSetConcurrency);
            }
            CallableStatement cs = prepareCallX(sql, resultSetType, resultSetConcurrency, holdability());
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareCall", cs);
            }
            return cs;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public CallableStatement prepareMessageProc(String sql) throws SqlException {
        checkForClosedConnection();

        CallableStatement cs = prepareCallX(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability());
        return cs;
    }

    // Per jdbc spec, when a result set type is unsupported, we downgrade and
    // issue a warning rather than to throw an exception.
    private int downgradeResultSetType(int resultSetType) {
        if (resultSetType == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) {
            accumulateWarning(new SqlWarning(agent_.logWriter_, 
                new ClientMessageId(SQLState.SCROLL_SENSITIVE_NOT_SUPPORTED)));
            return java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
        }
        return resultSetType;
    }

    public java.util.Map getTypeMap() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTypeMap");
            }
            checkForClosedConnection();
            java.util.Map map = new java.util.HashMap();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTypeMap", map);
            }
            return map;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public void setTypeMap(java.util.Map map) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setTypeMap", map);
            }
            checkForClosedConnection();
            if (map == null) {
                throw new SqlException(agent_.logWriter_,
                        new ClientMessageId (SQLState.INVALID_API_PARAMETER),
                        map, "map", "setTypeMap");
            }

            if (!(map.isEmpty())) {
                throw new SqlException(agent_.logWriter_, new ClientMessageId(
                        SQLState.NOT_IMPLEMENTED), "setTypeMap");
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }        
    }

    //--------------------------JDBC 3.0-----------------------------

    synchronized public void setHoldability(int holdability) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setHoldability", holdability);
            }
            checkForClosedConnection();
            // In an XA global transaction do not allow the
            // holdability to be set to hold cursors across
            // commits, as the engine does not support it.
            if (this.isXAConnection_ && this.xaState_ == XA_T1_ASSOCIATED)
            {
                if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT)
                    throw new SqlException(agent_.logWriter_, 
                            new ClientMessageId(SQLState.CANNOT_HOLD_CURSOR_XA));
            }
            this.holdability = holdability;
            
       }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getHoldability() throws SQLException {
        try
        {
            checkForClosedConnection();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getHoldability", holdability());
            }
            return holdability();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int dncGeneratedSavepointId_;
    // generated name used internally for unnamed savepoints
    public static final String dncGeneratedSavepointNamePrefix__ = "DNC_GENENERATED_NAME_";

    synchronized public java.sql.Savepoint setSavepoint() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setSavepoint");
            }
            checkForClosedConnection();
            if (autoCommit_) // Throw exception if auto-commit is on
            {
                throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId (SQLState.NO_SAVEPOINT_WHEN_AUTO));
            } 
            // create an un-named savepoint.
            if ((++dncGeneratedSavepointId_) < 0) {
                dncGeneratedSavepointId_ = 1; // restart from 1 when overflow.
            }
            Object s = setSavepointX(new Savepoint(agent_, dncGeneratedSavepointId_));
            return (java.sql.Savepoint) s;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public java.sql.Savepoint setSavepoint(String name) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setSavepoint", name);
            }
            checkForClosedConnection();
            if (name == null) // Throw exception if savepoint name is null
            {
                throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId (SQLState.NULL_NAME_FOR_SAVEPOINT));
            } else if (autoCommit_) // Throw exception if auto-commit is on
            {
                throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId (SQLState.NO_SAVEPOINT_WHEN_AUTO));
            }
            // create a named savepoint.
            Object s = setSavepointX(new Savepoint(agent_, name));
            return (java.sql.Savepoint) s;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private Savepoint setSavepointX(Savepoint savepoint) throws SQLException {
        // Construct and flow a savepoint statement to server.
        Statement stmt = null;
        try {
            stmt = (Statement) createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY,
                    holdability());
            String savepointName;
            try {
                savepointName = savepoint.getSavepointName();
            } catch (SQLException e) {
                // generate the name for an un-named savepoint.
                savepointName = dncGeneratedSavepointNamePrefix__ +
                        savepoint.getSavepointId();
            }
            String sql = "SAVEPOINT \"" + savepointName + "\" ON ROLLBACK RETAIN CURSORS";
            stmt.executeX(sql);
        } catch ( SqlException se )
        {
            throw se.getSQLException();
        } finally {
            if (stmt != null) {
                try {
                    stmt.closeX();
                } catch (SqlException doNothing) {
                }
            }
        }

        return savepoint;
    }

    synchronized public void rollback(java.sql.Savepoint savepoint) throws SQLException {
        try
        {
            int saveXaState = xaState_;
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "rollback", savepoint);
            }
            checkForClosedConnection();
            if (savepoint == null) // Throw exception if savepoint is null
            {
                throw new SqlException(agent_.logWriter_, 
                		new ClientMessageId (SQLState.XACT_SAVEPOINT_RELEASE_ROLLBACK_FAIL));
            } else if (autoCommit_) // Throw exception if auto-commit is on
            {
                throw new SqlException(agent_.logWriter_, 
                		new ClientMessageId (SQLState.NO_SAVEPOINT_ROLLBACK_OR_RELEASE_WHEN_AUTO));
            } 
            // Only allow to rollback to a savepoint from the connection that create the savepoint.
            try {
                if (this != ((Savepoint) savepoint).agent_.connection_) {
                    throw new SqlException(agent_.logWriter_,
                    		new ClientMessageId (SQLState.SAVEPOINT_NOT_CREATED_BY_CONNECTION));
                }
            } catch (java.lang.ClassCastException e) { // savepoint is not an instance of am.Savepoint
                throw new SqlException(agent_.logWriter_,
                		new ClientMessageId (SQLState.SAVEPOINT_NOT_CREATED_BY_CONNECTION));
            }

            // Construct and flow a savepoint rollback statement to server.
            Statement stmt = null;
            try {
                stmt = createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                        java.sql.ResultSet.CONCUR_READ_ONLY,
                        holdability());
                String savepointName;
                try {
                    savepointName = ((Savepoint) savepoint).getSavepointName();
                } catch (SQLException e) {
                    // generate the name for an un-named savepoint.
                    savepointName = dncGeneratedSavepointNamePrefix__ +
                            ((Savepoint) savepoint).getSavepointId();
                }
                String sql = "ROLLBACK TO SAVEPOINT \"" + savepointName + "\"";
                stmt.executeX(sql);
            } finally {
                if (stmt != null) {
                    try {
                        stmt.closeX();
                    } catch (SqlException doNothing) {
                    }
                }
                xaState_ = saveXaState;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
        try
        {
            int saveXaState = xaState_;
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "releaseSavepoint", savepoint);
            }
            checkForClosedConnection();
            if (savepoint == null) // Throw exception if savepoint is null
            {
                throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId (SQLState.XACT_SAVEPOINT_RELEASE_ROLLBACK_FAIL));
            } else if (autoCommit_) // Throw exception if auto-commit is on
            {
                throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId (SQLState.NO_SAVEPOINT_ROLLBACK_OR_RELEASE_WHEN_AUTO));
            } 
            // Only allow to release a savepoint from the connection that create the savepoint.
            try {
                if (this != ((Savepoint) savepoint).agent_.connection_) {
                    throw new SqlException(agent_.logWriter_, new ClientMessageId 
                            (SQLState.SAVEPOINT_NOT_CREATED_BY_CONNECTION));
                }
            } catch (java.lang.ClassCastException e) { // savepoint is not an instance of am.Savepoint
                    throw new SqlException(agent_.logWriter_, new ClientMessageId 
                            (SQLState.SAVEPOINT_NOT_CREATED_BY_CONNECTION));

            }

            // Construct and flow a savepoint release statement to server.
            Statement stmt = null;
            try {
                stmt = (Statement) createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                        java.sql.ResultSet.CONCUR_READ_ONLY,
                        holdability());
                String savepointName;
                try {
                    savepointName = ((Savepoint) savepoint).getSavepointName();
                } catch (SQLException e) {
                    // generate the name for an un-named savepoint.
                    savepointName = dncGeneratedSavepointNamePrefix__ +
                            ((Savepoint) savepoint).getSavepointId();
                }
                String sql = "RELEASE SAVEPOINT \"" + savepointName + "\"";
                stmt.executeX(sql);
            } finally {
                if (stmt != null) {
                    try {
                        stmt.closeX();
                    } catch (SqlException doNothing) {
                    }
                }
                xaState_ = saveXaState;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    synchronized public java.sql.Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency,
                                                           int resultSetHoldability) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "createStatement", resultSetType, resultSetConcurrency, resultSetHoldability);
            }
            Statement s = createStatementX(resultSetType, resultSetConcurrency, resultSetHoldability);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "createStatement", s);
            }
            return s;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private Statement createStatementX(int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SqlException {
        checkForClosedConnection();
        resultSetType = downgradeResultSetType(resultSetType);
        // In an XA global transaction do not allow the
        // holdability to be set to hold cursors across
        // commits, as the engine does not support it.
        // Downgrade the holdability to CLOSE_CURSORS_AT_COMMIT
        // and attach a warning. This is specified in
        // JDBC 4.0 (proposed final draft) section 16.1.3.1
        // Similar code is not needed for PreparedStatement
        // as the holdability gets pushed all the way to the
        // engine and handled there.
        if (this.isXAConnection_ && this.xaState_ == XA_T1_ASSOCIATED)
        {
            if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
                accumulateWarning(new SqlWarning(agent_.logWriter_, 
                        new ClientMessageId(SQLState.HOLDABLE_RESULT_SET_NOT_AVAILABLE)));
            }
        }
        Statement s = newStatement_(resultSetType, resultSetConcurrency, resultSetHoldability);
        s.cursorAttributesToSendOnPrepare_ = s.cacheCursorAttributesToSendOnPrepare();
        openStatements_.put(s, null);
        return s;
    }

    // not sure if holding on to cursorAttributesToSendOnPrepare and restoring it is the
    // right thing to do here... because if property on the dataSource changes, we may have
    // to send different attributes, i.e. SENSITIVE DYNAMIC, instead of SENSITIVE STATIC.
    protected void resetStatement(Statement s) throws SqlException {
        String cursorAttributesToSendOnPrepare = s.cursorAttributesToSendOnPrepare_;
        resetStatement_(s, s.resultSetType_, s.resultSetConcurrency_, s.resultSetHoldability_);
        s.cursorAttributesToSendOnPrepare_ = cursorAttributesToSendOnPrepare;
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql,
                                                                    int resultSetType,
                                                                    int resultSetConcurrency,
                                                                    int resultSetHoldability) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareStatement", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            }
            PreparedStatement ps = prepareStatementX(sql,
                    resultSetType,
                    resultSetConcurrency,
                    resultSetHoldability,
                    java.sql.Statement.NO_GENERATED_KEYS,
                    null, null);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareStatement", ps);
            }
            return ps;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // used by DBMD
    PreparedStatement prepareStatementX(String sql,
                                        int resultSetType,
                                        int resultSetConcurrency,
                                        int resultSetHoldability,
                                        int autoGeneratedKeys,
                                        String[] columnNames,
                                        int[] columnIndexes) throws SqlException {
        checkForClosedConnection();
        resultSetType = downgradeResultSetType(resultSetType);
        PreparedStatement ps = newPreparedStatement_(sql, resultSetType, resultSetConcurrency, resultSetHoldability, autoGeneratedKeys, columnNames,
                columnIndexes);
        ps.cursorAttributesToSendOnPrepare_ = ps.cacheCursorAttributesToSendOnPrepare();
        ps.prepare();
        openStatements_.put(ps,null);
        return ps;
    }

    // not sure if holding on to cursorAttributesToSendOnPrepare and restoring it is the
    // right thing to do here... because if property on the dataSource changes, we may have
    // to send different attributes, i.e. SENSITIVE DYNAMIC, instead of SENSITIVE STATIC.
    protected void resetPrepareStatement(PreparedStatement ps) throws SqlException {
        String cursorAttributesToSendOnPrepare = ps.cursorAttributesToSendOnPrepare_;
        resetPreparedStatement_(ps, ps.sql_, ps.resultSetType_, ps.resultSetConcurrency_, ps.resultSetHoldability_, ps.autoGeneratedKeys_, ps.generatedKeysColumnNames_,
                ps.generatedKeysColumnIndexes_);
        ps.cursorAttributesToSendOnPrepare_ = cursorAttributesToSendOnPrepare;
        ps.prepare();
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql,
                                                               int resultSetType,
                                                               int resultSetConcurrency,
                                                               int resultSetHoldability) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareCall", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            }
            CallableStatement cs = prepareCallX(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareCall", cs);
            }
            return cs;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    CallableStatement prepareCallX(String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) throws SqlException {
        checkForClosedConnection();
        resultSetType = downgradeResultSetType(resultSetType);
        CallableStatement cs = newCallableStatement_(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        cs.cursorAttributesToSendOnPrepare_ = cs.cacheCursorAttributesToSendOnPrepare();
        cs.prepare();
        openStatements_.put(cs,null);
        return cs;
    }

    protected void resetPrepareCall(CallableStatement cs) throws SqlException {
        String cursorAttributesToSendOnPrepare = cs.cursorAttributesToSendOnPrepare_;
        resetCallableStatement_(cs, cs.sql_, cs.resultSetType_, cs.resultSetConcurrency_, cs.resultSetHoldability_);
        cs.cursorAttributesToSendOnPrepare_ = cursorAttributesToSendOnPrepare;
        cs.prepare();
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareStatement", sql, autoGeneratedKeys);
            }
            PreparedStatement ps = prepareStatementX(sql,
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY,
                    holdability(),
                    autoGeneratedKeys,
                    null, null);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareStatement", ps);
            }
            return ps;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareStatement", sql, columnIndexes);
            }
            int genKeys = (columnIndexes == null ||
                    columnIndexes.length == 0
                    ? Statement.NO_GENERATED_KEYS: 
                Statement.RETURN_GENERATED_KEYS);
            PreparedStatement ps = prepareStatementX(sql,
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY,
                    holdability(),
                    genKeys,
                    null, columnIndexes);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareStatement", ps);
            } 
            return ps;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public java.sql.PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "prepareStatement", sql, columnNames);
            }
            int genKeys = (columnNames == null ||
                    columnNames.length == 0
                    ? Statement.NO_GENERATED_KEYS: 
                Statement.RETURN_GENERATED_KEYS);
            PreparedStatement ps = prepareStatementX(sql,
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY,
                    holdability(),
                    genKeys,
                    columnNames, null);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "prepareStatement", ps);
            }
            return ps;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }


    // ---------------------------------------------------------------------------

    protected abstract boolean allowCloseInUOW_();

    protected abstract boolean doCloseStatementsOnClose_();

    public abstract SectionManager newSectionManager(String collection,
                                                     Agent agent,
                                                     String databaseName);
    //--------------------Abstract material factory methods-----------------

    protected abstract Agent newAgent_(LogWriter logWriter, int loginTimeout, String serverName, int portNumber, int clientSSLMode) throws SqlException;


    protected abstract DatabaseMetaData newDatabaseMetaData_();

    protected abstract Statement newStatement_(int type,
                                               int concurrency,
                                               int holdability) throws SqlException;

    protected abstract void resetStatement_(Statement statement,
                                            int type,
                                            int concurrency,
                                            int holdability) throws SqlException;


    protected abstract PreparedStatement newPositionedUpdatePreparedStatement_(String sql, Section section) throws SqlException;

    protected abstract PreparedStatement newPreparedStatement_(String sql,
                                                               int type,
                                                               int concurrency,
                                                               int holdability,
                                                               int autoGeneratedKeys,
                                                               String[] columnNames, int[] columnIndexes) throws SqlException;

    protected abstract void resetPreparedStatement_(PreparedStatement ps,
                                                    String sql,
                                                    int resultSetType,
                                                    int resultSetConcurrency,
                                                    int resultSetHoldability,
                                                    int autoGeneratedKeys,
                                                    String[] columnNames,
                                                    int[] columnIndexes) throws SqlException;

    protected abstract CallableStatement newCallableStatement_(String sql,
                                                               int type,
                                                               int concurrency,
                                                               int holdability) throws SqlException;

    protected abstract void resetCallableStatement_(CallableStatement cs,
                                                    String sql,
                                                    int resultSetType,
                                                    int resultSetConcurrency,
                                                    int resultSetHoldability) throws SqlException;

    // ----------------------- abstract box car and callback methods ---------------------
    // All callbacks must be client-side only operations.


    public void completeConnect() throws SqlException {
        open_ = true;
        databaseMetaData_ = newDatabaseMetaData_();

        agent_.sectionManager_ =
                newSectionManager("NULLID",
                        agent_,
                        databaseName_);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceConnectExit(this);
        }
    }

    public abstract void writeCommitSubstitute_() throws SqlException;

    public abstract void readCommitSubstitute_() throws SqlException;

    public abstract void writeLocalXAStart_() throws SqlException;

    public abstract void readLocalXAStart_() throws SqlException;

    public abstract void writeLocalXACommit_() throws SqlException;
    
    protected abstract void writeXACommit_() throws SqlException;

    public abstract void readLocalXACommit_() throws SqlException;   
    
    protected abstract void readXACommit_() throws SqlException;   

    public abstract void writeLocalCommit_() throws SqlException;

    public abstract void readLocalCommit_() throws SqlException;
    
    protected abstract void writeXATransactionStart(Statement statement) 
                                                throws SqlException;

    public void completeLocalCommit() {
    	java.util.Set keySet = CommitAndRollbackListeners_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            UnitOfWorkListener listener = (UnitOfWorkListener) i.next();
            listener.completeLocalCommit(i);
        }
        inUnitOfWork_ = false;
        transactionID_++;
    }

    public abstract void writeLocalRollback_() throws SqlException;

    public abstract void readLocalRollback_() throws SqlException;

    // A callback for certain non-fatal exceptions that occur when parsing error replies.
    // This is a client-side only operation.
    // This method will only throw an exception on bug check.
    public void completeLocalRollback() {
    	java.util.Set keySet = CommitAndRollbackListeners_.keySet();
    	for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            UnitOfWorkListener listener = (UnitOfWorkListener) i.next();
            listener.completeLocalRollback(i);
        }
        inUnitOfWork_ = false;
        transactionID_++;
    }
    
    /**
     * 
     * Rollback the specific UnitOfWorkListener. 
     * @param uwl The UnitOfWorkLitener to be rolled back
     *
     */
    public void completeSpecificRollback(UnitOfWorkListener uwl) {
        java.util.Set keySet = CommitAndRollbackListeners_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            UnitOfWorkListener listener = (UnitOfWorkListener) i.next();
            if(listener == uwl) {
                listener.completeLocalRollback(i);
                break;
            }
        }
        inUnitOfWork_ = false;
    }


    public abstract void writeLocalXARollback_() throws SqlException;
    
    protected abstract void writeXARollback_() throws SqlException;

    public abstract void readLocalXARollback_() throws SqlException;
    
    protected abstract void readXARollback_() throws SqlException;

    public void writeTransactionStart(Statement statement) throws SqlException {
        if (isXAConnection_) {
            writeXATransactionStart (statement);
        }
    }

    public void readTransactionStart() throws SqlException {
        completeTransactionStart();
    }

    void completeTransactionStart() {
        inUnitOfWork_ = true;
    }

    // Occurs autonomously
    public void completeAbnormalUnitOfWork() {
        completeLocalRollback();
    }
    
    /**
     *
     * Rollback the UnitOfWorkListener specifically.
     * @param uwl The UnitOfWorkListener to be rolled back.
     *
     */
    public void completeAbnormalUnitOfWork(UnitOfWorkListener uwl) {
        completeSpecificRollback(uwl);
    }

    // Called by Connection.close(), NetConnection.errorRollbackDisconnect().
    // The Agent's client-side resources associated with database connection are reclaimed (eg. socket).
    // And this connection and all associated statements and result sets are marked closed.
    // This is a client-side only operation.
    // This method will only throw an exception if the agent cannot be closed.
    public void completeChainBreakingDisconnect() {
        open_ = false;
        completeLocalRollback();
        markStatementsClosed();
    }

    public void completeSqlca(Sqlca sqlca) {
        if (sqlca == null) {
        } else if (sqlca.getSqlCode() > 0) {
            accumulateWarning(new SqlWarning(agent_.logWriter_, sqlca));
        } else if (sqlca.getSqlCode() < 0) {
            agent_.accumulateReadException(new SqlException(agent_.logWriter_, sqlca));
        }
    }

    public void completePiggyBackIsolation(int pbIsolation) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(supportsSessionDataCaching());
        }
        isolation_ = pbIsolation;
    }

    public void completePiggyBackSchema(String pbSchema) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(supportsSessionDataCaching());
        }
        currentSchemaName_ = pbSchema;
    }

    public abstract void addSpecialRegisters(String s);

    // can this only be called by the PooledConnection
    // can this be called on a closed connection
    // can this be called in a unit of work
    // can this be called from within a stored procedure
    //
    synchronized public void reset(LogWriter logWriter)
            throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectResetEntry(this, logWriter, user_, 
                                             dataSource_);
        }
        try {
            reset_(logWriter);
        } catch (SqlException sqle) {
            DisconnectException de = new DisconnectException(agent_, 
                new ClientMessageId(SQLState.CONNECTION_FAILED_ON_RESET));
            de.setNextException(sqle);
            throw de;
        }
    }

    synchronized public void lightReset() throws SqlException {
        if (!open_ && !availableForReuse_) {
            return;
        }
        open_ = true;
        availableForReuse_ = false;
    }

    abstract protected void reset_(LogWriter logWriter) throws SqlException;

    /**
     * <br>NOTE:</br>The following comments are valid for the changes done as
     * part of implementing statement caching only (see DERBY-3313 and linked
     * issues).
     * <p>
     * We don't reset the isolation level to unknown unconditionally, as this
     * forces us to go to the server all the time. Since the value should now
     * be valid (DERBY-3192), we check if it has been changed from the default.
     *
     * @param closeStatementsOnClose is used to differentiate between
     *      cases where statement pooling is enabled or not. If {@code true}, it
     *      means statement pooling is disabled and the statements are fully
     *      reset, which includes a re-prepare. If {@code false}, statement
     *      pooling is enabled, and a more lightweight reset procedure is used.
     */
    protected void completeReset(boolean isDeferredReset,
                                 boolean closeStatementsOnClose)
            throws SqlException {
        open_ = true;

        completeLocalRollback(); // this will close the cursors if the physical connection hadn't been closed for reuse properly

        // Reopen physical statement resources associated with previous uses of this physical connection.
        // Notice that these physical statements may not belong to this logical connection.
        // Iterate through the physical statements and re-enable them for reuse.

        if (closeStatementsOnClose) {
            // NOTE: This is to match previous behavior.
            //       Investigate and check if it is really necessary.
            this.isolation_ = TRANSACTION_UNKNOWN;
            java.util.Set keySet = openStatements_.keySet();
            for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
                Object o = i.next();
                ((Statement) o).reset(closeStatementsOnClose);
            }
        } else {
            // Must reset transaction isolation level if it has been changed.
            if (isolation_ != Connection.TRANSACTION_READ_COMMITTED) {
                // This might not fare well with connection pools, if it has
                // been configured to deliver connection with a different
                // isolation level, i.e. it has to set the isolation level again
                // when it returns connection to client.
                // TODO: Investigate optimization options.
                setTransactionIsolationX(Connection.TRANSACTION_READ_COMMITTED);
            }
        }

        if (!isDeferredReset && agent_.loggingEnabled()) {
            agent_.logWriter_.traceConnectResetExit(this);
        }
    }
    
    /**
     * Reference to object with prepared statements for calling the locator
     * procedures. Makes it possible to reuse prepared statements within 
     * the connection.
     */
    private CallableLocatorProcedures lobProcs;
    
    /**
     * Get handle to the object that contains prepared statements for calling
     * locator procedures for this connection.  The object will be created on 
     * the first invocation.
     *
     * An example of how to call a stored procedure via this method:
     * <pre> <code>
     *    connection.locatorProcedureCall().blobReleaseLocator(locator);
     * </code> </pre>
     *
     * @return object with prepared statements for calling locator procedures
     */
    CallableLocatorProcedures locatorProcedureCall() 
    {
        if (lobProcs == null) {
            lobProcs = new CallableLocatorProcedures(this);
        }
        return lobProcs;
    }    


    //-------------------------------helper methods-------------------------------

    protected void checkForClosedConnection() throws SqlException {
        if (!open_) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.NO_CURRENT_CONNECTION));
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    public boolean isXAConnection() {
        return isXAConnection_;
    }

    public int getXAState() {
        return xaState_;
    }

    public void setXAState(int state) {
        xaState_ = state;
    }

    public void accumulateWarning(SqlWarning e) {
        if (warnings_ == null) {
            warnings_ = e;
        } else {
            warnings_.setNextException(e);
        }
    }

    public void accumulate440WarningForMessageProcFailure(SqlWarning e) {
        if (!accumulated440ForMessageProcFailure_) {
            accumulateWarning(e);
            accumulated440ForMessageProcFailure_ = true;
        }
    }

    public void accumulate444WarningForMessageProcFailure(SqlWarning e) {
        if (!accumulated444ForMessageProcFailure_) {
            accumulateWarning(e);
            accumulated444ForMessageProcFailure_ = true;
        }
    }

    // get the server version
    public int getServerVersion() {
        return databaseMetaData_.productLevel_.versionLevel_;
    }

    public void setInUnitOfWork(boolean inUnitOfWork) {
        inUnitOfWork_ = inUnitOfWork;
    }
    
    /**
     * Return the holdabilty for the Connection. Matches the
     * embedded driver in the restriction that while in a
     * global (XA) transaction the holdability is CLOSE_CURSORS_AT_COMMIT.
     * Otherwise return the holdability set by the user.
     */
    final int holdability()
    {
        if (this.isXAConnection_ && this.xaState_ == XA_T1_ASSOCIATED)
            return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        return holdability;
    }
    
    
    /**
     * Constructs an object that implements the <code>Clob</code> interface. 
     * The object returned initially contains no data.
     *
     * @return An object that implements the <code>Clob</code> interface
     * @throws java.sql.SQLException if an object that implements the
     * <code>Clob</code> interface can not be constructed.
     */
    
    public java.sql.Clob createClob() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "createClob");
        }
        
        try {
            checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        
        //Stores a locator value obtained by calling the
        //stored procedure CLOBCREATELOCATOR.
        int locator = INVALID_LOCATOR;

        //Stores the Clob instance that is returned.
        org.apache.derby.client.am.Clob clob = null;

        //Call the CLOBCREATELOCATOR stored procedure
        //that will return a locator value.
        try {
            locator = locatorProcedureCall().clobCreateLocator();
        }
        catch(SqlException sqle) {
            throw sqle.getSQLException();
        }

        //If the locator value is -1 it means that we do not
        //have locator support on the server.

        //The code here has been disabled because the Lob implementations
        //have still not been completely converted to use locators. Once
        //the Lob implementations are completed then this code can be enabled.
        if (locator != INVALID_LOCATOR) {
            //A valid locator value has been obtained.
            clob = new org.apache.derby.client.am.Clob(this.agent_, locator);
        }
        else {
            //A valid locator value could not be obtained.
            clob = new org.apache.derby.client.am.Clob
                    (this.agent_, "");
        }

        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "createClob", clob);
        }
        
        return clob;
    }

    /**
     * Constructs an object that implements the <code>Blob</code> interface. 
     * The object returned initially contains no data.
     *
     * @return An object that implements the <code>Blob</code> interface
     * @throws SQLException if an object that implements the
     * </code>Blob</code> interface can not be constructed.
     *
     */
    
    public java.sql.Blob createBlob() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "createBlob");
        }
        
        try {
            checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        
        //Stores a locator value obtained by calling the
        //stored procedure BLOBCREATELOCATOR.
        int locator = INVALID_LOCATOR;
        
        //Stores the Blob instance that is returned.
        org.apache.derby.client.am.Blob blob = null;

        //Call the BLOBCREATELOCATOR stored procedure
        //that will return a locator value.
        try {
            locator = locatorProcedureCall().blobCreateLocator();
        }
        catch(SqlException sqle) {
            throw sqle.getSQLException();
        }
        
        //If the locator value is -1 it means that we do not
        //have locator support on the server.
        
        if (locator != INVALID_LOCATOR) {
            //A valid locator value has been obtained.
            blob = new org.apache.derby.client.am.Blob(this.agent_, locator);
        } 
        else {
            //A valid locator value could not be obtained.
            blob = new org.apache.derby.client.am.Blob
                    (new byte[0],this.agent_, 0);
        }
        
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "createBlob", blob);
        }
        
        return blob;
    }
    
    

}
