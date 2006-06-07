/*

   Derby - Class org.apache.derby.client.am.Connection

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import org.apache.derby.jdbc.ClientDataSource;
import java.sql.SQLException;

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

    // ------------------------properties set for life of connection--------------

    // See ClientDataSource pre-connect settings
    public transient String user_;
    public boolean retrieveMessageText_;
    protected boolean jdbcReadOnly_;
    /**
     * Holdabilty for created statements.
     * Only access through the holdability method
     * to ensure the correct value is returned for an
     * XA connection.
     */
    private int holdability = ClientDataSource.HOLD_CURSORS_OVER_COMMIT;
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
    protected boolean availableForReuse_ = false;

    public int isolation_ = Configuration.defaultIsolation;
    public boolean autoCommit_ = true;
    protected boolean inUnitOfWork_ = false; // This means a transaction is in progress.

    private boolean accumulated440ForMessageProcFailure_ = false;
    private boolean accumulated444ForMessageProcFailure_ = false;
    private boolean accumulatedSetReadOnlyWarning_ = false;



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
    public org.apache.derby.jdbc.ClientDataSource dataSource_;
    public String serverNameIP_;
    public int portNumber_;

    public java.util.Hashtable clientCursorNameCache_ = new java.util.Hashtable();
    public boolean canUseCachedConnectBytes_ = false;
    public int commBufferSize_ = 32767;

    // indicates if a deferred reset connection is required
    public boolean resetConnectionAtFirstSql_ = false;

    //---------------------constructors/finalizer---------------------------------

    // For jdbc 2 connections
    protected Connection(org.apache.derby.client.am.LogWriter logWriter,
                         String user,
                         String password,
                         org.apache.derby.jdbc.ClientDataSource dataSource) throws SqlException {
        initConnection(logWriter, user, dataSource);
    }

    protected Connection(org.apache.derby.client.am.LogWriter logWriter,
                         String user,
                         String password,
                         boolean isXAConn,
                         org.apache.derby.jdbc.ClientDataSource dataSource) throws SqlException {
        isXAConnection_ = isXAConn;
        initConnection(logWriter, user, dataSource);
    }

    // For jdbc 2 connections
    protected void initConnection(org.apache.derby.client.am.LogWriter logWriter,
                                  String user,
                                  org.apache.derby.jdbc.ClientDataSource dataSource) throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectEntry(dataSource);
        }
        org.apache.derby.client.am.Configuration.checkForExceptionsFromLoadConfiguration(logWriter);

        user_ = user;

        // Extract common properties.
        // Derby-409 fix
        if (dataSource.getConnectionAttributes() != null) {
            databaseName_ = dataSource.getDatabaseName() + ";" + dataSource.getConnectionAttributes();
        } else {
            databaseName_ = dataSource.getDatabaseName();
        }
        retrieveMessageText_ = dataSource.getRetrieveMessageText();

        loginTimeout_ = dataSource.getLoginTimeout();
        dataSource_ = dataSource;

        serverNameIP_ = dataSource.getServerName();
        portNumber_ = dataSource.getPortNumber();


        agent_ = newAgent_(logWriter,
                loginTimeout_,
                serverNameIP_,
                portNumber_);
    }

    // For jdbc 2 connections
    protected Connection(org.apache.derby.client.am.LogWriter logWriter,
                         boolean isXAConn,
                         org.apache.derby.jdbc.ClientDataSource dataSource) throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectEntry(dataSource);
        }
        isXAConnection_ = isXAConn;
        org.apache.derby.client.am.Configuration.checkForExceptionsFromLoadConfiguration(logWriter);

        user_ = ClientDataSource.propertyDefault_user;

        // Extract common properties.
        databaseName_ = dataSource.getDatabaseName();
        retrieveMessageText_ = dataSource.getRetrieveMessageText();

        loginTimeout_ = dataSource.getLoginTimeout();
        dataSource_ = dataSource;

        serverNameIP_ = dataSource.getServerName();
        portNumber_ = dataSource.getPortNumber();


        agent_ = newAgent_(logWriter,
                loginTimeout_,
                serverNameIP_,
                portNumber_);
    }

    // This is a callback method, called by subsystem - NetConnection
    protected void resetConnection(LogWriter logWriter,
                                   String user,
                                   ClientDataSource ds,
                                   boolean recomputeFromDataSource) throws SqlException {
        // clearWarningsX() will re-initialize the following properties
        clearWarningsX();

        user_ = (user != null) ? user : user_;

        if (ds != null && recomputeFromDataSource) { // no need to reinitialize connection state if ds hasn't changed
            user_ = (user != null) ? user : ds.getUser();
            ;

            retrieveMessageText_ = ds.getRetrieveMessageText();


            // property encryptionManager_
            // if needed this will later be initialized by NET calls to initializePublicKeyForEncryption()
            encryptionManager_ = null;

            // property: open_
            // this should already be true

            isolation_ = Configuration.defaultIsolation;
            autoCommit_ = true;
            inUnitOfWork_ = false;

            loginTimeout_ = ds.getLoginTimeout();
            dataSource_ = ds;
            
            holdability = ClientDataSource.HOLD_CURSORS_OVER_COMMIT;
        }

        
        if (recomputeFromDataSource) {
            this.agent_.resetAgent(this, logWriter, loginTimeout_, serverNameIP_, portNumber_);
        }
    }

    protected void resetConnection(LogWriter logWriter,
                                   String databaseName,
                                   java.util.Properties properties) throws SqlException {
        // clearWarningsX() will re-initialize the following properties
        // warnings_, accumulated440ForMessageProcFailure_,
        // accumulated444ForMessageProcFailure_, and accumulatedSetReadOnlyWarning_
        clearWarningsX();

        databaseName_ = databaseName;
        user_ = ClientDataSource.getUser(properties);

        retrieveMessageText_ = ClientDataSource.getRetrieveMessageText(properties);


        // property encryptionManager_
        // if needed this will later be initialized by NET calls to initializePublicKeyForEncryption()
        encryptionManager_ = null;

        // property: open_
        // this should already be true

        isolation_ = Configuration.defaultIsolation;
        autoCommit_ = true;
        inUnitOfWork_ = false;

        this.agent_.resetAgent(this, logWriter, loginTimeout_, serverNameIP_, portNumber_);

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
        org.apache.derby.client.am.Configuration.checkForExceptionsFromLoadConfiguration(logWriter);

        databaseName_ = databaseName;

        // Extract common properties.
        user_ = ClientDataSource.getUser(properties);
        retrieveMessageText_ = ClientDataSource.getRetrieveMessageText(properties);

        loginTimeout_ = driverManagerLoginTimeout;
        serverNameIP_ = serverName;
        portNumber_ = portNumber;

        agent_ = newAgent_(logWriter,
                loginTimeout_,
                serverNameIP_,
                portNumber_);
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

    synchronized public java.sql.Statement createStatement() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "createStatement");
        }
        Statement s = createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability());
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "createStatement", s);
        }
        return s;
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareStatement", sql);
        }
        PreparedStatement ps = prepareStatementX(sql,
                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY,
                holdability(),
                java.sql.Statement.NO_GENERATED_KEYS,
                null);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareStatement", ps);
        }
        return ps;
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

    synchronized public java.sql.CallableStatement prepareCall(String sql) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareCall", sql);
        }
        CallableStatement cs = prepareCallX(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability());
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareCall", cs);
        }
        return cs;
    }

    synchronized PreparedStatement prepareDynamicCatalogQuery(String sql) throws SqlException {
        PreparedStatement ps = newPreparedStatement_(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, holdability(), java.sql.Statement.NO_GENERATED_KEYS, null);
        ps.isCatalogQuery_ = true;
        ps.prepare();
        openStatements_.put(ps, null);
        return ps;
    }

    public String nativeSQL(String sql) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "nativeSQL", sql);
        }
        String nativeSql = nativeSQLX(sql);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "nativeSQL", nativeSql);
        }
        return nativeSql;
    }

    synchronized public String nativeSQLX(String sql) throws SqlException {
        checkForClosedConnection();
        if (sql == null) {
            throw new SqlException(agent_.logWriter_, "Null SQL string passed.");
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

    synchronized public void setAutoCommit(boolean autoCommit) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setAutoCommit", autoCommit);
        }
        checkForClosedConnection();

        if (! allowLocalCommitRollback_()) {
            if (autoCommit) { // can't toggle to autocommit mode when between xars.start() and xars.end()
                throw new SqlException(agent_.logWriter_,
                        "setAutoCommit(true) invalid during global transaction",
                        SqlState._2D521, // Spec'ed by PROTOCOL
                        SqlCode.invalidSetAutoCommitUnderXA);
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

    public boolean getAutoCommit() throws SqlException {
        checkForClosedConnection();
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getAutoCommit", autoCommit_);
        }
        if (! allowLocalCommitRollback_()) { // autoCommit is always false between xars.start() and xars.end()
            return false;
        }
        return autoCommit_;
    }

    synchronized public void commit() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "commit");
        }

        // the following XA State check must be in commit instead of commitX since
        // external application call commit, the SqlException should be thrown
        // only if an external application calls commit during a Global Transaction,
        // internal code will call commitX which will ignore the commit request
        // while in a Global transaction
        checkForInvalidXAStateOnCommitOrRollback();
        checkForClosedConnection();
        flowCommit();
    }

    private void checkForInvalidXAStateOnCommitOrRollback() throws SqlException {
        if (! allowLocalCommitRollback_()) {
            throw new SqlException(agent_.logWriter_,
                    "COMMIT or ROLLBACK invalid for application execution environment",
                    SqlState._2D521, // Spec'ed by PROTOCOL
                    SqlCode.invalidCommitOrRollbackUnderXA);
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
    public void flowAutoCommit() throws SqlException {
        if (willAutoCommitGenerateFlow()) {
            flowCommit();
        }
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
            if ((xaState_ == XA_T0_NOT_ASSOCIATED) ) {
                writeLocalXACommit_();
            }
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
            if ((xaState_ == XA_T0_NOT_ASSOCIATED) ) {
                readLocalXACommit_();               
            }
        } else {
            readLocalCommit_();
        }
    }

    synchronized public void rollback() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "rollback");
        }
        checkForInvalidXAStateOnCommitOrRollback();
        checkForClosedConnection();
        flowRollback();
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
            writeLocalXARollback_();
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

    synchronized public void close() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "close");
        }
        closeX();
    }

    void checkForTransactionInProgress() throws SqlException {
        // The following precondition matches CLI semantics, see SQLDisconnect()
        if (!autoCommit_ && inUnitOfWork_ && !allowCloseInUOW_()) {
            throw new SqlException(agent_.logWriter_,
                    "java.sql.Connection.close() requested while a transaction is in progress on the connection." +
                    "The transaction remains active, and the connection cannot be closed.");
        }
    }

    // This is a no-op if the connection is already closed.
    synchronized public void closeX() throws SqlException {
        if (!open_) {
            return;
        }
        closeResourcesX();
    }

    // Close physical socket or attachment even if connection is marked close.
    // Used by ClientPooledConnection.close().
    synchronized public void closeResources() throws SqlException {
        if (open_ || (!open_ && availableForReuse_)) {
            availableForReuse_ = false;
            closeResourcesX();
        }
    }

    private void closeResourcesX() throws SqlException {
        checkForTransactionInProgress();
        resetConnectionAtFirstSql_ = false; // unset indicator of deferred reset
        SqlException accumulatedExceptions = null;
        if (setTransactionIsolationStmt != null) {
            try {
                setTransactionIsolationStmt.close();
            } catch (SqlException se) {
                accumulatedExceptions = se;
            }
        }
        setTransactionIsolationStmt = null;
        if (getTransactionIsolationStmt != null) {
            try {
                getTransactionIsolationStmt.close();
            } catch (SqlException se) {
                accumulatedExceptions = Utils.accumulateSQLException(
                        se, accumulatedExceptions);
            }
        }
        getTransactionIsolationStmt = null;
        try {
            flowClose();
        } catch (SqlException e) {
            accumulatedExceptions =
                    Utils.accumulateSQLException(e, accumulatedExceptions);
        }

        markClosed();
        try {
            agent_.close();
        } catch (SqlException e) {
            throw Utils.accumulateSQLException(e, accumulatedExceptions);
        }
    }

    protected abstract boolean isGlobalPending_();

    // Just like closeX except the socket is not pulled.
    // Physical resources are not closed.
    synchronized public void closeForReuse() throws SqlException {
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
            markClosedForReuse();
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

    public void markClosed() // called by LogicalConnection.close()
    {
        open_ = false;
        inUnitOfWork_ = false;
        markStatementsClosed();
        CommitAndRollbackListeners_.clear();
        markClosed_();
    }


    private void markClosedForReuse() {
        availableForReuse_ = true;
        markClosed();
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

    synchronized public void setTransactionIsolation(int level) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTransactionIsolation", level);
        }
        // Per jdbc spec (see java.sql.Connection.close() javadoc).
        checkForClosedConnection();

        // Javadoc for this method:
        //   If this method is called during a transaction, the result is implementation-defined.
        //
        //
        // REPEATABLE_READ = JDBC: TRANSACTION_SERIALIZABLE, DERBY: RR, PROTOCOL: repeatable read
        // READ_STABILITY = JDBC: TRANSACTION_REPEATABLE_READ, DERBY: RS, PROTOCOL: All
        // CURSOR_STABILITY = JDBC: TRANSACTION_READ_COMMITTED, DERBY: CS, PROTOCOL: Cursor stability
        // UNCOMMITTED_READ = JDBC: TRANSACTION_READ_UNCOMMITTED, DERBY: UR , PROTOCOL: Change
        // NO_COMMIT = JDBC: TRANSACTION_NONE, DERBY: NC, PROTOCOL: No commit
        //
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
            //   Note that Connection.TRANSACTION_NONE cannot be used because it specifies that transactions are not supported.
        case java.sql.Connection.TRANSACTION_NONE:
        default:
            throw new SqlException(agent_.logWriter_,
                    "Transaction isolation level " + level + " is an invalid argument for java.sql.Connection.setTransactionIsolation()." +
                    " See Javadoc specification for a list of valid arguments.", "XJ045");
        }
        if (setTransactionIsolationStmt == null  ||
            !(setTransactionIsolationStmt.openOnClient_ &&
              setTransactionIsolationStmt.openOnServer_)) {
            setTransactionIsolationStmt =
                    createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY,
                            holdability());
        }
        setTransactionIsolationStmt.executeUpdate("SET CURRENT ISOLATION = " + levelString);

        isolation_ = level;

    }

    public int getTransactionIsolation() throws SqlException {
    	
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
            
            // Set auto-commit to false when executing the statement as we do not want to
            // cause an auto-commit from getTransactionIsolation() method. 
            autoCommit_ = false;
            
            // DERBY-1148 - Client reports wrong isolation level. We need to get the isolation
            // level from the server. 'isolation_' maintained in the client's connection object
            // can be out of sync with the real isolation when in an XA transaction. This can 
            // also happen when isolation is set using SQL instead of JDBC. So we try to get the
            // value from the server by calling the "current isolation" function. If we fail to 
            // get the value, return the value stored in the client's connection object.
            if (getTransactionIsolationStmt == null  || 
            		!(getTransactionIsolationStmt.openOnClient_ &&
            				getTransactionIsolationStmt.openOnServer_)) {
                getTransactionIsolationStmt =
                        createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                java.sql.ResultSet.CONCUR_READ_ONLY,
                                holdability());
            }
            
            rs = getTransactionIsolationStmt.executeQuery("values current isolation");
            rs.next();
            String isolationStr = rs.getString(1);
            isolation_ = translateIsolation(isolationStr);
            rs.close();	
        }
        catch(SQLException se) {
        	throw new SqlException(agent_.logWriter_, se.getMessage());
        }
	    finally {
        	// Restore auto-commit value
        	autoCommit_ = currentAutoCommit;
        }
        
        return isolation_;
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

    public java.sql.SQLWarning getWarnings() {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getWarnings", warnings_);
        }
        return warnings_;
    }

    synchronized public void clearWarnings() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "clearWarnings");
        }
        clearWarningsX();
    }

    // An untraced version of clearWarnings()
    public void clearWarningsX() throws SqlException {
        warnings_ = null;
        accumulated440ForMessageProcFailure_ = false;
        accumulated444ForMessageProcFailure_ = false;
        accumulatedSetReadOnlyWarning_ = false;
    }

    //======================================================================
    // Advanced features:

    public java.sql.DatabaseMetaData getMetaData() throws SqlException {
        checkForClosedConnection();
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getMetaData", databaseMetaData_);
        }
        return databaseMetaData_;
    }

    synchronized public void setReadOnly(boolean readOnly) throws SqlException {
        // This is a hint to the driver only, so this request is silently ignored.
        // PROTOCOL can only flow a set-read-only before the connection is established.
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setReadOnly", readOnly);
        }
        checkForClosedConnection();
    }

    public boolean isReadOnly() throws SqlException {
        checkForClosedConnection();
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "isReadOnly", jdbcReadOnly_);
        }
        return false;
    }

    synchronized public void setCatalog(String catalog) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setCatalog", catalog);
        }
        checkForClosedConnection();
        // Per jdbc spec: if the driver does not support catalogs, it will silently ignore this request.
    }

    public String getCatalog() throws SqlException {
        checkForClosedConnection();
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getCatalog", (String) null);
        }
        return null;
    }

    //--------------------------JDBC 2.0-----------------------------

    synchronized public java.sql.Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "createStatement", resultSetType, resultSetConcurrency);
        }
        Statement s = createStatementX(resultSetType, resultSetConcurrency, holdability());
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "createStatement", s);
        }
        return s;
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql,
                                                                    int resultSetType,
                                                                    int resultSetConcurrency) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareStatement", sql, resultSetType, resultSetConcurrency);
        }
        PreparedStatement ps = prepareStatementX(sql,
                resultSetType,
                resultSetConcurrency,
                holdability(),
                java.sql.Statement.NO_GENERATED_KEYS,
                null);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareStatement", ps);
        }
        return ps;
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql,
                                                               int resultSetType,
                                                               int resultSetConcurrency) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareCall", sql, resultSetType, resultSetConcurrency);
        }
        CallableStatement cs = prepareCallX(sql, resultSetType, resultSetConcurrency, holdability());
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareCall", cs);
        }
        return cs;
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
            accumulateWarning(new SqlWarning(agent_.logWriter_, "Scroll sensitive result sets are not supported by server; remapping to forward-only cursor"));
            return java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
        }
        return resultSetType;
    }

    // Per jdbc spec, when a result set concurrency is unsupported, we downgrade and
    // issue a warning rather than to throw an exception.
    private int downgradeResultSetConcurrency(int resultSetConcurrency, int resultSetType) {
        if (resultSetConcurrency == java.sql.ResultSet.CONCUR_UPDATABLE &&
                resultSetType == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE) {
            accumulateWarning(new SqlWarning(agent_.logWriter_, "Insensitive updatable result sets are not supported by server; remapping to insensitive read-only cursor"));
            return java.sql.ResultSet.CONCUR_READ_ONLY;
        }
        return resultSetConcurrency;
    }

    public java.util.Map getTypeMap() throws SqlException {
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

    synchronized public void setTypeMap(java.util.Map map) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTypeMap", map);
        }
        checkForClosedConnection();
        throw new SqlException(agent_.logWriter_, "Connection.setTypeMap is not supported");
    }

    //--------------------------JDBC 3.0-----------------------------

    synchronized public void setHoldability(int holdability) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setHoldability", holdability);
        }
        checkForClosedConnection();
        // In an XA global transaction do not allow the
        // holdability to be set to hold cursors across
        // commits, as the engine does not support it.
        if (this.isXAConnection_ && this.xaState_ == XA_T1_ASSOCIATED)
        {
            if (holdability == ClientDataSource.HOLD_CURSORS_OVER_COMMIT)
               throw new SqlException(agent_.logWriter_, 
                 "Cannot set holdability ResultSet.HOLD_CURSORS_OVER_COMMIT for a global transaction.",
                 "XJ05C");
        }
        this.holdability = holdability;
    }

    public int getHoldability() throws SqlException {
        checkForClosedConnection();
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getHoldability", holdability());
        }
        return holdability();
    }

    public int dncGeneratedSavepointId_;
    // generated name used internally for unnamed savepoints
    public static final String dncGeneratedSavepointNamePrefix__ = "DNC_GENENERATED_NAME_";

    synchronized public java.sql.Savepoint setSavepoint() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setSavepoint");
        }
        checkForClosedConnection();
        if (autoCommit_) // Throw exception if auto-commit is on
        {
            throw new SqlException(agent_.logWriter_, "Cannot set savepoint when in auto-commit mode.");
        } 
        // create an un-named savepoint.
        if ((++dncGeneratedSavepointId_) < 0) {
            dncGeneratedSavepointId_ = 1; // restart from 1 when overflow.
        }
        Object s = setSavepointX(new Savepoint(agent_, dncGeneratedSavepointId_));
        return (java.sql.Savepoint) s;
    }

    synchronized public java.sql.Savepoint setSavepoint(String name) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setSavepoint", name);
        }
        checkForClosedConnection();
        if (name == null) // Throw exception if savepoint name is null
        {
            throw new SqlException(agent_.logWriter_, "Named savepoint needs a none-null name.");
        } else if (autoCommit_) // Throw exception if auto-commit is on
        {
            throw new SqlException(agent_.logWriter_, "Cannot set savepoint when in auto-commit mode.");
        }
        // create a named savepoint.
        Object s = setSavepointX(new Savepoint(agent_, name));
        return (java.sql.Savepoint) s;
    }

    private Savepoint setSavepointX(Savepoint savepoint) throws SqlException {
        // Construct and flow a savepoint statement to server.
        Statement stmt = null;
        try {
            stmt = (Statement) createStatementX(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY,
                    holdability());
            String savepointName;
            try {
                savepointName = savepoint.getSavepointName();
            } catch (SqlException e) {
                // generate the name for an un-named savepoint.
                savepointName = dncGeneratedSavepointNamePrefix__ +
                        savepoint.getSavepointId();
            }
            String sql = "SAVEPOINT \"" + savepointName + "\" ON ROLLBACK RETAIN CURSORS";
            stmt.executeX(sql);
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

    synchronized public void rollback(java.sql.Savepoint savepoint) throws SqlException {
        int saveXaState = xaState_;
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "rollback", savepoint);
        }
        checkForClosedConnection();
        if (savepoint == null) // Throw exception if savepoint is null
        {
            throw new SqlException(agent_.logWriter_, "Cannot rollback to a null savepoint.");
        } else if (autoCommit_) // Throw exception if auto-commit is on
        {
            throw new SqlException(agent_.logWriter_, "Cannot rollback to a savepoint when in auto-commit mode.");
        } 
        // Only allow to rollback to a savepoint from the connection that create the savepoint.
        try {
            if (this != ((Savepoint) savepoint).agent_.connection_) {
                throw new SqlException(agent_.logWriter_,
                        "Rollback to a savepoint not created by this connection.");
            }
        } catch (java.lang.ClassCastException e) { // savepoint is not an instance of am.Savepoint
            throw new SqlException(agent_.logWriter_,
                    "Rollback to a savepoint not created by this connection.");
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
            } catch (SqlException e) {
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

    synchronized public void releaseSavepoint(java.sql.Savepoint savepoint) throws SqlException {
        int saveXaState = xaState_;
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "releaseSavepoint", savepoint);
        }
        checkForClosedConnection();
        if (savepoint == null) // Throw exception if savepoint is null
        {
            throw new SqlException(agent_.logWriter_, "Cannot release a null savepoint.");
        } else if (autoCommit_) // Throw exception if auto-commit is on
        {
            throw new SqlException(agent_.logWriter_, "Cannot release a savepoint when in auto-commit mode.");
        } 
        // Only allow to release a savepoint from the connection that create the savepoint.
        try {
            if (this != ((Savepoint) savepoint).agent_.connection_) {
                throw new SqlException(agent_.logWriter_,
                        "Cannot release a savepoint that was not created by this connection.");
            }
        } catch (java.lang.ClassCastException e) { // savepoint is not an instance of am.Savepoint
            throw new SqlException(agent_.logWriter_,
                    "Cannot release a savepoint that was not created by this connection.");
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
            } catch (SqlException e) {
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

    synchronized public java.sql.Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency,
                                                           int resultSetHoldability) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "createStatement", resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        Statement s = createStatementX(resultSetType, resultSetConcurrency, resultSetHoldability);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "createStatement", s);
        }
        return s;
    }

    private Statement createStatementX(int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SqlException {
        checkForClosedConnection();
        resultSetType = downgradeResultSetType(resultSetType);
        resultSetConcurrency = downgradeResultSetConcurrency(resultSetConcurrency, resultSetType);
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
            if (resultSetHoldability == ClientDataSource.HOLD_CURSORS_OVER_COMMIT) {
                resultSetHoldability = ClientDataSource.CLOSE_CURSORS_AT_COMMIT;
                accumulateWarning(new SqlWarning(agent_.logWriter_, 
                        "ResultSetHoldability restricted to ResultSet.CLOSE_CURSORS_AT_COMMIT for a global transaction.", "01J07"));
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
                                                                    int resultSetHoldability) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareStatement", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        PreparedStatement ps = prepareStatementX(sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability,
                java.sql.Statement.NO_GENERATED_KEYS,
                null);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareStatement", ps);
        }
        return ps;
    }

    // used by DBMD
    PreparedStatement prepareStatementX(String sql,
                                        int resultSetType,
                                        int resultSetConcurrency,
                                        int resultSetHoldability,
                                        int autoGeneratedKeys,
                                        String[] columnNames) throws SqlException {
        checkForClosedConnection();
        checkAutoGeneratedKeysParameters(autoGeneratedKeys, columnNames);
        resultSetType = downgradeResultSetType(resultSetType);
        resultSetConcurrency = downgradeResultSetConcurrency(resultSetConcurrency, resultSetType);
        PreparedStatement ps = newPreparedStatement_(sql, resultSetType, resultSetConcurrency, resultSetHoldability, autoGeneratedKeys, columnNames);
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
        resetPreparedStatement_(ps, ps.sql_, ps.resultSetType_, ps.resultSetConcurrency_, ps.resultSetHoldability_, ps.autoGeneratedKeys_, ps.generatedKeysColumnNames_);
        ps.cursorAttributesToSendOnPrepare_ = cursorAttributesToSendOnPrepare;
        ps.prepare();
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql,
                                                               int resultSetType,
                                                               int resultSetConcurrency,
                                                               int resultSetHoldability) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareCall", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        CallableStatement cs = prepareCallX(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareCall", cs);
        }
        return cs;
    }

    private CallableStatement prepareCallX(String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) throws SqlException {
        checkForClosedConnection();
        resultSetType = downgradeResultSetType(resultSetType);
        resultSetConcurrency = downgradeResultSetConcurrency(resultSetConcurrency, resultSetType);
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

    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareStatement", sql, autoGeneratedKeys);
        }
        PreparedStatement ps = prepareStatementX(sql,
                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY,
                holdability(),
                autoGeneratedKeys,
                null);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareStatement", ps);
        }
        return ps;
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareStatement", sql, columnIndexes);
        }
        checkForClosedConnection();
        throw new SqlException(agent_.logWriter_, "Driver not capable");
    }

    public java.sql.PreparedStatement prepareStatement(String sql, String columnNames[]) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "prepareStatement", sql, columnNames);
        }
        PreparedStatement ps = prepareStatementX(sql,
                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY,
                holdability(),
                java.sql.Statement.RETURN_GENERATED_KEYS,
                columnNames);
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "prepareStatement", ps);
        }
        return ps;
    }


    // ---------------------------------------------------------------------------

    protected abstract boolean allowCloseInUOW_();

    protected abstract boolean doCloseStatementsOnClose_();

    public abstract SectionManager newSectionManager(String collection,
                                                     Agent agent,
                                                     String databaseName);
    //--------------------Abstract material factory methods-----------------

    protected abstract Agent newAgent_(LogWriter logWriter, int loginTimeout, String serverName, int portNumber) throws SqlException;


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
                                                               String[] columnNames) throws SqlException;

    protected abstract void resetPreparedStatement_(PreparedStatement ps,
                                                    String sql,
                                                    int resultSetType,
                                                    int resultSetConcurrency,
                                                    int resultSetHoldability,
                                                    int autoGeneratedKeys,
                                                    String[] columnNames) throws SqlException;

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

    public abstract void readLocalXACommit_() throws SqlException;

    public abstract void writeLocalCommit_() throws SqlException;

    public abstract void readLocalCommit_() throws SqlException;

    public void completeLocalCommit() {
    	java.util.Set keySet = CommitAndRollbackListeners_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            UnitOfWorkListener listener = (UnitOfWorkListener) i.next();
            listener.completeLocalCommit(i);
        }
        inUnitOfWork_ = false;
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
    }


    public abstract void writeLocalXARollback_() throws SqlException;

    public abstract void readLocalXARollback_() throws SqlException;

    public void writeTransactionStart(Statement statement) throws SqlException {
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

    public abstract void addSpecialRegisters(String s);

    // can this only be called by the PooledConnection
    // can this be called on a closed connection
    // can this be called in a unit of work
    // can this be called from within a stored procedure
    //
    synchronized public void reset(LogWriter logWriter, String user, String password, ClientDataSource ds, boolean recomputeFromDataSource) throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectResetEntry(this, logWriter, user, (ds != null) ? ds : dataSource_);
        }
        try {
            reset_(logWriter, user, password, ds, recomputeFromDataSource);
        } catch (SqlException sqle) {
            DisconnectException de = new DisconnectException(agent_, "An error occurred during connect reset and the connection has been terminated.  See chained exceptions for details.");
            de.setNextException(sqle);
            throw de;
        }
    }

    synchronized public void reset(LogWriter logWriter, ClientDataSource ds, boolean recomputeFromDataSource) throws SqlException {
        if (logWriter != null) {
            logWriter.traceConnectResetEntry(this, logWriter, null, (ds != null) ? ds : dataSource_);
        }
        try {
            reset_(logWriter, ds, recomputeFromDataSource);
        } catch (SqlException sqle) {
            DisconnectException de = new DisconnectException(agent_, "An error occurred during connect reset and the connection has been terminated.  See chained exceptions for details.");
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

    abstract protected void reset_(LogWriter logWriter, String user, String password, ClientDataSource ds, boolean recomputerFromDataSource) throws SqlException;

    abstract protected void reset_(LogWriter logWriter, ClientDataSource ds, boolean recomputerFromDataSource) throws SqlException;

    protected void completeReset(boolean isDeferredReset, boolean recomputeFromDataSource) throws SqlException {
        open_ = true;

        completeLocalRollback(); // this will close the cursors if the physical connection hadn't been closed for reuse properly

        // Reopen physical statement resources associated with previous uses of this physical connection.
        // Notice that these physical statements may not belong to this logical connection.
        // Iterate through the physical statements and re-enable them for reuse.

        java.util.Set keySet = openStatements_.keySet();
        for (java.util.Iterator i = keySet.iterator(); i.hasNext();) {
            Object o = i.next();
            ((Statement) o).reset(recomputeFromDataSource);

        }

        if (!isDeferredReset && agent_.loggingEnabled()) {
            agent_.logWriter_.traceConnectResetExit(this);
        }
    }


    //-------------------------------helper methods-------------------------------

    protected void checkForClosedConnection() throws SqlException {
        if (!open_) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, "invalid operation: connection closed");
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    void checkAutoGeneratedKeysParameters(int autoGeneratedKeys, String[] columnNames) throws SqlException {
        if (autoGeneratedKeys != java.sql.Statement.NO_GENERATED_KEYS &&
                autoGeneratedKeys != java.sql.Statement.RETURN_GENERATED_KEYS) {
            throw new SqlException(agent_.logWriter_, "Invalid argument: " +
                    "Statement auto-generated keys value " + autoGeneratedKeys +
                    " is invalid.");
        }

        if (columnNames != null) {
            throw new SqlException(agent_.logWriter_, "Driver not capable");
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
            return ClientDataSource.CLOSE_CURSORS_AT_COMMIT;
        return holdability;
    }
}
