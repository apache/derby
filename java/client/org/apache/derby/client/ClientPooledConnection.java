/*

   Derby - Class org.apache.derby.client.ClientPooledConnection

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
package org.apache.derby.client;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.derby.client.net.NetXAConnection;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.jdbc.ClientBaseDataSource;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.net.NetLogWriter;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * A physical connection to a data source, to be used for creating logical
 * connections to the same data source.
 */
public class ClientPooledConnection implements javax.sql.PooledConnection {

    /** Tells if this pooled connection is newly created. */
    private boolean newPC_ = true;

    //@GuardedBy("this")
    /** List of {@code ConnectionEventListener}s. Never {@code null}. */
    private ArrayList listeners_ = new ArrayList();

    /**
     * The number of iterators going through the list of connection event
     * listeners at the current time. Only one thread may be iterating over the
     * list at any time (because of synchronization), but a single thread may
     * have multiple iterators if for instance an event listener performs
     * database calls that trigger a new event.
     */
    private int eventIterators;

    org.apache.derby.client.am.Connection physicalConnection_ = null;
    org.apache.derby.client.net.NetConnection netPhysicalConnection_ = null;
    org.apache.derby.client.net.NetXAConnection netXAPhysicalConnection_ = null;

    /**
     * The statement cache for the underlying physical connection.
     * <p>
     * This will be {@code null} if statement caching is disabled (default).
     */
    private final JDBCStatementCache statementCache;

    /** The logical connection using the physical connection. */
    //@GuardedBy("this")
    org.apache.derby.client.am.LogicalConnection logicalConnection_ = null;

    protected org.apache.derby.client.am.LogWriter logWriter_ = null;

    /** Resource manager identificator. */
    protected int rmId_ = 0;

    /**
     * Constructor for non-XA pooled connections.
     * <p>
     * Using standard Java APIs, a CPDS is passed in. Arguments for
     * user/password overrides anything on the data source.
     * 
     * @param ds data source creating this pooled connection
     * @param logWriter destination for log messages
     * @param user user name
     * @param password user password
     * @throws SQLException if creating the pooled connection fails due problems
     *      in the database, or problems communicating with the database
     */
    public ClientPooledConnection(ClientBaseDataSource ds,
                                  org.apache.derby.client.am.LogWriter logWriter,
                                  String user,
                                  String password) throws SQLException {
        logWriter_ = logWriter;

        if (ds.maxStatementsToPool() <= 0) {
            this.statementCache = null;
        } else {
            this.statementCache =
                    new JDBCStatementCache(ds.maxStatementsToPool());
        }

        try {
            //pass the client pooled connection instance to this
            //instance of the NetConnection object 
            //this object is then used to pass the close and the error events 
            //that occur in the PreparedStatement object back to the 
            //PooledConnection which will then raise the events
            //on the listeners
            
            netPhysicalConnection_ = (org.apache.derby.client.net.NetConnection)
            ClientDriver.getFactory().newNetConnection(
                    (NetLogWriter) logWriter_,
                    user,
                    password,
                    ds,
                    -1,
                    false,
                    this);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        physicalConnection_ = netPhysicalConnection_;
    }

    /**
     * Constructor for XA pooled connections only.
     * <p>
     * Using standard Java APIs, a CPDS is passed in. Arguments for
     * user/password overrides anything on the data source.
     * 
     * @param ds data source creating this pooled connection
     * @param logWriter destination for log messages
     * @param user user name
     * @param password user password
     * @param rmId resource manager id
     * @throws SQLException if creating the pooled connection fails due problems
     *      in the database, or problems communicating with the database
     */
    public ClientPooledConnection(ClientBaseDataSource ds,
                                  org.apache.derby.client.am.LogWriter logWriter,
                                  String user,
                                  String password,
                                  int rmId) throws SQLException {
        logWriter_ = logWriter;
        rmId_ = rmId;

        if (ds.maxStatementsToPool() <= 0) {
            this.statementCache = null;
        } else {
            // NOTE: Disable statement pooling for XA for now.
            this.statementCache = null;
            //        new JDBCStatementCache(ds.maxStatementsToPool());
        }

        try {
            netXAPhysicalConnection_ = getNetXAConnection(ds,
                    (NetLogWriter) logWriter_,
                    user,
                    password,
                    rmId);
        } catch ( SqlException se ) {
            throw se.getSQLException();
        }
        physicalConnection_ = netXAPhysicalConnection_.getNetConnection();
    }

    /**
     * Tells is statement pooling is enabled or not.
     *
     * @return {@code true} if enabled, {@code false} if disabled.
     */
    public boolean isStatementPoolingEnabled() {
        return this.statementCache != null;
    }

    protected void finalize() throws java.lang.Throwable {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "finalize");
        }
        close();
    }

    /**
     * Closes the physical connection to the data source and frees all
     * assoicated resources.
     * 
     * @throws SQLException if closing the connection causes an error. Note that
     *      this connection can still be considered closed even if an error
     *      occurs.
     */
    public synchronized void close() throws SQLException {
        try
        {
            if (logWriter_ != null) {
                logWriter_.traceEntry(this, "close");
            }

            if (logicalConnection_ != null) {
                logicalConnection_.nullPhysicalConnection();
                logicalConnection_ = null;
            }

            if (physicalConnection_ == null) {
                return;
            }

            // Even if the physcial connection is marked closed (in the pool),
            // this will close its underlying resources.
            physicalConnection_.closeResources();
        }
        finally 
        {
            physicalConnection_ = null;
        }
    }

    /**
     * Creates a logical connection.
     * <p>
     * This is the standard API for getting a logical connection handle for a
     * pooled connection. No "resettable" properties are passed, so user,
     * password, and all other properties may not change.
     * 
     * @throws SQLException if creating a new logical connection fails
     */
    public synchronized java.sql.Connection getConnection() throws SQLException {
        try
        {
            if (logWriter_ != null) {
                logWriter_.traceEntry(this, "getConnection");
            }           
            createLogicalConnection();

            
            if (!newPC_) {
            	// DERBY-1144 changed the last parameter of this method to true
            	// to reset the connection state to the default on 
            	// PooledConnection.getConnection() otherwise the 
            	// isolation level and holdability was not correct and out of sync with the server.
                physicalConnection_.reset(logWriter_, true);
            }
            else {
                physicalConnection_.lightReset();    //poolfix
            }
            newPC_ = false;

            if (logWriter_ != null) {
                logWriter_.traceExit(this, "getConnection", logicalConnection_);
            }
            return logicalConnection_;
        }
        catch (SqlException se)
        {
            throw se.getSQLException();
        }
    }

    /**
     * Creates a new logical connection by performing all the required steps to
     * be able to reuse the physical connection.
     * <p>
     * 
     * @throws SqlException if there is no physical connection, or if any error
     *      occurs when recycling the physical connection or closing/craeting
     *      the logical connection
     */
    //@GuardedBy("this")
    private void createLogicalConnection() throws SqlException {
        if (physicalConnection_ == null) {
            throw new SqlException(logWriter_,
                new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION));
        }
        
        // Roll back any pending transactions.  Otherwise we get an exception
        // when we try to close the connection (even for re-use), with an error
        // saying we can't close the connection with active transactions
        // (this fixes DERBY-1004)
        try {
            if ( physicalConnection_.transactionInProgress() ) {
                physicalConnection_.rollback();
            }
        } catch ( SQLException sqle ) {
            throw new SqlException(sqle);
        }
        
        // Not the usual case, but if we have an existing logical connection,
        // then we must close it by spec. We close the logical connection
        // without notifying the pool manager that this pooled connection is
        // availabe for reuse.
        if (logicalConnection_ != null) {
            logicalConnection_.closeWithoutRecyclingToPool();
        }
        if (this.statementCache == null) {
            logicalConnection_ = ClientDriver.getFactory().newLogicalConnection(
                                                        physicalConnection_,
                                                        this);
        } else {
            logicalConnection_ = ClientDriver.getFactory().
                    newCachingLogicalConnection(
                            physicalConnection_, this, statementCache);
        }
    }

    public synchronized void addConnectionEventListener(
                                            ConnectionEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "addConnectionEventListener", listener);
        }
        if (eventIterators > 0) {
            // DERBY-3401: Someone is iterating over the ArrayList, and since
            // we were able to synchronize on this, that someone is us. Clone
            // the list of listeners in order to prevent invalidation of the
            // iterator.
            listeners_ = (ArrayList) listeners_.clone();
        }
        listeners_.add(listener);
    }

    public synchronized void removeConnectionEventListener(
                                            ConnectionEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "removeConnectionEventListener", listener);
        }
        if (eventIterators > 0) {
            // DERBY-3401: Someone is iterating over the ArrayList, and since
            // we were able to synchronize on this, that someone is us. Clone
            // the list of listeners in order to prevent invalidation of the
            // iterator.
            listeners_ = (ArrayList) listeners_.clone();
        }
        listeners_.remove(listener);
    }

    /**
     * Inform listeners that the logical connection has been closed and that the
     * physical connection is ready for reuse.
     * <p>
     * Not public, but needs to be visible to am.LogicalConnection
     */
    public synchronized void recycleConnection() {
        if (physicalConnection_.agent_.loggingEnabled()) {
            physicalConnection_.agent_.logWriter_.traceEntry(this, "recycleConnection");
        }

        // Null out the reference to the logical connection that is currently
        // being closed.
        this.logicalConnection_ = null;

        fireConnectionEventListeners(null);
    }

    /**
     * Inform listeners that an error has occured on the connection, if the
     * error severity is high enough.
     * <p>
     * Not public, but needs to be visible to am.LogicalConnection
     * 
     * @param exception the exception that occurred on the connection
     */
    public void informListeners(SqlException exception) {
		// only report fatal error  
		if (exception.getErrorCode() < ExceptionSeverity.SESSION_SEVERITY)
			return;

        synchronized (this) {
            fireConnectionEventListeners(exception);
        }
    }

    /**
     * Fire all the {@code ConnectionEventListener}s registered. Callers must
     * synchronize on {@code this} to prevent others from modifying the list of
     * listeners.
     *
     * @param exception the exception that caused the event, or {@code null} if
     * it is a close event
     */
    private void fireConnectionEventListeners(SqlException exception) {
        if (!listeners_.isEmpty()) {
            final ConnectionEvent event = (exception == null) ?
                new ConnectionEvent(this) :
                new ConnectionEvent(this, exception.getSQLException());
            eventIterators++;
            try {
                for (Iterator it = listeners_.iterator(); it.hasNext(); ) {
                    final ConnectionEventListener listener =
                        (ConnectionEventListener) it.next();
                    if (exception == null) {
                        listener.connectionClosed(event);
                    } else {
                        listener.connectionErrorOccurred(event);
                    }
                }
            } finally {
                eventIterators--;
            }
        }
    }

    /**
     * Used by {@code LogicalConnection.close} in some circumstances when
     * it disassociates itself from the pooled connection.
     */
    public synchronized void nullLogicalConnection() {
        logicalConnection_ = null;
    }
    
    /*-----------------------------------------------------------------*/
    /*
     * These methods are needed to provide StatementEvent support for 
     * derby. 
     * They are actually implemented in EmbedPooledConnection40 but have
     * a dummy implementation here
     */
    
    /**
     *
     * The onStatementClose contains the logic for raising the Statement Closed
     * events. This method has a dummy implementation here to avoid error when
     * this class is compiled with jdk1.4. The class the actual implementation 
     * in ClientPooledConnection40.
     *
     * @param statement The PreparedStatement that was closed
     *
     */
    public void onStatementClose(PreparedStatement statement) {
        
    }
    
    /**
     * The method contains the logic for raising the Statement error occurred
     * events. This method has a dummy implementation here to avoid error when
     * this class is compiled with jdk1.4. The class the actual implementation 
     * in ClientPooledConnection40.
     *
     * @param statement The PreparedStatement that was closed
     * @param sqle      The SQLException associated with the error that caused
     *                  the invalidation of this PreparedStatement
     */
    public void onStatementErrorOccurred(PreparedStatement statement,
                    SQLException sqle) {
        
    }
    
    /**
     * creates and returns NetXAConnection. 
     * Overwrite this method to create different version of NetXAConnection
     * @param ds 
     * @param logWriter 
     * @param user 
     * @param password 
     * @param rmId 
     * @return NetXAConnection
     */
    protected NetXAConnection getNetXAConnection (ClientBaseDataSource ds,
                                  NetLogWriter logWriter,
                                  String user,
                                  String password,
                                  int rmId) throws SqlException {
          return new NetXAConnection(logWriter,
                    user,
                    password,
                    ds,
                    rmId,
                    true,
                    this);
        
    }
}
