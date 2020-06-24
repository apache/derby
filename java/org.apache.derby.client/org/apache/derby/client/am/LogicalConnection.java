/*

   Derby - Class org.apache.derby.client.am.LogicalConnection

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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * A simple delegation wrapper handle for a physical connection.
 * <p>
 * All methods of the {@code Connection} interface are forwarded to the
 * underlying physical connection, except for {@link #close()} and
 * {@link #isClosed()}. When a physical connection is wrapped, it is non-null,
 * when the logical connection is closed, the wrapped physical connection is
 * always set to {@code null}.
 * Both the finalizer and the {@code close}-methods will always set the 
 * physical connection to {@code null}. After the physical connection has been
 * nulled out, only the {@code PooledConnection} instance will maintain a
 * handle to the physical connection.
 */
public class LogicalConnection implements Connection {
    /**
     * Underlying physical connection for this logical connection.
     * <p>
     * Set to {@code null} when this logical connection is closed.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClientConnection physicalConnection_;
    private ClientPooledConnection pooledConnection_ = null;
    /**
     * Logical database metadata object created on demand and then cached.
     * The lifetime of the metadata object is the same as this logical
     * connection, in the sense that it will raise exceptions on method
     * invocations after the logical connection has been closed.
     */
    private LogicalDatabaseMetaData logicalDatabaseMetaData = null;

    public LogicalConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection physicalConnection,
            ClientPooledConnection pooledConnection) throws SqlException {

        physicalConnection_ = physicalConnection;
        pooledConnection_ = pooledConnection;
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try {
            checkForNullPhysicalConnection();
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
    }

    //
    // This method in java.lang.Object was deprecated as of build 167
    // of JDK 9. See DERBY-6932.
    //
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        close();
    }

    // Used by ClientPooledConnection close when it disassociates itself from the LogicalConnection
    synchronized public void nullPhysicalConnection() {
        physicalConnection_ = null;
    }

    // ------------------------ logical connection close -------------------------
    // All methods are simply forwarded to the physical connection, except for close() and isClosed().

    public void close() throws SQLException {
        // The pooledConnection owns this LogicalConnection.  To ensure that
        //  there is no deadlock when calling back into the pooledConnection_.recycleConnection
        //  below, we first synchronize on the pooledConnection and then on this
        //  LogicalConnection
//IC see: https://issues.apache.org/jira/browse/DERBY-5560
        synchronized (pooledConnection_) {
            synchronized (this) {
                try {
                    // we also need to loop thru all the logicalStatements and close them
                    if (physicalConnection_ == null) {
                        return;
                    }
                    if (physicalConnection_.agent_.loggingEnabled()) {
                        physicalConnection_.agent_.logWriter_.traceEntry(this, "close");
                    }

                    if (physicalConnection_.isClosed()) // connection is closed or has become stale
                    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                        pooledConnection_.informListeners(new SqlException(null,
                                new ClientMessageId(
                                SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED)));
                    } else {
                        physicalConnection_.checkForTransactionInProgress();
//IC see: https://issues.apache.org/jira/browse/DERBY-3441
                        physicalConnection_.closeForReuse(
                                pooledConnection_.isStatementPoolingEnabled());
                        if (!physicalConnection_.isGlobalPending_()) {
                            pooledConnection_.recycleConnection();
                        }
                    }
                    physicalConnection_ = null;
                    pooledConnection_.nullLogicalConnection();
                } catch (SqlException se) {
                    throw se.getSQLException();
                }
            }
        }
    }

    synchronized public void closeWithoutRecyclingToPool() throws SqlException {
        if (physicalConnection_ == null) {
            return;
        }
        physicalConnection_.checkForTransactionInProgress();
        try {
            if (physicalConnection_.isClosed()) // connection is closed or has become stale
            {
                throw new SqlException(null, 
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
                    new ClientMessageId(SQLState.NO_CURRENT_CONNECTION)); // no call to informListeners()
            } else {
                ; // no call to recycleConnection()
            }
        } finally {
//IC see: https://issues.apache.org/jira/browse/DERBY-3441
            physicalConnection_.closeForReuse(
                    pooledConnection_.isStatementPoolingEnabled());  //poolfix
            physicalConnection_ = null;
        }
    }

    public boolean isClosed() throws SQLException {
        if (physicalConnection_ == null) {
            return true;
        }
        return physicalConnection_.isClosed();
    }

    // --------------------------- helper methods --------------------------------

    /**
     * Verifies that there is an underlying physical connection for this
     * logical connection.
     * <p>
     * If the physical connection has been nulled out it means that this
     * logical connection has been closed.
     *
     * @throws SQLException if this logical connection has been closed
     */
    protected final void checkForNullPhysicalConnection()
//IC see: https://issues.apache.org/jira/browse/DERBY-5561
            throws SQLException {
        if (physicalConnection_ == null) {
            SqlException se = new SqlException(null, 
                new ClientMessageId(SQLState.NO_CURRENT_CONNECTION));
            throw se.getSQLException();
        }
    }

    /**
     * Notifies listeners about exceptions of session level severity or higher.
     * <p>
     * The exception, even if the severity is sufficiently high, is ignored if
     * the underlying physical connection has been nulled out. Otherwise a 
     * {@code connectionErrorOccurred}-event is sent to all the registered
     * listeners.
     * 
     * @param sqle the cause of the notification
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    final void notifyException(SQLException sqle) {
        if (physicalConnection_ != null) 
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
            pooledConnection_.informListeners(new SqlException(sqle));
    }

    // ---------------------- wrapped public entry points ------------------------
    // All methods are forwarded to the physical connection in a standard way

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public Statement createStatement() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.createStatement();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public PreparedStatement prepareStatement(String sql)
            throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareStatement(sql);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public CallableStatement prepareCall(String sql)
            throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareCall(sql);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public String nativeSQL(String sql) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.nativeSQL(sql);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    synchronized public void setAutoCommit(boolean autoCommit) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setAutoCommit(autoCommit);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public boolean getAutoCommit() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getAutoCommit();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    synchronized public void commit() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.commit();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    synchronized public void rollback() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.rollback();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    synchronized public void setTransactionIsolation(int level) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setTransactionIsolation(level);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public int getTransactionIsolation() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getTransactionIsolation();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public SQLWarning getWarnings() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getWarnings();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    synchronized public void clearWarnings() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.clearWarnings();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Retrieves a {@code DatabaseMetaData} object that contains metadata about
     * the database to which this {@code Connection} object represents a
     * connection.
     * <p>
     * The database metadata object is logical in the sense that it has the
     * same lifetime as the logical connection. If the logical connection is
     * closed, the underlying physical connection will not be accessed to
     * obtain metadata, even if it is still open. Also, the reference to the
     * logical connection instead of the underlying physical connection will be
     * returned by {@link LogicalDatabaseMetaData#getConnection}.
     *
     * @return A database metadata object.
     * @throws SQLException if an error occurs
     */
    public synchronized DatabaseMetaData getMetaData()
//IC see: https://issues.apache.org/jira/browse/DERBY-3431
            throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        try {
            checkForNullPhysicalConnection();
            // Create metadata object on demand, then cache it for later use.
            if (this.logicalDatabaseMetaData == null) {
                this.logicalDatabaseMetaData = newLogicalDatabaseMetaData();
            }
            return this.logicalDatabaseMetaData;
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Returns a newly created logical database metadata object.
     * <p>
     * Subclasses should override this method to return an instance of the
     * correct implementation class of the logical metadata object.
     *
     * @return A logical database metadata object.
     * @throws java.sql.SQLException on error
     */
    protected LogicalDatabaseMetaData newLogicalDatabaseMetaData()
            throws SQLException {
        return new LogicalDatabaseMetaData(
                                this, physicalConnection_.agent_.logWriter_);
    }

    /**
     * Returns the real underlying database metadata object.
     *
     * @return The metadata object from the underlying physical connection.
     * @throws SQLException if the logical connection has been closed
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    final synchronized DatabaseMetaData getRealMetaDataObject()
            throws SQLException {
        // Check if the logical connection has been closed.
        // isClosed also checks if physicalConnection_ is null.
        if (isClosed()) {
            throw new SqlException(
                    // Log this if we can.
                    this.physicalConnection_ == null ?
                        null :
                        this.physicalConnection_.agent_.logWriter_,
                    new ClientMessageId(SQLState.NO_CURRENT_CONNECTION)
                ).getSQLException();
        }
        return this.physicalConnection_.getMetaData();
    }

    synchronized public void setReadOnly(boolean readOnly) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setReadOnly(readOnly);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public boolean isReadOnly() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.isReadOnly();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    synchronized public void setCatalog(String catalog) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setCatalog(catalog);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public String getCatalog() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getCatalog();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public PreparedStatement prepareStatement(String sql,
                                                                    int resultSetType,
                                                                    int resultSetConcurrency) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public CallableStatement prepareCall(String sql,
                                                               int resultSetType,
                                                               int resultSetConcurrency) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
    synchronized public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getTypeMap();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
    synchronized public void setTypeMap(Map map) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setTypeMap(map);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public Statement createStatement(
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public CallableStatement prepareCall(
            String sql,
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public PreparedStatement prepareStatement(
            String sql,
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareStatement(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public PreparedStatement prepareStatement(
            String sql,
            int autoGeneratedKeys) throws SQLException {

        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareStatement(sql, autoGeneratedKeys);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public PreparedStatement prepareStatement(
            String sql,
            int columnIndexes[]) throws SQLException {

        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareStatement(sql, columnIndexes);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public PreparedStatement prepareStatement(
            String sql,
            String columnNames[]) throws SQLException {

        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.prepareStatement(sql, columnNames);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public void setHoldability(int holdability) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setHoldability(holdability);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public int getHoldability() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getHoldability();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public Savepoint setSavepoint() throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.setSavepoint();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public Savepoint setSavepoint(String name)
            throws SQLException {
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.setSavepoint(name);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public void rollback(Savepoint savepoint) throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.rollback(savepoint);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized public void releaseSavepoint(Savepoint savepoint)
            throws SQLException {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.releaseSavepoint(savepoint);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Returns the client-side transaction id from am.Connection. 
     * <p>
     * <em>NOTE:</em> This method was added for testing purposes. Avoid use in
     * production code if possible.
     *
     * @return transaction id
     **/
    public int getTransactionID() {
        if (physicalConnection_ == null) {
            return -1;
        } else {
            return physicalConnection_.getTransactionID();
        }
    }
    
    //----------------------------------------------------------------------------

    public int getServerVersion() {
        if (physicalConnection_ == null) {
            return -1;
        } else {
            return physicalConnection_.getServerVersion();
        }
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.0 IN JAVA 6
    //
    ////////////////////////////////////////////////////////////////////
    public Array createArrayOf(String typeName, Object[] elements)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createArrayOf( typeName, elements );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public Blob createBlob()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createBlob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public Clob createClob()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public NClob createNClob()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createNClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public SQLXML createSQLXML()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createSQLXML();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createStruct( typeName, attributes );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * <code>getClientInfo</code> forwards to
     * <code>physicalConnection_</code>.
     * <code>getClientInfo</code> always returns an empty
     * <code>Properties</code> object since Derby doesn't support
     * ClientInfoProperties.
     *
     * @return an empty <code>Properties</code> object
     * @exception SQLException if an error occurs
     */
    public Properties getClientInfo()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.getClientInfo();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * <code>getClientInfo</code> forwards to
     * <code>physicalConnection_</code>. Always returns a <code>null
     * String</code> since Derby does not support
     * ClientInfoProperties.
     *
     * @param name a property key to get <code>String</code>
     * @return a property value <code>String</code>
     * @exception SQLException if an error occurs
     */
    public String getClientInfo(String name)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.getClientInfo(name);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Checks if the connection has not been closed and is still valid.
     * The validity is checked by running a simple query against the
     * database.
     *
     * @param timeout The time in seconds to wait for the database
     * operation used to validate the connection to complete. If the
     * timeout period expires before the operation completes, this
     * method returns false. A value of 0 indicates a timeout is not
     * applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the call on the physical connection throws an
     * exception.
     */
    synchronized public boolean isValid(int timeout) throws SQLException {
        try
        {
            // Check if we have a underlying physical connection
            if (physicalConnection_ == null) {
                return false;
            }
            return physicalConnection_.isValid(timeout);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }


    public boolean isWrapperFor(Class<?> interfaces)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return interfaces.isInstance(this);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * <code>setClientInfo</code> forwards to
     * <code>physicalConnection_</code>.
     *
     * @param properties a <code>Properties</code> object with the
     * properties to set
     * @exception SQLClientInfoException if an error occurs
     */
    public void setClientInfo(Properties properties)
        throws SQLClientInfoException {
        try
        {
            checkForNullPhysicalConnection();
            physicalConnection_.setClientInfo(properties);
        } catch (SQLClientInfoException cie) {
            notifyException(cie);
            throw cie;
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw new SQLClientInfoException
            (sqle.getMessage(), sqle.getSQLState(),
                    sqle.getErrorCode(),
                    (new FailedProperties40(properties)).getProperties());
        }
    }

    /**
     * <code>setClientInfo</code> forwards to
     * <code>physicalConnection_</code>.
     *
     * @param name a property key <code>String</code>
     * @param value a property value <code>String</code>
     * @exception SQLClientInfoException if an error occurs
     */
    public void setClientInfo(String name, String value)
        throws SQLClientInfoException {
        try
        {
            checkForNullPhysicalConnection();
            physicalConnection_.setClientInfo(name, value);
        } catch (SQLClientInfoException cie) {
            notifyException(cie);
            throw cie;
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw new SQLClientInfoException
            (sqle.getMessage(), sqle.getSQLState(),
                    sqle.getErrorCode(),
             new FailedProperties40
             (FailedProperties40.makeProperties
              (name,value)).getProperties());
        }
    }

    public <T>T unwrap(Class<T> interfaces)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            // Derby does not implement non-standard methods on JDBC objects
            try {
                return interfaces.cast(this);
            } catch (ClassCastException cce) {
                throw new SqlException(null,
                                       new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                                       interfaces).getSQLException();
            }
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * Get the name of the current schema.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public String   getSchema() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        try {
            checkForNullPhysicalConnection();
            return physicalConnection_.getSchema();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    /**
     * Set the default schema for the Connection.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-5561
    synchronized public void   setSchema(  String schemaName ) throws SQLException
    {
        try {
            checkForNullPhysicalConnection();
            physicalConnection_.setSchema( schemaName );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public  void    abort( Executor executor )  throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        try
        {
            if ( physicalConnection_ != null )
            {
                physicalConnection_.abort( executor );
            }
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public int getNetworkTimeout() throws SQLException
    {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.getNetworkTimeout();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
        try
        {
            checkForNullPhysicalConnection();
            physicalConnection_.setNetworkTimeout( executor, milliseconds );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

}
