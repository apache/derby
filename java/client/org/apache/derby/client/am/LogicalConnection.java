/*

   Derby - Class org.apache.derby.client.am.LogicalConnection

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


// A simple delegation wrapper handle for a physical connection.
// All methods are forwarded to the underlying physical connection except for close() and isClosed().
// When a physical connection is wrapped, it is non-null, when the logical connection
// is closed, the wrapped physical connection is always set to null.
// Both the finalizer and close() methods will always set the physical connection to null.
// After the physical conneciton is set to null,
// only the Pooled Connection instance will maintain a handle to the physical connection.

public class LogicalConnection implements java.sql.Connection {
    private Connection physicalConnection_ = null; // reset to null when the logical connection is closed.
    private org.apache.derby.client.ClientPooledConnection pooledConnection_ = null;

    public LogicalConnection(Connection physicalConnection,
                             org.apache.derby.client.ClientPooledConnection pooledConnection) throws SqlException {
        physicalConnection_ = physicalConnection;
        pooledConnection_ = pooledConnection;
        checkForNullPhysicalConnection();
    }

    protected void finalize() throws java.lang.Throwable {
        close();
    }

    // Used by ClientPooledConnection close when it disassociates itself from the LogicalConnection
    synchronized public void nullPhysicalConnection() {
        physicalConnection_ = null;
    }

    // ------------------------ logical connection close -------------------------
    // All methods are simply forwarded to the physical connection, except for close() and isClosed().

    synchronized public void close() throws SqlException {
        // we also need to loop thru all the logicalStatements and close them
        if (physicalConnection_ == null) {
            return;
        }
        if (physicalConnection_.agent_.loggingEnabled()) {
            physicalConnection_.agent_.logWriter_.traceEntry(this, "close");
        }

        if (physicalConnection_.isClosed()) // connection is closed or has become stale
        {
            pooledConnection_.trashConnection(new SqlException(null, "Connection is stale."));
        } else {
            physicalConnection_.closeForReuse();
            if (!physicalConnection_.isGlobalPending_()) {
                pooledConnection_.recycleConnection();
            }
        }
        physicalConnection_ = null;
        pooledConnection_.nullLogicalConnection();
    }

    synchronized public void closeWithoutRecyclingToPool() throws SqlException {
        if (physicalConnection_ == null) {
            return;
        }
        physicalConnection_.checkForTransactionInProgress();
        try {
            if (physicalConnection_.isClosed()) // connection is closed or has become stale
            {
                throw new SqlException(null, "Connection is stale."); // no call to trashConnection()
            } else {
                ; // no call to recycleConnection()
            }
        } finally {
            physicalConnection_.closeForReuse();  //poolfix
            physicalConnection_ = null;
        }
    }

    public boolean isClosed() throws SqlException {
        if (physicalConnection_ == null) {
            return true;
        }
        return physicalConnection_.isClosed();
    }

    // --------------------------- helper methods --------------------------------

    // this method doesn't wrap in the standard way, because it went out without a throws clause.
    // Unlike all other LogicalConnection methods, if the physical connection is null, it won't throw an exception, but will return false.

    private void checkForNullPhysicalConnection() throws SqlException {
        if (physicalConnection_ == null) {
            throw new SqlException(null, " Attempt to use a closed connection. ");
        }
    }

    // ---------------------- wrapped public entry points ------------------------
    // All methods are forwarded to the physical connection in a standard way

    synchronized public java.sql.Statement createStatement() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.createStatement();
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareStatement(sql);
    }

    synchronized public PreparedStatement preparePositionedUpdateStatement(String sql, Section querySection) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.preparePositionedUpdateStatement(sql, querySection);
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareCall(sql);
    }

    public String nativeSQL(String sql) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.nativeSQL(sql);
    }

    synchronized public void setAutoCommit(boolean autoCommit) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getAutoCommit();
    }

    synchronized public void commit() throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.commit();
    }

    synchronized public void rollback() throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.rollback();
    }

    synchronized public void setTransactionIsolation(int level) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getTransactionIsolation();
    }

    public java.sql.SQLWarning getWarnings() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getWarnings();
    }

    synchronized public void clearWarnings() throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.clearWarnings();
    }

    public java.sql.DatabaseMetaData getMetaData() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getMetaData();
    }

    synchronized public void setReadOnly(boolean readOnly) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.isReadOnly();
    }

    synchronized public void setCatalog(String catalog) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.setCatalog(catalog);
    }

    public String getCatalog() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getCatalog();
    }

    synchronized public java.sql.Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.createStatement(resultSetType, resultSetConcurrency);
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql,
                                                                    int resultSetType,
                                                                    int resultSetConcurrency) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql,
                                                               int resultSetType,
                                                               int resultSetConcurrency) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public java.util.Map getTypeMap() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getTypeMap();
    }

    synchronized public void setTypeMap(java.util.Map map) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.setTypeMap(map);
    }

    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public java.sql.CallableStatement prepareCall(String sql, int resultSetType,
                                                  int resultSetConcurrency,
                                                  int resultSetHoldability) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType,
                                                       int resultSetConcurrency, int resultSetHoldability)
            throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareStatement(sql, autoGeneratedKeys);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int columnIndexes[])
            throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareStatement(sql, columnIndexes);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, String columnNames[])
            throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.prepareStatement(sql, columnNames);
    }

    public void setHoldability(int holdability) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.setHoldability(holdability);
    }

    public int getHoldability() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.getHoldability();
    }

    public java.sql.Savepoint setSavepoint() throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.setSavepoint();
    }

    public java.sql.Savepoint setSavepoint(String name) throws SqlException {
        checkForNullPhysicalConnection();
        return physicalConnection_.setSavepoint(name);
    }

    public void rollback(java.sql.Savepoint savepoint) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.rollback(savepoint);
    }

    public void releaseSavepoint(java.sql.Savepoint savepoint) throws SqlException {
        checkForNullPhysicalConnection();
        physicalConnection_.releaseSavepoint(savepoint);
    }

    //----------------------------------------------------------------------------

    public int getServerVersion() {
        if (physicalConnection_ == null) {
            return -1;
        } else {
            return physicalConnection_.getServerVersion();
        }
    }

}
