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
import org.apache.derby.shared.common.reference.SQLState;

import java.sql.SQLException;


// A simple delegation wrapper handle for a physical connection.
// All methods are forwarded to the underlying physical connection except for close() and isClosed().
// When a physical connection is wrapped, it is non-null, when the logical connection
// is closed, the wrapped physical connection is always set to null.
// Both the finalizer and close() methods will always set the physical connection to null.
// After the physical conneciton is set to null,
// only the Pooled Connection instance will maintain a handle to the physical connection.

public class LogicalConnection implements java.sql.Connection {
    protected Connection physicalConnection_ = null; // reset to null when the logical connection is closed.
    private org.apache.derby.client.ClientPooledConnection pooledConnection_ = null;

    public LogicalConnection(Connection physicalConnection,
                             org.apache.derby.client.ClientPooledConnection pooledConnection) throws SqlException {
        physicalConnection_ = physicalConnection;
        pooledConnection_ = pooledConnection;
        try {
            checkForNullPhysicalConnection();
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
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

    synchronized public void close() throws SQLException {
        try
        {
            // we also need to loop thru all the logicalStatements and close them
            if (physicalConnection_ == null) {
                return;
            }
            if (physicalConnection_.agent_.loggingEnabled()) {
                physicalConnection_.agent_.logWriter_.traceEntry(this, "close");
            }

            if (physicalConnection_.isClosed()) // connection is closed or has become stale
            {
                pooledConnection_.informListeners(new SqlException(null, 
                    new ClientMessageId(
                        SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED)));
            } else {
                physicalConnection_.closeForReuse(
                        pooledConnection_.isStatementPoolingEnabled());
                if (!physicalConnection_.isGlobalPending_()) {
                    pooledConnection_.recycleConnection();
                }
            }
            physicalConnection_ = null;
            pooledConnection_.nullLogicalConnection();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
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
                    new ClientMessageId(SQLState.NO_CURRENT_CONNECTION)); // no call to informListeners()
            } else {
                ; // no call to recycleConnection()
            }
        } finally {
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

    // this method doesn't wrap in the standard way, because it went out without a throws clause.
    // Unlike all other LogicalConnection methods, if the physical connection is null, it won't throw an exception, but will return false.

    protected void checkForNullPhysicalConnection() throws SQLException {
        if (physicalConnection_ == null) {
            SqlException se = new SqlException(null, 
                new ClientMessageId(SQLState.NO_CURRENT_CONNECTION));
            throw se.getSQLException();
        }
    }

    /**
     * This method checks if the physcial connection underneath is null and
     * if yes, then it simply returns.
     * Otherwise, if the severity of exception is greater than equal to
     * ExceptionSeverity.SESSION_SEVERITY, then we will send 
     * connectionErrorOccurred event to all the registered listeners.
     * 
     * @param sqle SQLException An event will be sent to the listeners if the
     * exception's severity is >= ExceptionSeverity.SESSION_SEVERITY.
     */
	final void notifyException(SQLException sqle) {
        if (physicalConnection_ != null) 
        	pooledConnection_.informListeners(new SqlException(sqle));
	}

    // ---------------------- wrapped public entry points ------------------------
    // All methods are forwarded to the physical connection in a standard way

    synchronized public java.sql.Statement createStatement() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.createStatement();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.prepareStatement(sql);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    synchronized public PreparedStatement preparePositionedUpdateStatement(String sql, Section querySection) throws SqlException {
        try {
            checkForNullPhysicalConnection();
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
        return physicalConnection_.preparePositionedUpdateStatement(sql, querySection);
    }

    synchronized public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.prepareCall(sql);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public String nativeSQL(String sql) throws SQLException {
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

    public boolean getAutoCommit() throws SQLException {
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

    public int getTransactionIsolation() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.getTransactionIsolation();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.SQLWarning getWarnings() throws SQLException {
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

    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.getMetaData();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    synchronized public void setReadOnly(boolean readOnly) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        physicalConnection_.setReadOnly(readOnly);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public boolean isReadOnly() throws SQLException {
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

    public String getCatalog() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.getCatalog();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    synchronized public java.sql.Statement createStatement(int resultSetType,
                                                           int resultSetConcurrency) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.createStatement(resultSetType, resultSetConcurrency);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    synchronized public java.sql.PreparedStatement prepareStatement(String sql,
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

    synchronized public java.sql.CallableStatement prepareCall(String sql,
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

    public java.util.Map getTypeMap() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.getTypeMap();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    synchronized public void setTypeMap(java.util.Map map) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        physicalConnection_.setTypeMap(map);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.CallableStatement prepareCall(String sql, int resultSetType,
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

    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType,
                                                       int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.prepareStatement(sql, resultSetType, resultSetConcurrency,
	                resultSetHoldability);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.prepareStatement(sql, autoGeneratedKeys);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int columnIndexes[])
            throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.prepareStatement(sql, columnIndexes);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.PreparedStatement prepareStatement(String sql, String columnNames[])
            throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.prepareStatement(sql, columnNames);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public void setHoldability(int holdability) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        physicalConnection_.setHoldability(holdability);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public int getHoldability() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.getHoldability();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.Savepoint setSavepoint() throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.setSavepoint();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        return physicalConnection_.setSavepoint(name);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public void rollback(java.sql.Savepoint savepoint) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        physicalConnection_.rollback(savepoint);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
    }

    public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
		try {
	        checkForNullPhysicalConnection();
	        physicalConnection_.releaseSavepoint(savepoint);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
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

}
