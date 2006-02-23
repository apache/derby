/*

   Derby - Class org.apache.derby.client.ClientPooledConnection

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
package org.apache.derby.client;

import java.sql.SQLException;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.client.am.ClientJDBCObjectFactory;
import org.apache.derby.client.am.MessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetLogWriter;
import org.apache.derby.shared.common.reference.SQLState;

public class ClientPooledConnection implements javax.sql.PooledConnection {
    private boolean newPC_ = true;

    private java.util.Vector listeners_ = null;
    org.apache.derby.client.am.Connection physicalConnection_ = null;
    org.apache.derby.client.net.NetConnection netPhysicalConnection_ = null;
    org.apache.derby.client.net.NetXAConnection netXAPhysicalConnection_ = null;

    org.apache.derby.client.am.LogicalConnection logicalConnection_ = null;

    protected org.apache.derby.client.am.LogWriter logWriter_ = null;

    protected int rmId_ = 0;

    // Cached stuff from constructor
    private ClientDataSource ds_;
    private String user_;
    private String password_;

    // Constructor for Non-XA pooled connections.
    // Using standard Java APIs, a CPDS is passed in.
    // user/password overrides anything on the ds.
    public ClientPooledConnection(ClientDataSource ds,
                                  org.apache.derby.client.am.LogWriter logWriter,
                                  String user,
                                  String password) throws SQLException {
        try
        {
            logWriter_ = logWriter;
            ds_ = ds;
            user_ = user;
            password_ = password;
            listeners_ = new java.util.Vector();
            
            netPhysicalConnection_ = (org.apache.derby.client.net.NetConnection)
            ClientDriver.getFactory().newNetConnection(
                    (NetLogWriter) logWriter_,
                    user,
                    password,
                    ds,
                    -1,
                    false);
        
        physicalConnection_ = netPhysicalConnection_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Constructor for XA pooled connections only.
    // Using standard Java APIs, a CPDS is passed in.
    // user/password overrides anything on the ds.
    public ClientPooledConnection(ClientDataSource ds,
                                  org.apache.derby.client.am.LogWriter logWriter,
                                  String user,
                                  String password,
                                  int rmId) throws SQLException {
        try {
            logWriter_ = logWriter;
            ds_ = ds;
            user_ = user;
            password_ = password;
            rmId_ = rmId;
            listeners_ = new java.util.Vector();
            netXAPhysicalConnection_ = new org.apache.derby.client.net.NetXAConnection((NetLogWriter) logWriter_,
                    user,
                    password,
                    ds,
                    rmId,
                    true);
            physicalConnection_ = netXAPhysicalConnection_;
        } catch ( SqlException se ) {
            throw se.getSQLException();
        }
    }

    public ClientPooledConnection(ClientDataSource ds,
                                  org.apache.derby.client.am.LogWriter logWriter) throws SQLException {
        logWriter_ = logWriter;
        ds_ = ds;
        listeners_ = new java.util.Vector();
	try {
            netPhysicalConnection_ = (org.apache.derby.client.net.NetConnection)
            ClientDriver.getFactory().newNetConnection(
                    (NetLogWriter) logWriter_,
                    null,
                    null,
                    ds,
                    -1,
                    false);

            physicalConnection_ = netPhysicalConnection_;
        }
        catch (SqlException se)
        {
            throw se.getSQLException();
        }
    }

    protected void finalize() throws java.lang.Throwable {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "finalize");
        }
        close();
    }

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

    // This is the standard API for getting a logical connection handle for a pooled connection.
    // No "resettable" properties are passed, so user, password, and all other properties may not change.
    public synchronized java.sql.Connection getConnection() throws SQLException {
        try
        {
            if (logWriter_ != null) {
                logWriter_.traceEntry(this, "getConnection");
            }
            createLogicalConnection();

            if (!newPC_) {
                physicalConnection_.reset(logWriter_, user_, password_, ds_, false); // false means do not recompute
            }
            // properties from the dataSource
            // properties don't change
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

    private void createLogicalConnection() throws SqlException {
        if (physicalConnection_ == null) {
            throw new SqlException(logWriter_,
                new MessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION));
        }
        // Not the usual case, but if we have an existing logical connection, then we must close it by spec.
        // We close the logical connection without notifying the pool manager that this pooled connection is availabe for reuse.
        if (logicalConnection_ != null) {
            logicalConnection_.closeWithoutRecyclingToPool();
        }
        logicalConnection_ = new org.apache.derby.client.am.LogicalConnection(physicalConnection_, this);
    }

    public synchronized void addConnectionEventListener(javax.sql.ConnectionEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "addConnectionEventListener", listener);
        }
        listeners_.addElement(listener);
    }

    public synchronized void removeConnectionEventListener(javax.sql.ConnectionEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "removeConnectionEventListener", listener);
        }
        listeners_.removeElement(listener);
    }

    // Not public, but needs to be visible to am.LogicalConnection
    public void recycleConnection() {
        if (physicalConnection_.agent_.loggingEnabled()) {
            physicalConnection_.agent_.logWriter_.traceEntry(this, "recycleConnection");
        }

        for (java.util.Enumeration e = listeners_.elements(); e.hasMoreElements();) {
            javax.sql.ConnectionEventListener listener = (javax.sql.ConnectionEventListener) e.nextElement();
            javax.sql.ConnectionEvent event = new javax.sql.ConnectionEvent(this);
            listener.connectionClosed(event);
        }
    }

    // Not public, but needs to be visible to am.LogicalConnection
    public void trashConnection(SqlException exception) {
        for (java.util.Enumeration e = listeners_.elements(); e.hasMoreElements();) {
            javax.sql.ConnectionEventListener listener = (javax.sql.ConnectionEventListener) e.nextElement();
            java.sql.SQLException sqle = exception.getSQLException();
            javax.sql.ConnectionEvent event = new javax.sql.ConnectionEvent(this, sqle);
            listener.connectionErrorOccurred(event);
        }
    }

    // Used by LogicalConnection close when it disassociates itself from the ClientPooledConnection
    public synchronized void nullLogicalConnection() {
        logicalConnection_ = null;
    }
}
