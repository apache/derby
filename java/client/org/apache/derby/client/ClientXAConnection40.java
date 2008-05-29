/*

   Derby - Class org.apache.derby.client.ClientXAConnection40

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
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import org.apache.derby.jdbc.ClientXADataSource;

/**
 * jdbc4.0 implementation of XAConnection
 */ 
public class ClientXAConnection40 extends ClientXAConnection {
    
    /**
     * List of statement event listeners. The list is copied on each write,
     * ensuring that it can be safely iterated over even if other threads or
     * the listeners fired in the same thread add or remove listeners.
     */
    private final CopyOnWriteArrayList<StatementEventListener>
            statementEventListeners =
                     new CopyOnWriteArrayList<StatementEventListener>();
    
    /**
     * Constructor for ClientXAConnection40.
     * @param ds 
     * @param logWtr 
     * @param userId 
     * @param password 
     */
    public ClientXAConnection40 (ClientXADataSource ds,
                              org.apache.derby.client.net.NetLogWriter logWtr,
                              String userId,
                              String password) throws SQLException {
        super(ds, logWtr, userId, password);
    }
    
    
    /**
     * Removes the specified <code>StatementEventListener</code> from the list of
     * components that will be notified when the driver detects that a
     * <code>PreparedStatement</code> has been closed or is invalid.
     * <p>
     *
     * @param listener	the component which implements the
     * <code>StatementEventListener</code> interface that was previously
     * registered with this <code>PooledConnection</code> object
     * <p>
     */
    public void removeStatementEventListener(StatementEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "removeConnectionEventListener", listener);
        }
        statementEventListeners.remove(listener);
    }
    
    /**
     * Registers a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.  Components that
     * wish to be notified when  <code>PreparedStatement</code>s created by the
     * connection are closed or are detected to be invalid may use this method
     * to register a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.
     * <p>
     *
     * @param listener	an component which implements the 
     *      <code>StatementEventListener</code> interface that is to be 
     *      registered with this <code>PooledConnection</code> object
     * <p>
     */
    public void addStatementEventListener(StatementEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "addStatementEventListener", listener);
        }
        if (listener != null) {
            statementEventListeners.add(listener);
        }
    }
    
    /**
     * Raise the statementClosed event for all the listeners when the 
     * corresponding events occurs
     * @param statement The PreparedStatement that was closed
     */
    public void onStatementClose(PreparedStatement statement) {
        if (!statementEventListeners.isEmpty()) {
            StatementEvent event = new StatementEvent(this,statement);
            for (StatementEventListener l : statementEventListeners) {
                l.statementClosed(event);
            }
        }
    }
    
    /**
     *
     * Raise the statementErrorOccurred event for all the listeners when the 
     * corresponding events occurs.
     *
     * @param statement The PreparedStatement on which error occurred
     * @param sqle      The SQLException associated with the error that
     *                  caused the invalidation of the PreparedStatements
     *
     */
    public void onStatementErrorOccurred(PreparedStatement statement,
                    SQLException sqle) {
        if (!statementEventListeners.isEmpty()) {
            StatementEvent event = new StatementEvent(this,statement,sqle);
            for (StatementEventListener l : statementEventListeners) {
                l.statementErrorOccurred(event);
            }
        }
    }   
}
