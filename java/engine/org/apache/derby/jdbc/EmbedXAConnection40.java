/*
 
   Derby - class org.apache.derby.jdbc.EmbedXAConnection40
 
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


package org.apache.derby.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import org.apache.derby.iapi.jdbc.ResourceAdapter;

/**
 * This class implements jdbc4.0 methods of XAConnection
 */
final class EmbedXAConnection40 extends EmbedXAConnection
        implements XAConnection {
    
    /**
     * List of statement event listeners. The list is copied on each write,
     * ensuring that it can be safely iterated over even if other threads or
     * the listeners fired in the same thread add or remove listeners.
     */
    private final CopyOnWriteArrayList<StatementEventListener>
            statementEventListeners =
                    new CopyOnWriteArrayList<StatementEventListener>();
    
    /**
     * Creates EmbedXAConnection40.
     * @param ds 
     * @param ra 
     * @param user 
     * @param password 
     * @param requestPassword 
     */
    	EmbedXAConnection40 (EmbeddedDataSource ds, ResourceAdapter ra, 
                String user, String password, 
                boolean requestPassword) throws SQLException {
		super(ds, ra, user, password, requestPassword);
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
     * @since 1.6
     */
    public void removeStatementEventListener(StatementEventListener listener) {
        if (listener == null)
            return;
        statementEventListeners.remove(listener);
    }

    /**
     * Registers a <code>StatementEventListener</code> with this 
     * <code>PooledConnection</code> object.  Components that 
     * wish to be notified when  <code>PreparedStatement</code>s created by the
     * connection are closed or are detected to be invalid may use this method 
     * to register a <code>StatementEventListener</code> with this 
     * <code>PooledConnection</code> object.
     * <p>
     * 
     * @param listener	an component which implements the 
     * <code>StatementEventListener</code> interface that is to be registered
     * with this <code>PooledConnection</code> object
     * <p>
     * @since 1.6
     */
    public void addStatementEventListener(StatementEventListener listener) {
         if (!isActive)
            return;
        if (listener == null)
            return;
        statementEventListeners.add(listener);
    }
    
    /**
     * Raise the statementClosed event for all the listeners when the
     * corresponding events occurs
     * @param statement PreparedStatement
     */
    public void onStatementClose(PreparedStatement statement) {
        if (!statementEventListeners.isEmpty()){
            StatementEvent event = new StatementEvent(this,statement);
            for (StatementEventListener l : statementEventListeners) {
                l.statementClosed(event);
            }
        }
    }
    
    /**
     * Raise the statementErrorOccurred event for all the listeners when the
     * corresponding events occurs
     * @param statement PreparedStatement
     * @param sqle      SQLException
     */
    public void onStatementErrorOccurred(PreparedStatement statement,SQLException sqle) {
        if (!statementEventListeners.isEmpty()){
            StatementEvent event = new StatementEvent(this,statement,sqle);
            for (StatementEventListener l : statementEventListeners) {
                l.statementErrorOccurred(event);
            }
        }
    }
   
}