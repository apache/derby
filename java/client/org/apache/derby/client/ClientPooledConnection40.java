/*
 
   Derby - Class org.apache.derby.client.ClientPooledConnection40
 
   Copyright (c) 2005 The Apache Software Foundation or its licensors, where applicable.
 
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Vector;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.StatementEvent;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetXAConnection;
import org.apache.derby.jdbc.ClientBaseDataSource;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetLogWriter;

/**
 *
 * The class extends from the ClientPooledConnection class 
 * and contains implementations for the JDBC 4.0 specific 
 * methods in the javax.sql.PooledConnection interface.
 *
 */

public class ClientPooledConnection40 extends ClientPooledConnection {
    //using generics to avoid casting problems
     protected final Vector<StatementEventListener> statementEventListeners = 
             new Vector<StatementEventListener>();

    public ClientPooledConnection40(ClientBaseDataSource ds,
        org.apache.derby.client.am.LogWriter logWriter,
        String user,
        String password) throws SQLException {
        super(ds,logWriter,user,password);
        
    }
    
    
    public ClientPooledConnection40(ClientBaseDataSource ds,
        org.apache.derby.client.am.LogWriter logWriter,
        String user,
        String password,
        int rmId) throws SQLException {
        super(ds,logWriter,user,password,rmId);
        
    }
    
    public ClientPooledConnection40(ClientBaseDataSource ds,
        org.apache.derby.client.am.LogWriter logWriter) throws SQLException {
        super(ds,logWriter);
    }
    
     /**
     *
     * Registers a StatementEventListener with this PooledConnection object. 
     * Components that wish to be informed of events associated with the 
     * PreparedStatement object created by this PooledConnection like the close 
     * or error occurred event can register a StatementEventListener with this 
     * PooledConnection object.
     *
     * @param  listener A component that implements the StatementEventListener
     *                  interface and wants to be notified of Statement closed or 
     *                  or Statement error occurred events
     */
    public void addStatementEventListener(StatementEventListener listener){
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "addStatementEventListener", listener);
        }
        statementEventListeners.addElement(listener);
    }
    
    /**
     *
     * Removes the specified previously registered listener object from the list
     * of components that would be informed of events with a PreparedStatement 
     * object.
     * 
     * @param listener The previously registered event listener that needs to be
     *                 removed from the list of components
     */
    public void removeStatementEventListener(StatementEventListener listener){
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "removeConnectionEventListener", listener);
        }
        statementEventListeners.removeElement(listener);
    }
    
    /**
     *
     * Raise the statementClosed event for all the listeners when the 
     * corresponding events occurs.
     *
     * @param statement The PreparedStatement that was closed
     *
     */
    public void onStatementClose(PreparedStatement statement) {
        if (!statementEventListeners.isEmpty()) {
            StatementEvent event = new StatementEvent(this,statement);
            //synchronized block on statementEventListeners to make it thread
            //safe
            synchronized(statementEventListeners) {
                for (StatementEventListener l : statementEventListeners) {
                    l.statementClosed(event);
                }
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
            //synchronized block on statementEventListeners to make it thread
            //safe
            synchronized(statementEventListeners) {
                for (StatementEventListener l : statementEventListeners) {
                    l.statementErrorOccurred(event);
                }
            }
        }
    }   
}
