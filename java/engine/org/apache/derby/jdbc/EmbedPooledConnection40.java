/*

   Derby - Class org.apache.derby.jdbc.EmbedPooledConnection40

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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Vector;
import java.sql.PreparedStatement;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

/** 
	A PooledConnection object is a connection object that provides hooks for
	connection pool management.

	<P>This is Derby's implementation of a PooledConnection for use in
	the following environments:
	<UL>
	<LI> JDBC 4.0 - J2SE 6.0
	</UL>

 */
class EmbedPooledConnection40 extends EmbedPooledConnection {
    
    //using generics to avoid casting problems
    protected final Vector<StatementEventListener> statementEventListeners =
            new Vector<StatementEventListener>();
    

    EmbedPooledConnection40 (ReferenceableDataSource ds, String user, 
                 String password, boolean requestPassword) throws SQLException {
        super (ds, user, password, requestPassword);
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
        statementEventListeners.removeElement(listener);
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
        statementEventListeners.addElement(listener);
    }
    
    /**
     * Raise the statementClosed event for all the listeners when the
     * corresponding events occurs
     * @param statement PreparedStatement
     */
    public void onStatementClose(PreparedStatement statement) {
        if (!statementEventListeners.isEmpty()){
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
     * Raise the statementErrorOccurred event for all the listeners when the
     * corresponding events occurs
     * @param statement PreparedStatement
     * @param sqle      SQLException
     */
    public void onStatementErrorOccurred(PreparedStatement statement,SQLException sqle) {
        if (!statementEventListeners.isEmpty()){
            StatementEvent event = new StatementEvent(this,statement,sqle);
            //synchronized block on statementEventListeners to make it thread
            //safe
            synchronized(statementEventListeners) {
                for (StatementEventListener l : statementEventListeners){
                    l.statementErrorOccurred(event);
                }
            }
        }
    }
}
