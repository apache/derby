/*

   Derby - Class org.apache.derby.jdbc.EmbedPooledConnection40

   Copyright 2001, 2005 The Apache Software Foundation or its licensors, 
 as applicable.

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

package org.apache.derby.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.StatementEventListener;

/**
 * This class supports jdbc4.0 javax.sql.PooledConnection
 * older methods are inherited from EmbedPooledConnection
 */
class EmbedPooledConnection40 extends EmbedPooledConnection {

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
        throw new UnsupportedOperationException (
                "addStatementEventListener(StatementEventListener listener)");
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
        throw new UnsupportedOperationException (
                "addStatementEventListener(StatementEventListener listener)");
    }
}
