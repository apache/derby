/*

   Derby - Class org.apache.derby.client.ClientXAConnection40

   Copyright (c) 2006 The Apache Software Foundation or its licensors, where applicable.

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
import javax.sql.StatementEventListener;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetLogWriter;
import org.apache.derby.client.net.NetXAConnection;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.ClientXADataSource;

/**
 * jdbc4.0 implementation of XAConnection
 */ 
public class ClientXAConnection40 extends ClientXAConnection {
    
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
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }
}
