/*
 
   Derby - Class org.apache.derby.client.net.NetDatabaseMetaData40
 
   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derby.client.net;

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.apache.derby.client.am.SQLExceptionFactory;

public class NetDatabaseMetaData40 extends org.apache.derby.client.net.NetDatabaseMetaData {
    
    
    public NetDatabaseMetaData40(NetAgent netAgent, NetConnection netConnection) {
        super(netAgent,netConnection);
    }
    
    /**
     * Indicates whether or not this data source supports the SQL
     * <code>ROWID</code> type. Since Derby does not support the
     * <code>ROWID</code> type, return <code>ROWID_UNSUPPORTED</code>.
     *
     * @return <code>ROWID_UNSUPPORTED</code>
     * @exception SQLException if a database access error occurs
     */
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        checkForClosedConnection();
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }
    
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSchemas (String, String)");
    }
    
    public ResultSet getClientInfoProperties()
    throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getClientInfoProperties ()");
    }
}
