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

import java.sql.SQLException;
import org.apache.derby.client.net.NetXAConnection;
import org.apache.derby.jdbc.ClientBaseDataSource;
import org.apache.derby.jdbc.ClientDataSource;
import javax.sql.StatementEventListener;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetLogWriter;

public class ClientPooledConnection40 extends ClientPooledConnection {
    
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
    
    public void addStatementEventListener(StatementEventListener listener){
        throw new java.lang.UnsupportedOperationException();
    }
    
    public void removeStatementEventListener(StatementEventListener listener){
        throw new java.lang.UnsupportedOperationException();
    }
}
