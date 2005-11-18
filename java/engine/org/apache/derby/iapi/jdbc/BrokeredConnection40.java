/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnection40
 
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

package org.apache.derby.iapi.jdbc;

import java.sql.Clob;
import java.sql.ClientInfoException;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import org.apache.derby.impl.jdbc.Util;


public class BrokeredConnection40 extends BrokeredConnection30 {
    
    /** Creates a new instance of BrokeredConnection40 */
    public BrokeredConnection40(BrokeredConnectionControl control) {
        super(control);
    }
    
    public Clob createClob() throws SQLException{
        throw Util.notImplemented();
    }
    
    
    public NClob createNClob() throws SQLException{
        throw Util.notImplemented();
    }
    
    public SQLXML createSQLXML() throws SQLException{
        throw Util.notImplemented();
    }
    
    
    public boolean isValid(int timeout) throws SQLException{
        throw Util.notImplemented();
    }
    
    
    public void setClientInfo(String name, String value)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public void setClientInfo(Properties properties)
    throws ClientInfoException{
        SQLException temp= Util.notImplemented();
        ClientInfoException clientInfoException = new ClientInfoException
            (temp.getMessage(),temp.getSQLState(),(Properties) null);
        throw clientInfoException;
    }
    
    public String getClientInfo(String name)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public Properties getClientInfo()
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public <T> T createQueryObject(Class<T> ifc) throws SQLException{
        throw Util.notImplemented();
    }
    
    public BrokeredPreparedStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql, Object generatedKeys) throws SQLException {
        return new BrokeredPreparedStatement40(statementControl, getJDBCLevel(), sql, generatedKeys);
    }
    public BrokeredCallableStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql) throws SQLException {
        return new BrokeredCallableStatement40(statementControl, getJDBCLevel(), sql);
    }
    
    public java.util.Map<String,Class<?>> getTypeMap() throws SQLException {
        throw Util.notImplemented();
    }
    
    protected int getJDBCLevel() { return 4;}
    
    
}
