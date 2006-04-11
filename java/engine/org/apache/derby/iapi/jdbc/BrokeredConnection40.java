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
import org.apache.derby.iapi.reference.SQLState;


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
    
    /**
     * returns an instance of JDBC4.0 speccific class BrokeredStatement40
     * @param  statementControl BrokeredStatementControl
     * @return an instance of BrokeredStatement40 
     * throws java.sql.SQLException
     */
    public BrokeredStatement newBrokeredStatement
            (BrokeredStatementControl statementControl) throws SQLException {
		return new BrokeredStatement40(statementControl, getJDBCLevel());
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
    
    int getJDBCLevel() { return 4;}
    
    /**
     * Returns false unless <code>interfaces</code> is implemented 
     * 
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or 
     *                                directly or indirectly wraps an object 
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining 
     *                                whether this is a wrapper for an object 
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        return interfaces.isInstance(this);
    }
    
    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLExption if no object if found that implements the 
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces) 
                            throws SQLException{
        //Derby does not implement non-standard methods on 
        //JDBC objects
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    interfaces);
        }
    }
    
}
