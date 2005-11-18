/*
 
   Derby - Class org.apache.derby.impl.jdbc.EmbedConnection40
 
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

package org.apache.derby.impl.jdbc;

import java.sql.Blob;
import java.sql.ClientInfoException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;

import org.apache.derby.jdbc.InternalDriver;

public class EmbedConnection40 extends EmbedConnection30 {
    
    /** Creates a new instance of EmbedConnection40 */
    public EmbedConnection40(EmbedConnection inputConnection) {
        super(inputConnection);
    }
    
    public EmbedConnection40(
        InternalDriver driver,
        String url,
        Properties info)
        throws SQLException {
        super(driver, url, info);
    }
    
    /*
     *-------------------------------------------------------
     * JDBC 4.0
     *-------------------------------------------------------
     */
    
    
    public Clob createClob() throws SQLException {
        throw Util.notImplemented();
    }
    
    public Blob createBlob() throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob createNClob() throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML createSQLXML() throws SQLException {
        throw Util.notImplemented();
    }
    
    public boolean isValid(int timeout) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setClientInfo(String name, String value)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public void setClientInfo(Properties properties)
    throws ClientInfoException {
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
    
    public <T> T createQueryObject(Class<T> ifc) throws SQLException {
        throw Util.notImplemented();
    }
    
    public java.util.Map<String,Class<?>> getTypeMap() {
        throw new java.lang.UnsupportedOperationException();
    }
}
