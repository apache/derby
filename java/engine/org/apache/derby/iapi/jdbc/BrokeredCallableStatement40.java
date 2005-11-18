/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredCallableStatement40
 
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

import java.io.Reader;
import java.io.InputStream;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;

public class BrokeredCallableStatement40 extends  BrokeredCallableStatement30{
    
    public BrokeredCallableStatement40(BrokeredStatementControl control, int jdbcLevel, String sql) throws SQLException{
        super(control,jdbcLevel,sql);
    }
    
    public RowId getRowId(int parameterIndex) throws SQLException{
        return getCallableStatement().getRowId(parameterIndex);
    }
    
    public RowId getRowId(String parameterName) throws SQLException{
        return getCallableStatement().getRowId(parameterName);
    }
    
    public void setRowId(String parameterName, RowId x) throws SQLException{
        getCallableStatement().setRowId(parameterName,x);
    }
    
    
    public void setNString(String parameterName, String value)
    throws SQLException{
        getCallableStatement().setNString(parameterName,value);
    }
    
    public void setNCharacterStream(String parameterName,Reader value,long length)
    throws SQLException{
        getCallableStatement().setNCharacterStream(parameterName,value,length);
    }
    
    public void setNClob(String parameterName, NClob value) throws SQLException{
        getCallableStatement().setNClob(parameterName,value);
    }
    
    public void setClob(String parameterName, Reader reader, long length)
    throws SQLException{
        getCallableStatement().setClob(parameterName,reader,length);
    }
    
    public void setBlob(String parameterName, InputStream inputStream, long length)
    throws SQLException{
        getCallableStatement().setBlob(parameterName,inputStream,length);
    }
    
    public void setNClob(String parameterName, Reader reader, long length)
    throws SQLException{
        getCallableStatement().setNClob(parameterName,reader,length);
    }
    
    public NClob getNClob(int i) throws SQLException{
        return getCallableStatement().getNClob(i);
    }
    
    public NClob getNClob(String parameterName) throws SQLException{
        return getCallableStatement().getNClob(parameterName);
    }
    
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException{
        getCallableStatement().setSQLXML(parameterName,xmlObject);
    }
    
    public SQLXML getSQLXML(int parameterIndex) throws SQLException{
        return getCallableStatement().getSQLXML(parameterIndex);
    }
    
    public SQLXML getSQLXML(String parametername) throws SQLException{
        return getCallableStatement().getSQLXML(parametername);
    }
    
    /************************************************************************
     *PreparedStatement40 methods
     *************************************************************************/
    public void setRowId(int parameterIndex, RowId x) throws SQLException{
        getPreparedStatement().setRowId(parameterIndex,x);
    }
    
    public void setNString(int index, String value) throws SQLException{
        getPreparedStatement().setNString(index,value);
    }
    
    public void setNCharacterStream(int index, Reader value, long length) throws SQLException{
        getPreparedStatement().setNCharacterStream(index,value,length);
    }
    
    public void setNClob(int index, NClob value) throws SQLException{
        getPreparedStatement().setNClob(index,value);
    }
    
    public void setClob(int parameterIndex, Reader reader, long length)
    throws SQLException{
        getPreparedStatement().setClob(parameterIndex,reader,length);
    }
    
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
    throws SQLException{
        getPreparedStatement().setBlob(parameterIndex,inputStream,length);
    }
    public void setNClob(int parameterIndex, Reader reader, long length)
    throws SQLException{
        getPreparedStatement().setNClob(parameterIndex,reader,length);
    }
    
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException{
        getPreparedStatement().setSQLXML(parameterIndex,xmlObject);
    }
    
    public void setPoolable(boolean poolable)
    throws SQLException{
        getPreparedStatement().setPoolable(poolable);
    }
    
    public boolean isPoolable()
    throws SQLException{
        return getPreparedStatement().isPoolable();
    }
    
    public java.lang.Object unwrap(java.lang.Class<?> interfaces) throws SQLException{
        return getPreparedStatement().unwrap(interfaces);
    }
    
    public boolean isWrapperFor(java.lang.Class<?> interfaces) throws java.sql.SQLException{
        return getPreparedStatement().isWrapperFor(interfaces);
    }
    
}
