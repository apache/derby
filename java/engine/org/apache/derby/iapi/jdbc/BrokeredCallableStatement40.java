/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredCallableStatement40
 
   Copyright 2005, 2006 The Apache Software Foundation or its licensors, as applicable.
 
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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.impl.jdbc.Util;

public class BrokeredCallableStatement40 extends  BrokeredCallableStatement30{
    
    public BrokeredCallableStatement40(BrokeredStatementControl control, int jdbcLevel, String sql) throws SQLException{
        super(control,jdbcLevel,sql);
    }
    
    public Reader getCharacterStream(int parameterIndex)
        throws SQLException {
        return getCallableStatement().getCharacterStream(parameterIndex);
    }
    
    public Reader getCharacterStream(String parameterName)
        throws SQLException {
        return getCallableStatement().getCharacterStream(parameterName);
    }
    
    public Reader getNCharacterStream(int parameterIndex)
        throws SQLException {
        return getCallableStatement().getNCharacterStream(parameterIndex);
    }
    
    public Reader getNCharacterStream(String parameterName)
        throws SQLException {
        return getCallableStatement().getNCharacterStream(parameterName);
    }

    public String getNString(int parameterIndex)
        throws SQLException {
        return getCallableStatement().getNString(parameterIndex);
    }

    public String getNString(String parameterName)
        throws SQLException {
        return getCallableStatement().getNString(parameterName);
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
    
    public void setBlob(String parameterName, Blob x)
        throws SQLException {
        getCallableStatement().setBlob(parameterName, x);
    }

    public void setClob(String parameterName, Clob x)
        throws SQLException {
        getCallableStatement().setClob(parameterName, x);
    }

    public void setNString(String parameterName, String value)
    throws SQLException{
        getCallableStatement().setNString(parameterName,value);
    }

    public final void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        getCallableStatement().setNCharacterStream(parameterName, value);
    }

    public void setNCharacterStream(String parameterName,Reader value,long length)
    throws SQLException{
        getCallableStatement().setNCharacterStream(parameterName,value,length);
    }
    
    public void setNClob(String parameterName, NClob value) throws SQLException{
        getCallableStatement().setNClob(parameterName,value);
    }

    public final void setClob(String parameterName, Reader reader)
            throws SQLException {
        getCallableStatement().setClob(parameterName, reader);
    }

    public void setClob(String parameterName, Reader reader, long length)
    throws SQLException{
        getCallableStatement().setClob(parameterName,reader,length);
    }

    public final void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        getCallableStatement().setBlob(parameterName, inputStream);
    }

    public void setBlob(String parameterName, InputStream inputStream, long length)
    throws SQLException{
        getCallableStatement().setBlob(parameterName,inputStream,length);
    }

    public final void setNClob(String parameterName, Reader reader)
            throws SQLException {
        getCallableStatement().setNClob(parameterName, reader);
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

    /**
     * Sets the designated parameter to the given input stream.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the Java input stream that contains the ASCII parameter value
     * @throws SQLException if a database access error occurs or this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public final void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException {
        getCallableStatement().setAsciiStream(parameterIndex, x);
    }

    /**
     * Sets the designated parameter to the given input stream.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the binary parameter value
     * @throws SQLException if a database access error occurs or this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public final void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        getCallableStatement().setBinaryStream(parameterIndex, x);
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code> object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader the <code>java.io.Reader</code> object that contains the
     *      Unicode data
     * @throws SQLException if a database access error occurs or this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public final void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException {
        getCallableStatement().setCharacterStream(parameterIndex, reader);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException{
        getPreparedStatement().setRowId(parameterIndex,x);
    }
    
    public void setNString(int index, String value) throws SQLException{
        getPreparedStatement().setNString(index,value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        getCallableStatement().setNCharacterStream(parameterIndex, value);
    }

    public void setNCharacterStream(int index, Reader value, long length) throws SQLException{
        getPreparedStatement().setNCharacterStream(index,value,length);
    }
    
    public void setNClob(int index, NClob value) throws SQLException{
        getPreparedStatement().setNClob(index,value);
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream(int,Reader)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>CLOB</code>.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader an object that contains the data to set the parameter
     *      value to.
     * @throws SQLException if a database access error occurs, this method is
     *      called on a closed PreparedStatement
     */
    public final void setClob(int parameterIndex, Reader reader)
            throws SQLException {
        getCallableStatement().setClob(parameterIndex, reader);
    }

    public void setClob(int parameterIndex, Reader reader, long length)
    throws SQLException{
        getPreparedStatement().setClob(parameterIndex,reader,length);
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * This method differs from the <code>setBinaryStream(int, InputStream)
     * </code>  method because it informs the driver that the parameter value
     * should be sent to the server as a <code>BLOB</code>.
     *
     * @param inputStream an object that contains the data to set the parameter
     *      value to.
     * @throws SQLException if a database access error occurs, this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public final void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException {
        getCallableStatement().setBlob(parameterIndex, inputStream);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length)
    throws SQLException{
        getPreparedStatement().setBlob(parameterIndex,inputStream,length);
    }

    public final void setNClob(int parameterIndex, Reader reader)
            throws SQLException {
        getCallableStatement().setNClob(parameterIndex, reader);
    }

    public void setNClob(int parameterIndex, Reader reader, long length)
    throws SQLException{
        getPreparedStatement().setNClob(parameterIndex,reader,length);
    }
    
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException{
        getPreparedStatement().setSQLXML(parameterIndex,xmlObject);
    }
    
    /**
     * Checks if the statement is closed.
     *
     * @return <code>true</code> if the statement is closed,
     * <code>false</code> otherwise
     * @exception SQLException if an error occurs
     */
    public final boolean isClosed() throws SQLException {
        return getCallableStatement().isClosed();
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
        checkIfClosed();
        //Derby does not implement non-standard methods on 
        //JDBC objects
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    interfaces);
        }
    }
    /** 
     * Forwards to the real CallableStatement.
     * @return true if the underlying CallableStatement is poolable,
     * false otherwise.
     * @throws SQLException if the forwarding call fails.
     */
    public boolean isPoolable() throws SQLException {
        return getStatement().isPoolable();
    }

    /** 
     * Forwards to the real CallableStatement.
     * @param poolable new value for the poolable hint.
     * @throws SQLException if the forwarding call fails.
     */
    public void setPoolable(boolean poolable) throws SQLException {
        getStatement().setPoolable(poolable);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setAsciiStream(int parameterIndex, InputStream x, long length)
    throws SQLException {
        getCallableStatement().setAsciiStream(parameterIndex,x,length);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setBinaryStream(int parameterIndex, InputStream x, long length)
    throws SQLException {
        getCallableStatement().setBinaryStream(parameterIndex,x,length);
    }

    /**
     * Sets the designated parameter to the given Reader, which will have
     * the specified number of bytes.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setCharacterStream(int parameterIndex, Reader x, long length)
    throws SQLException {
        getCallableStatement().setCharacterStream(parameterIndex,x,length);
    }

    public final void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        getCallableStatement().setAsciiStream(parameterName, x);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the parameter 
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setAsciiStream(String parameterName, InputStream x, long length)
    throws SQLException {
        getCallableStatement().setAsciiStream(parameterName,x,length);
    }

    public final void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        getCallableStatement().setBinaryStream(parameterName, x);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setBinaryStream(String parameterName, InputStream x, long length)
    throws SQLException {
        getCallableStatement().setBinaryStream(parameterName,x,length);
    }

    public final void setCharacterStream(String parameterName, Reader x)
            throws SQLException {
        getCallableStatement().setCharacterStream(parameterName, x);
    }

    /**
     * Sets the designated parameter to the given Reader, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the parameter
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setCharacterStream(String parameterName, Reader x, long length)
    throws SQLException {
        getCallableStatement().setCharacterStream(parameterName,x,length);
    }
}
