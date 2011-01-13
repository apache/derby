/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedCallableStatement40

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.derby.iapi.reference.SQLState;

public class EmbedCallableStatement40 extends EmbedCallableStatement30 {
    
        
    /** Creates a new instance of EmbedCallableStatement40 */
    public EmbedCallableStatement40(EmbedConnection conn, String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException{
        super(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);     
    }
    
    public Reader getCharacterStream(String parameterName)
        throws SQLException {
        throw Util.notImplemented();
    }

    public Reader getNCharacterStream(int parameterIndex)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    public Reader getNCharacterStream(String parameterName)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    public String getNString(int parameterIndex)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    public String getNString(String parameterName)
        throws SQLException {
        throw Util.notImplemented();
    }

    public void setBlob(String parameterName, Blob x)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setClob(String parameterName, Clob x)
        throws SQLException {
        throw Util.notImplemented();
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        throw Util.notImplemented();
    }
    
    public RowId getRowId(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw Util.notImplemented();
    }
    
    
    public void setNString(String parameterName, String value)
    throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setNCharacterStream(String parameterName, Reader value, long length)
    throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setClob(String parameterName, Reader reader, long length)
    throws SQLException{
        throw Util.notImplemented();
        
    }
    
    public void setBlob(String parameterName, InputStream inputStream, long length)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public void setNClob(String parameterName, Reader reader, long length)
    throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob getNClob(int i) throws SQLException {
        throw Util.notImplemented();
    }
    
    
    public NClob getNClob(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw Util.notImplemented();
        
    }
    
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML getSQLXML(String parametername) throws SQLException {
        throw Util.notImplemented();
    }
    
    
    
    
    
    /************************************************************************
     * The prepared statement methods
     *************************************************************************/
    
    
    public void setRowId(int parameterIndex, RowId x) throws SQLException{
        throw Util.notImplemented("setRowId(int, RowId)");
    }
    
    public void setNString(int index, String value) throws SQLException{
        throw Util.notImplemented("setNString (int,value)");
    }
    
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        throw Util.notImplemented();
    }

    public void setNCharacterStream(int index, Reader value, long length) throws SQLException{
        throw Util.notImplemented ("setNCharacterStream (int, Reader, long)");
    }
    
    public void setNClob(int index, NClob value) throws SQLException{
        throw Util.notImplemented ("setNClob (int, NClob)");
    }

    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException {
        throw Util.notImplemented();
    }

    public void setNClob(int parameterIndex, Reader reader, long length)
    throws SQLException{
        throw Util.notImplemented ("setNClob(int,reader,length)");
    }    
    
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException{
        throw Util.notImplemented ("setSQLXML (int, SQLXML)");
    }  
    
    /**
    * JDBC 4.0
    *
    * Retrieves the number, types and properties of this CallableStatement
    * object's parameters.
    *
    * @return a ParameterMetaData object that contains information about the
    * number, types and properties of this CallableStatement object's parameters.
    * @exception SQLException if a database access error occurs
    *
    */
    public ParameterMetaData getParameterMetaData()
        throws SQLException
    {
	  checkStatus();
	  return new EmbedParameterMetaData40(
				getParms(), preparedStatement.getParameterTypes());
    }
    
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
        checkStatus();
        return interfaces.isInstance(this);
    }

    public void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        throw Util.notImplemented("setAsciiStream(String,InputStream)");
    }

    public void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        throw Util.notImplemented("setBinaryStream(String,InputStream)");
    }

    public void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        throw Util.notImplemented("setBlob(String,InputStream)");
    }

    public void setCharacterStream(String parameterName, Reader reader)
            throws SQLException {
        throw Util.notImplemented("setCharacterStream(String,Reader)");
    }

    public void setClob(String parameterName, Reader reader)
            throws SQLException {
        throw Util.notImplemented("setClob(String,Reader)");
    }

    public void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        throw Util.notImplemented("setNCharacterStream(String,Reader)");
    }

    public void setNClob(String parameterName, Reader reader)
            throws SQLException {
        throw Util.notImplemented("setNClob(String,Reader)");
    }

    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLException if no object if found that implements the
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces) 
                            throws SQLException{
        checkStatus();
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw newSQLException(SQLState.UNABLE_TO_UNWRAP,interfaces);
        }
    }
    
    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setAsciiStream(String parameterName, InputStream x, long length)
    throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setBinaryStream(String parameterName, InputStream x, long length)
    throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * Sets the designated parameter to the given Reader, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setCharacterStream(String parameterName, Reader x, long length)
    throws SQLException {
        throw Util.notImplemented();
    }
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////
    
    public <T> T getObject( int parameterIndex, Class<T> type )
        throws SQLException
    {
        checkStatus();

        if ( type == null )
        {
            throw mismatchException( "NULL", parameterIndex );
        }

        Object   retval;
            
        if ( String.class.equals( type ) ) { retval = getString( parameterIndex ); }
        else if ( BigDecimal.class.equals( type ) ) { retval = getBigDecimal( parameterIndex ); }
        else if ( Boolean.class.equals( type ) ) { retval = Boolean.valueOf( getBoolean(parameterIndex ) ); }
        else if ( Byte.class.equals( type ) ) { retval = Byte.valueOf( getByte( parameterIndex ) ); }
        else if ( Short.class.equals( type ) ) { retval = Short.valueOf( getShort( parameterIndex ) ); }
        else if ( Integer.class.equals( type ) ) { retval = Integer.valueOf( getInt( parameterIndex ) ); }
        else if ( Long.class.equals( type ) ) { retval = Long.valueOf( getLong( parameterIndex ) ); }
        else if ( Float.class.equals( type ) ) { retval = Float.valueOf( getFloat( parameterIndex ) ); }
        else if ( Double.class.equals( type ) ) { retval = Double.valueOf( getDouble( parameterIndex ) ); }
        else if ( Date.class.equals( type ) ) { retval = getDate( parameterIndex ); }
        else if ( Time.class.equals( type ) ) { retval = getTime( parameterIndex ); }
        else if ( Timestamp.class.equals( type ) ) { retval = getTimestamp( parameterIndex ); }
        else if ( Blob.class.equals( type ) ) { retval = getBlob( parameterIndex ); }
        else if ( Clob.class.equals( type ) ) { retval = getClob( parameterIndex ); }
        else if ( type.isArray() && type.getComponentType().equals( byte.class ) ) { retval = getBytes( parameterIndex ); }
        else { retval = getObject( parameterIndex ); }

        if ( wasNull() ) { retval = null; }

        if ( (retval == null) || (type.isInstance( retval )) ) { return type.cast( retval ); }
        
        throw mismatchException( type.getName(), parameterIndex );
    }
    private SQLException    mismatchException( String targetTypeName, int parameterIndex )
        throws SQLException
    {
        String sourceTypeName = getParameterMetaData().getParameterTypeName( parameterIndex );
        SQLException se = newSQLException( SQLState.LANG_DATA_TYPE_GET_MISMATCH, targetTypeName, sourceTypeName );

        return se;
    }

    public <T> T getObject(String parameterName, Class<T> type)
        throws SQLException
    {
        throw Util.notImplemented();
    }
    
}
