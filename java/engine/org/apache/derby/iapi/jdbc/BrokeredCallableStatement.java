/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredCallableStatement

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

package org.apache.derby.iapi.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.math.BigDecimal;

import java.util.Calendar;
import java.util.Map;


/**
 * Brokered CallableStatement.
 * This class implements the JDBC 4.1 interface.
 */
public class BrokeredCallableStatement extends BrokeredPreparedStatement
          implements CallableStatement
{

	public BrokeredCallableStatement(BrokeredStatementControl control, String sql) throws SQLException {
        super(control, sql, null);
	}

    // JDBC 2.0 methods

    public final void registerOutParameter(int parameterIndex,
                                     int sqlType)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType);
    }

    public final void registerOutParameter(int parameterIndex,
                                     int sqlType,
                                     int scale)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType, scale);
    }

    public final boolean wasNull()
        throws SQLException
    {
        return getCallableStatement().wasNull();
    }

    @Override
    public final void close() throws SQLException {
        control.closeRealCallableStatement();
    }
    
    public final String getString(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getString( parameterIndex);
    }

    public final boolean getBoolean(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBoolean( parameterIndex);
    }

    public final byte getByte(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getByte( parameterIndex);
    }

    public final short getShort(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getShort( parameterIndex);
    }

    public final int getInt(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getInt( parameterIndex);
    }

    public final long getLong(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getLong( parameterIndex);
    }

    public final float getFloat(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getFloat( parameterIndex);
    }

    public final double getDouble(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getDouble( parameterIndex);
    }

    /** @deprecated */
    public final BigDecimal getBigDecimal(int parameterIndex,
                                              int scale)
        throws SQLException
    {
        return getCallableStatement().getBigDecimal( parameterIndex, scale);
    }

    public final byte[] getBytes(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBytes( parameterIndex);
    }

    public final Date getDate(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getDate( parameterIndex);
    }

    public final Date getDate(int parameterIndex,
                        Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getDate( parameterIndex, cal);
    }

    public final Time getTime(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getTime( parameterIndex);
    }

    public final Timestamp getTimestamp(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getTimestamp( parameterIndex);
    }

    public final Object getObject(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getObject( parameterIndex);
    }

    public final BigDecimal getBigDecimal(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBigDecimal( parameterIndex);
    }

    public final Object getObject(int i, Map<String, Class<?>> map)
        throws SQLException
    {
        return getCallableStatement().getObject( i, map);
    }

    public final Ref getRef(int i)
        throws SQLException
    {
        return getCallableStatement().getRef( i);
    }

    public final Blob getBlob(int i)
        throws SQLException
    {
        return getCallableStatement().getBlob( i);
    }

    public final Clob getClob(int i)
        throws SQLException
    {
        return getCallableStatement().getClob( i);
    }

    public final Array getArray(int i)
        throws SQLException
    {
        return getCallableStatement().getArray( i);
    }

    public final Time getTime(int parameterIndex,
                        Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getTime( parameterIndex, cal);
    }

    public final Timestamp getTimestamp(int parameterIndex,
                                  Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getTimestamp( parameterIndex, cal);
    }

    public final void registerOutParameter(int paramIndex,
                                     int sqlType,
                                     String typeName)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( paramIndex, sqlType, typeName);
    }

    // JDBC 3.0 methods

    public final void setURL(String parameterName, java.net.URL value) throws SQLException {
        getCallableStatement().setURL(parameterName, value);
    }

    public final void setNull(String parameterName, int type) throws SQLException {
        getCallableStatement().setNull(parameterName, type);
    }

    public final void setBoolean(String parameterName, boolean value) throws SQLException {
        getCallableStatement().setBoolean(parameterName, value);
    }

    public final void setByte(String parameterName, byte value) throws SQLException {
        getCallableStatement().setByte(parameterName, value);
    }

    public final void setShort(String parameterName, short value) throws SQLException {
        getCallableStatement().setShort(parameterName, value);
    }

    public final void setInt(String parameterName, int value) throws SQLException {
        getCallableStatement().setInt(parameterName, value);
    }

    public final void setLong(String parameterName, long value) throws SQLException {
        getCallableStatement().setLong(parameterName, value);
    }

    public final void setFloat(String parameterName, float value) throws SQLException {
        getCallableStatement().setFloat(parameterName, value);
    }

    public final void setDouble(String parameterName, double value) throws SQLException {
        getCallableStatement().setDouble(parameterName, value);
    }

    public final void setBigDecimal(String parameterName, BigDecimal value) throws SQLException {
        getCallableStatement().setBigDecimal(parameterName, value);
    }

    public final void setString(String parameterName, String value) throws SQLException {
        getCallableStatement().setString(parameterName, value);
    }

    public final void setBytes(String parameterName, byte[] value) throws SQLException {
        getCallableStatement().setBytes(parameterName, value);
    }

    public final void setDate(String parameterName, Date value) throws SQLException {
        getCallableStatement().setDate(parameterName, value);
    }

    public final void setTime(String parameterName, Time value) throws SQLException {
        getCallableStatement().setTime(parameterName, value);
    }

    public final void setTimestamp(String parameterName, Timestamp value) throws SQLException {
        getCallableStatement().setTimestamp(parameterName, value);
    }

    public final void setAsciiStream(String parameterName, java.io.InputStream value, int length) throws SQLException {
        getCallableStatement().setAsciiStream(parameterName, value, length);
    }

    public final void setBinaryStream(String parameterName, java.io.InputStream value, int length) throws SQLException {
        getCallableStatement().setBinaryStream(parameterName, value, length);
    }

    public final void setObject(String parameterName, Object value, int a, int b) throws SQLException {
        getCallableStatement().setObject(parameterName, value, a, b);
    }

    public final void setObject(String parameterName, Object value, int a) throws SQLException {
        getCallableStatement().setObject(parameterName, value, a);
    }

    public final void setObject(String parameterName, Object value) throws SQLException {
        getCallableStatement().setObject(parameterName, value);
    }

    public final void setCharacterStream(String parameterName, java.io.Reader value, int length) throws SQLException {
        getCallableStatement().setCharacterStream(parameterName, value, length);
    }

    public final void setDate(String parameterName, Date value, Calendar cal) throws SQLException {
        getCallableStatement().setDate(parameterName, value, cal);
    }

    public final void setTime(String parameterName, Time value, Calendar cal) throws SQLException {
        getCallableStatement().setTime(parameterName, value, cal);
    }

    public final void setTimestamp(String parameterName, Timestamp value, Calendar cal) throws SQLException {
        getCallableStatement().setTimestamp(parameterName, value, cal);
    }

    public final void setNull(String parameterName, int a, String b) throws SQLException {
        getCallableStatement().setNull(parameterName, a, b);
    }

    public final String getString(String parameterName) throws SQLException {
        return getCallableStatement().getString(parameterName);
    }

    public final boolean getBoolean(String parameterName) throws SQLException {
        return getCallableStatement().getBoolean(parameterName);
    }

    public final byte getByte(String parameterName) throws SQLException {
        return getCallableStatement().getByte(parameterName);
    }

    public final short getShort(String parameterName) throws SQLException {
        return getCallableStatement().getShort(parameterName);
    }

    public final int getInt(String parameterName) throws SQLException {
        return getCallableStatement().getInt(parameterName);
    }

    public final long getLong(String parameterName) throws SQLException {
        return getCallableStatement().getLong(parameterName);
    }

    public final float getFloat(String parameterName) throws SQLException {
        return getCallableStatement().getFloat(parameterName);
    }

    public final double getDouble(String parameterName) throws SQLException {
        return getCallableStatement().getDouble(parameterName);
    }

    public final byte[] getBytes(String parameterName) throws SQLException {
        return getCallableStatement().getBytes(parameterName);
    }

    public final Date getDate(String parameterName) throws SQLException {
        return getCallableStatement().getDate(parameterName);
    }

    public final Time getTime(String parameterName) throws SQLException {
        return getCallableStatement().getTime(parameterName);
    }

    public final Timestamp getTimestamp(String parameterName) throws SQLException {
        return getCallableStatement().getTimestamp(parameterName);
    }

    public final Object getObject(String parameterName) throws SQLException {
        return getCallableStatement().getObject(parameterName);
    }

    public final BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getCallableStatement().getBigDecimal(parameterName);
    }

    public final Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getCallableStatement().getObject(parameterName, map);
    }

    public final Ref getRef(String parameterName) throws SQLException {
        return getCallableStatement().getRef(parameterName);
    }

    public final Blob getBlob(String parameterName) throws SQLException {
        return getCallableStatement().getBlob(parameterName);
    }

    public final Clob getClob(String parameterName) throws SQLException {
        return getCallableStatement().getClob(parameterName);
    }

    public final Array getArray(String parameterName) throws SQLException {
        return getCallableStatement().getArray(parameterName);
    }

    public final Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getCallableStatement().getDate(parameterName, cal);
    }

    public final Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getCallableStatement().getTime(parameterName, cal);
    }

    public final Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getCallableStatement().getTimestamp(parameterName, cal);
    }

    public final java.net.URL getURL(String parameterName) throws SQLException {
        return getCallableStatement().getURL(parameterName);
    }

    public final java.net.URL getURL(int i) throws SQLException {
        return getCallableStatement().getURL(i);
    }

    public final void registerOutParameter(String a, int b) throws SQLException {
        getCallableStatement().registerOutParameter(a, b);
    }

    public final void registerOutParameter(String a, int b, int c) throws SQLException {
        getCallableStatement().registerOutParameter(a, b, c);
    }

    public final void registerOutParameter(String a, int b, String c) throws SQLException {
        getCallableStatement().registerOutParameter(a, b, c);
    }

    // JDBC 4.0 methods

    public final Reader getCharacterStream(int parameterIndex)
            throws SQLException {
        return getCallableStatement().getCharacterStream(parameterIndex);
    }

    public final Reader getCharacterStream(String parameterName)
            throws SQLException {
        return getCallableStatement().getCharacterStream(parameterName);
    }

    public final Reader getNCharacterStream(int parameterIndex)
            throws SQLException {
        return getCallableStatement().getNCharacterStream(parameterIndex);
    }

    public final Reader getNCharacterStream(String parameterName)
            throws SQLException {
        return getCallableStatement().getNCharacterStream(parameterName);
    }

    public final String getNString(int parameterIndex)
            throws SQLException {
        return getCallableStatement().getNString(parameterIndex);
    }

    public final String getNString(String parameterName)
            throws SQLException {
        return getCallableStatement().getNString(parameterName);
    }

    public final RowId getRowId(int parameterIndex) throws SQLException {
        return getCallableStatement().getRowId(parameterIndex);
    }

    public final RowId getRowId(String parameterName) throws SQLException {
        return getCallableStatement().getRowId(parameterName);
    }

    public final void setRowId(String parameterName, RowId x)
            throws SQLException {
        getCallableStatement().setRowId(parameterName, x);
    }

    public final void setBlob(String parameterName, Blob x)
            throws SQLException {
        getCallableStatement().setBlob(parameterName, x);
    }

    public final void setClob(String parameterName, Clob x)
            throws SQLException {
        getCallableStatement().setClob(parameterName, x);
    }

    public final void setNString(String parameterName, String value)
            throws SQLException {
        getCallableStatement().setNString(parameterName, value);
    }

    public final void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        getCallableStatement().setNCharacterStream(parameterName, value);
    }

    public final void setNCharacterStream(String parameterName, Reader value,
                                          long length)
            throws SQLException {
        getCallableStatement().setNCharacterStream(
                parameterName, value, length);
    }

    public final void setNClob(String parameterName, NClob value)
            throws SQLException {
        getCallableStatement().setNClob(parameterName, value);
    }

    public final void setClob(String parameterName, Reader reader)
            throws SQLException {
        getCallableStatement().setClob(parameterName, reader);
    }

    public final void setClob(String parameterName, Reader reader, long length)
            throws SQLException {
        getCallableStatement().setClob(parameterName, reader, length);
    }

    public final void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        getCallableStatement().setBlob(parameterName, inputStream);
    }

    public final void setBlob(String parameterName, InputStream inputStream,
                              long length)
            throws SQLException {
        getCallableStatement().setBlob(parameterName, inputStream, length);
    }

    public final void setNClob(String parameterName, Reader reader)
            throws SQLException {
        getCallableStatement().setNClob(parameterName, reader);
    }

    public final void setNClob(String parameterName, Reader reader, long length)
            throws SQLException {
        getCallableStatement().setNClob(parameterName, reader, length);
    }

    public NClob getNClob(int i) throws SQLException {
        return getCallableStatement().getNClob(i);
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return getCallableStatement().getNClob(parameterName);
    }

    public final void setSQLXML(String parameterName, SQLXML xmlObject)
            throws SQLException {
        getCallableStatement().setSQLXML(parameterName, xmlObject);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return getCallableStatement().getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parametername) throws SQLException {
        return getCallableStatement().getSQLXML(parametername);
    }

    public final void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        getCallableStatement().setAsciiStream(parameterName, x);
    }

    public final void setAsciiStream(String parameterName, InputStream x,
                                     long length)
            throws SQLException {
        getCallableStatement().setAsciiStream(parameterName, x, length);
    }

    public final void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        getCallableStatement().setBinaryStream(parameterName, x);
    }

    public final void setBinaryStream(String parameterName, InputStream x,
                                      long length)
            throws SQLException {
        getCallableStatement().setBinaryStream(parameterName, x, length);
    }

    public final void setCharacterStream(String parameterName, Reader x)
            throws SQLException {
        getCallableStatement().setCharacterStream(parameterName, x);
    }

    public final void setCharacterStream(String parameterName, Reader x,
                                         long length)
            throws SQLException {
        getCallableStatement().setCharacterStream(parameterName, x, length);
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////
    public final <T> T getObject(int parameterIndex, Class<T> type)
            throws SQLException {
        return ((EngineCallableStatement) getCallableStatement())
                .getObject(parameterIndex, type);
    }

    public final <T> T getObject(String parameterName, Class<T> type)
            throws SQLException {
        return ((EngineCallableStatement) getCallableStatement())
                .getObject(parameterName, type);
    }

	/*
	** Control methods
	*/

    /**
     * Access the underlying CallableStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    final CallableStatement getCallableStatement() throws SQLException {
		return control.getRealCallableStatement();
	}
	
    /**
     * Access the underlying PreparedStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    @Override
    final PreparedStatement getPreparedStatement() throws SQLException {
		return getCallableStatement();
	}
	/**
		Create a duplicate CalableStatement to this, including state, from the passed in Connection.
	*/
	public CallableStatement createDuplicateStatement(Connection conn, CallableStatement oldStatement) throws SQLException {

        CallableStatement newStatement = conn.prepareCall(
            sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		setStatementState(oldStatement, newStatement);

		return newStatement;
	}
}
