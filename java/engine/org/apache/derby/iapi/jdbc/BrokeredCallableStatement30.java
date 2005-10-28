/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredCallableStatement30

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.*;
import java.math.BigDecimal;
import java.net.URL;

import java.util.Calendar;
import java.util.Map;


/**
	JDBC 3 brokered CallableStatement
 */
public class BrokeredCallableStatement30 extends BrokeredCallableStatement
{

	public BrokeredCallableStatement30(BrokeredStatementControl control, int jdbcLevel, String sql) throws SQLException {
		super(control, jdbcLevel, sql);
	}
    public final void setURL(java.lang.String parameterName, java.net.URL value) throws SQLException {
		getCallableStatement().setURL(parameterName, value);
	}
    public final void setNull(java.lang.String parameterName, int type) throws SQLException {
		getCallableStatement().setNull(parameterName, type);
	}
    public final void setBoolean(java.lang.String parameterName, boolean value) throws SQLException {
		getCallableStatement().setBoolean(parameterName, value);
	}
    public final void setByte(java.lang.String parameterName, byte value) throws SQLException {
		getCallableStatement().setByte(parameterName, value);
	}
    public final void setShort(java.lang.String parameterName, short value) throws SQLException {
		getCallableStatement().setShort(parameterName, value);
	}
    public final void setInt(java.lang.String parameterName, int value) throws SQLException {
		getCallableStatement().setInt(parameterName, value);
	}
    public final void setLong(java.lang.String parameterName, long value) throws SQLException {
		getCallableStatement().setLong(parameterName, value);
	}
    public final void setFloat(java.lang.String parameterName, float value) throws SQLException {
		getCallableStatement().setFloat(parameterName, value);
	}
    public final void setDouble(java.lang.String parameterName, double value) throws SQLException {
		getCallableStatement().setDouble(parameterName, value);
	}
    public final void setBigDecimal(java.lang.String parameterName, java.math.BigDecimal value) throws SQLException {
		getCallableStatement().setBigDecimal(parameterName, value);
	}
    public final void setString(java.lang.String parameterName, java.lang.String value) throws SQLException {
		getCallableStatement().setString(parameterName, value);
	}
    public final void setBytes(java.lang.String parameterName, byte[] value) throws SQLException {
		getCallableStatement().setBytes(parameterName, value);
	}
    public final void setDate(java.lang.String parameterName, java.sql.Date value) throws SQLException {
		getCallableStatement().setDate(parameterName, value);
	}
    public final void setTime(java.lang.String parameterName, java.sql.Time value) throws SQLException {
		getCallableStatement().setTime(parameterName, value);
	}
    public final void setTimestamp(java.lang.String parameterName, java.sql.Timestamp value) throws SQLException {
		getCallableStatement().setTimestamp(parameterName, value);
	}
    public final void setAsciiStream(java.lang.String parameterName, java.io.InputStream value, int length) throws SQLException {
		getCallableStatement().setAsciiStream(parameterName, value, length);
	}
    public final void setBinaryStream(java.lang.String parameterName, java.io.InputStream value, int length) throws SQLException {
		getCallableStatement().setBinaryStream(parameterName, value, length);
	}
    public final void setObject(java.lang.String parameterName, java.lang.Object value, int a, int b) throws SQLException {
		getCallableStatement().setObject(parameterName, value, a, b);
	}
    public final void setObject(java.lang.String parameterName, java.lang.Object value, int a) throws SQLException {
		getCallableStatement().setObject(parameterName, value, a);
	}
    public final void setObject(java.lang.String parameterName, java.lang.Object value) throws SQLException {
		getCallableStatement().setObject(parameterName, value);
	}
    public final void setCharacterStream(java.lang.String parameterName, java.io.Reader value, int length) throws SQLException {
		getCallableStatement().setCharacterStream(parameterName, value, length);
	}
    public final void setDate(java.lang.String parameterName, java.sql.Date value, java.util.Calendar cal) throws SQLException {
		getCallableStatement().setDate(parameterName, value, cal);
	}
    public final void setTime(java.lang.String parameterName, java.sql.Time value, java.util.Calendar cal) throws SQLException {
		getCallableStatement().setTime(parameterName, value, cal);
	}
    public final void setTimestamp(java.lang.String parameterName, java.sql.Timestamp value, java.util.Calendar cal) throws SQLException {
		getCallableStatement().setTimestamp(parameterName, value, cal);
	}
    public final void setNull(java.lang.String parameterName, int a, java.lang.String b) throws SQLException {
		getCallableStatement().setNull(parameterName, a, b);
	}
    public final java.lang.String getString(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getString(parameterName);
	}
    public final boolean getBoolean(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getBoolean(parameterName);
	}
    public final byte getByte(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getByte(parameterName);
	}
    public final short getShort(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getShort(parameterName);
	}
    public final int getInt(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getInt(parameterName);
	}
    public final long getLong(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getLong(parameterName);
	}
    public final float getFloat(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getFloat(parameterName);
	}
    public final double getDouble(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getDouble(parameterName);
	}
    public final byte[] getBytes(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getBytes(parameterName);
	}
    public final java.sql.Date getDate(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getDate(parameterName);
	}
    public final java.sql.Time getTime(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getTime(parameterName);
	}
    public final java.sql.Timestamp getTimestamp(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getTimestamp(parameterName);
	}
    public final java.lang.Object getObject(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getObject(parameterName);
	}
    public final java.math.BigDecimal getBigDecimal(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getBigDecimal(parameterName);
	}
    public final java.lang.Object getObject(java.lang.String parameterName, java.util.Map map) throws SQLException {
		return getCallableStatement().getObject(parameterName, map);
	}
    public final java.sql.Ref getRef(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getRef(parameterName);
	}
    public final java.sql.Blob getBlob(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getBlob(parameterName);
	}
    public final java.sql.Clob getClob(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getClob(parameterName);
	}
    public final java.sql.Array getArray(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getArray(parameterName);
	}
    public final java.sql.Date getDate(java.lang.String parameterName, java.util.Calendar cal) throws SQLException {
		return getCallableStatement().getDate(parameterName, cal);
	}
    public final java.sql.Time getTime(java.lang.String parameterName, java.util.Calendar cal) throws SQLException {
		return getCallableStatement().getTime(parameterName, cal);
	}
    public final java.sql.Timestamp getTimestamp(java.lang.String parameterName, java.util.Calendar cal) throws SQLException {
		return getCallableStatement().getTimestamp(parameterName, cal);
	}
    public final java.net.URL getURL(java.lang.String parameterName) throws SQLException {
		return getCallableStatement().getURL(parameterName);
	}
    public final java.net.URL getURL(int i) throws SQLException {
		return getCallableStatement().getURL(i);
	}
    public final void registerOutParameter(String a, int b) throws SQLException {
		getCallableStatement().registerOutParameter(a,b);
	}
    public final void registerOutParameter(String a, int b, int c) throws SQLException {
		getCallableStatement().registerOutParameter(a,b,c);
	}
    public final void registerOutParameter(String a, int b, String c) throws SQLException {
		getCallableStatement().registerOutParameter(a,b,c);
	}
	/*
	** JDBC 3.0 PreparedStatement methods
	*/

	public final void setURL(int i, URL x)
        throws SQLException
    {
        getPreparedStatement().setURL( i, x);
    }
    public final ParameterMetaData getParameterMetaData()
        throws SQLException
    {
        return getPreparedStatement().getParameterMetaData();
    }
	/*
	** Control methods
	*/

	/**
		Create a duplicate CalableStatement to this, including state, from the passed in Connection.
	*/
	public CallableStatement createDuplicateStatement(Connection conn, CallableStatement oldStatement) throws SQLException {

		CallableStatement newStatement = conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		setStatementState(oldStatement, newStatement);

		return newStatement;
	}
}
