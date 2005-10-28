/*

   Derby - Class org.apache.derby.vti.UpdatableVTITemplate

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.vti;

import org.apache.derby.iapi.reference.JDBC20Translation;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Ref;
import java.sql.Clob;
import java.sql.Array;

import java.io.Reader;

import java.util.Calendar;

import java.io.InputStream;

/**

   An abstract implementation of PreparedStatement (JDK1.1/JDBC 1.2) that is useful
	when writing a read-write (updatable) virtual table interface (VTI).
	
	This class implements
	the methods of the JDBC1.2 version of PreparedStatement plus the 
	JDBC2.0 getMetaData() method, each one throwing a SQLException
	with the name of the method. A concrete subclass can then just implement
	the methods not implemented here and override any methods it needs
	to implement for correct functionality.

 */
public abstract class UpdatableVTITemplate implements PreparedStatement 
{

    //
    // java.sql.Statement calls, passed through to our preparedStatement.
    //
	/**
		@deprecated Cloudscape 5.1 no longer supports read-write VTI's with JDBC 1.2. Applications
		can use the UpdatableVTITemplate to implement a read-write VTI.
	*/
	public UpdatableVTITemplate() {}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet executeQuery(String sql) throws SQLException
	{
        throw new SQLException("executeQuery");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int executeUpdate(String sql) throws SQLException
	{
        throw new SQLException("executeUpdate");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() throws SQLException
	{
        throw new SQLException("close");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public SQLWarning getWarnings() throws SQLException
	{
        throw new SQLException("getWarnings");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void clearWarnings() throws SQLException
	{
        throw new SQLException("clearWarnings");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getMaxFieldSize() throws SQLException
	{
        throw new SQLException("getMaxFieldSize");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setMaxFieldSize(int max) throws SQLException
	{
        throw new SQLException("setMaxFieldSize");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getMaxRows() throws SQLException
	{
        throw new SQLException("getMaxRows");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setMaxRows(int max) throws SQLException
	{
        throw new SQLException("setMaxRows");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
        throw new SQLException("setEscapeProcessing");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getQueryTimeout() throws SQLException
	{
        throw new SQLException("getQueryTimeout");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setQueryTimeout(int seconds) throws SQLException
	{
        throw new SQLException("setQueryTimeout");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void addBatch(String sql) throws SQLException
	{
        throw new SQLException("addBatch");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void clearBatch() throws SQLException
	{
        throw new SQLException("clearBatch");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int[] executeBatch() throws SQLException
	{
        throw new SQLException("executeBatch");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void cancel() throws SQLException
	{
        throw new SQLException("cancel");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setCursorName(String name) throws SQLException
	{
        throw new SQLException("setCursorName");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean execute(String sql) throws SQLException
	{
        throw new SQLException("execute");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet getResultSet() throws SQLException
	{
        throw new SQLException("getResultSet");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getUpdateCount() throws SQLException
	{
        throw new SQLException("getUpdateCount");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean getMoreResults() throws SQLException
	{
        throw new SQLException("getMoreResults");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getResultSetConcurrency() throws SQLException
	{
        return JDBC20Translation.CONCUR_UPDATABLE;
	}

    //
    // java.sql.PreparedStatement calls, passed through to our preparedStatement.
    //

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet executeQuery() throws SQLException
	{
        throw new SQLException("executeQuery");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int executeUpdate() throws SQLException
	{
        throw new SQLException("executeUpdate");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setNull(int parameterIndex, int jdbcType) throws SQLException
	{
        throw new SQLException("setNull");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setNull(int parameterIndex, int jdbcType, String typeName) throws SQLException
	{
        throw new SQLException("setNull");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
        throw new SQLException("setBoolean");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
        throw new SQLException("setByte");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setShort(int parameterIndex, short x) throws SQLException
	{
        throw new SQLException("setShort");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setInt(int parameterIndex, int x) throws SQLException
	{
        throw new SQLException("setInt");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setLong(int parameterIndex, long x) throws SQLException
	{
        throw new SQLException("setLong");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
        throw new SQLException("setFloat");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
        throw new SQLException("setDouble");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
        throw new SQLException("setBigDecimal");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setString(int parameterIndex, String x) throws SQLException
	{
        throw new SQLException("setString");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBytes(int parameterIndex, byte x[]) throws SQLException
	{
        throw new SQLException("setBytes");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
        throw new SQLException("setDate");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
        throw new SQLException("setTime");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
        throw new SQLException("setTimestamp");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
        throw new SQLException("setAsciiStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
        throw new SQLException("setUnicodeStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
        throw new SQLException("setBinaryStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void clearParameters() throws SQLException
	{
        throw new SQLException("clearParameters");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setObject(int parameterIndex, Object x, int targetJdbcType, int scale) throws SQLException
	{
        throw new SQLException("setObject");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setObject(int parameterIndex, Object x, int targetJdbcType) throws SQLException
	{
        throw new SQLException("setObject");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
        throw new SQLException("setObject");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean execute() throws SQLException
	{
        throw new SQLException("execute");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
        throw new SQLException("ResultSetMetaData");
	}
	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public int getResultSetType() throws SQLException {
		throw new SQLException("getResultSetType");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setBlob(int i, Blob x) throws SQLException {
		throw new SQLException("setBlob");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("setFetchDirection");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLException("setFetchSize");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void addBatch() throws SQLException {
		throw new SQLException("addBatch");
	}


	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setCharacterStream(int parameterIndex,
									Reader reader,
									int length) throws SQLException {
		throw new SQLException("setCharacterStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public Connection getConnection() throws SQLException {
		throw new SQLException("getConnection");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public int getFetchDirection() throws SQLException {
		throw new SQLException("getFetchDirection");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
								throws SQLException {
		throw new SQLException("setTime");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
								throws SQLException {
		throw new SQLException("setTimestamp");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public int getFetchSize() throws SQLException {
		throw new SQLException("getFetchSize");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setRef(int i, Ref x) throws SQLException {
		throw new SQLException("setRef");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
						throws SQLException {
		throw new SQLException("setDate");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setClob(int i, Clob x) throws SQLException {
		throw new SQLException("setClob");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setArray(int i, Array x) throws SQLException {
		throw new SQLException("setArray");
	}
}
