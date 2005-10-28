/*

   Derby - Class org.apache.derby.catalog.TriggerOldTransitionRows

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.catalog;

import org.apache.derby.iapi.db.Factory;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.math.BigDecimal;

/**
 * Provides information about a set of rows before a trigger action
 * changed them.
 * 
 *
 * <p>
 * This class implements only JDBC 1.2, not JDBC 2.0.  You cannot
 * compile this class with JDK1.2, since it implements only the
 * JDBC 1.2 ResultSet interface and not the JDBC 2.0 ResultSet
 * interface.  You can only use this class in a JDK 1.2 runtime 
 * environment if no JDBC 2.0 calls are made against it.
 *
 * @author jamie
 */
public class TriggerOldTransitionRows extends org.apache.derby.vti.VTITemplate
{

	private ResultSet resultSet;

	/**
	 * Construct a VTI on the trigger's old row set.
	 * The old row set is the before image of the rows
	 * that are changed by the trigger.  For a trigger
	 * on a delete, this is all the rows that are deleted.	
	 * For a trigger on an update, this is the rows before
	 * they are updated.  For an insert, this throws an 
	 * exception.
	 *
	 * @exception SQLException thrown if no trigger active
	 */
	public TriggerOldTransitionRows() throws SQLException
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		if (tec == null)
		{
			throw new SQLException("There are no active triggers", "38000");
		}
		resultSet = tec.getOldRowSet();

		if (resultSet == null)
		{
			throw new SQLException("There is no old transition rows result set for this trigger", "38000");
		}
    }  

	/**
	 * Provide the metadata for VTI interface.
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	static public ResultSetMetaData getResultSetMetaData() throws SQLException
	{
		throw new SQLException("getResultSetMetaData() should not be called", "38000");
    }

    //
    // java.sql.ResultSet calls, passed through to our result set.
    //

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean next() throws SQLException {
        return resultSet.next();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public void close() throws SQLException {
        resultSet.close();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean wasNull() throws SQLException {
        return resultSet.wasNull();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public String getString(int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean getBoolean(int columnIndex) throws SQLException {
        return resultSet.getBoolean(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte getByte(int columnIndex) throws SQLException {
        return resultSet.getByte(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public short getShort(int columnIndex) throws SQLException {
        return resultSet.getShort(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public int getInt(int columnIndex) throws SQLException {
        return resultSet.getInt(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public long getLong(int columnIndex) throws SQLException {
        return resultSet.getLong(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public float getFloat(int columnIndex) throws SQLException {
        return resultSet.getFloat(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public double getDouble(int columnIndex) throws SQLException {
        return resultSet.getDouble(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnIndex,scale);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte[] getBytes(int columnIndex) throws SQLException {
        return resultSet.getBytes(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        return resultSet.getDate(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        return resultSet.getTime(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        return resultSet.getTimestamp(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException {
        return resultSet.getAsciiStream(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return resultSet.getUnicodeStream(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getBinaryStream(int columnIndex)
        throws SQLException {
            return resultSet.getBinaryStream(columnIndex);
            }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public String getString(String columnName) throws SQLException {
        return resultSet.getString(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean getBoolean(String columnName) throws SQLException {
        return resultSet.getBoolean(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte getByte(String columnName) throws SQLException {
        return resultSet.getByte(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public short getShort(String columnName) throws SQLException {
        return resultSet.getShort(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public int getInt(String columnName) throws SQLException {
        return resultSet.getInt(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public long getLong(String columnName) throws SQLException {
        return resultSet.getLong(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public float getFloat(String columnName) throws SQLException {
        return resultSet.getFloat(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public double getDouble(String columnName) throws SQLException {
        return resultSet.getDouble(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnName,scale);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte[] getBytes(String columnName) throws SQLException {
        return resultSet.getBytes(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Date getDate(String columnName) throws SQLException {
        return resultSet.getDate(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Time getTime(String columnName) throws SQLException {
        return resultSet.getTime(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException {
        return resultSet.getTimestamp(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getAsciiStream(String columnName) throws SQLException {
        return resultSet.getAsciiStream(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException {
        return resultSet.getUnicodeStream(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getBinaryStream(String columnName)
        throws SQLException {
        return resultSet.getBinaryStream(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public SQLWarning getWarnings() throws SQLException {
        return resultSet.getWarnings();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public void clearWarnings() throws SQLException {
        resultSet.clearWarnings();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public String getCursorName() throws SQLException {
        return resultSet.getCursorName();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSet.getMetaData();
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public Object getObject(int columnIndex) throws SQLException {
        return resultSet.getObject(columnIndex);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public Object getObject(String columnName) throws SQLException {
        return resultSet.getObject(columnName);
    }

	/**
	 * @see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
   public int findColumn(String columnName) throws SQLException {
        return resultSet.findColumn(columnName);
    }

}
