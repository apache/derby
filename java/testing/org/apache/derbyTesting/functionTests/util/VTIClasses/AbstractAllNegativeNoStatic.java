/*

   Derby - Class org.apache.derbyTesting.functionTests.util.VTIClasses.AbstractAllNegativeNoStatic

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util.VTIClasses;

import org.apache.derby.vti.VTITemplate;

import java.io.InputStream;

import java.math.BigDecimal;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;


/**
 * An abstract class that implements ResultSet where all methods, including
 * the constructor, throw a SQLException.
 * This class does not implement the optional static getResultSetMetaData()
 * method.
 */
public abstract class AbstractAllNegativeNoStatic extends VTITemplate
{

	public AbstractAllNegativeNoStatic() throws SQLException
	{
		throw new SQLException("constructor", "VTI00");
	}

	// non-public constructor which doesn't throw an exception
	AbstractAllNegativeNoStatic(int bogus) throws SQLException
	{
	}

	// ResultSet interface

	public boolean next() throws SQLException
	{
		throw new SQLException("next", "VTI00");
	}

	public void close() throws SQLException
	{
		// we don't throw an exception here because
		// it masks other exceptions we throw 
		// because close() is always called when
		// cleaning up from an exception
		//throw new SQLException("close", "VTI00");
	}

	public boolean wasNull() throws SQLException
	{
		throw new SQLException("wasNull", "VTI00");
	}

	public String getString(int columnIndex) throws SQLException
	{
		throw new SQLException("getString", "VTI00");
	}

	public boolean getBoolean(int columnIndex) throws SQLException
	{
		throw new SQLException("getBoolean", "VTI00");
	}

	public byte getByte(int columnIndex) throws SQLException
	{
		throw new SQLException("getByte", "VTI00");
	}

	public short getShort(int columnIndex) throws SQLException
	{
		throw new SQLException("getShort", "VTI00");
	}

	public int getInt(int columnIndex) throws SQLException
	{
		throw new SQLException("getInt", "VTI00");
	}

	public long getLong(int columnIndex) throws SQLException
	{
		throw new SQLException("getLong", "VTI00");
	}

	public float getFloat(int columnIndex) throws SQLException
	{
		throw new SQLException("getFloat", "VTI00");
	}

	public double getDouble(int columnIndex) throws SQLException
	{
		throw new SQLException("getDouble", "VTI00");
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
	{
		throw new SQLException("getBigDecimal", "VTI00");
	}

	public byte[] getBytes(int columnIndex) throws SQLException
	{
		throw new SQLException("getBytes", "VTI00");
	}

	public Date getDate(int columnIndex) throws SQLException
	{
		throw new SQLException("getDate", "VTI00");
	}

	public Time getTime(int columnIndex) throws SQLException
	{
		throw new SQLException("getTime", "VTI00");
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		throw new SQLException("getTimestamp", "VTI00");
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		throw new SQLException("getAsciiStream", "VTI00");
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		throw new SQLException("getUnicodeStream", "VTI00");
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		throw new SQLException("getBinaryStream", "VTI00");
	}

	public String getString(String columnName) throws SQLException
	{
		throw new SQLException("getString", "VTI00");
	}

	public boolean getBoolean(String columnName) throws SQLException
	{
		throw new SQLException("getBoolean", "VTI00");
	}

	public byte getByte(String columnName) throws SQLException
	{
		throw new SQLException("getByte", "VTI00");
	}

	public short getShort(String columnName) throws SQLException
	{
		throw new SQLException("getShort", "VTI00");
	}

	public int getInt(String columnName) throws SQLException
	{
		throw new SQLException("getInt", "VTI00");
	}

	public long getLong(String columnName) throws SQLException
	{
		throw new SQLException("getLong", "VTI00");
	}

	public float getFloat(String columnName) throws SQLException
	{
		throw new SQLException("getFloat", "VTI00");
	}

	public double getDouble(String columnName) throws SQLException
	{
		throw new SQLException("getDouble", "VTI00");
	}

	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException
	{
		throw new SQLException("getBigDecimal", "VTI00");
	}

	public byte[] getBytes(String columnName) throws SQLException
	{
		throw new SQLException("getBytes", "VTI00");
	}

	public Date getDate(String columnName) throws SQLException
	{
		throw new SQLException("getDate", "VTI00");
	}

	public Time getTime(String columnName) throws SQLException
	{
		throw new SQLException("getTime", "VTI00");
	}

	public Timestamp getTimestamp(String columnName) throws SQLException
	{
		throw new SQLException("getTimestamp", "VTI00");
	}

	public InputStream getAsciiStream(String columnName) throws SQLException
	{
		throw new SQLException("getAsciiStream", "VTI00");
	}

	public InputStream getUnicodeStream(String columnName) throws SQLException
	{
		throw new SQLException("getUnicodeStream", "VTI00");
	}

	public InputStream getBinaryStream(String columnName) throws SQLException
	{
		throw new SQLException("getBinaryStream", "VTI00");
	}

	public SQLWarning getWarnings() throws SQLException
	{
		throw new SQLException("getWarnings", "VTI00");
	}

	public void clearWarnings() throws SQLException
	{
		throw new SQLException("clearWarnings", "VTI00");
	}

	public String getCursorName() throws SQLException
	{
		throw new SQLException("getCursorName", "VTI00");
	}

	public ResultSetMetaData getMetaData() throws SQLException
	{
		throw new SQLException("getMetaData", "VTI00");
	}

	public Object getObject(int columnIndex) throws SQLException
	{
		throw new SQLException("getObject", "VTI00");
	}

	public Object getObject(String columName) throws SQLException
	{
		throw new SQLException("getObject", "VTI00");
	}

	public int findColumn(String columName) throws SQLException
	{
		throw new SQLException("findColumn", "VTI00");
	}
}
