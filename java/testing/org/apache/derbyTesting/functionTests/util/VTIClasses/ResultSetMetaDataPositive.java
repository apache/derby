/*

   Derby - Class org.apache.derbyTesting.functionTests.util.VTIClasses.ResultSetMetaDataPositive

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

import org.apache.derby.vti.VTIMetaDataTemplate;

import java.io.InputStream;

import java.math.BigDecimal;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;


/**
 * A class that implements ResultSetMetaData where all methods that we expect
 * to call during testing return valid results.
 * There are 2 constructors, the 0 argument constructor is for getting the
 * default values, the other for caller specified values.
 */
public class ResultSetMetaDataPositive extends VTIMetaDataTemplate
{

	int			columnCount;
	int[]		nullable;
	String[]	columnName;
	int[]		precision;
	int[]		scale;
	int[]		columnType;
	int[]		columnDisplaySize;

	ResultSetMetaDataPositive() throws SQLException
	{
		this(0, (int[]) null, (String[]) null, (int[]) null,
			 (int[]) null, (int[]) null, (int[]) null);
	}

	ResultSetMetaDataPositive(int columnCount) throws SQLException
	{
		this(columnCount, (int[]) null, (String[]) null, (int[]) null,
			 (int[]) null, (int[]) null, (int[]) null);
	}

	ResultSetMetaDataPositive(int columnCount, int[] nullable,
							  String[] columnName, int[] precision,
							  int[] scale, int[] columnType,
							  int[] columnDisplaySize)
	{
		this.columnCount = columnCount;
		this.nullable = nullable;
		this.columnName = columnName;
		this.precision = precision;
		this.scale = scale;
		this.columnType = columnType;
		this.columnDisplaySize = columnDisplaySize;
	}

	// Methods on the interface that we expect to call

	public int getColumnCount() throws SQLException
	{
		return columnCount;
	}

	public int isNullable(int column) throws SQLException
	{
		return nullable[column - 1];
	}

	public String getColumnName(int column) throws SQLException
	{
		return columnName[column - 1];
	}

	public int getPrecision(int column) throws SQLException
	{
		return precision[column - 1];
	}

	public int getScale(int column) throws SQLException
	{
		return scale[column - 1];
	}

	public int getColumnType(int column) throws SQLException
	{
		return columnType[column - 1];
	}

	public int getColumnDisplaySize(int column) throws SQLException
	{
		return columnDisplaySize[column - 1];
	}

	// Methods on the interface that we don't expect to be called.

	public boolean isAutoIncrement(int column) throws SQLException
	{
		throw new SQLException("isAutoIncrement", "VTI01");
	}

	public boolean isCaseSensitive(int column) throws SQLException
	{
		throw new SQLException("isCaseSensitive", "VTI01");
	}

	public boolean isCurrency(int column) throws SQLException
	{
		throw new SQLException("isCurrency", "VTI01");
	}

	public boolean isSigned(int column) throws SQLException
	{
		throw new SQLException("isSigned", "VTI01");
	}

	public String getColumnLabel(int column) throws SQLException
	{
		throw new SQLException("getColumnLabel", "VTI01");
	}

	public String getSchemaName(int column) throws SQLException
	{
		throw new SQLException("getSchemaName", "VTI01");
	}

	public String getTableName(int column) throws SQLException
	{
		throw new SQLException("getTableName", "VTI01");
	}

	public String getCatalogName(int column) throws SQLException
	{
		throw new SQLException("getCatalogName", "VTI01");
	}

	public String getColumnTypeName(int column) throws SQLException
	{
		throw new SQLException("getColumnTypeName", "VTI01");
	}

	public boolean isReadOnly(int column) throws SQLException
	{
		throw new SQLException("isReadOnly", "VTI01");
	}

	public boolean isWritable(int column) throws SQLException
	{
		throw new SQLException("isWritable", "VTI01");
	}

	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		throw new SQLException("isDefinitelyWritable", "VTI01");
	}

	public boolean isSearchable(int column) throws SQLException
	{
		throw new SQLException("isSearchable", "VTI01");
	}
}
