/*

   Derby - Class org.apache.derby.iapi.sql.ResultColumnDescriptor

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * A ResultColumnDescriptor describes a result column in a ResultSet.
 *
 * @author Jeff Lichtman
 */

public interface ResultColumnDescriptor
{
	/**
	 * Returns a DataTypeDescriptor for the column. This DataTypeDescriptor
	 * will not represent an actual value, it will only represent the type
	 * that all values in the column will have.
	 *
	 * @return	A DataTypeDescriptor describing the type of the column.
	 */
	DataTypeDescriptor	getType();

	/**
	 * Returns the name of the Column.
	 *
	 * @return	A String containing the name of the column.
	 */
	String	getName();

	/**
	 * Get the name of the schema the Column is in, if any.
	 *
	 * @return	A String containing the name of the schema the Column
	 *		is in.  If the column is not in a schema (i.e. is a
	 * 		derived column), it returns NULL.
	 */
	String	getSchemaName();

	/**
	 * Get the name of the table the Column is in, if any.
	 *
	 * @return	A String containing the name of the table the Column
	 *		is in. If the column is not in a table (i.e. is a
	 * 		derived column), it returns NULL.
	 */
	String	getSourceTableName();

	/**
	 * Get the position of the Column.
	 * NOTE - position is 1-based.
	 *
	 * @return	An int containing the position of the Column
	 *		within the table.
	 */
	int	getColumnPosition();

	/**
	 * Tell us if the column is an autoincrement column or not.
	 * 
	 * @return TRUE, if the column is a base column of a table and is an
	 * autoincrement column.
	 */
	boolean isAutoincrement();

	/*
	 * NOTE: These interfaces are intended to support JDBC. There are some
	 * JDBC methods on java.sql.ResultSetMetaData that have no equivalent
	 * here, mainly because they are of questionable use to us.  They are:
	 * getCatalogName() (will we support catalogs?), getColumnLabel(),
	 * isCaseSensitive(), isCurrency(),
	 * isDefinitelyWritable(), isReadOnly(), isSearchable(), isSigned(),
	 * isWritable()). The JDBC driver implements these itself, using
	 * the data type information and knowing data type characteristics.
	 */
}
