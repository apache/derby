/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.SystemColumn

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

package org.apache.derby.iapi.sql.dictionary;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

/**
 * Implements the description of a column in a system table.
 *
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public interface SystemColumn
{
	/**
	 * Gets the name of this column.
	 *
	 * @return	The column name.
	 */
	public String	getName();

	/**
	 * Gets the id of this column.
	 *
	 * @return	The column id.
	 */
	public int	getID();

	/**
	 * Gets the precision of this column.
	 *
	 * @return	The precision of data stored in this column.
	 */
	public int	getPrecision();

	/**
	 * Gets the scale of this column.
	 *
	 * @return	The scale of data stored in this column.
	 */
	public int	getScale();

	/**
	 * Gets the nullability of this column.
	 *
	 * @return	True if this column is nullable. False otherwise.
	 */
	public boolean	getNullability();

	/**
	 * Gets the datatype of this column.
	 *
	 * @return	The datatype of this column.
	 */
	public String	getDataType();

	/**
	 * Is it a built-in type?
	 *
	 * @return	True if it's a built-in type.
	 */
	public boolean	builtInType();

	/**
	 * Gets the maximum length of this column.
	 *
	 * @return	The maximum length of data stored in this column.
	 */
	public int	getMaxLength();
}

