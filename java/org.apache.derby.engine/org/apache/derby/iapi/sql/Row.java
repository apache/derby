/*

   Derby - Class org.apache.derby.iapi.sql.Row

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.shared.common.error.StandardException;

/**
 * The Row interface provides methods to get information about the columns
 * in a result row.
 * It uses simple, position (1-based) access to get to columns.
 * Searching for columns by name should be done from the ResultSet
 * interface, where metadata about the rows and columns is available.
 * <p>
 *
 * @see ResultSet
 *
 * @see org.apache.derby.iapi.sql.execute.ExecRow
 */

public interface Row
{
	public int nColumns();

	/**
	 * Get a DataValueDescriptor in a Row by ordinal position (1-based).
	 *
	 * @param position	The ordinal position of the column.
	 *
     * @exception   StandardException Thrown on failure.
	 * @return		The DataValueDescriptor, null if no such column exists
	 */
	DataValueDescriptor	getColumn (int position) throws StandardException;

	/**
	 * Set a DataValueDescriptor in a Row by ordinal position (1-based).
	 *
	 * @param position	The ordinal position of the column.
	 */
	void	setColumn (int position, DataValueDescriptor value);

}
