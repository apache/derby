/*

   Derby - Class org.apache.derby.iapi.sql.ResultDescription

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

/**
 * The ResultDescription interface provides methods to get metadata on the
 * results returned by a statement.
 *
 * @author Jeff Lichtman
 */

public interface ResultDescription
{
	/**
	 * Returns an identifier that tells what type of statement has been
	 * executed. This can be used to determine what other methods to call
	 * to get the results back from a statement. For example, a SELECT
	 * statement returns rows and columns, while other statements don't,
	 * so you would only call getColumnCount() or getColumnType() for
	 * SELECT statements.
	 *
	 * @return	A String identifier telling what type of statement this
	 *		is.
	 */
	String	getStatementType();	

	/**
	 * Returns the number of columns in the result set.
	 *
	 * @return	The number of columns in the result set.
	 */
	int	getColumnCount();

	/**
		Return information about all the columns.
	*/
	public ResultColumnDescriptor[] getColumnInfo();

	/**
	 * Returns a ResultColumnDescriptor for the column, given the oridinal
	 * position of the column.
	 * NOTE - position is 1-based.
	 *
	 * @param position	The oridinal position of a column in the
	 *			ResultSet.
	 *
	 * @return		A ResultColumnDescriptor describing the
	 *			column in the ResultSet.
	 */
	ResultColumnDescriptor	getColumnDescriptor(int position);

	/**
	 * Get a new result description that has been truncated
	 * from input column number.   If the input column is
	 * 5, then columns 5 to getColumnCount() are removed.
	 * The new ResultDescription points to the same
	 * ColumnDescriptors (this method performs a shallow
	 * copy.
	 *
	 * @param truncateFrom the starting column to remove,
	 * 1-based.
	 *
	 * @return a new ResultDescription
	 */
	public ResultDescription truncateColumns(int truncateFrom);
}
