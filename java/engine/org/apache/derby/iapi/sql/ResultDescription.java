/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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
