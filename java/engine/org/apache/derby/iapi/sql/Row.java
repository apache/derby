/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.error.StandardException;

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
 * @author Jeff Lichtman
 * @see org.apache.derby.iapi.sql.execute.ExecRow
 */

public interface Row
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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
	 *
	 * @return		The DataValueDescriptor, null if no such column exists
	 */
	void	setColumn (int position, DataValueDescriptor value);

}
