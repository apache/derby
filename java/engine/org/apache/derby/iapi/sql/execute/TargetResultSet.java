/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.types.RowLocation;

/**
 * The TargetResultSet interface is used to provide additional
 * operations on result sets that are the target of a bulk insert 
 * or update.  This is useful because bulk insert is upside down -
 * the insert is done via the store.
 *
 * @author jerry
 */
public interface TargetResultSet extends ResultSet
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Pass a changed row and the row location for that row
	 * to the target result set.
	 *
	 * @param execRow		The changed row.
	 * @param rowLocation	The row location of the row.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void changedRow(ExecRow execRow, RowLocation rowLocation) throws StandardException;

	/**
	 * Preprocess the source row prior to getting it back from the source.
	 * This is useful for bulk insert where the store stands between the target and 
	 * the source.
	 *
	 * @param sourceRow	The source row.
	 *
	 * @return The preprocessed source row.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public ExecRow preprocessSourceRow(ExecRow sourceRow) throws StandardException;
}
