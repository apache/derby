/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * This is a class that is used to temporarily
 * (non-persistently) hold rows that are used in
 * language execution.  It will store them in an
 * array, or a temporary conglomerate, depending
 * on the number of rows.  
 * <p>
 * It is used for deferred DML processing.
 *
 * @author jamie
 */
public interface TemporaryRowHolder
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Insert a row
	 *
	 * @param row the row to insert 
	 *
	 * @exception StandardException on error
 	 */
	public void insert(ExecRow inputRow)
		throws StandardException;

	/**
	 * Get a result set for scanning what has been inserted
 	 * so far.
	 *
	 * @return a result set to use
	 */
	public CursorResultSet getResultSet();

	/**
	 * Clean up
	 *
	 * @exception StandardException on error
	 */
	public void close() throws StandardException;


	//returns the conglomerate number it created
	public long getTemporaryConglomId();

	//return the conglom id of the position index it maintains
	public long getPositionIndexConglomId();

	//sets the type of the temporary row holder to unique stream
	public void setRowHolderTypeToUniqueStream();

}
