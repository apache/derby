/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

/**
 * This is an extension of ExecRow for use
 * with indexes and sorting.
 *
 * @author jeff after ames
 */
public interface ExecIndexRow extends ExecRow  { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	 * These two methods are a sort of a hack.  The store implements ordered
	 * null semantics for start and stop positioning, which is correct for
	 * IS NULL and incorrect for everything else.  To work around this,
	 * TableScanResultSet will check whether the start and stop positions
	 * have NULL in any column position other than for an IS NULL check.
	 * If so, it won't do the scan (that is, it will return no rows).
	 *
	 * This method is to inform this ExecIndexRow (which can be used for
	 * start and stop positioning) that the given column uses ordered null
	 * semantics.
	 *
	 * @param columnPosition	The position of the column that uses ordered
	 *							null semantics (zero-based).
	 */
	void orderedNulls(int columnPosition);

	/**
	 * Return true if orderedNulls was called on this ExecIndexRow for
	 * the given column position.
	 *
	 * @param columnPosition	The position of the column (zero-based) for
	 *							which we want to check if ordered null semantics
	 *							are used.
	 *
	 * @return	true if we are to use ordered null semantics on the given column
	 */
	boolean areNullsOrdered(int columnPosition);

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 *
	 * @return Nothing.
	 */
	public void execRowToExecIndexRow(ExecRow valueRow);
}
