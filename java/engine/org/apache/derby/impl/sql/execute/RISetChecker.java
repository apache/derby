/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Checks a set or referential integrity constraints.  Used
 * to shield the caller from ReferencedKeyRIChecker and
 * ForeignKeyRICheckers.
 */
public class RISetChecker
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	private GenericRIChecker[] 	checkers;

	/**
	 * @param tc		the xact controller
	 * @param fkInfo[]	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public RISetChecker(TransactionController tc, FKInfo fkInfo[])
		throws StandardException
	{
		if (fkInfo == null)
		{
			return;
		}

		checkers = new GenericRIChecker[fkInfo.length];

		for (int i = 0; i < fkInfo.length; i++)
		{
			checkers[i] = (fkInfo[i].type == FKInfo.FOREIGN_KEY) ?
				(GenericRIChecker)new ForeignKeyRIChecker(tc, fkInfo[i]) :
				(GenericRIChecker)new ReferencedKeyRIChecker(tc, fkInfo[i]);
		}
	}

	/**
	 * Do any work needed to reopen our ri checkers
	 * for another round of checks.  Must do a close()
	 * first.
	 *
	 * @exception StandardException on error
	 */
	void reopen() throws StandardException
	{
		// currently a noop
	}

	/**
	 * Check that there are no referenced primary keys in
	 * the passed in row.  So for each foreign key that
	 * references a primary key constraint, make sure
	 * that there is no row that matches the values in
	 * the passed in row.
	 *
	 * @param row	the row to check
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
	public void doPKCheck(ExecRow row, boolean restrictCheckOnly) throws StandardException
	{
		if (checkers == null)
			return;

		for (int i = 0; i < checkers.length; i++)
		{
			if (checkers[i] instanceof ReferencedKeyRIChecker)
			{
				checkers[i].doCheck(row,restrictCheckOnly);
			}
		}
	}

	/**
	 * Check that everything in the row is ok, i.e.
	 * that there are no foreign keys in the passed
	 * in row that have invalid values.
	 *
	 * @param row	the row to check
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
	public void doFKCheck(ExecRow row) throws StandardException
	{
		if (checkers == null)
			return;

		for (int i = 0; i < checkers.length; i++)
		{
			if (checkers[i] instanceof ForeignKeyRIChecker)
			{
				checkers[i].doCheck(row);
			}
		}
	}

	/**
	 * Execute the specific RI check on the passed in row.
	 *
	 * @param fkIndex	index into fkInfo
	 * @param row		the row to check
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
	public void doRICheck(int index, ExecRow row, boolean restrictCheckOnly) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (checkers == null)
			{
				SanityManager.THROWASSERT("no checkers, how can i execute checker "+index);
			}

			if (index >= checkers.length)
			{
				SanityManager.THROWASSERT("there are only "+
					checkers.length+" checkers, "+index+" is invalid");
			}
		}

		checkers[index].doCheck(row, restrictCheckOnly);
	}

	/**
	 * clean up
	 *
	 * @exception StandardException on error
	 */
	public void close() throws StandardException
	{
		if (checkers == null)
			return;

		for (int i = 0; i < checkers.length; i++)
		{
			checkers[i].close();
		}
	}	
}

