/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A CreateStatementNode represents a DDL statement that creates something.
 * It contains the name of the object to be created.
 *
 * @author Jeff Lichtman
 */

public abstract class CreateStatementNode extends DDLStatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	// Synchronization stubs

	/**
	 * Add or drop an item in a given list.
	 *
	 * @param newClause		The item to add or drop
	 * @param listNumber	The identifying number of the list to add to
	 *						or drop from.
	 * @param ddlType		ADD_TYPE or DROP_TYPE
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void alterItem
	(
		QueryTreeNode			newClause,
		int					listNumber,
		int					ddlType
	)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"alterItem() not expected to be called on a " +
				getClass().getName());
		}
	}

	/**
	  Return true if this is a create publication node ad false if it is
	  an alter publication node.
	  */
	public boolean isCreate()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"isCreate() not expected to be called on a " +
				getClass().getName());
		}

		return false;
	}
}
