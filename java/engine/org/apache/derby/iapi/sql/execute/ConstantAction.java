/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.catalog.UUID;

/**
 *	This interface describes actions that are ALWAYS performed for a
 *	Statement at Execution time. For instance, it is used for DDL
 *	statements to describe what they should stuff into the catalogs.
 *	<p>
 *	An object satisfying this interface is put into the PreparedStatement
 *	and run at Execution time.
 *
 *	@author Rick Hillegas
 */

public interface ConstantAction
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/* Types of Statistics commands */
	public static final int STATISTICSTIMING = 1;
	public static final int RUNTIMESTATISTICS = 2;

	/**
	 *	Run the ConstantAction.
	 *
	 * @param	activation	The execution environment for this constant action.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException;


	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId the other table id
	 * 
	 * @exception StandardException on error
	 */
	public boolean modifiesTableId(UUID tableId) throws StandardException;

    /**
	  *	Reports whether these constants are up-to-date. This returns true
	  *	for homogenous Cloudscape/Cloudsync. For the Plugin, this may
	  *	return false;
	  *
	  *	@return	true if these constants are up-to-date
	  *			false otherwise
	  */
	public	boolean	upToDate()  throws StandardException;
}
