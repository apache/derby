/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.sanity.SanityManager;


/**
 *	This is a wrapper class which invokes the Execution-time logic for
 *	SET TRANSACTION statements. The real Execution-time logic lives inside the
 *	executeConstantAction() method of the Execution constant.
 *
 *	@author Jerry Brenner
 */

class SetTransactionResultSet extends MiscResultSet
{
	/**
     * Construct a SetTransactionResultSet
	 *
	 *  @param activation		Describes run-time environment.
	 *
	 *  @exception StandardException Standard Cloudscape error policy.
     */
    SetTransactionResultSet(Activation activation)
		 throws StandardException
    {
		super(activation);
	}

	/**
	 * Does this ResultSet cause a commit or rollback.
	 *
	 * @return Whether or not this ResultSet cause a commit or rollback.
	 */
	public boolean doesCommit()
	{
		return true;
	}
}
