/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.Activation;


/**
 *	This is a wrapper class which invokes the Execution-time logic for
 *	Misc statements. The real Execution-time logic lives inside the
 *	executeConstantAction() method of the Execution constant.
 *
 *	@author jamie
 */

class MiscResultSet extends NoRowsResultSetImpl
{
	private final ConstantAction constantAction;

	/**
     * Construct a MiscResultSet
	 *
	 *  @param activation		Describes run-time environment.
	 *
	 *  @exception StandardException Standard Cloudscape error policy.
     */
    MiscResultSet(Activation activation)
		 throws StandardException
    {
		super(activation);
		constantAction = activation.getConstantAction();
	}
    
	public void open() throws StandardException
	{
		constantAction.executeConstantAction(activation);
		super.close();
	}

	/**
	 * @see org.apache.derby.iapi.sql.ResultSet#cleanUp
	 */
	public void	cleanUp() 
	{
	}
}
