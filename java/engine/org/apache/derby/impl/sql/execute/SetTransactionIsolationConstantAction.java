/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.TransactionController;


/**
 *	This class  describes actions that are ALWAYS performed for a
 *	SET TRANSACTION ISOLATION Statement at Execution time.
 *
 *	@author Jerry Brenner.
 */

class SetTransactionIsolationConstantAction extends GenericConstantAction
{

	private final int isolationLevel;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a SET TRANSACTION ISOLATION statement.
	 *
	 *  @param isolationLevel	The new isolation level
	 */
	SetTransactionIsolationConstantAction(
								int		isolationLevel)
	{
		this.isolationLevel = isolationLevel;
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "SET TRANSACTION ISOLATION LEVEL = " + isolationLevel;
	}

	// INTERFACE METHODS
	
	/**
	 *	This is the guts of the Execution-time logic for SET TRANSACTION ISOLATION.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		activation.getLanguageConnectionContext().setIsolationLevel(isolationLevel);
	}
}
