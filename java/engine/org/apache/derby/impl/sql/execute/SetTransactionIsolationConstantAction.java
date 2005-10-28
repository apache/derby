/*

   Derby - Class org.apache.derby.impl.sql.execute.SetTransactionIsolationConstantAction

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
