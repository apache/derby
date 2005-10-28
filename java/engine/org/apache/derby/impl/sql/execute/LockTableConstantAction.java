/*

   Derby - Class org.apache.derby.impl.sql.execute.LockTableConstantAction

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;


/**
 *	This class describes actions that are ALWAYS performed for a
 *	LOCK TABLE Statement at Execution time.
 *
 *	@author jamie 
 */

class LockTableConstantAction extends GenericConstantAction
{

	private final String					fullTableName;
	private final long					conglomerateNumber;
	private final boolean					exclusiveMode;
	
	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a LOCK TABLE statement.
	 *
	 *  @param fulltableName		Full name of the table.
	 *  @param conglomerateNumber	Conglomerate number for the heap
	 *  @param exclusiveMode		Whether or not to get an exclusive lock.
	 */
	LockTableConstantAction(String fullTableName,
									long conglomerateNumber, boolean exclusiveMode)
	{
		this.fullTableName = fullTableName;
		this.conglomerateNumber = conglomerateNumber;
		this.exclusiveMode = exclusiveMode;
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "LOCK TABLE " + fullTableName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for LOCK TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConglomerateController	cc;
		TransactionController	tc;

		/* Get a ConglomerateController for the base conglomerate */
		tc = activation.getTransactionController();

		try
		{
			cc = tc.openConglomerate(
	                conglomerateNumber,
                    false,
					(exclusiveMode) ?
						(TransactionController.OPENMODE_FORUPDATE | 
							TransactionController.OPENMODE_FOR_LOCK_ONLY) :
						TransactionController.OPENMODE_FOR_LOCK_ONLY,
			        TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);
			cc.close();
		}
		catch (StandardException se)
		{
			String msgId = se.getMessageId();
			if (msgId.equals(SQLState.DEADLOCK) || msgId.equals(SQLState.LOCK_TIMEOUT) || msgId.equals(SQLState.LOCK_TIMEOUT_LOG)) {
				String mode = (exclusiveMode) ? "EXCLUSIVE" : "SHARE";
				se = StandardException.newException(SQLState.LANG_CANT_LOCK_TABLE, se, fullTableName, mode);
			}

			throw se;
		}
	}
}
