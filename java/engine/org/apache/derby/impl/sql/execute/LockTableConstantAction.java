/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
