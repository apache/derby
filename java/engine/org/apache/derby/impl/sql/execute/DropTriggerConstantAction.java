/*

   Derby - Class org.apache.derby.impl.sql.execute.DropTriggerConstantAction

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP TRIGGER Statement at Execution time.
 *
 *	@author Jamie
 */
class DropTriggerConstantAction extends DDLSingleTableConstantAction
{

	private final String			triggerName;
	private final SchemaDescriptor	sd;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP TRIGGER statement.
	 *
	 * @param	sd					Schema that stored prepared statement lives in.
	 * @param	triggerName			Name of the Trigger
	 * @param	tableId				The table upon which the trigger is defined
	 *
	 */
	DropTriggerConstantAction
	(
		SchemaDescriptor	sd,
		String				triggerName,
		UUID				tableId
	)
	{
		super(tableId);
		this.sd = sd;
		this.triggerName = triggerName;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
		}
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP STATEMENT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TriggerDescriptor 			triggerd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();


		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		TableDescriptor td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tableId.toString());
		}
		TransactionController tc = lcc.getTransactionExecute();
		lockTableForDDL(tc, td.getHeapConglomerateId(), true);
		// get td again in case table shape is changed before lock is acquired
		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tableId.toString());
		}

		/* 
		** Get the trigger descriptor.  We're responsible for raising
		** the error if it isn't found 
		*/
		triggerd = dd.getTriggerDescriptor(triggerName, sd);

		if (triggerd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "TRIGGER",
					(sd.getSchemaName() + "." + triggerName));
		}

		/* 
	 	** Prepare all dependents to invalidate.  (This is there chance
		** to say that they can't be invalidated.  For example, an open
		** cursor referencing a table/trigger that the user is attempting to
		** drop.) If no one objects, then invalidate any dependent objects.
		*/
		dropTriggerDescriptor(lcc, dm, dd, tc, triggerd, activation);
	}

	static void dropTriggerDescriptor
	(
		LanguageConnectionContext	lcc,
		DependencyManager 			dm,
		DataDictionary				dd,
		TransactionController		tc,
		TriggerDescriptor			triggerd,
		Activation					activation
	) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(triggerd!=null, "trigger descriptor is null");
		}

		dm.invalidateFor(triggerd, DependencyManager.DROP_TRIGGER, lcc);

		// Drop the trigger
		dd.dropTriggerDescriptor(triggerd, tc);

		// Clear the dependencies for the trigger 
		dm.clearDependencies(lcc, triggerd);

		// Drop the spses
		SPSDescriptor spsd = dd.getSPSDescriptor(triggerd.getActionId());

		// there shouldn't be any dependencies, but in case
		// there are, lets clear them
		dm.invalidateFor(spsd, DependencyManager.DROP_TRIGGER, lcc);
		dm.clearDependencies(lcc, spsd);
		dd.dropSPSDescriptor(spsd, tc);
		
		if (triggerd.getWhenClauseId() != null)
		{	
			spsd = dd.getSPSDescriptor(triggerd.getWhenClauseId());
			dm.invalidateFor(spsd, DependencyManager.DROP_TRIGGER, lcc);
			dm.clearDependencies(lcc, spsd);
			dd.dropSPSDescriptor(spsd, tc);
		}
	}

	public String toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP TRIGGER "+triggerName;
	}
}
