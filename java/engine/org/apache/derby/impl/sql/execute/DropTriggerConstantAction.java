/*

   Derby - Class org.apache.derby.impl.sql.execute.DropTriggerConstantAction

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP TRIGGER Statement at Execution time.
 *
 */
public class DropTriggerConstantAction extends DDLSingleTableConstantAction
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
        triggerd.drop(lcc);
	}

	public String toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP TRIGGER "+triggerName;
	}
}
