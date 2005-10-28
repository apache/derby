/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateTriggerConstantAction

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

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

import java.sql.Timestamp;

/**
 * This class  describes actions that are ALWAYS performed for a
 * CREATE TRIGGER Statement at Execution time.  
 *
 *	@author Jamie
 */
class CreateTriggerConstantAction extends DDLSingleTableConstantAction
{

	private String					triggerName;
	private String					triggerSchemaName;
	private TableDescriptor			triggerTable;		// null after readExternal
	private UUID					triggerTableId;		// set in readExternal
	private int						eventMask;
	private boolean					isBefore;
	private boolean					isRow;
	private boolean					isEnabled;
	private boolean					referencingOld;
	private boolean					referencingNew;
	private UUID					whenSPSId;
	private String					whenText;
	private UUID					actionSPSId;
	private String					actionText;
	private String					originalActionText;
	private String					oldReferencingName;
	private String					newReferencingName;
	private UUID					spsCompSchemaId;
	private Timestamp				creationTimestamp;
	private int[]					referencedCols;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE TRIGGER statement.
	 *
	 * @param triggerSchemaName	name for the schema that trigger lives in.
	 * @param triggerName	Name of trigger
	 * @param eventMask		TriggerDescriptor.TRIGGER_EVENT_XXXX
	 * @param isBefore		is this a before (as opposed to after) trigger 
	 * @param isRow			is this a row trigger or statement trigger
	 * @param isEnabled		is this trigger enabled or disabled
	 * @param triggerTable	the table upon which this trigger is defined
	 * @param whenSPSId		the sps id for the when clause (may be null)
	 * @param whenText		the text of the when clause (may be null)
	 * @param actionSPSId	the spsid for the trigger action (may be null)
	 * @param actionText	the text of the trigger action
	 * @param spsCompSchemaId	the compilation schema for the action and when
	 *							spses.   If null, will be set to the current default
	 *							schema
	 * @param creationTimestamp	when was this trigger created?  if null, will be
	 *						set to the time that executeConstantAction() is invoked
	 * @param referencedCols	what columns does this trigger reference (may be null)
	 * @param originalActionText The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERENCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERENCING clause
	 */
	CreateTriggerConstantAction
	(
		String				triggerSchemaName,
		String				triggerName,
		int					eventMask,
		boolean				isBefore,
		boolean 			isRow,
		boolean 			isEnabled,
		TableDescriptor		triggerTable,
		UUID				whenSPSId,
		String				whenText,
		UUID				actionSPSId,
		String				actionText,
		UUID				spsCompSchemaId,
		Timestamp			creationTimestamp,
		int[]				referencedCols,
		String				originalActionText,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
		String				newReferencingName
	)
	{
		super(triggerTable.getUUID());
		this.triggerName = triggerName;
		this.triggerSchemaName = triggerSchemaName;
		this.triggerTable = triggerTable;
		this.eventMask = eventMask;
		this.isBefore = isBefore;
		this.isRow = isRow;
		this.isEnabled = isEnabled;
		this.whenSPSId = whenSPSId;
		this.whenText = whenText;
		this.actionSPSId = actionSPSId;
		this.actionText = actionText;
		this.spsCompSchemaId = spsCompSchemaId;
		this.creationTimestamp = creationTimestamp;
		this.referencedCols = referencedCols;
		this.originalActionText = originalActionText;
		this.referencingOld = referencingOld;
		this.referencingNew = referencingNew;
		this.oldReferencingName = oldReferencingName;
		this.newReferencingName = newReferencingName;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(triggerSchemaName != null, "triggerSchemaName sd is null");
			SanityManager.ASSERT(triggerName != null, "trigger name is null");
			SanityManager.ASSERT(triggerTable != null, "triggerTable is null");
			SanityManager.ASSERT(actionText != null, "actionText is null");
		}
	}

	/**
	 * This is the guts of the Execution-time logic for CREATE TRIGGER.
	 *
	 * @see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction(Activation activation)
						throws StandardException
	{
		SPSDescriptor				whenspsd = null;
		SPSDescriptor				actionspsd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/*
		** Indicate that we are about to modify the data dictionary.
		** 
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		SchemaDescriptor triggerSd = getSchemaDescriptorForCreate(dd, activation, triggerSchemaName);

		String tabName;
		if (triggerTable != null)
		{
			triggerTableId = triggerTable.getUUID();
			tabName = triggerTable.getName();
		}
		else
			tabName = "with UUID " + triggerTableId;

		/* We need to get table descriptor again.  We simply can't trust the
		 * one we got at compile time, the lock on system table was released
		 * when compile was done, and the table might well have been dropped.
		 */
		triggerTable = dd.getTableDescriptor(triggerTableId);
		if (triggerTable == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tabName);
		}
		/* Lock the table for DDL.  Otherwise during our execution, the table
		 * might be changed, even dropped.  Beetle 4269
		 */
		lockTableForDDL(tc, triggerTable.getHeapConglomerateId(), true);
		/* get triggerTable again for correctness, in case it's changed before
		 * the lock is aquired
		 */
		triggerTable = dd.getTableDescriptor(triggerTableId);
		if (triggerTable == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tabName);
		}

		/*
		** Send an invalidate on the table from which
		** the triggering event emanates.  This it
		** to make sure that DML statements on this table
		** will be recompiled.  Do this before we create
		** our trigger spses lest we invalidate them just
		** after creating them.
		*/
		dm.invalidateFor(triggerTable, DependencyManager.CREATE_TRIGGER, lcc);

		/*
		** Lets get our trigger id up front, we'll use it when
	 	** we create our spses.
		*/
		UUID tmpTriggerId = dd.getUUIDFactory().createUUID();

		/*	
		** If we have a WHEN action we create it now.
		*/ 
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		if (whenText != null)
		{
			whenspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
						whenSPSId, spsCompSchemaId, whenText, true, triggerTable);
		}

		/*
		** Create the trigger action
		*/
		actionspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
						actionSPSId, spsCompSchemaId, actionText, false, triggerTable);
		
		TriggerDescriptor triggerd = 
				ddg.newTriggerDescriptor(
									triggerSd,
									tmpTriggerId,
									triggerName,
									eventMask,
									isBefore,
									isRow,
									isEnabled,
									triggerTable,
									whenspsd == null ? null : whenspsd.getUUID(),
									actionspsd.getUUID(),
									creationTimestamp == null ? new Timestamp(System.currentTimeMillis()) : creationTimestamp,
									referencedCols,
									originalActionText,
									referencingOld,
									referencingNew,
									oldReferencingName,
									newReferencingName);


		dd.addDescriptor(triggerd, triggerSd,
								DataDictionary.SYSTRIGGERS_CATALOG_NUM, false,
								tc);

		/*
		** Make underlying spses dependent on the trigger.
		*/
		if (whenspsd != null)
		{
			dm.addDependency(triggerd, whenspsd, lcc.getContextManager());
		}
		dm.addDependency(triggerd, actionspsd, lcc.getContextManager());
		dm.addDependency(triggerd, triggerTable, lcc.getContextManager());
		dm.addDependency(actionspsd, triggerTable, lcc.getContextManager());
	}


	/*
	** Create an sps that is used by the trigger.
	*/
	private SPSDescriptor createSPS
	(
		LanguageConnectionContext	lcc,
		DataDescriptorGenerator 	ddg,
		DataDictionary				dd,
		TransactionController		tc,
		UUID						triggerId,
		SchemaDescriptor			sd,
		UUID						spsId,
		UUID						compSchemaId,
		String						text,
		boolean						isWhen,
		TableDescriptor				triggerTable
	) throws StandardException	
	{
		if (text == null)
		{
			return null; 
		}

		/*
		** Note: the format of this string is very important.
		** Dont change it arbitrarily -- see sps code.
		*/
		String spsName = "TRIGGER" + 
						(isWhen ? "WHEN_" : "ACTN_") + 
						triggerId + "_" + triggerTable.getUUID().toString();

		SPSDescriptor spsd = new SPSDescriptor(dd, spsName,
									(spsId == null) ?
										dd.getUUIDFactory().createUUID() :
										spsId,
									sd.getUUID(),
									compSchemaId == null ?
										lcc.getDefaultSchema().getUUID() :
										compSchemaId,
									SPSDescriptor.SPS_TYPE_TRIGGER,
									true,				// it is valid
									text,				// the text
									(String)null,		// no using clause
									(Object[])null,
									true );	// no defaults

		/*
		** Prepared the stored prepared statement
		** and release the activation class -- we
		** know we aren't going to execute statement
		** after create it, so for now we are finished.
		*/
		spsd.prepareAndRelease(lcc, triggerTable);


		dd.addSPSDescriptor(spsd, tc, true);

		return spsd;
	}

	public String toString()
	{
		return constructToString("CREATE TRIGGER ", triggerName);		
	}
}


