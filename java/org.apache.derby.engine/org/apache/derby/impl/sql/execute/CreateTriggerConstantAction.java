/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateTriggerConstantAction

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

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptorList;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

import java.sql.Timestamp;

/**
 * This class  describes actions that are ALWAYS performed for a
 * CREATE TRIGGER Statement at Execution time.  
 *
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
    private final String            originalWhenText;
	private String					originalActionText;
	private String					oldReferencingName;
	private String					newReferencingName;
	private UUID					spsCompSchemaId;
	private int[]					referencedCols;
	private int[]					referencedColsInTriggerAction;
    private final ProviderInfo[]    providerInfo;

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
	 * @param referencedCols	what columns does this trigger reference (may be null)
	 * @param referencedColsInTriggerAction	what columns does the trigger 
	 *						action reference through old/new transition variables
	 *						(may be null)
     * @param originalWhenText The original user text of the WHEN clause (may be null)
	 * @param originalActionText The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERENCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERENCING clause
     * @param providerInfo  array of providers that the trigger depends on
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
		int[]				referencedCols,
		int[]				referencedColsInTriggerAction,
        String              originalWhenText,
		String				originalActionText,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
        String              newReferencingName,
        ProviderInfo[]      providerInfo
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
		this.referencedCols = referencedCols;
		this.referencedColsInTriggerAction = referencedColsInTriggerAction;
		this.originalActionText = originalActionText;
        this.originalWhenText = originalWhenText;
		this.referencingOld = referencingOld;
		this.referencingNew = referencingNew;
		this.oldReferencingName = oldReferencingName;
		this.newReferencingName = newReferencingName;
        this.providerInfo = providerInfo;
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

		if (spsCompSchemaId == null) {
			SchemaDescriptor def = lcc.getDefaultSchema();
			if (def.getUUID() == null) {
				// Descriptor for default schema is stale,
				// look it up in the dictionary
				def = dd.getSchemaDescriptor(def.getDescriptorName(), tc, 
											 false);
			}
			
			/* 
			** It is possible for spsCompSchemaId to be null.  For instance, 
			** the current schema may not have been physically created yet but 
			** it exists "virtually".  In this case, its UUID will have the 
			** value of null meaning that it is not persistent.  e.g.:   
			**
			** CONNECT 'db;create=true' user 'ernie';
			** CREATE TABLE bert.t1 (i INT);
			** CREATE TRIGGER bert.tr1 AFTER INSERT ON bert.t1 
			**    FOR EACH STATEMENT MODE DB2SQL 
			**    SELECT * FROM SYS.SYSTABLES;
			**
			** Note that in the above case, the trigger action statement have a 
			** null compilation schema.  A compilation schema with null value 
			** indicates that the trigger action statement text does not have 
			** any dependencies with the CURRENT SCHEMA.  This means:
			**
			** o  It is safe to compile this statement in any schema since 
			**    there is no dependency with the CURRENT SCHEMA. i.e.: All 
			**    relevent identifiers are qualified with a specific schema.
			**
			** o  The statement cache mechanism can utilize this piece of 
			**    information to enable better statement plan sharing across 
			**    connections in different schemas; thus, avoiding unnecessary 
			**    statement compilation.
			*/ 
			if (def != null)
				spsCompSchemaId = def.getUUID();
		}

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

		actionSPSId = (actionSPSId == null) ? 
			dd.getUUIDFactory().createUUID() : actionSPSId;

        if (whenSPSId == null && whenText != null) {
            whenSPSId = dd.getUUIDFactory().createUUID();
        }
 
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		/*
		** Create the trigger descriptor first so the trigger action
		** compilation can pick up the relevant trigger especially in 
		** the case of self triggering.
		*/
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
                                    whenSPSId,
									actionSPSId,
                                    makeCreationTimestamp(dd),
									referencedCols,
									referencedColsInTriggerAction,
									originalActionText,
									referencingOld,
									referencingNew,
									oldReferencingName,
                                    newReferencingName,
                                    originalWhenText);


		dd.addDescriptor(triggerd, triggerSd,
								DataDictionary.SYSTRIGGERS_CATALOG_NUM, false,
								tc);


		/*	
		** If we have a WHEN action we create it now.
		*/
		if (whenText != null)
		{
            // The WHEN clause is just a search condition and not a full
            // SQL statement. Turn in into a VALUES statement.
            String whenValuesStmt = "VALUES " + whenText;
			whenspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
                whenSPSId, spsCompSchemaId, whenValuesStmt, true, triggerTable);
		}

		/*
		** Create the trigger action
		*/
		actionspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
						actionSPSId, spsCompSchemaId, actionText, false, triggerTable);
		
		/*
		** Make underlying spses dependent on the trigger.
		*/
		if (whenspsd != null)
		{
			dm.addDependency(triggerd, whenspsd, lcc.getContextManager());
		}
		dm.addDependency(triggerd, actionspsd, lcc.getContextManager());
		dm.addDependency(triggerd, triggerTable, lcc.getContextManager());

        // Make the TriggerDescriptor dependent on all objects referenced
        // from the triggered statement or the WHEN clause.
        for (ProviderInfo info : providerInfo) {
            Provider provider = (Provider) info.getDependableFinder()
                    .getDependable(dd, info.getObjectId());
            dm.addDependency(triggerd, provider, lcc.getContextManager());
        }

		//store trigger's dependency on various privileges in the dependeny system
//IC see: https://issues.apache.org/jira/browse/DERBY-1330
		storeViewTriggerDependenciesOnPrivileges(activation, triggerd);		
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
									true );	// no defaults

		/*
		** Prepared the stored prepared statement
		** and release the activation class -- we
		** know we aren't going to execute statement
		** after create it, so for now we are finished.
		*/
		spsd.prepareAndRelease(lcc, triggerTable);


		dd.addSPSDescriptor(spsd, tc);
//IC see: https://issues.apache.org/jira/browse/DERBY-3850
//IC see: https://issues.apache.org/jira/browse/DERBY-177
//IC see: https://issues.apache.org/jira/browse/DERBY-3693

		return spsd;
	}

	public String toString()
	{
		return constructToString("CREATE TRIGGER ", triggerName);		
	}

    /**
     * Construct the creation timestamp for the trigger. DERBY-5866: Also make
     * sure the creation timestamp is higher than any timestamp on an existing
     * trigger on the same table. Otherwise, the triggers may not fire in the
     * correct order.
     */
    private Timestamp makeCreationTimestamp(DataDictionary dd)
            throws StandardException {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        // Allow overriding the timestamp in debug mode for testing of
        // specific scenarios.
        if (SanityManager.DEBUG) {
            String val = PropertyUtil.getSystemProperty(
                    "derby.debug.overrideTriggerCreationTimestamp");
            if (val != null) {
                now.setTime(Long.parseLong(val));
            }
        }

        TriggerDescriptorList tdl = dd.getTriggerDescriptors(triggerTable);
        int numTriggers = tdl.size();

        if (numTriggers == 0) {
            // This is the first trigger on the table, so no need to check
            // if there are any higher timestamps.
            return now;
        }

        // Get the timestamp of the most recent existing trigger on the table.
        Timestamp highest = tdl.get(numTriggers - 1).getCreationTimestamp();

        if (now.after(highest)) {
            // The current timestamp is higher than the most recent existing
            // trigger on the table, so it is OK.
            return now;
        }

        // Otherwise, there is an existing trigger on the table with a
        // timestamp that is at least as high as the current timestamp. Adjust
        // the current timestamp so that it is one millisecond higher than the
        // timestamp of the existing trigger. This ensures that the triggers
        // will fire in the same order as they were created.

        now.setTime(highest.getTime() + 1);

        return now;
    }
}
