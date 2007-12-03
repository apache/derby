/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerEventActivator

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

import java.util.Vector;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Responsible for firing a trigger or set of triggers
 * based on an event.
 */
class TriggerEventActivator
{
	private LanguageConnectionContext		lcc; 
	private TriggerInfo 					triggerInfo; 
	private InternalTriggerExecutionContext	tec;
	private	GenericTriggerExecutor[][]		executors;
	private	Activation						activation;
	private	ConnectionContext				cc;
	private String							statementText;
	private int								dmlType;
	private UUID							tableId;
	private String							tableName;
	private Vector							aiCounters;

	/**
	 * Basic constructor
	 *
	 * @param lcc			the lcc
	 * @param tc			the xact controller
	 * @param triggerInfo	the trigger information 
	 * @param dmlType		Type of DML for which this trigger is being fired.
	 * @param activation	the activation.
	 * @param aiCounters	vector of ai counters 
	 *
	 * @exception StandardException on error
	 */
	TriggerEventActivator
	(
		LanguageConnectionContext	lcc, 
		TransactionController 		tc, 
		UUID						tableId,
		TriggerInfo 				triggerInfo,
		int							dmlType,
		Activation					activation,
		Vector						aiCounters
	) throws StandardException
	{
		if (triggerInfo == null)
		{
			return;
		}

		// extrapolate the table name from the triggerdescriptors
		tableName = triggerInfo.triggerArray[0].getTableDescriptor().getQualifiedName();
	
		this.lcc = lcc;
		this.activation = activation;
		this.tableId = tableId;
		this.dmlType = dmlType;
		this.triggerInfo = triggerInfo;

		cc = (ConnectionContext)lcc.getContextManager().
									getContext(ConnectionContext.CONTEXT_ID);

		this.statementText = lcc.getStatementContext().getStatementText();

		this.tec = ((GenericExecutionFactory)lcc.getLanguageConnectionFactory().getExecutionFactory()).
						getTriggerExecutionContext(
								lcc,
								cc,
								statementText,
								dmlType,
								triggerInfo.columnIds,					
								triggerInfo.columnNames,
								tableId,	
								tableName, aiCounters
								);

		setupExecutors(triggerInfo);
	}

	/**
	 * Reopen the trigger activator.  Just creates a new trigger execution
	 * context.  Note that close() still must be called when you
	 * are done -- you cannot just do a reopen() w/o a first doing
	 * a close.
	 *
	 * @exception StandardException on error
	 */
	void reopen() throws StandardException
	{
		this.tec = ((GenericExecutionFactory)lcc.getLanguageConnectionFactory().getExecutionFactory()).
						getTriggerExecutionContext(
								lcc,
								cc,
								statementText,
								dmlType,
								triggerInfo.columnIds,					
								triggerInfo.columnNames,
								tableId,	
								tableName, aiCounters
								);
		setupExecutors(triggerInfo);
	}

	private void setupExecutors(TriggerInfo triggerInfo) throws StandardException
	{
		executors = new GenericTriggerExecutor[TriggerEvent.MAX_EVENTS][];
		Vector[] executorLists = new Vector[TriggerEvent.MAX_EVENTS];
		for (int i = 0; i < TriggerEvent.MAX_EVENTS; i++)
		{
			executorLists[i] = new Vector();
		}

		for (int i = 0; i < triggerInfo.triggerArray.length; i++)
		{
			TriggerDescriptor td = triggerInfo.triggerArray[i];
			switch (td.getTriggerEventMask())
			{
				case TriggerDescriptor.TRIGGER_EVENT_INSERT:
					if (td.isBeforeTrigger())
					{
						executorLists[TriggerEvent.BEFORE_INSERT].addElement(td);
					}
					else
					{
						executorLists[TriggerEvent.AFTER_INSERT].addElement(td);
					}
					break;


				case TriggerDescriptor.TRIGGER_EVENT_DELETE:
					if (td.isBeforeTrigger())
					{
						executorLists[TriggerEvent.BEFORE_DELETE].addElement(td);
					}
					else
					{
						executorLists[TriggerEvent.AFTER_DELETE].addElement(td);
					}
					break;

				case TriggerDescriptor.TRIGGER_EVENT_UPDATE:
					if (td.isBeforeTrigger())
					{
						executorLists[TriggerEvent.BEFORE_UPDATE].addElement(td);
					}
					else
					{
						executorLists[TriggerEvent.AFTER_UPDATE].addElement(td);
					}
					break;
				default:
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("bad trigger event "+td.getTriggerEventMask());
					}
			}
		}

		for (int i = 0; i < executorLists.length; i++)
		{
			int size = executorLists[i].size();
			if (size > 0)
			{
				executors[i] = new GenericTriggerExecutor[size];
				for (int j = 0; j < size; j++)
				{
					TriggerDescriptor td = (TriggerDescriptor)executorLists[i].elementAt(j);
					executors[i][j] =  (td.isRowTrigger()) ? 
								(GenericTriggerExecutor)new RowTriggerExecutor(tec, td, activation, lcc) :
								(GenericTriggerExecutor)new StatementTriggerExecutor(tec, td, activation, lcc);
				}
			}
		}
	}

	/**
	 * Handle the given event.
	 * 
	 * @param event	a trigger event
 	 * @param brs the before result set.  Typically
	 * 		a TemporaryRowHolderResultSet but sometimes a
	 * 		BulkTableScanResultSet
 	 * @param ars the after result set. Typically
	 * 		a TemporaryRowHolderResultSet but sometimes a
	 * 		BulkTableScanResultSet
	 *
 	 * @exception StandardException on error
	 */
	void notifyEvent
	(
		TriggerEvent 		event,
		CursorResultSet		brs,
		CursorResultSet		ars
	) throws StandardException
	{
		if (executors == null)
		{
			return;
		}

		int eventNumber = event.getNumber();
		if (executors[eventNumber] == null)
		{
			return;
		}

		tec.setCurrentTriggerEvent(event);
		try
		{
			if (brs != null)
			{
				brs.open();
			}
			if (ars != null)
			{
				ars.open();
			}

			lcc.pushExecutionStmtValidator(tec);
			for (int i = 0; i < executors[eventNumber].length; i++)
			{
				if (i > 0)
				{
					
					if (brs != null)
					{
						((NoPutResultSet)brs).reopenCore();
					}
					if (ars != null)
					{
						((NoPutResultSet)ars).reopenCore();
					}
				}
				// Reset the AI counters to the beginning before firing next
				// trigger. 
				tec.resetAICounters(true);				
				executors[eventNumber][i].fireTrigger(event, brs, ars);
			}
		}
		finally
		{
			lcc.popExecutionStmtValidator(tec);
			tec.clearCurrentTriggerEvent();
		}
	}	
	
	/**
	 * Clean up and release resources.
	 *
	 * @exception StandardException on unexpected error
	 */
	void cleanup() throws StandardException
	{
		if (tec != null)
		{
			tec.cleanup();
		}
	}
}
