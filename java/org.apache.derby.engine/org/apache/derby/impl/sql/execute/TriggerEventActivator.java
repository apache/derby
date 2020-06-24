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

import java.util.ArrayList;
import java.util.List;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.catalog.UUID;

import java.util.Vector;

/**
 * Responsible for firing a trigger or set of triggers
 * based on an event.
 */
public class TriggerEventActivator
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

	/**
	 * Basic constructor
	 *
	 * @param lcc			the lcc
	 * @param triggerInfo	the trigger information 
	 * @param dmlType		Type of DML for which this trigger is being fired.
	 * @param activation	the activation.
	 * @param aiCounters	vector of ai counters 
	 *
	 * @exception StandardException on error
	 */
    @SuppressWarnings("UseOfObsoleteCollectionType")
	public TriggerEventActivator
	(
		LanguageConnectionContext	lcc, 
		UUID						tableId,
		TriggerInfo 				triggerInfo,
		int							dmlType,
		Activation					activation,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        Vector<AutoincrementCounter> aiCounters
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
								tableId,	
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                                tableName,
                                null);
		setupExecutors(triggerInfo);
	}
    
	private void setupExecutors(TriggerInfo triggerInfo) throws StandardException
	{
		executors = new GenericTriggerExecutor[TriggerEvent.MAX_EVENTS][];
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        List<List<TriggerDescriptor>> executorLists =
            new ArrayList<List<TriggerDescriptor>>(TriggerEvent.MAX_EVENTS);
		for (int i = 0; i < TriggerEvent.MAX_EVENTS; i++)
		{
            executorLists.add(new ArrayList<TriggerDescriptor>());
		}

		for (int i = 0; i < triggerInfo.triggerArray.length; i++)
		{
			TriggerDescriptor td = triggerInfo.triggerArray[i];
			switch (td.getTriggerEventMask())
			{
				case TriggerDescriptor.TRIGGER_EVENT_INSERT:
					if (td.isBeforeTrigger())
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                        executorLists.get(TriggerEvent.BEFORE_INSERT).add(td);
					}
					else
					{
                        executorLists.get(TriggerEvent.AFTER_INSERT).add(td);
					}
					break;


				case TriggerDescriptor.TRIGGER_EVENT_DELETE:
					if (td.isBeforeTrigger())
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                        executorLists.get(TriggerEvent.BEFORE_DELETE).add(td);
					}
					else
					{
                        executorLists.get(TriggerEvent.AFTER_DELETE).add(td);
					}
					break;

				case TriggerDescriptor.TRIGGER_EVENT_UPDATE:
					if (td.isBeforeTrigger())
					{
                        executorLists.get(TriggerEvent.BEFORE_UPDATE).add(td);
					}
					else
					{
                        executorLists.get(TriggerEvent.AFTER_UPDATE).add(td);
					}
					break;
				default:
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("bad trigger event "+td.getTriggerEventMask());
					}
			}
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (int i = 0; i < executorLists.size(); i++)
		{
            List<TriggerDescriptor> descriptors = executorLists.get(i);
            int size = descriptors.size();
			if (size > 0)
			{
				executors[i] = new GenericTriggerExecutor[size];
				for (int j = 0; j < size; j++)
				{
                    TriggerDescriptor td = descriptors.get(j);
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
	 * @param colsReadFromTable   columns required from the trigger table
	 *   by the triggering sql
	 *
 	 * @exception StandardException on error
	 */
	public void notifyEvent
	(
		TriggerEvent 		event,
		CursorResultSet		brs,
		CursorResultSet		ars,
		int[]	colsReadFromTable
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
				executors[eventNumber][i].fireTrigger(event, brs, ars, colsReadFromTable);
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
	public void cleanup() throws StandardException
	{
		if (tec != null)
		{
			tec.cleanup();
		}
	}
}
