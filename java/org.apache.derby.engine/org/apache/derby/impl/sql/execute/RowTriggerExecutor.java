/*

   Derby - Class org.apache.derby.impl.sql.execute.RowTriggerExecutor

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

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.Activation;

/**
 * A row trigger executor is an object that executes
 * a row trigger.  It is instantiated at execution time.
 * There is one per row trigger.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
class RowTriggerExecutor extends GenericTriggerExecutor
{
	/**
	 * Constructor
	 *
	 * @param tec the execution context
	 * @param triggerd the trigger descriptor
	 * @param activation the activation
	 * @param lcc the lcc
	 */
	RowTriggerExecutor
	(
		InternalTriggerExecutionContext tec, 
		TriggerDescriptor 				triggerd,
		Activation						activation,
		LanguageConnectionContext		lcc
	)
	{
		super(tec, triggerd, activation, lcc);
	}

	/**
	 * Fire the trigger based on the event.
	 *
	 * @param event the trigger event
	 * @param brs   the before result set
	 * @param ars   the after result set
	 * @param colsReadFromTable   columns required from the trigger table
	 *   by the triggering sql
	 *
	 * @exception StandardExcetion on error or general trigger
	 *	exception
	 */
	void fireTrigger 
	(
		TriggerEvent 		event, 
		CursorResultSet 	brs, 
		CursorResultSet 	ars,
		int[]	colsReadFromTable
	) throws StandardException
	{
		tec.setTrigger(triggerd);
		
		try
		{
			while (true)
			{
				if (brs != null)
				{
					if (brs.getNextRow() == null)	
						break;
				}
	
				if (ars != null)
				{
					if (ars.getNextRow() == null)	
						break;
				}
	
				tec.setBeforeResultSet(brs == null ? 
						null : 
						TemporaryRowHolderResultSet.
						   getNewRSOnCurrentRow(triggerd, activation, brs, colsReadFromTable));
					
				tec.setAfterResultSet(ars == null ? 
									  null : 
									  TemporaryRowHolderResultSet.
									  getNewRSOnCurrentRow(triggerd, activation, ars, colsReadFromTable));

				/* 	
					This is the key to handling autoincrement values that might
					be seen by insert triggers. For an AFTER ROW trigger, update
					the autoincrement counters before executing the SPS for the
					trigger.
				*/
				if (event.isAfter()) 
					tec.updateAICounters();

                executeWhenClauseAndAction();
				
				/*
				  For BEFORE ROW triggers, update the ai values after the SPS
				  has been executed. This way the SPS will see ai values from
				  the previous row.
				*/
				if (event.isBefore())
					tec.updateAICounters();
			}
		} 
		finally
		{
			clearSPS();
			tec.clearTrigger();
		}
	}
}
