/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.Activation;

/**
 * A row trigger executor is an object that executes
 * a row trigger.  It is instantiated at execution time.
 * There is one per row trigger.
 */
public class RowTriggerExecutor extends GenericTriggerExecutor
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
	 *
	 * @exception StandardExcetion on error or general trigger
	 *	exception
	 */
	void fireTrigger 
	(
		TriggerEvent 		event, 
		CursorResultSet 	brs, 
		CursorResultSet 	ars
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
						TemporaryRowHolderResultSet.getNewRSOnCurrentRow(activation.getTransactionController(), brs));
					
				tec.setAfterResultSet(ars == null ? 
									  null : 
									  TemporaryRowHolderResultSet.getNewRSOnCurrentRow(activation.getTransactionController(), ars));

				/* 	
					This is the key to handling autoincrement values that might
					be seen by insert triggers. For an AFTER ROW trigger, update
					the autoincrement counters before executing the SPS for the
					trigger.
				*/
				if (event.isAfter()) 
					tec.updateAICounters();

				executeSPS(getAction());
				
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
