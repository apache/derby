/*

   Derby - Class org.apache.derby.impl.sql.execute.StatementTriggerExecutor

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

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.Activation;

/**
 * A statement trigger executor is an object that executes
 * a statement trigger.  It is instantiated at execution
 * time.  There is one per statement trigger.
 */
public class StatementTriggerExecutor extends GenericTriggerExecutor
{
	/**
	 * Constructor
	 *
	 * @param tec the execution context
	 * @param triggerd the trigger descriptor
	 * @param activation the activation
	 * @param lcc the lcc
	 */
	StatementTriggerExecutor
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
	 * @param event	the trigger event
	 * @param brs   the before result set
	 * @param ars   the after result set
	 *
	 * @exception StandardException on error or general trigger
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
		tec.setBeforeResultSet(brs);
		tec.setAfterResultSet(ars);
		
		try
		{
			executeSPS(getAction());
		}
		finally
		{
			clearSPS();
			tec.clearTrigger();	
		}
	}
}
