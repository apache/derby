/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
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
