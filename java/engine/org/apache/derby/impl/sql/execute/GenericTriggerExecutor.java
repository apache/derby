/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericTriggerExecutor

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.reference.SQLState;

/**
 * A trigger executor is an object that executes
 * a trigger.  It is subclassed by row and statement
 * executors.
 */
public abstract class GenericTriggerExecutor
{
	protected InternalTriggerExecutionContext	tec;
	protected TriggerDescriptor					triggerd;
	protected Activation						activation;
	protected LanguageConnectionContext			lcc;

	private	boolean			whenClauseRetrieved;
	private	boolean			actionRetrieved;
	private SPSDescriptor	whenClause; 
	private SPSDescriptor	action;

	private ExecPreparedStatement	ps;
	private Activation 				spsActivation;

	/**
	 * Constructor
	 *
	 * @param tec the execution context
	 * @param triggerd the trigger descriptor
	 * @param activation the activation
	 * @param lcc the lcc
	 */
	GenericTriggerExecutor
	(
		InternalTriggerExecutionContext tec, 
		TriggerDescriptor 				triggerd,
		Activation						activation,
		LanguageConnectionContext		lcc
	)
	{
		this.tec = tec;
		this.triggerd = triggerd;
		this.activation = activation;
		this.lcc = lcc;
	}

	/**
	 * Fire the trigger based on the event.
	 *
	 * @param event	    the trigger event
	 * @param brs		the before result set
	 * @param ars		the after result set
	 *
	 * @exception StandardException on error or user exception
	 * from trigger action
	 */
	abstract void fireTrigger
	(
		TriggerEvent 		event, 
		CursorResultSet 	brs, 
		CursorResultSet 	ars
	) throws StandardException;

	protected SPSDescriptor getWhenClause() throws StandardException
	{
		if (!whenClauseRetrieved)
		{
			whenClauseRetrieved = true;
			whenClause = triggerd.getWhenClauseSPS();
		}
		return whenClause;
	}

	protected SPSDescriptor getAction() throws StandardException
	{
		if (!actionRetrieved)
		{
			actionRetrieved = true;
			action = triggerd.getActionSPS(lcc);
		}
		return action;
	}

	/**
	 * Execute the given stored prepared statement.  We
	 * just grab the prepared statement from the spsd,
	 * get a new activation holder and let er rip.
	 *
	 * @exception StandardException on error
	 */
	protected void executeSPS(SPSDescriptor sps) throws StandardException
	{
		boolean recompile = false;

		while (true) {
			/*
			** Only grab the ps the 1st time through.  This
			** way a row trigger doesn't do any unnecessary
			** setup work.
			*/
			if (ps == null || recompile)
			{
				/*
				** We need to clone the prepared statement so we don't
				** wind up marking that ps that is tied to sps as finished
				** during the course of execution.
				*/
				ps = sps.getPreparedStatement();
				ps = ps.getClone();
				// it should be valid since we've just prepared for it
				ps.setValid();
				spsActivation = ps.getActivation(lcc, false);

				/*
				** Normally, we want getSource() for an sps invocation
				** to be EXEC STATEMENT xxx, but in this case, since
				** we are executing the SPS in our own fashion, we want
				** the text to be the trigger action.  So set it accordingly.
				*/
				ps.setSource(sps.getText());
				ps.setSPSAction();
			}

			/*
			** Execute the activation.  If we have an error, we
			** are going to go to some extra work to pop off
			** our statement context.  This is because we are
			** a nested statement (we have 2 activations), but
			** we aren't a nested connection, so we have to
			** pop off our statementcontext to get error handling	
			** to work correctly.  This is normally a no-no, but
			** we are an unusual case.
			*/
			try
			{
				ResultSet rs = ps.execute(spsActivation, false, false, false);
                if( rs.returnsRows())
                {
                    // Fetch all the data to ensure that functions in the select list or values statement will
                    // be evaluated and side effects will happen. Why else would the trigger action return
                    // rows, but for side effects?
                    // The result set was opened in ps.execute()
                    while( rs.getNextRow() != null)
                    {
                    }
                }
                rs.close();
			} 
			catch (StandardException e)
			{
				/* Handle dynamic recompiles */
				if (e.getMessageId().equals(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE))
				{
					StatementContext sc = lcc.getStatementContext();
					sc.cleanupOnError(e);
					recompile = true;
					sps.revalidate(lcc);
					continue;
				}
				lcc.popStatementContext(lcc.getStatementContext(), e);
				spsActivation.close();
				throw e;
			}

			/* Done with execution without any recompiles */
			break;
		}
	}

	/**
	 * Cleanup after executing an sps.
	 *
	 * @exception StandardException on error
	 */
	protected void clearSPS() throws StandardException
	{
		if (spsActivation != null)
		{
			spsActivation.close();
		}
		ps = null;
		spsActivation = null;
	}
} 
