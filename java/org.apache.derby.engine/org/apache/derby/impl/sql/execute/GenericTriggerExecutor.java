/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericTriggerExecutor

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A trigger executor is an object that executes
 * a trigger.  It is subclassed by row and statement
 * executors.
 */
abstract class GenericTriggerExecutor
{
    final InternalTriggerExecutionContext   tec;
    final TriggerDescriptor                 triggerd;
    final Activation                        activation;
    private final LanguageConnectionContext lcc;

	private	boolean			whenClauseRetrieved;
	private	boolean			actionRetrieved;
	private SPSDescriptor	whenClause; 
	private SPSDescriptor	action;

    // Cached prepared statement and activation for WHEN clause and
    // trigger action.
    private ExecPreparedStatement   whenPS;
    private Activation              spsWhenActivation;
    private ExecPreparedStatement   actionPS;
    private Activation              spsActionActivation;

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
	 * @param colsReadFromTable   columns required from the trigger table
	 *   by the triggering sql
	 *
	 * @exception StandardException on error or user exception
	 * from trigger action
	 */
	abstract void fireTrigger
	(
		TriggerEvent 		event, 
		CursorResultSet 	brs, 
		CursorResultSet 	ars,
		int[]	colsReadFromTable
	) throws StandardException;

    private SPSDescriptor getWhenClause() throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		if (!whenClauseRetrieved)
		{
			whenClauseRetrieved = true;
            whenClause = triggerd.getWhenClauseSPS(lcc);
		}
		return whenClause;
	}

    private SPSDescriptor getAction() throws StandardException
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
     * @param sps the SPS to execute
     * @param isWhen {@code true} if the SPS is for the WHEN clause,
     *               {@code false} otherwise
     * @return {@code true} if the SPS is for a WHEN clause and it evaluated
     *         to {@code TRUE}, {@code false} otherwise
	 * @exception StandardException on error
	 */
    private boolean executeSPS(SPSDescriptor sps, boolean isWhen)
            throws StandardException
	{
		boolean recompile = false;
        boolean whenClauseWasTrue = false;

        // The prepared statement and the activation may already be available
        // if the trigger has been fired before in the same statement. (Only
        // happens with row triggers that are triggered by a statement that
        // touched multiple rows.) The WHEN clause and the trigger action have
        // their own prepared statement and activation. Fetch the correct set.
        ExecPreparedStatement ps = isWhen ? whenPS : actionPS;
        Activation spsActivation = isWhen
                ? spsWhenActivation : spsActionActivation;

		while (true) {
			/*
			** Only grab the ps the 1st time through.  This
			** way a row trigger doesn't do any unnecessary
			** setup work.
			*/
			if (ps == null || recompile)
			{
                // The SPS activation will set its parent activation from
                // the statement context. Reset it to the original parent
                // activation first so that it doesn't use the activation of
                // the previously executed SPS as parent. DERBY-6348.
                lcc.getStatementContext().setActivation(activation);

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

                // Cache the prepared statement and activation in case the
                // trigger fires multiple times.
                if (isWhen) {
                    whenPS = ps;
                    spsWhenActivation = spsActivation;
                } else {
                    actionPS = ps;
                    spsActionActivation = spsActivation;
                }
			}

			// save the active statement context for exception handling purpose
//IC see: https://issues.apache.org/jira/browse/DERBY-2195
			StatementContext active_sc = lcc.getStatementContext();
			
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
                // This is a substatement; for now, we do not set any timeout
                // for it. We might change this behaviour later, by linking
                // timeout to its parent statement's timeout settings.
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
				ResultSet rs = ps.executeSubStatement
					(activation, spsActivation, false, 0L);

                if (isWhen)
                {
                    // This is a WHEN clause. Expect a single BOOLEAN value
                    // to be returned.
                    ExecRow row = rs.getNextRow();
                    if (SanityManager.DEBUG && row.nColumns() != 1) {
                        SanityManager.THROWASSERT(
                            "Expected WHEN clause to have exactly "
                            + "one column, found: " + row.nColumns());
                    }

                    DataValueDescriptor value = row.getColumn(1);
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(value instanceof SQLBoolean);
                    }

                    whenClauseWasTrue =
                            !value.isNull() && value.getBoolean();

                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(rs.getNextRow() == null,
                                "WHEN clause returned more than one row");
                    }
                }
                else if (rs.returnsRows())
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
				/* 
				** When a trigger SPS action is executed and results in 
				** an exception, the system needs to clean up the active 
				** statement context(SC) and the trigger execution context
				** (TEC) in language connection context(LCC) properly (e.g.:  
				** "Maximum depth triggers exceeded" exception); otherwise, 
				** this will leave old TECs lingering and may result in 
				** subsequent statements within the same connection to throw 
				** the same exception again prematurely.  
				**    
				** A new statement context will be created for the SPS before
				** it is executed.  However, it is possible for some 
				** StandardException to be thrown before a new statement 
				** context is pushed down to the context stack; hence, the 
				** trigger executor needs to ensure that the current active SC 
				** is associated with the SPS, so that it is cleaning up the 
				** right statement context in LCC. 
                **
                ** It is also possible that the error has already been handled
                ** on a lower level, especially if the trigger re-enters the
                ** JDBC layer. In that case, the current SC will be null.
				**    
				** When the active SC is cleaned up, the TEC will be removed
				** from LCC and the SC object will be popped off from the LCC 
				** as part of cleanupOnError logic.  
				 */
				
				/* retrieve the current active SC */
				StatementContext sc = lcc.getStatementContext();
				
				/* make sure that the cleanup is on the new SC */
//IC see: https://issues.apache.org/jira/browse/DERBY-5736
				if (sc != null && active_sc != sc)
				{
					sc.cleanupOnError(e);
				}
				
				/* Handle dynamic recompiles */
				if (e.getMessageId().equals(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE))
				{
					recompile = true;
					sps.revalidate(lcc);
					continue;
				}
				
				spsActivation.close();
				throw e;
			}

			/* Done with execution without any recompiles */
            return whenClauseWasTrue;
		}
	}

	/**
     * Cleanup after executing the SPS for the WHEN clause and trigger action.
	 *
	 * @exception StandardException on error
	 */
	protected void clearSPS() throws StandardException
	{
        if (spsActionActivation != null) {
            spsActionActivation.close();
        }
        actionPS = null;
        spsActionActivation = null;

        if (spsWhenActivation != null) {
            spsWhenActivation.close();
        }
        whenPS = null;
        spsWhenActivation = null;
	}

    /**
     * <p>
     * Execute the WHEN clause SPS and the trigger action SPS.
     * </p>
     *
     * <p>
     * If there is no WHEN clause, the trigger action should always be
     * executed. If there is a WHEN clause, the trigger action should only
     * be executed if the WHEN clause returns TRUE.
     * </p>
     *
     * @throws StandardException if trigger execution fails
     */
    final void executeWhenClauseAndAction() throws StandardException {
        SPSDescriptor whenClauseDescriptor = getWhenClause();
        if (whenClauseDescriptor == null ||
                executeSPS(whenClauseDescriptor, true)) {
            executeSPS(getAction(), false);
        }
    }
} 
