/*

   Derby - Class org.apache.derby.impl.sql.conn.GenericStatementContext

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

package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.services.context.Context;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.timer.TimerFactory;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.conn.SQLSessionContext;

import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.services.context.ContextImpl;

import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.reference.SQLState;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TimerTask;

/**
 * GenericStatementContext is pushed/popped around a statement prepare and execute
 * so that any statement specific clean up can be performed.
 *
 *
 */
final class GenericStatementContext 
	extends ContextImpl implements StatementContext
{
	private boolean		setSavePoint;
	private String		internalSavePointName;
	private ResultSet	topResultSet;
	private ArrayList<Dependency>		dependencies;
	private NoPutResultSet[] subqueryTrackingArray;
	private NoPutResultSet[] materializedSubqueries;
	private	final LanguageConnectionContext lcc;
	private boolean		inUse = true;

    // This flag satisfies all the conditions
    // for using volatile instead of synchronized.
    // (Source: Doug Lea, Concurrent Programming in Java, Second Edition,
    // section 2.2.7.4, page 97)
    // true if statement has been cancelled
    private volatile boolean cancellationFlag = false;

    // Reference to the TimerTask that will time out this statement.
    // Needed for stopping the task when execution completes before timeout.
    private CancelQueryTask cancelTask = null;
        
    private	boolean		parentInTrigger;	// whetherparent started with a trigger on stack
    private	boolean		isForReadOnly = false;	
    private	boolean		isAtomic;	
	private boolean		isSystemCode;
	private boolean		rollbackParentContext;
    private boolean     statementWasInvalidated;
    private	String		stmtText;
    private	ParameterValueSet			pvs;

	/**
		Set to one of RoutineAliasInfo.{MODIFIES_SQL_DATA, READS_SQL_DATA, CONTAINS_SQL, NO_SQL}
	*/
	private short			sqlAllowed = -1;

	/**
	 * The activation associated with this context, or null
	 */
	private Activation activation;

	/**
	 * The SQLSessionContext associated with a statement context.
	 */
	private SQLSessionContext sqlSessionContext;

	/*
	   constructor
		@param tc transaction
	*/
	GenericStatementContext(LanguageConnectionContext lcc) 
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
		super(lcc.getContextManager(), org.apache.derby.shared.common.reference.ContextId.LANG_STATEMENT);
		this.lcc = lcc;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((lcc != null),
					"Failed to get language connection context");
		}

        internalSavePointName = lcc.getUniqueSavepointName();
	}

    /**
     * This is a TimerTask that is responsible for timing out statements,
     * typically when an application has called Statement.setQueryTimeout().
     *
     * When the application invokes execute() on a statement object, or
     * fetches data on a ResultSet, a StatementContext object is allocated
     * for the duration of the execution in the engine (until control is
     * returned to the application).
     *
     * When the StatementContext object is assigned with setInUse(),
     * a CancelQueryTask is scheduled if a timeout &gt; 0 has been set.
     */
    private static class CancelQueryTask
//IC see: https://issues.apache.org/jira/browse/DERBY-31
        extends
            TimerTask
    {
        /**
         * Reference to the StatementContext for the executing statement
         * which might time out.
         */
        private StatementContext statementContext;

        /**
         * Initializes a new task for timing out a statement's execution.
         * This does not schedule it for execution, the caller is
         * responsible for calling Timer.schedule() with this object
         * as parameter.
         */
        public CancelQueryTask(StatementContext ctx)
        {
            statementContext = ctx;
        }

        /**
         * Invoked by a Timer class to cancel an executing statement.
         * This method just sets a volatile flag in the associated
         * StatementContext object by calling StatementContext.cancel();
         * it is the responsibility of the thread executing the statement
         * to check this flag regularly.
         */
        public void run()
        {
            synchronized (this) {
                if (statementContext != null) {
                    statementContext.cancel();
                }
            }
        }

        /**
         * Stops this task and prevents it from cancelling a statement.
         * Guarantees that after this method returns, the associated
         * StatementContext object will not be tampered with by this task.
         * Thus, the StatementContext object may safely be allocated to
         * other executing statements.
         */
        public void forgetContext() {
            synchronized (this) {
                statementContext = null;
            }
            getTimerFactory().cancel(this);
        }
    }

    private static TimerFactory getTimerFactory() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return GenericLanguageConnectionFactory.getMonitor().getTimerFactory();
    }

	// StatementContext Interface

	public void setInUse
	( 
		boolean parentInTrigger,
		boolean isAtomic, 
                boolean isForReadOnly,
		String stmtText,
//IC see: https://issues.apache.org/jira/browse/DERBY-31
		ParameterValueSet pvs,
        long timeoutMillis
	) 
	{
		inUse = true;

		this.parentInTrigger = parentInTrigger;
//IC see: https://issues.apache.org/jira/browse/DERBY-231
		this.isForReadOnly = isForReadOnly;
		this.isAtomic = isAtomic;
		this.stmtText = stmtText;
		this.pvs = pvs;
		rollbackParentContext = false;
        if (timeoutMillis > 0) {
            cancelTask = new CancelQueryTask(this);
            getTimerFactory().schedule(cancelTask, timeoutMillis);
        }
	}

	public void clearInUse() {
		/* We must clear out the current top ResultSet to prepare for
		 * reusing a StatementContext.
		 */
		stuffTopResultSet( null, null );
		inUse = false;

		parentInTrigger = false;
		isAtomic = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-231
		isForReadOnly = false;
		this.stmtText = null;
		sqlAllowed = -1;
		isSystemCode = false;
		rollbackParentContext = false;
        statementWasInvalidated = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-4849

//IC see: https://issues.apache.org/jira/browse/DERBY-31
        if (cancelTask != null) {
            cancelTask.forgetContext();
            cancelTask = null;
        }
        cancellationFlag = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
        activation = null;
		sqlSessionContext = null;
    }

	/**
	 * @see StatementContext#setSavePoint
	 * @exception StandardException Thrown on error
	 */
	public void setSavePoint() throws StandardException {
		
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("traceSavepoints"))
			{
				SanityManager.DEBUG_PRINT(
									"GenericStatementContext.setSavePoint()",
									internalSavePointName);
			}
		}
			
		pleaseBeOnStack();
		

		lcc.getTransactionExecute().setSavePoint(internalSavePointName, null);
		setSavePoint = true;
	}

	/**
	 * Resets the savepoint to the current spot if it is
	 * set, otherwise, noop.  Used when a commit is
	 * done on a nested connection.
	 *
	 * @see StatementContext#resetSavePoint
	 * @exception StandardException Thrown on error
	 */
	public void resetSavePoint() throws StandardException {
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("traceSavepoints"))
			{
				SanityManager.DEBUG_PRINT(
					"GenericStatementContext.resetSavePoint()",
					internalSavePointName);
			}
		}
			
		if (inUse && setSavePoint)
		{		
			// RESOLVE PLUGIN ???. For the plugin, there will be no transaction controller
			lcc.getTransactionExecute().setSavePoint(internalSavePointName, null);
			// stage buffer management
		}
	}

	/**
	 * @see StatementContext#clearSavePoint
	 * @exception StandardException Thrown on error
	 */
	public void clearSavePoint() throws StandardException {

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("traceSavepoints"))
			{
				SanityManager.DEBUG_PRINT("GenericStatementContext.clearSavePoint()",
										  internalSavePointName);
			}
		}

		pleaseBeOnStack();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(setSavePoint, "setSavePoint is expected to be true");
		}

		// RESOLVE PLUGIN ???. For the plugin, there will be no transaction controller
		lcc.getTransactionExecute().releaseSavePoint(internalSavePointName, null);
		setSavePoint = false;
	}

	/**
	 * Set the top ResultSet in the ResultSet tree for close down on
	 * an error.
	 *
	 * @exception StandardException thrown on error.
	 */
	public void setTopResultSet(ResultSet topResultSet, 
							    NoPutResultSet[] subqueryTrackingArray)
		 throws StandardException
	{
		pleaseBeOnStack();

		/* We have to handle both materialize and non-materialized subqueries.
		 * Materialized subqueries are attached before the top result set is 
		 * set.  If there are any, then we must copy them into the new
		 * subqueryTrackingArray.
		 */
		if (materializedSubqueries != null)
		{
			// Do the merging into the passed in array.
			if (subqueryTrackingArray != null)
			{
				if (SanityManager.DEBUG)
				{
					if (this.materializedSubqueries.length != subqueryTrackingArray.length)
					{
						SanityManager.THROWASSERT(
							"this.ms.length (" + this.materializedSubqueries.length +
							") expected to = sta.length(" + subqueryTrackingArray.length +
							")");
					}
				}
				for (int index = 0; index < subqueryTrackingArray.length; index++)
				{
                    if (this.materializedSubqueries[index] != null)
					{
						subqueryTrackingArray[index] = this.materializedSubqueries[index];
					}
				}
			}
			else
			{
				subqueryTrackingArray = this.materializedSubqueries;
			}
			materializedSubqueries = null;
		}

		stuffTopResultSet( topResultSet, subqueryTrackingArray );
	}

	/**
	  *	Private minion of setTopResultSet() and clearInUse()
	  *
	  *	@param	topResultSet	make this the top result set
	  *	@param	subqueryTrackingArray	where to keep track of subqueries in this statement
	  */
	private	void	stuffTopResultSet(ResultSet topResultSet, 
									  NoPutResultSet[] subqueryTrackingArray)
	{
		this.topResultSet = topResultSet;
		this.subqueryTrackingArray = subqueryTrackingArray;
		dependencies = null;
	}


	/**
	 * Set the appropriate entry in the subquery tracking array for
	 * the specified subquery.
	 * Useful for closing down open subqueries on an exception.
	 *
	 * @param subqueryNumber	The subquery # for this subquery
	 * @param subqueryResultSet	The ResultSet at the top of the subquery
	 * @param numSubqueries		The total # of subqueries in the entire query
	 *
	 * @exception StandardException thrown on error.
	 */
	public void setSubqueryResultSet(int subqueryNumber,
									 NoPutResultSet subqueryResultSet,
									 int numSubqueries)
		throws StandardException
	{
		pleaseBeOnStack();
		
		/* NOTE: In degenerate cases, it is possible that there is no top
		 * result set.  For example:
		 *		call (select 1 from systables).valueOf('111');
		 * In that case, we allocate our own subquery tracking array on
		 * each call. (Gross!)
		 * (Trust me, this is only done in degenerate cases.  The tests passed,
		 * except for the degenerate cases, before making this change, so we
		 * know that the top result set and array reuse is working for
		 * the non-degenerate cases.)
		 */
		if (subqueryTrackingArray == null)
		{
			if (topResultSet == null)
			{
				subqueryTrackingArray = new NoPutResultSet[numSubqueries];
				materializedSubqueries = new NoPutResultSet[numSubqueries];
			}
			else
			{
				subqueryTrackingArray = 
					topResultSet.getSubqueryTrackingArray(numSubqueries);
			}
		}
		subqueryTrackingArray[subqueryNumber] = subqueryResultSet;
		if (materializedSubqueries != null)
		{
			materializedSubqueries[subqueryNumber] = subqueryResultSet;
		}
	}

	/**
	 * Get the subquery tracking array for this query.
	 * (Useful for runtime statistics.)
	 *
	 * @return NoPutResultSet[]	The	(sparse) array of tops of subquery ResultSet trees
	 * @exception StandardException thrown on error.
	 */
	public NoPutResultSet[] getSubqueryTrackingArray()
		throws StandardException
	{
		pleaseBeOnStack();
		
		return subqueryTrackingArray;
	}

	/**
	 * Track a Dependency within this StatementContext.
	 * (We need to clear any dependencies added within this
	 * context on an error.
	 *
	 * @param dy	The dependency to track.
	 *
	 * @exception StandardException thrown on error.
	 */
	public void addDependency(Dependency dy)
		throws StandardException
	{
		pleaseBeOnStack();
		
		if (dependencies == null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			dependencies = new ArrayList<Dependency>();
		}
		dependencies.add(dy);
	}

	/**
	 * Returns whether we started from within the context of a trigger
	 * or not.
	 *
	 * @return	true if we are in a trigger context
	 */
	public	boolean	inTrigger()
	{
		return	parentInTrigger;
	}

	//
	// Context interface
	//
	/**
	 * Close down the top ResultSet, if relevant, and rollback to the
	 * internal savepoint, if one was set.
	 *
	 * @exception StandardException thrown on error. REVISIT: don't want
	 * cleanupOnError's to throw exceptions.
	 */
	public void cleanupOnError(Throwable error) throws StandardException
	{

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("traceSavepoints"))
			{
				SanityManager.DEBUG_PRINT(
						"GenericStatementContext.cleanupOnError()",
						String.valueOf( hashCode() ) );
			}
		}

		try {
		/*
		** If it isn't a StandardException, then assume
		** session severity.  It is probably an unexpected
		** java error somewhere in the language.
        ** Store layer treats JVM error as session severity, 
        ** hence to be consistent and to avoid getting rawstore
        ** protocol violation errors, we treat java errors here
        ** to be of session severity.  
        */
        int severity = ExceptionSeverity.SESSION_SEVERITY;
//IC see: https://issues.apache.org/jira/browse/DERBY-4849
        if (error instanceof StandardException) {
            StandardException se = (StandardException)error;
            // Update the severity.
            severity = se.getSeverity();
            // DERBY-4849: Remember that the plan was invalidated, such that
            // we can avoid performing certain actions more than once
            // (for correctness, not optimization).
            if (SQLState.LANG_STATEMENT_NEEDS_RECOMPILE.equals(
                    se.getMessageId())) {
                statementWasInvalidated = true;
            }
        }


		/**
		 * Don't clean up this statement context if it's not in use.
		 * This can happen if you get an error while calling one of
		 * the JDBC getxxxx() methods on a ResultSet, since no statement
		 * context is pushed when those calls occur.
		 */
		if (! inUse)
		{
			return;
		}

		/* Clean up the ResultSet, if one exists */
		if (topResultSet != null)
		{
			topResultSet.cleanUp();
		}

		/* Close down any open subqueries */
		if (subqueryTrackingArray != null)
		{
			for (int index = 0; index < subqueryTrackingArray.length; index++)
			{
				/* Remember, the array is sparse, so only check
				 * non-null entries.
				 */
				if (subqueryTrackingArray[index] != null)
				{
					subqueryTrackingArray[index].cleanUp();
				}
			}
		}

		/* Clean up any dependencies */
		if (dependencies != null)
		{
			DependencyManager dmgr = lcc.getDataDictionary().getDependencyManager();

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			for (Iterator<Dependency> iterator = dependencies.iterator(); iterator.hasNext(); ) 
			{
				Dependency dy = iterator.next();
				dmgr.clearInMemoryDependency(dy);
			}

			dependencies = null;
		}

		if (severity <= ExceptionSeverity.STATEMENT_SEVERITY
			&& setSavePoint)
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("traceSavepoints"))
				{
					SanityManager.DEBUG_PRINT(
						"GenericStatementContext.cleanupOnError",
						"rolling back to: " + internalSavePointName);
				}
			}

			lcc.internalRollbackToSavepoint( internalSavePointName, false, null);

			clearSavePoint();
		}

		if (severity >= ExceptionSeverity.TRANSACTION_SEVERITY )
		{
			// transaction severity errors roll back the transaction.

			/*
			** We call clearSavePoint() above only for statement errors.
			** We don't call clearSavePoint() for transaction errors because
			** the savepoint will be rolled back anyway.  So in this case,
			** we need to indicate that the savepoint is not set.
			*/
			setSavePoint = false;
		}

		/* Pop the context */
		lcc.popStatementContext(this, error);
		} catch(Exception ex) {
			//DERBY-6722(GenericStatementContext.cleanupOnError()  
			//needs protection from later errors during statement 
			//cleanup
			ex.initCause(error);
			throw StandardException.unexpectedUserException(ex);
		}
	}

	/**
	 * @see Context#isLastHandler
	 */
	public boolean isLastHandler(int severity)
	{
        // For JVM errors, severity gets mapped to 
        // ExceptionSeverity.NO_APPLICABLE_SEVERITY
        // in ContextManager.cleanupOnError. It is necessary to 
        // let outer contexts take corrective action for jvm errors, so 
        // return false as this will not be the last handler for such 
        // errors.
//IC see: https://issues.apache.org/jira/browse/DERBY-1732
		return inUse && !rollbackParentContext && 
            ( severity == ExceptionSeverity.STATEMENT_SEVERITY );
	}

	/**
	  *	Reports whether this StatementContext is on the context stack.
	  *
	  *	@return	true if this StatementContext is on the context stack. false otherwise.
	  */
    public	boolean	onStack() { return inUse; }

	/**
	 * Indicates whether the statement needs to be executed atomically
	 * or not, i.e., whether a commit/rollback is permitted by a
 	 * connection nested in this statement.
	 *
	 * @return true if needs to be atomic
	 */
	public boolean isAtomic()
	{
		return isAtomic;
	}

	/**
	 * Return the text of the current statement.
	 * Note that this may be null.  It is currently
	 * not set up correctly for ResultSets that aren't
	 * single row result sets (e.g SELECT), replication,
	 * and setXXXX/getXXXX jdbc methods.
	 *
	 * @return the statement text
	 */
	public String getStatementText()
	{
		return stmtText;
	}

	//
	// class implementation
	//

	/**
	  *	Raise an exception if this Context is not in use, that is, on the
	  * Context Stack.
	  *
	  * @exception StandardException thrown on error.
	  */
	private	void	pleaseBeOnStack() throws StandardException
	{
		if ( !inUse ) { throw StandardException.newException(SQLState.LANG_DEAD_STATEMENT); }
	}

	public boolean inUse()
	{
		return inUse;
	}
    public boolean isForReadOnly()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-231
	return isForReadOnly;
    }
        
    /**
     * Tests whether the statement which has allocated this StatementContext
     * object has been cancelled. This method is typically called from the
     * thread which is executing the statement, to test whether execution
     * should continue or stop.
     *
     * @return whether the statement which has allocated this StatementContext
     *  object has been cancelled.
     */
    public boolean isCancelled()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-31
        return cancellationFlag;
    }

    /**
     * Cancels the statement which has allocated this StatementContext object.
     * This is done by setting a flag in the StatementContext object. For
     * this to have any effect, it is the responsibility of the executing
     * statement to check this flag regularly.
     */
    public void cancel()
    {
        cancellationFlag = true;
    }

	public void setSQLAllowed(short allow, boolean force) {

		// cannot override a stricter setting.
		// -1 is no routine restriction in place
		// 0 is least restrictive
		// 4 is most
		if (force || (allow > sqlAllowed))
			sqlAllowed = allow;

	}
	public short getSQLAllowed() {
		if (!inUse)
			return org.apache.derby.catalog.types.RoutineAliasInfo.NO_SQL;

		return sqlAllowed;
	}

	/**
	 * Indicate that, in the event of a statement-level exception,
	 * this context is NOT the last one that needs to be rolled
	 * back--rather, it is nested within some other statement
	 * context, and that other context needs to be rolled back,
	 * too.
	*/
	public void setParentRollback() {
		rollbackParentContext = true;
	}

	/**
		Set to indicate statement is system code.
		For example a system procedure, view, function etc.
	*/
	public void setSystemCode() {
		isSystemCode = true;
	}

	/**
		Return true if this statement is system code.
	*/
	public boolean getSystemCode() {
		return isSystemCode;
	}

	public StringBuffer appendErrorInfo() {

		StringBuffer sb = ((ContextImpl) lcc).appendErrorInfo();
		if (sb != null) {

			sb.append("Failed Statement is: ");

			sb.append(getStatementText());

//IC see: https://issues.apache.org/jira/browse/DERBY-2606
			if ((pvs != null) && pvs.getParameterCount() > 0)
			{
				String pvsString = " with " + pvs.getParameterCount() +
						" parameters " + pvs.toString();
				sb.append(pvsString);
			}
		}
		return sb;

	}

	/**
	 * @see StatementContext#setActivation(Activation a)
	 */
	public void setActivation(Activation a) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
		activation = a;
	}

	/**
	 * @see StatementContext#getActivation
	 */
	public Activation getActivation() {
		return activation;
	}

	/**
	 * @see StatementContext#getSQLSessionContext
	 */
	public SQLSessionContext getSQLSessionContext() {
		return sqlSessionContext;
	}

	/**
	 * @see StatementContext#setSQLSessionContext(SQLSessionContext ctx)
	 */
	public void setSQLSessionContext(SQLSessionContext ctx) {
		sqlSessionContext = ctx;
	}

    public boolean getStatementWasInvalidated() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4849
        return statementWasInvalidated;
    }
}
