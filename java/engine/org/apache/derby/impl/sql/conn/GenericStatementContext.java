/*

   Derby - Class org.apache.derby.impl.sql.conn.GenericStatementContext

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.services.context.Context;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.context.ContextImpl;

import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.reference.SQLState;
import java.util.ArrayList;
import java.util.Iterator;
import java.sql.SQLException;

/**
 * GenericStatementContext is pushed/popped around a statement prepare and execute
 * so that any statement specific clean up can be performed.
 *
 * @author jerry
 *
 */
final class GenericStatementContext 
	extends ContextImpl implements StatementContext
{
	private final TransactionController tc;

	private boolean		setSavePoint;
	private String		internalSavePointName;
	private ResultSet	topResultSet;
	private ArrayList		dependencies;
	private NoPutResultSet[] subqueryTrackingArray;
	private NoPutResultSet[] materializedSubqueries;
	private	final LanguageConnectionContext lcc;
	private boolean		inUse = true;
    private	boolean		parentInTrigger;	// whetherparent started with a trigger on stack
    private	boolean		isAtomic;	
	private boolean		isSystemCode;
	private boolean		rollbackParentContext;
    private	String		stmtText;
    private	ParameterValueSet			pvs;

	/**
		Set to one of RoutineAliasInfo.{MODIFIES_SQL_DATA, READS_SQL_DATA, CONTAINS_SQL, NO_SQL}
	*/
	private short			sqlAllowed = -1;

	/*
	   constructor
		@param tc transaction
	*/
	GenericStatementContext(LanguageConnectionContext lcc, TransactionController tc) 
	{
		super(lcc.getContextManager(), org.apache.derby.iapi.reference.ContextId.LANG_STATEMENT);
		this.lcc = lcc;
		this.tc = tc;

		internalSavePointName = "ISSP" + hashCode();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((lcc != null),
					"Failed to get language connection context");
		}

	}

	// StatementContext Interface

	public void setInUse
	( 
		boolean parentInTrigger,
		boolean isAtomic, 
		String stmtText,
		ParameterValueSet pvs
	) 
	{
		inUse = true;

		this.parentInTrigger = parentInTrigger;
		this.isAtomic = isAtomic;
		this.stmtText = stmtText;
		this.pvs = pvs;
		rollbackParentContext = false;
	}

	public void clearInUse() {
		/* We must clear out the current top ResultSet to prepare for
		 * reusing a StatementContext.
		 */
		stuffTopResultSet( null, null );
		inUse = false;

		parentInTrigger = false;
		isAtomic = false;
		this.stmtText = null;
		sqlAllowed = -1;
		isSystemCode = false;
		rollbackParentContext = false;
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
		

		// RESOLVE PLUGIN ???. For the plugin, there will be no transaction controller
		if ( tc != null ) { tc.setSavePoint(internalSavePointName, null); }
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
			if ( tc != null ) { tc.setSavePoint(internalSavePointName, null); }
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
		if ( tc != null ) { tc.releaseSavePoint(internalSavePointName, null); }
		setSavePoint = false;
	}

	/**
	 * Set the top ResultSet in the ResultSet tree for close down on
	 * an error.
	 *
	 * @exception StandardException thrown on error.
	 * @return Nothing.
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
					if (this.subqueryTrackingArray[index] != null)
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
	  *
	  * @return Nothing.
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
	 * @return Nothing.
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
	 * @return Nothing.
	 * @exception StandardException thrown on error.
	 */
	public void addDependency(Dependency dy)
		throws StandardException
	{
		pleaseBeOnStack();
		
		if (dependencies == null)
		{
			dependencies = new ArrayList();
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

		/*
		** If it isn't a StandardException, then assume
		** xact severity.  It is probably an unexpected
		** java error somewhere in the language.
		*/
		int severity = (error instanceof StandardException) ?
			((StandardException) error).getSeverity() :
			ExceptionSeverity.STATEMENT_SEVERITY;


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

			for (Iterator iterator = dependencies.iterator(); iterator.hasNext(); ) 
			{
				Dependency dy = (Dependency) iterator.next();
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
	}

	/**
	 * @see Context#isLastHandler
	 */
	public boolean isLastHandler(int severity)
	{
		return inUse && !rollbackParentContext && ((severity == ExceptionSeverity.STATEMENT_SEVERITY) ||
						(severity == ExceptionSeverity.NO_APPLICABLE_SEVERITY));
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

			if (lcc.getLogStatementText() && (pvs != null) && pvs.getParameterCount() > 0)
			{
				String pvsString = " with " + pvs.getParameterCount() +
						" parameters " + pvs.toString();
				sb.append(pvsString);
			}
		}
		return sb;

	}
}
