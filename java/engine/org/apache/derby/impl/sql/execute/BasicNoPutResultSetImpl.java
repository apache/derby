/*

   Derby - Class org.apache.derby.impl.sql.execute.BasicNoPutResultSetImpl

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.Activation;


import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.sql.Timestamp;
import java.sql.SQLWarning;

/**
 * Abstract ResultSet for for operations that return rows but
 * do not allow the caller to put data on output pipes. This
 * basic implementation does not include support for an Activiation.
 * See NoPutResultSetImpl.java for an implementaion with support for
 * an activiation.
 * <p>
 * This abstract class does not define the entire ResultSet
 * interface, but leaves the 'get' half of the interface
 * for subtypes to implement. It is package-visible only,
 * with its methods being public for exposure by its subtypes.
 * <p>
 */
abstract class BasicNoPutResultSetImpl
implements NoPutResultSet
{
	/* Modified during the life of this object */
    protected boolean isOpen;
    protected boolean finished;
	protected ExecRow	  currentRow;
	protected boolean isTopResultSet;
	protected LanguageConnectionContext	lcc;
	private SQLWarning	warnings;

	/* Run time statistics variables */
	public int numOpens;
	public int rowsSeen;
	public int rowsFiltered;
	protected long startExecutionTime;
	protected long endExecutionTime;
	public long beginTime;
	public long constructorTime;
	public long openTime;
	public long nextTime;
	public long closeTime;

	public double optimizerEstimatedRowCount;
	public double optimizerEstimatedCost;

	// set on demand during execution
	private StatementContext			statementContext;
	public NoPutResultSet[]			subqueryTrackingArray;
	ExecRow compactRow;

	// Set in the constructor and not modified
	protected Activation	    activation;
	private boolean				statisticsTimingOn;

	ResultDescription resultDescription;

	private transient ExecutionFactory			exFactory;
	private transient TransactionController	tc;

	private int[] baseColumnMap;

	/**
	 *  Constructor.
	    <BR>
		Sets beginTime for all children to use to measue constructor time.
	 *
	 *  @param  resultDescription the result description. May be null.
	 *	@param	activation			The activation
	 *	@param	optimizerEstimatedRowCount	The optimizer's estimate of the
	 *										total number of rows for this
	 *										result set
	 *	@param	optimizerEstimatedCost		The optimizer's estimated cost for
	 *										this result set
	 */
	BasicNoPutResultSetImpl(ResultDescription resultDescription,
							Activation activation,
							double optimizerEstimatedRowCount,
							double optimizerEstimatedCost)
	{
		this.activation = activation;
		statisticsTimingOn = (activation != null && getLanguageConnectionContext().getStatisticsTiming());
		beginTime = startExecutionTime = getCurrentTimeMillis();
		this.resultDescription = resultDescription;
		this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
		this.optimizerEstimatedCost = optimizerEstimatedCost;
	}

	// NoPutResultSet interface

	/**
	 * @see NoPutResultSet#openCore
	 * @exception StandardException thrown if cursor finished.
     */
	public abstract void openCore() throws StandardException;

	/**
	 * This is the default implementation of reopenCore().
	 * It simply does a close() followed by an open().  If
	 * there are optimizations to be made (caching, etc), this
	 * is a good place to do it -- this will be overridden
	 * by a number of resultSet imlementations.  and SHOULD
	 * be overridden by any node that can get between a base
	 * table and a join.
	 *
	 * @see NoPutResultSet#openCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public void reopenCore() throws StandardException
	{
		close();
		openCore();	
	}

	/**
	 * @see NoPutResultSet#getNextRowCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public abstract ExecRow	getNextRowCore() throws StandardException;

	/**
	 * @see NoPutResultSet#getPointOfAttachment
	 */
	public int getPointOfAttachment()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getPointOfAttachment() not expected to be called for " +
				getClass().getName());
		}
		return -1;
	}

	/**
	 * Mark the ResultSet as the topmost one in the ResultSet tree.
	 * Useful for closing down the ResultSet on an error.
	 *
	 * @return Nothing.
	 */
	public void markAsTopResultSet()
	{
		isTopResultSet = true;
	}

	/**
	 * @see NoPutResultSet#getScanIsolationLevel
	 */
	public int getScanIsolationLevel()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getScanIsolationLevel() not expected to be called for " +
				getClass().getName());
		}
		return 0;
	}

	/** @see NoPutResultSet#getEstimatedRowCount */
	public double getEstimatedRowCount()
	{
		return optimizerEstimatedRowCount;
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"requiresRelocking() not expected to be called for " +
				getClass().getName());
		}
		return false;
	}

	// ResultSet interface

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * NOTE: This method should only be called on the top ResultSet
	 * of a ResultSet tree to ensure that the entire ResultSet
	 * tree gets closed down on an error.  the openCore() method
	 * will be called for all other ResultSets in the tree.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public final void	open() throws StandardException 
	{
		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
				SanityManager.THROWASSERT(
				this + "expected to be the top ResultSet");
		}

		finished = false;

		attachStatementContext();

		try {

			openCore();

		} catch (StandardException se) {
			activation.checkStatementValidity();
			throw se;
		}

		activation.checkStatementValidity();
	}

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getAbsoluteRow(int row) throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, ABSOLUTE);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getAbsoluteRow() not expected to be called for " + getClass().getName());
		}

		return null;
	}

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getRelativeRow(int row) throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, RELATIVE);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getRelativeRow() not expected to be called for " + getClass().getName());
		}

		return null;
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setBeforeFirstRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, FIRST);
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"setBeforeFirstRow() not expected to be called for " + getClass().getName());
		}

		return null;
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     */
    public boolean checkRowPosition(int isType) throws StandardException
	{
		return false;
	}

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).
	 *
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	public int getRowNumber()
	{
		return 0;
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getFirstRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, FIRST);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getFirstRow() not expected to be called for " + getClass().getName());
		}

		return null;
	}

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * NOTE: This method should only be called on the top ResultSet
	 * of a ResultSet tree to ensure that the entire ResultSet
	 * tree gets closed down on an error.  the getNextRowCore() method
	 * will be called for all other ResultSets in the tree.
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public final ExecRow	getNextRow() throws StandardException 
	{
	    if ( ! isOpen ) {
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, NEXT);
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
				SanityManager.THROWASSERT(
				this + "expected to be the top ResultSet");
		}

		attachStatementContext();

		return getNextRowCore();
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getPreviousRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, PREVIOUS);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getPreviousRow() not expected to be called.");
		}

		return null;
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getLastRow()
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, LAST);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getLastRow() not expected to be called.");
		}

		return null;
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setAfterLastRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, LAST);
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"setAfterLastRow() not expected to be called.");
		}

		return null;
	}


    /**
     * Returns true.
	 */
	 public boolean	returnsRows() { return true; }

	public final int	modifiedRowCount() { return 0; }

	/**
     * Clean up on error
	 * @exception StandardException		Thrown on failure
	 *
	 */
	public void	cleanUp() throws StandardException
	{
		if (isOpen) {
			close();
		}
	}

	/**
		Report if closed.
	 */
	public boolean	isClosed() {
	    return ( ! isOpen );
	}

	public void	finish() throws StandardException
	{
		finishAndRTS();
	}

	/**
	 * @exception StandardException on error
	 */	
	protected final void finishAndRTS() throws StandardException
	{

		if (!finished) {
			/*
			** If run time statistics tracing is turned on, then now is the
			** time to dump out the information.
			*/
			if (isTopResultSet) {

				LanguageConnectionContext lcc = getLanguageConnectionContext();
				if (lcc.getRunTimeStatisticsMode())
				{
					endExecutionTime = getCurrentTimeMillis();

					lcc.setRunTimeStatisticsObject(
						lcc.getExecutionContext().getResultSetStatisticsFactory().getRunTimeStatistics(activation, this, subqueryTrackingArray));

					HeaderPrintWriter istream = lcc.getLogQueryPlan() ? Monitor.getStream() : null;
					if (istream != null)
					{
						istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
												  lcc.getTransactionExecute().getTransactionIdString() +
												  "), " +
												  LanguageConnectionContext.lccStr +
												  lcc.getInstanceNumber() +
												  "), " +
												  lcc.getRunTimeStatisticsObject().getStatementText() + " ******* " +
												  lcc.getRunTimeStatisticsObject().getStatementExecutionPlanText());
					}
				}

			}

			if (!isClosed())
				close();

			finished = true;

			if (isTopResultSet && activation.isSingleExecution())
				activation.close();
		}
	}

	/* The following methods are common to almost all sub-classes.
	 * They are overriden in selected cases.
	 */

	/**
     * Returns the description of the table's rows
	 */
	public ResultDescription getResultDescription() {
	    return resultDescription;
	}

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
		return getTimeSpent(ResultSet.ENTIRE_RESULTSET_TREE);
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		if (startExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(startExecutionTime);
		}
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		if (endExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(endExecutionTime);
		}
	}

	/**
	 * @see ResultSet#getSubqueryTrackingArray
	 */
	public final NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries)
	{
		if (subqueryTrackingArray == null)
		{
			subqueryTrackingArray = new NoPutResultSet[numSubqueries];
		}

		return subqueryTrackingArray;
	}

	/**
	 * Return the current time in milliseconds, if DEBUG and RunTimeStats is
	 * on, else return 0.  (Only pay price of system call if need to.)
	 *
	 * @return long		Current time in milliseconds.
	 */
	protected final long getCurrentTimeMillis()
	{
		if (statisticsTimingOn)
		{
			return System.currentTimeMillis();
		}
		else
		{
			return 0;
		}
	}

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
	 * Return the elapsed time in milliseconds, between now and the beginTime, if
	 * DEBUG and RunTimeStats is on, else return 0.  
	 * (Only pay price of system call if need to.)
	 *
	 * @return long		Elapsed time in milliseconds.
	 */

	protected final long getElapsedMillis(long beginTime)
	{
		if (statisticsTimingOn)
		{
			return (System.currentTimeMillis() - beginTime);
		}
		else
		{
			return 0;
		}
	}

	/**
	 * Dump out the time information for run time stats.
	 *
	 * @return Nothing.
	 */
	protected final String dumpTimeStats(String indent, String subIndent)
	{
		return 
			indent +
			  MessageService.getTextMessage(SQLState.LANG_TIME_SPENT_THIS) +
			  " " + getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY) + "\n" +
			indent +
			  MessageService.getTextMessage(
				SQLState.LANG_TIME_SPENT_THIS_AND_BELOW) +
			  " " + getTimeSpent(NoPutResultSet.ENTIRE_RESULTSET_TREE) + "\n" +
			indent +
			  MessageService.getTextMessage(
				SQLState.LANG_TOTAL_TIME_BREAKDOWN) + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_CONSTRUCTOR_TIME) +
			  " " + constructorTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_OPEN_TIME) +
			  " " + openTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_NEXT_TIME) +
			  " " + nextTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_CLOSE_TIME) +
			  " " + closeTime;
	}


	/**
	  *	Attach this result set to the top statement context on the stack.
	  *	Result sets can be directly read from the JDBC layer. The JDBC layer
	  * will push and pop a statement context around each ResultSet.getNext().
	  * There's no guarantee that the statement context used for the last
	  * getNext() will be the context used for the current getNext(). The
	  * last statement context may have been popped off the stack and so
	  *	will not be available for cleanup if an error occurs. To make sure
	  *	that we will be cleaned up, we always attach ourselves to the top	
	  *	context.
	  *
	  *	The fun and games occur in nested contexts: using JDBC result sets inside
	  * user code that is itself invoked from queries or CALL statements.
	  *
	  *
	  * @exception StandardException thrown if cursor finished.
	  */
	protected	void	attachStatementContext() throws StandardException
	{
		if (isTopResultSet)
		{
			if (statementContext == null || !statementContext.onStack() )
			{
				statementContext = getLanguageConnectionContext().getStatementContext();
			}
			statementContext.setTopResultSet(this, subqueryTrackingArray);
			// Pick up any materialized subqueries
			if (subqueryTrackingArray == null)
			{
				subqueryTrackingArray = statementContext.getSubqueryTrackingArray();
			}
		}

	}

	/**
	  *	Cache the language connection context. Return it.
	  *
	  *	@return	the language connection context
	  */
	protected	final LanguageConnectionContext	getLanguageConnectionContext()
	{
		if ( lcc == null )
		{
			/* We don't always have an activation.  Get the LCC
			 * from the activation when we have one.
			 */
			if (activation != null)
			{
				lcc = activation.getLanguageConnectionContext();
			}
			else
			{
				lcc = (LanguageConnectionContext) ContextService.getContext(LanguageConnectionContext.CONTEXT_ID); 
			}
		}

		return	lcc;
	}

	/** @see NoPutResultSet#resultSetNumber() */
	public int resultSetNumber() {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT(
				"resultSetNumber() should not be called on a " +
				this.getClass().getName()
				);
		}

		return 0;
	}

	//////////////////////////////////////////////////////
	//
	// UTILS	
	//
	//////////////////////////////////////////////////////

	/**
	 * Get a execution factory
	 *
	 * @return the execution factory
	 */
	final ExecutionFactory getExecutionFactory() 
	{
		if (exFactory == null) {
			exFactory = activation.getExecutionFactory();
		}
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(exFactory!=null,"unable to get execution factory");
		return exFactory;
	}

	/**
	 * Get the current transaction controller.
	 *
	 */
  	final TransactionController getTransactionController()
 	{
  		if (tc == null)
  		{
			tc = getLanguageConnectionContext().getTransactionExecute();
  		}
  		return tc;
  	}

	/**
	 * Get a compacted version of the candidate row according to the
	 * columns specified in the bit map. Share the holders between rows.
	 * If there is no bit map, use the candidate row as the compact row.
	 *
	 * Also, create an array of ints mapping base column positions to
	 * compact column positions, to make it cheaper to copy columns to
	 * the compact row, if we ever have to do it again.
	 *
	 * @param candidate		The row to get the columns from
	 * @param accessedCols	A bit map of the columns that are accessed in
	 *						the candidate row
	 * @param otherCols		An bit map of other column ids - this is used
	 *						in case columns from an index row will be
	 *						copied into a heap row - in this case, we
	 *						need to be sure there are enough columns in
	 *						the compact row. This parameter is null if
	 *						columns will not be copied from an index row
	 *						to a compact heap row. The column numbers in
	 *						the bit map are zero-based.
	 * @param isKeyed		Tells whether to return a ValueRow or an IndexRow
	 *
	 * @return		A compact row.
	 */
	protected ExecRow getCompactRow(ExecRow candidate,
									FormatableBitSet accessedCols,
									FormatableBitSet otherCols,
									boolean isKeyed)
									throws StandardException
	{
		int		numCandidateCols = candidate.nColumns();

		if (accessedCols == null)
		{
			compactRow =  candidate;
			baseColumnMap = new int[numCandidateCols];
			for (int i = 0; i < baseColumnMap.length; i++)
				baseColumnMap[i] = i;
		}
		else
		{
			FormatableBitSet allCols;

			if (otherCols == null)
			{
				allCols = accessedCols;
			}
			else
			{
				allCols = new FormatableBitSet(accessedCols);
				allCols.or(otherCols);
			}

			int numCols = allCols.getNumBitsSet();
			baseColumnMap = new int[numCols];

			if (compactRow == null)
			{
				ExecutionFactory ex = lcc.getExecutionContext().getExecutionFactory();

				if (isKeyed)
				{
					compactRow = ex.getIndexableRow(numCols);
				}
				else
				{
					compactRow = ex.getValueRow(numCols);
				}
			}

			int position = 0;
			for (int i = allCols.anySetBit();
					i != -1;
					i = allCols.anySetBit(i))
			{
				// Stop looking if there are columns beyond the columns
				// in the candidate row. This can happen due to the
				// otherCols bit map.
				if (i >= numCandidateCols)
					break;

				DataValueDescriptor sc = candidate.getColumn(i+1);
				if (sc != null)
				{
					compactRow.setColumn(
									position + 1,
									sc
									);
				}
				baseColumnMap[position] = i;
				position++;
			}
		}

		return compactRow;
	}

	/**
	 * Copy columns from the candidate row from the store to the given
	 * compact row. If there is no column map, just use the candidate row.
	 *
	 * This method assumes the above method (getCompactRow()) was called
	 * first. getCompactRow() sets up the baseColumnMap.
	 *
	 * @param candidateRow	The candidate row from the store
	 * @param compactRow	The compact row to fill in
	 *
	 * @return	The compact row to use
	 */
	protected ExecRow setCompactRow(ExecRow candidateRow, ExecRow compactRow)
	{
		ExecRow	retval;

		//System.out.println("base col map " + baseColumnMap);
		if (baseColumnMap == null)
		{
			retval = candidateRow;
		}
		else
		{
			retval = compactRow;

			setCompatRow(compactRow, candidateRow.getRowArray());
		}

		return retval;
	}


	protected final void setCompatRow(ExecRow compactRow, Object[] sourceRow) {

		Object[] destRow = compactRow.getRowArray();
		int[] lbcm = baseColumnMap;

		for (int i = 0; i < lbcm.length; i++)
		{

			destRow[i] = sourceRow[lbcm[i]];

		}
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * This method will be overriden in the inherited Classes
	 * if it is true
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return false;
	}

	protected final void addWarning(SQLWarning w) {

		if (isTopResultSet) {
			if (warnings == null)
				warnings = w;
			else 
				warnings.setNextWarning(w);
			return;
		}

		if (activation != null) {

			ResultSet rs = activation.getResultSet();
			if (rs instanceof BasicNoPutResultSetImpl) {
				((BasicNoPutResultSetImpl) rs).addWarning(w);
			}

		}
	}

	public final SQLWarning getWarnings() {
		SQLWarning w = warnings;
		warnings = null;
		return w;
	}
}
