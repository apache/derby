/*

   Derby - Class org.apache.derby.impl.sql.execute.NoRowsResultSetImpl

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.reference.SQLState;


import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import java.sql.Timestamp;
import java.sql.SQLWarning;

/**
 * This implementation of ResultSet
 * is meant to be overridden by subtypes
 * in the execution engine. Its primary users
 * will be DDL, which only need to define a
 * constructor to create the DDL object being
 * defined. All other ResultSet operations will
 * be handled by this superclass -- i.e., nothing
 * is allowed to be done to a DDL Result Set, since
 * it has no rows to provide.
 * <p>
 * This abstract class does not define the entire ResultSet
 * interface, but leaves the 'get' half of the interface
 * for subtypes to implement. It is package-visible only,
 * with its methods being public for exposure by its subtypes.
 * <p>
 *
 * @author ames
 */
abstract class NoRowsResultSetImpl implements ResultSet
{
	protected final Activation    activation;
	private boolean isTopResultSet = true;
	private boolean dumpedStats;
	protected NoPutResultSet[]	subqueryTrackingArray;

	private final boolean statisticsTimingOn;
	private boolean isClosed;

	/* fields used for formating run time statistics output */
	protected String indent;
	protected String subIndent;
	protected int sourceDepth;

	/* Run time statistics variables */
	protected final LanguageConnectionContext lcc;
	protected long beginTime;
	protected long endTime;
	protected long beginExecutionTime;
	protected long endExecutionTime;

	NoRowsResultSetImpl(Activation activation)
		throws StandardException
	{
		this.activation = activation;

		if (SanityManager.DEBUG) {
			if (activation == null)
				SanityManager.THROWASSERT("activation is null in result set " + getClass());
		}

		lcc = activation.getLanguageConnectionContext();
		statisticsTimingOn = lcc.getStatisticsTiming();

		/* NOTE - We can't get the current time until after setting up the
		 * activation, as we end up using the activation to get the 
		 * LanguageConnectionContext.
		 */
		beginTime = getCurrentTimeMillis();
		beginExecutionTime = beginTime;

		StatementContext sc = lcc.getStatementContext();
		sc.setTopResultSet(this, (NoPutResultSet[]) null);

		// Pick up any materialized subqueries
		if (subqueryTrackingArray == null)
		{
			subqueryTrackingArray = sc.getSubqueryTrackingArray();
		}
	}

    /**
	 * Returns FALSE
	 */
	 public final boolean	returnsRows() { return false; }

	/**
	 * Returns zero.
	 */
	public int	modifiedRowCount() { return 0; }

	/**
	 * Returns null.
	 */
	public ResultDescription	getResultDescription()
	{
	    return (ResultDescription)null;
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "absolute");
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "relative");
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "beforeFirst");
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "first");
	}

	/**
     * No rows to return, so throw an exception.
	 *
	 * @exception StandardException		Always throws a
	 *									StandardException to indicate
	 *									that this method is not intended to
	 *									be used.
	 */
	public ExecRow	getNextRow() throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "next");
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "previous");
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "last");
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
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "afterLast");
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     */
    public boolean checkRowPosition(int isType)
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
     * No rows to return, does nothing
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{ 
		isClosed = true;
	}

	/**
		Just report that it is always closed.
		RESOLVE: if we don't report that we are closed,
		then we will wind up with a dependency problem when
		we send an invalidateFor on our own Statement.  It
		will call lcc.verifyNoOpenResultSets(), which is really
		supposed to be verify that there are no read only
		result sets that are open.
	 */
	public boolean isClosed() {
		return isClosed;
		//return true;
	}

	/**
	 *	doesn't need to do anything, as no calls
	 *	are made that need to be restricted once
	 *	the result set is 'finished'.
	 *
	 * @exception StandardException on error
	 */
	public void finish() throws StandardException {
		if (! dumpedStats)
		{
			/*
			** If run time statistics tracing is turned on, then now is the
			** time to dump out the information.
			** NOTE - We make a special exception for commit.  If autocommit
			** is on, then the run time statistics from the autocommit is the
			** only one that the user would ever see.  So, we don't overwrite
			** the run time statistics object for a commit.
			*/
			if (lcc.getRunTimeStatisticsMode() &&
				! doesCommit())
			{
				endExecutionTime = getCurrentTimeMillis();

				ExecutionContext ec = lcc.getExecutionContext();
				ResultSetStatisticsFactory rssf;
				rssf = ec.getResultSetStatisticsFactory();

				lcc.setRunTimeStatisticsObject(
					rssf.getRunTimeStatistics(activation, this, subqueryTrackingArray));

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
			dumpedStats = true;
		}

		/* This is the top ResultSet, 
		 * close all of the open subqueries.
		 */
		int staLength = (subqueryTrackingArray == null) ? 0 :
							subqueryTrackingArray.length;

		for (int index = 0; index < staLength; index++)
		{
			if (subqueryTrackingArray[index] == null)
			{
				continue;
			}
			if (subqueryTrackingArray[index].isClosed())
			{
				continue;
			}
			subqueryTrackingArray[index].close();
		}
	}

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
		return endTime - beginTime;
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		if (beginExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(beginExecutionTime);
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
	 * RESOLVE - This method will go away once it is overloaded in all subclasses.
	 * Return the query plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The query plan as a String.
	 */
	public String getQueryPlanText(int depth)
	{
		return MessageService.getTextMessage(
				SQLState.LANG_GQPT_NOT_SUPPORTED,
				getClass().getName());
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type)
	{
		/* RESOLVE - this should be overloaded in all subclasses */
		return 0;
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
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
		Return the cursor name, null in this case.

		@see ResultSet#getCursorName
	*/
	public String getCursorName() {
		return null;
	}

	// class implementation

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
	  *	Run a check constraint against the current row. Raise an error if
	  * the check constraint is violated.
	  *
	  *	@param	checkGM			Generated code to run the check constraint.
	  * @param	checkName		Name of the constraint to check.
	  *	@param	heapConglom		Number of heap conglomerate.
	  *	@param	Activation		Class in which checkGM lives.
	  *
	  * @exception StandardException thrown on error
	  */
	public	static	void	evaluateACheckConstraint
	(
	  GeneratedMethod checkGM,
	  String checkName,
	  long heapConglom,
	  Activation activation
	)
		throws StandardException
	{
		if (checkGM != null)
		{
			DataValueDescriptor checkBoolean;

			checkBoolean = (DataValueDescriptor) checkGM.invoke(activation);

			/* Throw exception if check constraint is violated.
			 * (Only if check constraint evaluates to false.)
			 */ 
			if ((checkBoolean != null) &&
				(! checkBoolean.isNull()) &&
				(! checkBoolean.getBoolean()))
			{
				/* Now we have a lot of painful work to get the
				 * table name for the error message.  All we have 
				 * is the conglomerate number to work with.
				 */
				DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
				ConglomerateDescriptor cd = dd.getConglomerateDescriptor( heapConglom );
				TableDescriptor td = dd.getTableDescriptor(cd.getTableID());

				StandardException se = StandardException.newException(SQLState.LANG_CHECK_CONSTRAINT_VIOLATED, 
					td.getQualifiedName(), checkName);

				throw se;
			}
		}

	}

	/**
	  *	Run check constraints against the current row. Raise an error if
	  * a check constraint is violated.
	  *
	  *	@param	checkGM			Generated code to run the check constraint.
	  *	@param	Activation		Class in which checkGM lives.
	  *
	  * @exception StandardException thrown on error
	  */
	public	static	void	evaluateCheckConstraints
	(
	  GeneratedMethod checkGM,
	  Activation activation
	)
		throws StandardException
	{
		if (checkGM != null)
		{
			// Evaluate the expression containing the check constraints.
			// This expression will throw an exception if there is a
			// violation, so there is no need to check the result.
			checkGM.invoke(activation);
		}

	}
	  
	/**
	 * Does this ResultSet cause a commit or rollback.
	 *
	 * @return Whether or not this ResultSet cause a commit or rollback.
	 */
	public boolean doesCommit()
	{
		return false;
	}

	public java.sql.SQLWarning getWarnings() {
		return null;
	}

}
