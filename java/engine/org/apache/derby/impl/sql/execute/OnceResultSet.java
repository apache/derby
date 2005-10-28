/*

   Derby - Class org.apache.derby.impl.sql.execute.OnceResultSet

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

/**
 * Takes an expression subquery's result set and verifies that only
 * a single scalar value is being returned.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 * @author jerry
 */
public class OnceResultSet extends NoPutResultSetImpl
{
	/* Statics for cardinality check */
	public static final int DO_CARDINALITY_CHECK		= 1;
	public static final int NO_CARDINALITY_CHECK		= 2;
	public static final int UNIQUE_CARDINALITY_CHECK	= 3;

	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
	private GeneratedMethod emptyRowFun;
	private int cardinalityCheck;
	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //
    public OnceResultSet(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
						 int cardinalityCheck, int resultSetNumber,
						 int subqueryNumber, int pointOfAttachment,
						 double optimizerEstimatedRowCount,
						 double optimizerEstimatedCost)
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		this.emptyRowFun = emptyRowFun;
		this.cardinalityCheck = cardinalityCheck;
		this.subqueryNumber = subqueryNumber;
		this.pointOfAttachment = pointOfAttachment;
		constructorTime += getElapsedMillis(beginTime);
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		/* NOTE: We can't get code generation
		 * to generate calls to reopenCore() for
		 * subsequent probes, so we just handle
		 * it here.
		 */
		if (isOpen)
		{
			reopenCore();
			return;
		}

		beginTime = getCurrentTimeMillis();

        source.openCore();

		/* Notify StatementContext about ourself so that we can
		 * get closed down, if necessary, on an exception.
		 */
		if (statementContext == null)
		{
			statementContext = getLanguageConnectionContext().getStatementContext();
		}
		statementContext.setSubqueryResultSet(subqueryNumber, this, 
											  activation.getNumSubqueries());

		numOpens++;
	    isOpen = true;
		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * reopen a scan on the table. scan parameters are evaluated
	 * at each open, so there is probably some way of altering
	 * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void	reopenCore() throws StandardException 
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT(isOpen, "OnceResultSet already open");

        source.reopenCore();
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * Return the requested value computed from the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 *			  StandardException ScalarSubqueryCardinalityViolation
	 *						Thrown if scalar subquery returns more than 1 row.
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
	    ExecRow candidateRow = null;
		ExecRow secondRow = null;
	    ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		// This is an ASSERT and not a real error because this is never
		// outermost in the tree and so a next call when closed will not occur.
		if (SanityManager.DEBUG)
        	SanityManager.ASSERT( isOpen, "OpenResultSet not open");

	    if ( isOpen ) 
		{
			candidateRow = source.getNextRowCore();

			if (candidateRow != null)
			{
				switch (cardinalityCheck)
				{
					case DO_CARDINALITY_CHECK:
					case NO_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						if (cardinalityCheck == DO_CARDINALITY_CHECK)
						{
							/* Raise an error if the subquery returns > 1 row 
							 * We need to make a copy of the current candidateRow since
							 * the getNextRow() for this check will wipe out the underlying
							 * row.
							 */
							secondRow = source.getNextRowCore();
							if (secondRow != null)
							{
								close();
								StandardException se = StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
								throw se;
							}
						}
						result = candidateRow;
						break;

					case UNIQUE_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						secondRow = source.getNextRowCore();
						DataValueDescriptor orderable1 = candidateRow.getColumn(1);
						while (secondRow != null)
						{
							DataValueDescriptor orderable2 = secondRow.getColumn(1);
							if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true)))
							{
								close();
								StandardException se = StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
								throw se;
							}
							secondRow = source.getNextRowCore();
						}
						result = candidateRow;
						break;

					default:
						if (SanityManager.DEBUG)
						{
							SanityManager.THROWASSERT(
								"cardinalityCheck not unexpected to be " +
								cardinalityCheck);
						}
						break;
				}
			}
			else if (rowWithNulls == null)
			{
				rowWithNulls = (ExecRow) emptyRowFun.invoke(activation);
				result = rowWithNulls;
			}
			else
			{
				result = rowWithNulls;
			}
	    }

		currentRow = result;
		setCurrentRow(result);
		rowsSeen++;

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
	    if ( isOpen ) 
		{
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

			currentRow = null;
	        source.close();

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of OnceResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	}

	/**
	 * @see NoPutResultSet#getPointOfAttachment
	 */
	public int getPointOfAttachment()
	{
		return pointOfAttachment;
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
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}
}
