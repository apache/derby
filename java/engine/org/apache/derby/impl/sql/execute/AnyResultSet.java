/*

   Derby - Class org.apache.derby.impl.sql.execute.AnyResultSet

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

import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.error.StandardException;

/**
 * Takes a quantified predicate subquery's result set.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 * @author jerry
 */
public class AnyResultSet extends NoPutResultSetImpl
{

	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public final NoPutResultSet source;
	private GeneratedMethod emptyRowFun;
	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //
    public AnyResultSet(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
						int resultSetNumber, int subqueryNumber,
						int pointOfAttachment,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost)
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		this.emptyRowFun = emptyRowFun;
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
	    	SanityManager.ASSERT(isOpen, "AnyResultSet already open");

        source.reopenCore();
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	public void	finish() throws StandardException
	{
		source.finish();
		finishAndRTS();
	}

	/**
     * Return the requested value computed from the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
	    ExecRow candidateRow = null;
		ExecRow secondRow = null;
	    ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		// This is an ASSERT and not a real error because this is never
		// outermost in the tree and so a next call when closed will not occur.
		if (SanityManager.DEBUG) {
        	SanityManager.ASSERT( isOpen, "AnyResultSet not open");
		}

	    if ( isOpen ) 
		{
			candidateRow = source.getNextRowCore();
			if (candidateRow != null) 
			{
				result = candidateRow;
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
				SanityManager.DEBUG("CloseRepeatInfo","Close of AnyResultSet repeated");

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
