/*

   Derby - Class org.apache.derby.impl.sql.execute.RowResultSet

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

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.types.RowLocation;

/**
 * Takes a constant row value and returns it as
 * a result set.
 * <p>
 * This class actually probably never underlies a select statement,
 * but in case it might and because it has the same behavior as the
 * ones that do, we have it implement CursorResultSet and give
 * reasonable answers.
 *
 * @author ames
 */
public class RowResultSet extends NoPutResultSetImpl
	implements CursorResultSet {

	/* Run time statistics variables */
	public int rowsReturned;

	private boolean canCacheRow;
	private boolean next;
	private GeneratedMethod row;
	protected GeneratedMethod closeCleanup;
	private ExecRow		cachedRow;

    //
    // class interface
    //
    RowResultSet
	(
		Activation 	activation, 
		GeneratedMethod row, 
		boolean 		canCacheRow,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost,
		GeneratedMethod closeCleanup
	)
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

        this.row = row;
		this.closeCleanup = closeCleanup;
		this.canCacheRow = canCacheRow;
		constructorTime += getElapsedMillis(beginTime);
    }

	/* This constructor takes in a constant row value, as the cache row.  See the
	 * usage in beetle 4373 for materializing subquery.
	 */
    RowResultSet
	(
		Activation 		activation, 
		ExecRow 		constantRow, 
		boolean 		canCacheRow,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost,
		GeneratedMethod closeCleanup
	)
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

		beginTime = getCurrentTimeMillis();
        this.cachedRow = constantRow;
		this.closeCleanup = closeCleanup;
		this.canCacheRow = canCacheRow;
		constructorTime += getElapsedMillis(beginTime);
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * Sets state to 'open'.
	 *
	 * @exception StandardException thrown if activation closed.
     */
	public void	openCore() throws StandardException 
	{
	   	next = false;
		beginTime = getCurrentTimeMillis();
	    isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * If open and not returned yet, returns the row
     * after plugging the parameters into the expressions.
	 *
	 * @exception StandardException thrown on failure.
     */
	public ExecRow	getNextRowCore() throws StandardException {

		currentRow = null;
		beginTime = getCurrentTimeMillis();
		if (isOpen) 
		{
			if (!next) 
			{
	            next = true;
				if (currentRow == null)
				{
					if (cachedRow != null)
					{
						currentRow = cachedRow;
					}
					else if (row != null)
					{
						currentRow = (ExecRow) row.invoke(activation);
						if (canCacheRow)
						{
							cachedRow = currentRow;
						}
					}
				}
				rowsReturned++;
			}
			setCurrentRow(currentRow);

			nextTime += getElapsedMillis(beginTime);
	    }
	    return currentRow;
	}

	/**
     * @see org.apache.derby.iapi.sql.ResultSet#close
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if (isOpen) {
			if (closeCleanup != null) {
				closeCleanup.invoke(activation); // let activation tidy up
			}

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
	    	next = false;

			super.close();
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of RowResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
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
		return totTime;
	}

	//
	// CursorResultSet interface
	//

	/**
	 * This is not operating against a stored table,
	 * so it has no row location to report.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	/**
	 * This is not used in positioned update and delete,
	 * so just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}
}
