/*

   Derby - Class org.apache.derby.impl.sql.execute.NestedLoopLeftOuterJoinResultSet

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;


/**
 * Takes 2 NoPutResultSets and a join filter and returns
 * the join's rows satisfying the filter as a result set
 * plus the rows from the left joined with a null row from
 * the right when there is no matching row in the right
 * result set.
 */
public class NestedLoopLeftOuterJoinResultSet extends NestedLoopJoinResultSet
{
	protected GeneratedMethod emptyRowFun;
	/* Was this originally a right outer join? */
	private boolean wasRightOuterJoin;

	/* Have we found a matching row from the right yet? */
	private boolean matchRight = false;
	private boolean returnedEmptyRight = false;
	private ExecRow rightEmptyRow = null;

	public int emptyRightRowsReturned = 0;

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException		Thrown on error
	 * @exception StandardException		ResultSetNotOpen thrown if closed
	 * @return the next row in the join result
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
	    ExecRow result = null;
		boolean haveRow = false;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;

		beginTime = getCurrentTimeMillis();
		if (! isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		/* Close right and advance left if we found no match
		 * on right on last next().
		 */
		if (returnedEmptyRight)
		{
			/* Current scan on right is exhausted.  Need to close old scan 
			 * and open new scan with new "parameters".  openRight will
	 		 * reopen the scan.
			 */
			leftRow = leftResultSet.getNextRowCore();
			if (leftRow == null)
			{
				closeRight();
			}
			else
			{
				rowsSeenLeft++;
				openRight();
			}
			returnedEmptyRight = false;
		}

		while (leftRow != null && !haveRow)
		{
			rightRow = rightResultSet.getNextRowCore();

			if (rightRow == null)
			{
				/* If we haven't found a match on the right, then
				 * we join the left with a row of nulls from the
				 * right.
				 */
				if (! matchRight)
				{
					haveRow = true;
					returnedEmptyRight = true;
					if (rightEmptyRow == null)
					{
						rightEmptyRow = (ExecRow) emptyRowFun.invoke(activation);
					}

					getMergedRow(leftRow, rightEmptyRow);
					emptyRightRowsReturned++;
					continue;
				}

				/* Current scan on right is exhausted.  Need to close old scan 
				 * and open new scan with new "parameters".  openRight()
				 * will reopen the scan.
				 */
				matchRight = false;
				leftRow = leftResultSet.getNextRowCore();
				if (leftRow == null)
				{
					closeRight();
				}
				else
				{
					rowsSeenLeft++;
					openRight();
				}
			}
			else
			{
				rowsSeenRight++;

				if (restriction != null)
				{
					restrictBoolean =
						(DataValueDescriptor) restriction.invoke(activation);

			        // if the result is null, we make it false --
					// so the row won't be returned.
					restrict = (! restrictBoolean.isNull()) &&
									restrictBoolean.getBoolean();

					if (! restrict)
					{
						/* Update the run time statistics */
						rowsFiltered++;
						continue;
					}
				}

				matchRight = true;

				getMergedRow(leftRow, rightRow);
				haveRow = true;
			}
		}

		/* Do we have a row to return? */
	    if (haveRow)
	    {
			result = mergedRow;
			setCurrentRow(mergedRow);
			rowsReturned++;
	    }
		else
		{
			clearCurrentRow();
		}

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}

	protected void getMergedRow(ExecRow leftRow, ExecRow rightRow) 
			throws StandardException
	{
		int colInCtr;
		int colOutCtr;
		int leftNumCols;
		int rightNumCols;

		/* Reverse left and right for return of row if this was originally
		 * a right outer join.  (Result columns ordered according to
		 * original query.)
		 */
		if (wasRightOuterJoin)
		{
			ExecRow tmp;

			tmp = leftRow;
			leftRow = rightRow;
			rightRow = tmp;
			leftNumCols = this.rightNumCols;
			rightNumCols = this.leftNumCols;
		}
		else
		{
			leftNumCols = this.leftNumCols;
			rightNumCols = this.rightNumCols;
		}

		/* Merge the rows, doing just in time allocation for mergedRow.
		 * (By convention, left Row is to left of right Row.)
		 */
		if (mergedRow == null)
		{
			mergedRow = getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
		}

		for (colInCtr = 1, colOutCtr = 1; colInCtr <= leftNumCols;
			 colInCtr++, colOutCtr++)
		{
			 mergedRow.setColumn(colOutCtr, 
								 leftRow.getColumn(colInCtr));
		}
		for (colInCtr = 1; colInCtr <= rightNumCols; 
			 colInCtr++, colOutCtr++)
		{
			 mergedRow.setColumn(colOutCtr, 
								 rightRow.getColumn(colInCtr));
		}
	}

	/**
	 * Clear any private state that changes during scans.
	 * This includes things like the last row seen, etc.
	 * THis does not include immutable things that are
	 * typically set up in the constructor.
	 * <p>
	 * This method is called on open()/close() and reopen()
	 * <p>
	 * WARNING: this should be implemented in every sub
	 * class and it should always call super.clearScanState().
	 */
	void clearScanState()
	{
		matchRight = false;
		returnedEmptyRight = false;
		rightEmptyRow = null;
		emptyRightRowsReturned = 0;
		super.clearScanState();
	}


    /*
     * class interface
     *
     */
    public NestedLoopLeftOuterJoinResultSet(
						NoPutResultSet leftResultSet,
						int leftNumCols,
						NoPutResultSet rightResultSet,
						int rightNumCols,
						Activation activation,
						GeneratedMethod restriction,
						int resultSetNumber,
						GeneratedMethod emptyRowFun,
						boolean wasRightOuterJoin,
					    boolean oneRowRightSide,
					    boolean notExistsRightSide,
 					    double optimizerEstimatedRowCount,
						double optimizerEstimatedCost,
						GeneratedMethod closeCleanup)
    {
		super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
			  activation, restriction, resultSetNumber, 
			  oneRowRightSide, notExistsRightSide,
			  optimizerEstimatedRowCount, optimizerEstimatedCost, 
			  closeCleanup);
		this.emptyRowFun = emptyRowFun;
		this.wasRightOuterJoin = wasRightOuterJoin;
    }
}
