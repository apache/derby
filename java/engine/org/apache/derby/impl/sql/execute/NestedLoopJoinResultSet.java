/*

   Derby - Class org.apache.derby.impl.sql.execute.NestedLoopJoinResultSet

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
 * the join's rows satisfying the filter as a result set.
 */
public class NestedLoopJoinResultSet extends JoinResultSet
{
	private boolean returnedRowMatchingRightSide = false;
	private ExecRow rightTemplate;

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

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
		returnedRowMatchingRightSide = false;
		super.clearScanState();
	}

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
		int colInCtr;
		int colOutCtr;
	    DataValueDescriptor restrictBoolean;

		beginTime = getCurrentTimeMillis();
		if (! isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		/* If we have a row from the left side and the right side is not open, 
		 * then we get an error on the previous next, either on the next on
		 * the left or the open on the right.  So, we do a next on the left
		 * and then open the right if that succeeds.
		 */
		if (! isRightOpen && leftRow != null)
		{		 
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

		while (leftRow != null && !haveRow)
		{
			if (oneRowRightSide && returnedRowMatchingRightSide)
			{
				rightRow = null;
				returnedRowMatchingRightSide = false;
			}
			else
			{
				rightRow = rightResultSet.getNextRowCore();

				/* If this is a NOT EXISTS join, we just need to reverse the logic
				 * of EXISTS join.  To make the implementation simple, we create a
				 * right side template, which is never really needed. (beetle 5173)
				 */
				if (notExistsRightSide)
				{
					if (rightRow == null)      //none satisfied
						rightRow = rightTemplate;  //then we are
					else
						rightRow = null;
				}

				returnedRowMatchingRightSide = (rightRow != null);
			}

			if (rightRow == null)
			{
				/* Current scan on right is exhausted.  Need to close old scan 
				 * and open new scan with new "parameters".  openRight()	
				 * will reopen if already open.
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
				if (! notExistsRightSide)
				{
					for (colInCtr = 1; colInCtr <= rightNumCols; 
						 colInCtr++, colOutCtr++)
					{
						 mergedRow.setColumn(colOutCtr, 
											 rightRow.getColumn(colInCtr));
					}
				}

				setCurrentRow(mergedRow);
				haveRow = true;
			}
		}

		/* Do we have a row to return? */
	    if (haveRow)
	    {
			result = mergedRow;
			rowsReturned++;
	    }
		else
		{
			clearCurrentRow();
		}

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
	    if ( isOpen )
	    {
			beginTime = getCurrentTimeMillis();

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

			super.close();
			returnedRowMatchingRightSide = false;
			closeTime += getElapsedMillis(beginTime);
	    }

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
			return	totTime - leftResultSet.getTimeSpent(ENTIRE_RESULTSET_TREE) -
							  rightResultSet.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}

    /*
     * class interface
     *
     */
    public NestedLoopJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod restriction,
								   int resultSetNumber,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
    {
		super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
			  activation, restriction, resultSetNumber, 
			  oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost, closeCleanup);
		if (notExistsRightSide)
			rightTemplate = getExecutionFactory().getValueRow(rightNumCols);
    }
}
