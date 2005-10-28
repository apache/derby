/*

   Derby - Class org.apache.derby.impl.sql.execute.MergeJoinResultSet

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
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

/////////////////////////////////////////////
// WARNING: THIS HAS NOT BEEN TESTED (OR USED)
// YET, SO USE AT YOUR OWN RISK
/////////////////////////////////////////////

/**
 * Merge two result sets.  The left result set (the outer
 * result set) MUST be unique for this to work correctly.
 *
 */
public class MergeJoinResultSet extends JoinResultSet
{
	private static final int GREATER_THAN = 1;
	private static final int EQUAL = 0;
	private static final int LESS_THAN = -1;

	private GeneratedMethod leftGreaterThanRight;

    /**
     * Create a MergeJoinResultSet
	 * 
	 * @param leftResultSet		the left (outer) result set	
	 * @param leftNumCols		columns in left row
	 * @param rightResultSet	the right (outer) result set	
	 * @param rightNumCols		columns in right row
	 * @param activation		activation
	 * @param leftGreaterThanRight a generated method that is used to
	 *			ascertain whether the row from the left result set is
	 *			greater than the row from the right result set.  returns
	 *			1,0,or -1 to indicate greater than, equal, or less than,
	 *			respectively
	 * @param  restriction		generated method for additional qualification
	 * @param resultSetNumber	the result set number
	 * @param oneRowRightSide	ignored
	 * @param optimizerEstimatedRowCount	self-explanatory
	 * @param optimizerEstimatedCost		self-explanatory
	 * @param closeCleanup					self-explanatory
     */
    public MergeJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod leftGreaterThanRight,
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

		this.leftGreaterThanRight = leftGreaterThanRight;
    }

	//////////////////////////////////////////////////////////////////////
	//
	// ResultSet interface (leftover from NoPutResultSet)
	//
	//////////////////////////////////////////////////////////////////////
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
		beginTime = getCurrentTimeMillis();
		if (! isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		if (!isRightOpen)
		{
			openRight();
		}

		int compareResult;

		/*
		** For each row in the outer table
		*/
		while (leftRow != null)
		{
			/*
			** If outer > inner, then go to the
			** next row in the inner table
			*/
			while ((compareResult = ((Integer)leftGreaterThanRight.invoke(activation)).intValue()) 
						== GREATER_THAN)
			{
				rightRow = rightResultSet.getNextRowCore();
				rowsSeenRight++;

				/*
				** If there are no more rows in the right
				** result set, then done.
				*/
				if (rightRow == null)
				{
		   			clearCurrentRow();
					return (ExecRow)null;
				}
			}

			/*
			** If they match and the restriction passes,
			** then return the row.
			*/
			if ((compareResult == EQUAL) && restrictionIsTrue())
			{
				ExecRow returnRow = getReturnRow(leftRow, rightRow);

				/*
				** Move the left scan up one for the next
				** getNextRowCore() call.
				*/
				leftRow = leftResultSet.getNextRowCore();

				return returnRow;
			}

			/*
			** Next row left
			*/	
			leftRow = leftResultSet.getNextRowCore();
			rowsSeenLeft++;
		}

		clearCurrentRow();
		return (ExecRow)null;
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
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( isOpen, "MergeJoinResultSet not open");

	    if ( isOpen )
	    {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		   clearCurrentRow();

			super.close();
	    }

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


	//////////////////////////////////////////////////////////////////
	//
	// SERVILE METHODS
	//
	//////////////////////////////////////////////////////////////////
	private ExecRow getReturnRow(ExecRow leftRow, ExecRow rightRow)
		throws StandardException
	{
		int colInCtr;
		int colOutCtr;

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

		setCurrentRow(mergedRow);
		rowsReturned++;
		nextTime += getElapsedMillis(beginTime);

		return mergedRow;
	}

	private boolean restrictionIsTrue()
		throws StandardException
	{
		if (restriction != null)
		{
	    	DataValueDescriptor restrictBoolean =
				(DataValueDescriptor) restriction.invoke(activation);

	        /*
			** if the result is null, we make it false --
			** so the row won't be returned.
			*/
			if (restrictBoolean.isNull() ||
					!restrictBoolean.getBoolean())
			{
				/* Update the run time statistics */
				rowsFiltered++;
				return false;
			}
		}
		return true;
	}

}
