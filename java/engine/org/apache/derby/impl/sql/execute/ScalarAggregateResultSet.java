/*

   Derby - Class org.apache.derby.impl.sql.execute.ScalarAggregateResultSet

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

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;

import java.util.Properties;
import java.util.Vector;
import java.util.Enumeration;

/**
 * This ResultSet evaluates scalar, non distinct aggregates.
 * It will scan the entire source result set and calculate
 * the scalar aggregates when scanning the source during the 
 * first call to next().
 *
 * @author jerry (broken out from SortResultSet)
 */
public class ScalarAggregateResultSet extends GenericAggregateResultSet
	implements CursorResultSet 
{

	/* Run time statistics variables */
	public int rowsInput;

    // set in constructor and not altered during
    // life of object.
	public 		boolean 			singleInputRow;
	protected 	ExecIndexRow 		sortTemplateRow;
	protected 	boolean 			isInSortedOrder;		// true if source results in sorted order

	// Cache ExecIndexRow for scalar aggregates
	protected ExecIndexRow sourceExecIndexRow;

	// Remember whether or not a next() has been satisfied
	private boolean nextSatisfied;

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine. 
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    public ScalarAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					Activation a,
					GeneratedMethod ra,
					int resultSetNumber,
					boolean singleInputRow,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost,
					GeneratedMethod c) throws StandardException 
	{
		super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, c);
		this.isInSortedOrder = isInSortedOrder;
		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"SARS(), source expected to be non-null");
		}
		sortTemplateRow = getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		this.singleInputRow = singleInputRow;

		if (SanityManager.DEBUG)
		{
			SanityManager.DEBUG("AggregateTrace","execution time: "+ 
					a.getPreparedStatement().getSavedObject(aggregateItem));
		}
		constructorTime += getElapsedMillis(beginTime);
    }


	///////////////////////////////////////////////////////////////////////////////
	//
	// ResultSet interface (leftover from NoPutResultSet)
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Open the scan.  Load the sorter and prepare to get
	 * rows from it.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		beginTime = getCurrentTimeMillis();

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"SARS.openCore(), source expected to be non-null");
	    	SanityManager.ASSERT( ! isOpen, "ScalarAggregateResultSet already open");
		}

		sourceExecIndexRow = getExecutionFactory().getIndexableRow(sortTemplateRow);

        source.openCore();

	    isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}


	protected int countOfRows;

	/* RESOLVE - THIS NEXT METHOD IS OVERRIDEN IN DistinctScalarResultSet
	 * BEACAUSE OF A JIT ERROR. THERE IS NO OTHER
	 * REASON TO OVERRIDE IT IN DistinctScalarAggregateResultSet.  THE BUG WAS FOUND IN
	 * 1.1.6 WITH THE JIT.
	 */
	/**
	 * Return the next row.  If it is a scalar aggregate scan
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
		if (nextSatisfied)
		{
			clearCurrentRow();
			return null;
		}

	    ExecIndexRow sortResult = null;
	    ExecRow result = null;
	    ExecIndexRow execIndexRow = null;
	    ExecIndexRow aggResult = null;
		//only care if it is a minAgg if we have a singleInputRow, then we know
		//we are only looking at one aggregate
		boolean minAgg = (singleInputRow && aggregates[0].aggInfo.aggregateName.equals("MIN"));
		beginTime = getCurrentTimeMillis();
	    if (isOpen)
	    {
			/*
			** We are dealing with a scalar aggregate.
			** Zip through each row and accumulate.
			** Accumulate into the first row.  Only
			** the first row is cloned.
			*/
	        while ((execIndexRow = getRowFromResultSet(false)) != null)
	        {
				/*
				** Use a clone of the first row as our result.  
				** We need to get a clone since we will be reusing
				** the original as the wrapper of the source row.
				** Turn cloning off since we wont be keeping any
				** other rows.
				*/
				if (aggResult == null)
				{
					/* No need to clone the row when doing the min/max 
					 * optimization for MIN, since we will not do another
					 * next on the underlying result set.
					 */
					aggResult = (singleInputRow && minAgg) ?
								execIndexRow :
								(ExecIndexRow) execIndexRow.getClone();
					
					initializeScalarAggregation(aggResult);
				}
				else
				{
					accumulateScalarAggregation(execIndexRow, aggResult, false);
				}

				/* Only need to look at first single row if 
				 * min/max optimization is on and operation is MIN
				 * or if operation is MAX first non-null row since null sorts
				 * as highest in btree
				 * Note only 1 aggregate is allowed in a singleInputRow 
                 * optimization so we only need to look at the first aggregate
				 */
				if (singleInputRow && 
					(minAgg || 
                     !aggResult.getColumn(aggregates[0].aggregatorColumnId).isNull()))
				{
					break;
				}
	        }

			/*
			** If we have aggregates, we need to generate a
			** value for them now.  Only finish the aggregation
			** if we haven't yet (i.e. if countOfRows == 0).
			** If there weren't any input rows, we'll allocate
			** one here.
			*/
			if (countOfRows == 0)
			{
				aggResult = finishAggregation(aggResult);
				currentRow = aggResult;
				setCurrentRow(aggResult);
				countOfRows++;
			}
	    }

		nextSatisfied = true;
		nextTime += getElapsedMillis(beginTime);
		return aggResult;
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
			if (closeCleanup != null) {
				closeCleanup.invoke(activation); // let activation tidy up
			}

	        countOfRows = 0;
			currentRow = null;
			sourceExecIndexRow = null;
			source.close();

			super.close();
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of SortResultSet repeated");

		closeTime += getElapsedMillis(beginTime);

		nextSatisfied = false;
		isOpen = false;
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
		long totTime = constructorTime + openTime + nextTime + 
						closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime - originalSource.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// CursorResultSet interface
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * This result set has its row location from
	 * the last fetch done. Always returns null.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation() throws StandardException
	{
		return null;
	}

	/**
	 * This result set has its row from the last fetch done. 
	 * If the cursor is closed, a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned;
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOpen, "SortResultSet expected to be open");

		return currentRow;
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// SCAN ABSTRACTION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Get a row from the input result set.  
	 *
	 * @param doClone - true of the row should be cloned
	 *
	 * @exception StandardException Thrown on error
	 */	
	public ExecIndexRow getRowFromResultSet(boolean doClone)
		throws StandardException
	{
		ExecRow					sourceRow;
		ExecIndexRow			inputRow = null;	

		if ((sourceRow = source.getNextRowCore()) != null)
		{
			rowsInput++;
			sourceExecIndexRow.execRowToExecIndexRow(
					doClone ? sourceRow.getClone() : sourceRow);
			inputRow = sourceExecIndexRow;
		}

		return inputRow;
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
	    	SanityManager.ASSERT(isOpen, "NormalizeResultSet already open");

		source.reopenCore();
		numOpens++;
        countOfRows = 0;
		nextSatisfied = false;

		openTime += getElapsedMillis(beginTime);
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// AGGREGATION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Run accumulation on every aggregate in this
	 * row.  This method is useful when draining the source
	 * or sorter, depending on whether or not there were any
	 * distinct aggregates.  Remember, if there are distinct
	 * aggregates, then the non-distinct aggregates were
	 * calculated on the way into the sorter and only the
	 * distinct aggregates will be accumulated here.
	 * Otherwise, all aggregates will be accumulated here.
	 *
	 * @param	the input row
	 * @param	the row with the accumulator (may be the same as
	 *			the input row.
	 * @param	hasDistinctAggregates does this scan have distinct
	 *			aggregates.  Used to figure out whether to merge
	 *			or accumulate nondistinct aggregates.
	 *
	 * @exception StandardException Thrown on error
	 */
	protected void accumulateScalarAggregation
	(
		ExecRow inputRow, 
		ExecRow accumulateRow, 
		boolean hasDistinctAggregates
	)
		throws StandardException
	{
		int size = aggregates.length;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((inputRow != null) && (accumulateRow != null), 
					"Null row passed to accumulateScalarAggregation");
		}
		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			if	(hasDistinctAggregates && 
				 !currAggregate.getAggregatorInfo().isDistinct())
			{
				currAggregate.merge(inputRow, accumulateRow);
			}
			else
			{
				currAggregate.accumulate(inputRow, accumulateRow);
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// CLASS SPECIFIC
	//
	///////////////////////////////////////////////////////////////////////////////

	/*
	** Run the aggregator initialization method for
	** each aggregator in the row. 
	**
	** @param	row	the row to initialize
	**
	** @return Nothing.
	**
	** @exception	standard cloudscape exception
	*/
	private void initializeScalarAggregation(ExecRow row)
		throws StandardException
	{
		int size = aggregates.length;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row != null, 
					"Null row passed to initializeScalarAggregation");
		}

		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			currAggregate.initialize(row);
			currAggregate.accumulate(row, row);
		}
	}
}
