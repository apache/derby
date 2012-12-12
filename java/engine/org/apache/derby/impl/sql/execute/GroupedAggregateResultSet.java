/*

   Derby - Class org.apache.derby.impl.sql.execute.GroupedAggregateResultSet

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.HashSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * This ResultSet evaluates grouped, non distinct aggregates.
 * It will scan the entire source result set and calculate
 * the grouped aggregates when scanning the source during the 
 * first call to next().
 *
 * The implementation is capable of computing multiple levels of grouping
 * in a single result set (this is requested using GROUP BY ROLLUP).
 *
 * This implementation has 3 variations, which it chooses according to
 * the following rules:
 * - If the data are guaranteed to arrive already in sorted order, we make
 *   a single pass over the data, computing the aggregates in-line as the
 *   data are read.
 * - If the statement requests either multiple ROLLUP levels, or a DISTINCT
 *   grouping, then the data are first sorted, then we make a single
 *   pass over the data as above.
 * - Otherwise, the data are sorted, and a SortObserver is used to compute
 *   the aggregations inside the sort, and the results are read back directly
 *   from the sorter.
 *
 * Note that, as of the introduction of the ROLLUP support, we no longer
 * ALWAYS compute the aggregates using a SortObserver, which is an
 * arrangement by which the sorter calls back into the aggregates during
 * the sort process each time it consolidates two rows with the same
 * sort key. Using aggregate sort observers is an efficient technique, but
 * it was complex to extend it to the ROLLUP case, so to simplify the code
 * we just have one path for both already-sorted and un-sorted data sources
 * in the ROLLUP case.
 *
 */
class GroupedAggregateResultSet extends GenericAggregateResultSet
	implements CursorResultSet {

	/* Run time statistics variables */
	public int rowsInput;
	public int rowsReturned;

    // set in constructor and not altered during
    // life of object.
	private ColumnOrdering[] order;
	public	boolean	hasDistinctAggregate;	// true if distinct aggregate
	public	boolean isInSortedOrder;				// true if source results in sorted order
	private	int numDistinctAggs = 0;
	private int maxRowSize;

	// set in open and not modified thereafter
    private ScanController scanController;

	// Cache ExecIndexRow
	private ExecIndexRow sourceExecIndexRow;

	private ExecIndexRow sortResultRow;

	// - resultRows: This is the current accumulating grouped result that
	//   we are computing, at each level of aggregation. If we are not
	//   doing a ROLLUP, then there is only one entry in resultRows, and
	//   it contains the currently-accumulating aggregated result. If we
	//   are doing a ROLLUP, then there are N+1 entries in resultRows,
	//   as follows (imagine we're doing ROLLUP(a,b,c,d):
	//   [0]: GROUP BY ()
	//   [1]: GROUP BY (A)
	//   [2]: GROUP BY (A,B)
	//   [3]: GROUP BY (A,B,C)
	//   [4]: GROUP BY (A,B,C,D)
	// - finishedResults: this list is used only when a ROLLUP is computing
	//   multiple levels of aggregation at once, and the results for
	//   several groupings have been completed, but not yet returned to
	//   our caller.
	// - distinctValues: used only if DISTINCT aggregates are present,
	//   this is a HashSet for each aggregate for each level of grouping,
	//   and the HashSet instances contain the values this aggregate
	//   has seen during this group instance, to eliminate duplicates.
	//
	private boolean resultsComplete;
	private List finishedResults;
	private ExecIndexRow[]			resultRows;
	private HashSet [][]			distinctValues;

	private boolean rollup;
	private boolean usingAggregateObserver = false;

	private long genericSortId;
	private TransactionController tc;

	// RTS
	public Properties sortProperties = new Properties();

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine.  
	 * @param	orderingItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		ColumOrdering array used by this routine
	 * @param	a				activation
	 * @param	ra				saved object that builds an empty output row
	 * @param	maxRowSize		approx row size, passed to sorter
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    GroupedAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					int ra,
					int maxRowSize,
					int resultSetNumber,
				    double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					boolean isRollup) throws StandardException 
	{
		super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.isInSortedOrder = isInSortedOrder;
		rollup = isRollup;
		finishedResults = new ArrayList();
		order = (ColumnOrdering[])
					((FormatableArrayHolder)
						(a.getPreparedStatement().getSavedObject(orderingItem)))
					.getArray(ColumnOrdering.class);

		if (SanityManager.DEBUG)
		{
			SanityManager.DEBUG("AggregateTrace","execution time: "+ 
					a.getPreparedStatement().getSavedObject(aggregateItem));
		}
		hasDistinctAggregate = aggInfoList.hasDistinct();
		// If there is no ROLLUP, and no DISTINCT, and the data are
		// not in sorted order, then we can use AggregateSortObserver
		// to compute the aggregation in the sorter:
		usingAggregateObserver =
			!isInSortedOrder &&
			!rollup &&
			!hasDistinctAggregate;

		recordConstructorTime();
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
		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "GroupedAggregateResultSet already open");

        sortResultRow = (ExecIndexRow) getRowTemplate().getClone();
        sourceExecIndexRow = (ExecIndexRow) getRowTemplate().getClone();

        source.openCore();

		try {
		/* If this is an in-order group by then we do not need the sorter.
		 * (We can do the aggregation ourselves.)
		 * We save a clone of the first row so that subsequent next()s
		 * do not overwrite the saved row.
		 */
		if (!isInSortedOrder)
			scanController = loadSorter();

		ExecIndexRow currSortedRow = getNextRowFromRS();
		resultsComplete = (currSortedRow == null);
		if (usingAggregateObserver)
		{
			if (currSortedRow != null)
				finishedResults.add(
					finishAggregation(currSortedRow).getClone());
		}
		else if (!resultsComplete)
		{
			if (rollup)
				resultRows = new ExecIndexRow[numGCols()+1];
			else
				resultRows = new ExecIndexRow[1];
			if (aggInfoList.hasDistinct())
			    distinctValues = new HashSet[resultRows.length][aggregates.length];
			for (int r = 0; r < resultRows.length; r++)
			{
				resultRows[r] =
					(ExecIndexRow) currSortedRow.getClone();
				initializeVectorAggregation(resultRows[r]);
				if (aggInfoList.hasDistinct())
					distinctValues[r] = new HashSet[aggregates.length];
				initializeDistinctMaps(r, true);
			}
		}
		} catch (StandardException e) {
			// DERBY-4330 Result set tree must be atomically open or
			// closed for reuse to work (after DERBY-827).

			isOpen = true; // to make close do its thing:
			try { close(); } catch (StandardException ee) {}
			throw e;
		}

	    isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * Load up the sorter.  Feed it every row from the
	 * source scan.  When done, close
	 * the source scan and open the sort.  Return the sort
	 * scan controller.
	 *
	 * @exception StandardException thrown on failure.
	 *
	 * @return	the sort controller
 	 */
	private ScanController loadSorter()
		throws StandardException
	{
		SortController 			sorter;
		ExecRow 				inputRow;
		int						inputRowCountEstimate = (int) optimizerEstimatedRowCount;
        ExecIndexRow            sortTemplateRow = getRowTemplate();

		tc = getTransactionController();

		SortObserver observer;
		if (usingAggregateObserver)
			observer = new AggregateSortObserver(true, aggregates,
				aggregates, sortTemplateRow);
		else
			observer = new BasicSortObserver(true, false,
				sortTemplateRow, true);

		genericSortId = tc.createSort((Properties)null, 
				sortTemplateRow.getRowArray(),
				order,
				observer,
				false,
				inputRowCountEstimate, // est rows
				maxRowSize			// est rowsize 
		);
		sorter = tc.openSort(genericSortId);
	
		/* The sorter is responsible for doing the cloning */
		while ((inputRow = getNextRowFromRS()) != null) 
		{
			sorter.insert(inputRow.getRowArray());
		}
		source.close();
		sorter.completedInserts();
		sortProperties = sorter.getSortInfo().
			getAllSortInfo(sortProperties);
		if (aggInfoList.hasDistinct())
		{
			/*
			** If there was a distinct aggregate, then that column
			** was automatically included as the last column in
			** the sort ordering. But we don't want it to be part
			** of the ordering anymore, because we aren't grouping
			** by that column, we just sorted it so that distinct
			** aggregation would see the values in order.
			*/
			// Although it seems like N aggs could have been
			// added at the end, in fact only one has been
			// FIXME -- need to get GroupByNode to handle this
			// correctly, but that requires understanding
			// scalar distinct aggregates.
			numDistinctAggs = 1;
		}
		return tc.openSortScan(genericSortId,
			activation.getResultSetHoldability());
	}

	/**
	 * Return the number of grouping columns.
	 *
	 * Since some additional sort columns may have been included
	 * in the sort for DISTINCT aggregates, this function is
	 * used to ignore those columns when computing the grouped
	 * results.
	 */
	private int numGCols() { return order.length - numDistinctAggs; }

	/**
	 * Return the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
		if (!isOpen)
		{
			return null;
		}

		beginTime = getCurrentTimeMillis();

		if (finishedResults.size() > 0)
			return makeCurrent(finishedResults.remove(0));
		else if (resultsComplete)
			return null;

		ExecIndexRow nextRow = getNextRowFromRS();
		// No rows, no work to do
		if (nextRow == null)
			return finalizeResults();

		// If the aggregation was performed using the SortObserver, the
		// result row from the sorter is complete and ready to return:
		if (usingAggregateObserver)
			return finishAggregation(nextRow);

		/* Drain and merge rows until we find new distinct values for the grouping columns. */
		while (nextRow != null)
		{
			/* We found a new set of values for the grouping columns.  
			 * Update the current row and return this group. 
			 *
			 * Note that in the case of GROUP BY ROLLUP,
			 * there may be more than one level of grouped
			 * aggregates which is now complete. We can
			 * only return 1, and the other completed
			 * groups are held in finishedResults until
			 * our caller calls getNextRowCore() again to
			 * get the next level of results.
			 */
			ExecIndexRow currSortedRow =
				    resultRows[resultRows.length-1];
                        ExecRow origRow = (ExecRow)nextRow.getClone();;
                        initializeVectorAggregation(nextRow);
			int distinguisherCol = 
				    sameGroupingValues(currSortedRow, nextRow);

			for (int r = 0; r < resultRows.length; r++)
			{
				boolean sameGroup = (rollup ?
				    r <= distinguisherCol :
				    distinguisherCol == numGCols());
				if (sameGroup)
				{
					/* Same group - initialize the new
					   row and then merge the aggregates */
					//initializeVectorAggregation(nextRow);
					mergeVectorAggregates(nextRow, resultRows[r], r);
				}
				else
				{
					setRollupColumnsToNull(resultRows[r],r);
					finishedResults.add(finishAggregation(resultRows[r]));
					/* Save a clone of the new row so
					   that it doesn't get overwritten */
					resultRows[r] = (ExecIndexRow)
						    origRow.getClone();
					initializeVectorAggregation(resultRows[r]);
					initializeDistinctMaps(r, false);
				}
			}
			if (finishedResults.size() > 0)
			{
				nextTime += getElapsedMillis(beginTime);
				rowsReturned++;
                                return makeCurrent(finishedResults.remove(0));
			}

			// Get the next row
			nextRow = getNextRowFromRS();
		}

		return finalizeResults();
	}
	// Return the passed row, after ensuring that we call setCurrentRow
	private ExecRow makeCurrent(Object row)
		throws StandardException
	{
		ExecRow resultRow = (ExecRow)row;
		setCurrentRow(resultRow);
		return resultRow;
	}
	private ExecRow finalizeResults()
		throws StandardException
	{
		// We've drained the source, so no more rows to return
		resultsComplete = true;
		if (! usingAggregateObserver )
		{
			for (int r = 0; r < resultRows.length; r++)
			{
				setRollupColumnsToNull(resultRows[r],r);
				finishedResults.add(finishAggregation(resultRows[r]));
			}
		}
		nextTime += getElapsedMillis(beginTime);
		if (finishedResults.size() > 0)
			return makeCurrent(finishedResults.remove(0));
		else
			return null;
	}

	/**
	 * Return whether or not the new row has the same values for the 
	 * grouping columns as the current row.  (This allows us to process in-order
	 * group bys without a sorter.)
	 *
	 * @param currRow	The current row.
	 * @param newRow	The new row.
	 *
	 * @return	The order index number which first distinguished
	 *			these rows, or order.length if the rows match.
	 *
	 * @exception StandardException thrown on failure to get row location
	 */
	private int sameGroupingValues(ExecRow currRow, ExecRow newRow)
		throws StandardException
	{
		for (int index = 0; index < numGCols(); index++)
		{
			DataValueDescriptor currOrderable = currRow.getColumn(order[index].getColumnId() + 1);
			DataValueDescriptor newOrderable = newRow.getColumn(order[index].getColumnId() + 1);
			if (! (currOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS, newOrderable, true, true)))
			{
				return index;
			}
		}
		return numGCols();
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

			sortResultRow = null;
			sourceExecIndexRow = null;
			closeSource();

			if (!isInSortedOrder)
			{
				tc.dropSort(genericSortId);
			}
			super.close();
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of SortResultSet repeated");

		closeTime += getElapsedMillis(beginTime);

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
	 * the last fetch done. If the cursor is closed,
	 * a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation() throws StandardException
	{
		if (! isOpen) return null;

		// REVISIT: could we reuse the same rowlocation object
		// across several calls?
		RowLocation rl;
		rl = scanController.newRowLocationTemplate();
		scanController.fetchLocation(rl);
		return rl;
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
	 * Get the next output row for processing
	 */
	private ExecIndexRow getNextRowFromRS()
		throws StandardException
	{
		return (scanController == null) ?
			getRowFromResultSet() :
			getRowFromSorter();
	}

	/**
	 * Get a row from the input result set.  
	 */	
	private ExecIndexRow getRowFromResultSet()
		throws StandardException
	{
		ExecRow					sourceRow;
		ExecIndexRow			inputRow = null;	

		if ((sourceRow = source.getNextRowCore()) != null)
		{
			rowsInput++;
			sourceExecIndexRow.execRowToExecIndexRow(sourceRow);
			inputRow = sourceExecIndexRow;
		}

		return inputRow;
	}


	// We are performing a ROLLUP aggregation and
	// we need to set the N rolled-up columns in this
	// row to NULL.
	private void setRollupColumnsToNull(ExecRow row, int resultNum)
		throws StandardException
	{
		int numRolledUpCols = resultRows.length - resultNum - 1;
		for (int i = 0; i < numRolledUpCols; i++)
		{
			int rolledUpColIdx = numGCols() - 1 - i;
			DataValueDescriptor rolledUpColumn =
				row.getColumn(order[rolledUpColIdx].getColumnId() + 1);
			rolledUpColumn.setToNull();
		}
	}

	/**
	 * Get a row from the sorter.  Side effects:
	 * sets currentRow.
	 */
	private ExecIndexRow getRowFromSorter()
		throws StandardException
	{
		ExecIndexRow			inputRow = null;	
		
		if (scanController.next())
		{
			// REMIND: HACKALERT we are assuming that result will
			// point to what sortResult is manipulating when
			// we complete the fetch.
			currentRow = sortResultRow;

			inputRow = getExecutionFactory().getIndexableRow(currentRow);

			scanController.fetch(inputRow.getRowArray());

		}
		return inputRow;
	}

	/**
	 * Close the source of whatever we have been scanning.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	closeSource() throws StandardException
	{
		if (scanController == null)
		{
			/*
			** NOTE: do not null out source, we
			** may be opened again, in which case
			** we will open source again.
			*/
			source.close();
		}
		else
		{
			scanController.close();
			scanController = null;
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// AGGREGATION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////
	/**
	 * Run the aggregator initialization method for
	 * each aggregator in the row.  Accumulate the
	 * input column.  WARNING: initializiation performs
	 * accumulation -- no need to accumulate a row
	 * that has been passed to initialization.
	 *
	 * @param	row	the row to initialize
	 *
	 * @exception	standard Derby exception
	 */
	private void initializeVectorAggregation(ExecRow row)
		throws StandardException
	{
		int size = aggregates.length;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row != null, 
					"Null row passed to initializeVectorAggregation");
		}

		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];

			// initialize the aggregator
			currAggregate.initialize(row);

			// get the first value, accumulate it into itself
			currAggregate.accumulate(row, row);
		}
	}

	/**
	 * Run the aggregator merge method for
	 * each aggregator in the row.  
	 *
	 * @param	newRow	the row to merge
	 * @param	currRow the row to merge into
	 *
	 * @exception	standard Derby exception
	 */
	private void mergeVectorAggregates(ExecRow newRow, ExecRow currRow,
		int level)
		throws StandardException
	{
		for (int i = 0; i < aggregates.length; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			AggregatorInfo aInfo = (AggregatorInfo)
					aggInfoList.elementAt(i);
			if (aInfo.isDistinct())
			{
				DataValueDescriptor newValue = currAggregate.getInputColumnValue(newRow);
				// A NULL value is always distinct, so we only
				// have to check for duplicate values for
				// non-NULL values.
				if (newValue.getString() != null)
				{
					if (distinctValues[level][i].contains(
						    newValue.getString()))
						continue;
					distinctValues[level][i].add(
						newValue.getString());
				}
			}

			// merge the aggregator
			currAggregate.merge(newRow, currRow);
		}
	}

	private void initializeDistinctMaps(int r, boolean allocate)
	    throws StandardException
	{
		for (int a = 0; a < aggregates.length; a++)
		{
			AggregatorInfo aInfo = (AggregatorInfo)
						aggInfoList.elementAt(a);
			if (aInfo.isDistinct())
			{
				if (allocate)
					distinctValues[r][a] = new HashSet();
				else
					distinctValues[r][a].clear();
				DataValueDescriptor newValue =
					aggregates[a].getInputColumnValue(resultRows[r]);
				distinctValues[r][a].add(newValue.getString());
			}
		}
	}

        private void dumpAllRows(int cR)
            throws StandardException
        {
            System.out.println("dumpAllRows("+cR+"/"+resultRows.length+"):");
            for (int r = 0; r < resultRows.length; r++)
                System.out.println(dumpRow(resultRows[r]));
        }
	private String dumpRow(ExecRow r)
		throws StandardException
	{
            if (r == null)
                return "<NULL ROW>";
	    StringBuffer buf = new StringBuffer();
	    int nCols = r.nColumns();
	    for (int d = 0; d < nCols; d++)
	    {
		if (d > 0) buf.append(",");
                DataValueDescriptor o = r.getColumn(d+1);
                buf.append(o.getString());
                if (o instanceof ExecAggregator)
                    buf.append("[").
                        append(((ExecAggregator)o).getResult().getString()).
                        append("]");
	    }
	    return buf.toString();
	}
}
