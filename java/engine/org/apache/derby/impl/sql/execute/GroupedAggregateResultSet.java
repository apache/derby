/*

   Derby - Class org.apache.derby.impl.sql.execute.GroupedAggregateResultSet

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
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;

import java.util.Properties;
import java.util.Vector;
import java.util.Enumeration;

/**
 * This ResultSet evaluates grouped, non distinct aggregates.
 * It will scan the entire source result set and calculate
 * the grouped aggregates when scanning the source during the 
 * first call to next().
 *
 * @author jerry (broken out from SortResultSet)
 */
public class GroupedAggregateResultSet extends GenericAggregateResultSet
	implements CursorResultSet {

	/* Run time statistics variables */
	public int rowsInput;
	public int rowsReturned;

    // set in constructor and not altered during
    // life of object.
	private ColumnOrdering[] order;
	private ExecIndexRow sortTemplateRow;
	public	boolean	hasDistinctAggregate;	// true if distinct aggregate
	public	boolean isInSortedOrder;				// true if source results in sorted order
	private int maxRowSize;

	// set in open and not modified thereafter
    private ScanController scanController;

	// Cache ExecIndexRow
	private ExecIndexRow sourceExecIndexRow;

	private ExecIndexRow sortResultRow;

	// In order group bys
	private ExecIndexRow currSortedRow;
	private boolean nextCalled;

	// used to track and close sorts
	private long distinctAggSortId;
	private boolean dropDistinctAggSort;
	private long genericSortId;
	private boolean dropGenericSort;
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
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	maxRowSize		approx row size, passed to sorter
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    public GroupedAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost,
					GeneratedMethod c) throws StandardException 
	{
		super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, c);
		this.isInSortedOrder = isInSortedOrder;
		sortTemplateRow = getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		order = (ColumnOrdering[])
					((FormatableArrayHolder)
						(a.getPreparedStatement().getSavedObject(orderingItem)))
					.getArray(ColumnOrdering.class);

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
		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "GroupedAggregateResultSet already open");

		sortResultRow = getExecutionFactory().getIndexableRow(sortTemplateRow.getClone());
		sourceExecIndexRow = getExecutionFactory().getIndexableRow(sortTemplateRow.getClone());

        source.openCore();

		/* If this is an in-order group by then we do not need the sorter.
		 * (We can do the aggregation ourselves.)
		 * We save a clone of the first row so that subsequent next()s
		 * do not overwrite the saved row.
		 */
		if (isInSortedOrder)
		{
			currSortedRow = getNextRowFromRS();
			if (currSortedRow != null)
			{
				currSortedRow = (ExecIndexRow) currSortedRow.getClone();
				initializeVectorAggregation(currSortedRow);
			}
		}
		else
		{
			/*
			** Load up the sorter
			*/
			scanController = loadSorter();
		}

	    isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * Load up the sorter.  Feed it every row from the
	 * source scan.  If we have a vector aggregate, initialize
	 * the aggregator for each source row.  When done, close
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
		long 					sortId;
		ExecRow 				sourceRow;
		ExecRow 				inputRow;
		int						inputRowCountEstimate = (int) optimizerEstimatedRowCount;
		boolean					inOrder = isInSortedOrder;

		tc = getTransactionController();

		ColumnOrdering[] currentOrdering = order;

		/*
		** Do we have any distinct aggregates?  If so, we'll need
		** a separate sort.  We use all of the sorting columns and
		** drop the aggregation on the distinct column.  Then
		** we'll feed this into the sorter again w/o the distinct
		** column in the ordering list.
		*/
		if (aggInfoList.hasDistinct())
		{
			hasDistinctAggregate = true;
			
			GenericAggregator[] aggsNoDistinct = getSortAggregators(aggInfoList, true,
						activation.getLanguageConnectionContext(), source);
			SortObserver sortObserver = new AggregateSortObserver(true, aggsNoDistinct, aggregates,
																  sortTemplateRow);

			sortId = tc.createSort((Properties)null, 
					sortTemplateRow.getRowArray(),
					order,
					sortObserver,
					false,			// not in order
					inputRowCountEstimate,				// est rows, -1 means no idea	
					maxRowSize		// est rowsize
					);
			sorter = tc.openSort(sortId);
			distinctAggSortId = sortId;
			dropDistinctAggSort = true;
				
			while ((sourceRow = source.getNextRowCore())!=null) 
			{
				sorter.insert(sourceRow.getRowArray());
				rowsInput++;
			}

			/*
			** End the sort and open up the result set
			*/
			source.close();
			sortProperties = sorter.getSortInfo().getAllSortInfo(sortProperties);
			sorter.close();

			scanController = 
                tc.openSortScan(sortId, activation.getResultSetHoldability());
			
			/*
			** Aggs are initialized and input rows
			** are in order.  All we have to do is
			** another sort to remove (merge) the 
			** duplicates in the distinct column
			*/	
			inOrder = true;
			inputRowCountEstimate = rowsInput;
	
			/*
			** Drop the last column from the ordering.  The
			** last column is the distinct column.  Don't
			** pay any attention to the fact that the ordering
			** object's name happens to correspond to a techo
			** band from the 80's.
			**
			** If there aren't any ordering columns other
			** than the distinct (i.e. for scalar distincts)
			** just skip the 2nd sort altogether -- we'll
			** do the aggregate merge ourselves rather than
			** force a 2nd sort.
			*/
			if (order.length == 1)
			{
				return scanController;
			}

			ColumnOrdering[] newOrder = new ColumnOrdering[order.length - 1];
			System.arraycopy(order, 0, newOrder, 0, order.length - 1);
			currentOrdering = newOrder;
		}

		SortObserver sortObserver = new AggregateSortObserver(true, aggregates, aggregates,
															  sortTemplateRow);

		sortId = tc.createSort((Properties)null, 
						sortTemplateRow.getRowArray(),
						currentOrdering,
						sortObserver,
						inOrder,
						inputRowCountEstimate, // est rows
					 	maxRowSize			// est rowsize 
						);
		sorter = tc.openSort(sortId);
		genericSortId = sortId;
		dropGenericSort = true;
	
		/* The sorter is responsible for doing the cloning */
		while ((inputRow = getNextRowFromRS()) != null) 
		{
			sorter.insert(inputRow.getRowArray());
		}
		source.close();
		sortProperties = sorter.getSortInfo().getAllSortInfo(sortProperties);
		sorter.close();

		return tc.openSortScan(sortId, activation.getResultSetHoldability());
	}


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

		// In order group by
		if (isInSortedOrder)
		{
			// No rows, no work to do
			if (currSortedRow == null)
			{
				nextTime += getElapsedMillis(beginTime);
				return null;
			}

		    ExecIndexRow nextRow = getNextRowFromRS();

			/* Drain and merge rows until we find new distinct values for the grouping columns. */
			while (nextRow != null)
			{
				/* We found a new set of values for the grouping columns.  
				 * Update the current row and return this group. 
				 */
				if (! sameGroupingValues(currSortedRow, nextRow))
				{
					ExecIndexRow result = currSortedRow;

					/* Save a clone of the new row so that it doesn't get overwritten */
					currSortedRow = (ExecIndexRow) nextRow.getClone();
					initializeVectorAggregation(currSortedRow);

					nextTime += getElapsedMillis(beginTime);
					rowsReturned++;
					return finishAggregation(result);
				}
				else
				{
					/* Same group - initialize the new row and then merge the aggregates */
					initializeVectorAggregation(nextRow);
					mergeVectorAggregates(nextRow, currSortedRow);
				}

				// Get the next row
				nextRow = getNextRowFromRS();
			}

			// We've drained the source, so no more rows to return
			ExecIndexRow result = currSortedRow;
			currSortedRow = null;
			nextTime += getElapsedMillis(beginTime);
			return finishAggregation(result);
		}
		else
		{
		    ExecIndexRow sortResult = null;

	        if ((sortResult = getNextRowFromRS()) != null)
			{
				setCurrentRow(sortResult);
			}

			/*
			** Only finish the aggregation
			** if we have a return row.  We don't generate
			** a row on a vector aggregate unless there was
			** a group.
			*/
			if (sortResult != null)
			{
				sortResult = finishAggregation(sortResult);
				currentRow = sortResult;
			}

			if (sortResult != null)
			{
				rowsReturned++;
			}

			nextTime += getElapsedMillis(beginTime);
		    return sortResult;
		}
	}

	/**
	 * Return whether or not the new row has the same values for the 
	 * grouping columns as the current row.  (This allows us to process in-order
	 * group bys without a sorter.)
	 *
	 * @param currRow	The current row.
	 * @param newRow	The new row.
	 *
	 * @return	Whether or not to filter out the new row has the same values for the 
	 *			grouping columns as the current row.
	 *
	 * @exception StandardException thrown on failure to get row location
	 */
	private boolean sameGroupingValues(ExecRow currRow, ExecRow newRow)
		throws StandardException
	{
		for (int index = 0; index < order.length; index++)
		{
			DataValueDescriptor currOrderable = currRow.getColumn(order[index].getColumnId() + 1);
			DataValueDescriptor newOrderable = newRow.getColumn(order[index].getColumnId() + 1);
			if (! (currOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS, newOrderable, true, true)))
			{
				return false;
			}
		}
		return true;
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

			currentRow = null;
			sortResultRow = null;
			sourceExecIndexRow = null;
			closeSource();

			if (dropDistinctAggSort)
			{
				tc.dropSort(distinctAggSortId);
				dropDistinctAggSort = false;
			}

			if (dropGenericSort)
			{
				tc.dropSort(genericSortId);
				dropGenericSort = false;
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
	 * @exception	standard cloudscape exception
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
	 * @exception	standard cloudscape exception
	 */
	private void mergeVectorAggregates(ExecRow newRow, ExecRow currRow)
		throws StandardException
	{
		for (int i = 0; i < aggregates.length; i++)
		{
			GenericAggregator currAggregate = aggregates[i];

			// merge the aggregator
			currAggregate.merge(newRow, currRow);
		}
	}

}
