/*

   Derby - Class org.apache.derby.impl.sql.execute.DistinctScalarAggregateResultSet

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

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;

import java.util.Properties;


/**
 * This ResultSet evaluates scalar aggregates where
 * 1 (or more, in the future) of the aggregates are distinct.
 * It will scan the entire source result set and calculate
 * the scalar aggregates when scanning the source during the 
 * first call to next().
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
class DistinctScalarAggregateResultSet extends ScalarAggregateResultSet
{
	private ColumnOrdering[] order;
	private int maxRowSize;
	private boolean dropDistinctAggSort;
	private	long sortId;

	// set in open and not modified thereafter
    private ScanController scanController;

	private ExecIndexRow sortResultRow;

	// remember whether or not any sort was performed
	private boolean sorted;

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine. 
	 * @param	a				activation
	 * @param	ra				saved object that builds an empty output row
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
    DistinctScalarAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					int ra,
					int maxRowSize,
					int resultSetNumber,
					boolean singleInputRow,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost) throws StandardException 
	{
		super(s, isInSortedOrder, aggregateItem, a, ra,
			  resultSetNumber, 
			  singleInputRow,
			  optimizerEstimatedRowCount,
			  optimizerEstimatedCost);
//IC see: https://issues.apache.org/jira/browse/DERBY-1700

//IC see: https://issues.apache.org/jira/browse/DERBY-6292
        order = ((FormatableArrayHolder)
                    (a.getPreparedStatement().getSavedObject(orderingItem)))
                        .getArray(ColumnOrdering[].class);

		this.maxRowSize = maxRowSize;

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
	    	SanityManager.ASSERT( ! isOpen, "DistinctScalarResultSet already open");

//IC see: https://issues.apache.org/jira/browse/DERBY-6003
        sortResultRow = (ExecIndexRow) getRowTemplate().getClone();
        sourceExecIndexRow = (ExecIndexRow) getRowTemplate().getClone();

        source.openCore();

//IC see: https://issues.apache.org/jira/browse/DERBY-4330
		try {
			/*
			** Load up the sorter because we have something to sort.
			*/
			scanController = loadSorter();
		} catch (StandardException e) {
			// DERBY-4330 Result set tree must be atomically open or
			// closed for reuse to work (after DERBY-827).

			isOpen = true; // to make close do its thing:
			try { close(); } catch (StandardException ee) {}
			throw e;
		}

		sorted = true;

	    isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/* RESOLVE - THIS NEXT METHOD IS ONLY INCLUDED BECAUSE OF A JIT ERROR. THERE IS NO OTHER
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6216
		if( isXplainOnlyMode() )
			return null;

	    ExecIndexRow execIndexRow = null;
	    ExecIndexRow aggResult = null;
		boolean	cloneArg = true;

		beginTime = getCurrentTimeMillis();
	    if (isOpen)
	    {
			/*
			** We are dealing with a scalar aggregate.
			** Zip through each row and accumulate.
			** Accumulate into the first row.  Only
			** the first row is cloned.
			*/
	        while ((execIndexRow = getRowFromResultSet(cloneArg)) != null)
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
					cloneArg = false;
					aggResult = (ExecIndexRow) execIndexRow.getClone();
				}
				else
				{
					/*
					** Accumulate all aggregates.  For the distinct
					** aggregates, we'll be accumulating, for the nondistinct
					** we'll be merging.
					*/
					accumulateScalarAggregation(execIndexRow, aggResult, true);
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
				setCurrentRow(aggResult);
				countOfRows++;
			}
	    }

		nextTime += getElapsedMillis(beginTime);
		return aggResult;
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

		if (scanController != null)
		{
			scanController.close();
			scanController = null;
		}

		source.reopenCore();

		/*
		** Load up the sorter because we have something to sort.
		*/
		scanController = loadSorter();
		sorted = true;
		numOpens++;
        countOfRows = 0;

		openTime += getElapsedMillis(beginTime);
	}

        /**
         * If the result set has been opened,
		 * close the open scan.
         */
        public void close() throws StandardException
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-756
            super.close();
            closeSource();
        }
	///////////////////////////////////////////////////////////////////////////////
	//
	// SCAN ABSTRACTION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Get a row from the sorter.  Side effects:
	 * sets currentRow.
	 *
	 * @exception StandardException Thrown on error
	 */
	public ExecIndexRow getRowFromResultSet(boolean doClone)
		throws StandardException
	{
		ExecIndexRow			inputRow = null;	
		
		if (scanController.next())
		{
			// REMIND: HACKALERT we are assuming that result will
			// point to what sortResult is manipulating when
			// we complete the fetch.
			currentRow = doClone ? 
				sortResultRow.getClone() : sortResultRow;

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
	protected void	closeSource() throws StandardException
	{
		if (scanController != null)
		{
			if (dropDistinctAggSort)
			{
				try
				{
					getTransactionController().dropSort(sortId);
				}
				catch (StandardException se)
				{
					// Eat all errors at close() time
				}
				dropDistinctAggSort = false;
			}
			scanController.close();
			scanController = null;
		}
		source.close();
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// MISC UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

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
		ExecRow 				sourceRow;
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
        ExecIndexRow            sortTemplateRow = getRowTemplate();
		int						inputRowCountEstimate = (int) optimizerEstimatedRowCount;

		TransactionController tc = getTransactionController();

		/*
		** We have a distinct aggregate so, we'll need
		** to do a sort.  We use all of the sorting columns and
		** drop the aggregation on the distinct column.  Then
		** we'll feed this into the sorter again w/o the distinct
		** column in the ordering list.
		*/
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
		dropDistinctAggSort = true;
				
		while ((sourceRow = source.getNextRowCore())!=null) 
		{
			sorter.insert(sourceRow.getRowArray());
			rowsInput++;
		}

		/*
		** End the sort and open up the result set
		*/
		sorter.completedInserts();
//IC see: https://issues.apache.org/jira/browse/DERBY-2486

		scanController = 
            tc.openSortScan(sortId, activation.getResultSetHoldability());
			
		/*
		** Aggs are initialized and input rows
		** are in order.
		*/	
		inputRowCountEstimate = rowsInput;
	
		return scanController;
	}

}
