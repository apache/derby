/*

   Derby - Class org.apache.derby.impl.sql.execute.SortResultSet

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

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;

import java.util.Properties;
import java.util.Vector;
import java.util.Enumeration;

/**
 * Takes a source result set, sends it to the sorter,
 * and returns the results.  If distinct is true, removes
 * all but one copy of duplicate rows using DistinctAggregator,
 * which really doesn't aggregate anything at all -- the sorter
 * assumes that the presence of an aggregator means that
 * it should return a single row for each set with identical
 * ordering columns.
 * <p>
 * If aggregate is true, then it feeds any number of aggregates
 * to the sorter.  Each aggregate is an instance of GenericAggregator
 * which knows which Aggregator to call to perform the
 * aggregation.
 * <p>
 * Brief background on the sorter and aggregates: the sorter
 * has some rudimentary knowledge about aggregates.  If
 * it is passed aggregates, it will eliminate duplicates
 * on the ordering columns.  In the process it will call the
 * aggregator on each row that is discarded.
 * <p> 
 * Note that a DISTINCT on the SELECT list and an aggregate cannot 
 * be processed by the same SortResultSet(), if there are both
 * aggregates (distinct or otherwise) and a DISTINCT on the select
 * list, then 2 separate SortResultSets are required (the DISTINCT
 * is a sort on the output of the sort with the aggregation). 
 * <p>
 * Currently, all rows are fed through the sorter.  This is
 * true even if there is no sorting needed.  In this case
 * we feed every row in and just pull every row out (this is
 * an obvious area for a performance improvement).  We'll
 * need to know if the rows are sorted before we can make
 * any optimizations in this area.
 * <p>
 * <B>CLONING</B>: Cloning and sorts are an important topic.
 * Currently we do a lot of cloning.  We clone the following: <UL>
 * <LI> every row that is inserted into the sorter.  We
 * need to clone the rows because the source result set might
 * be reusing rows, and we need to be able to accumulate the
 * entire result set in the sorter. </LI>
 * <p>
 * There are two cloning APIs: cloning by the sorter on
 * rows that are not discarded as duplicates or cloning
 * in the SortResultSet prior to inserting into the sorter.
 * If we have any aggregates at all we always clone prior
 * to inserting into the sorter.  We need to do this 
 * because we have to set up the aggregators before passing
 * them into the sorter.  When we don't have aggregates
 * we let the sorter to the cloning to avoid unnecessary
 * clones on duplicate rows that are going to be discarded
 * anyway.
 *
 * @author ames, rewrite for aggregates by jamie, aggregate removal by jerry
 */
public class SortResultSet extends NoPutResultSetImpl
	implements CursorResultSet 
{

	/* Run time statistics variables */
	public int rowsInput;
	public int rowsReturned;
	public boolean distinct;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
    private GeneratedMethod closeCleanup;
	private GeneratedMethod rowAllocator;
	private ColumnOrdering[] order;
	private ColumnOrdering[] savedOrder;
	private SortObserver observer;
	private ExecRow sortTemplateRow;
	public	boolean isInSortedOrder;				// true if source results in sorted order
	private	NoPutResultSet	originalSource; // used for run time stats only
	private int maxRowSize;

	// set in open and not modified thereafter
    private ScanController scanController;

	// argument to getNextRowFromRS()

	private ExecRow sortResultRow;

	// In order distincts
	private ExecRow currSortedRow;
	private boolean nextCalled;
	private int numColumns;

	// used to track and close sorts
	private long genericSortId;
	private boolean dropGenericSort;

	// remember whether or not any sort was performed
	private boolean sorted;

	// RTS
	public Properties sortProperties = new Properties();

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	distinct	if this is a DISTINCT select list.  
	 *		Also set to true for a GROUP BY w/o aggretates
	 * @param	isInSortedOrder	true if the source results are in sorted order
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
    public SortResultSet(NoPutResultSet s,
					boolean distinct,
					boolean isInSortedOrder,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost,
					GeneratedMethod c) throws StandardException 
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.distinct = distinct;
		this.isInSortedOrder = isInSortedOrder;
        source = s;
        originalSource = s;
		rowAllocator = ra;
		this.maxRowSize = maxRowSize;
        closeCleanup = c;
		sortTemplateRow = (ExecRow) rowAllocator.invoke(activation);
		order = (ColumnOrdering[])
					((FormatableArrayHolder)
						(a.getPreparedStatement().getSavedObject(orderingItem)))
					.getArray(ColumnOrdering.class);

		/* NOTE: We need to save order to another variable
		 * in the constructor and reset it on every open.
		 * This is important because order can get reset in the
		 * guts of execution below.  Subsequent sorts could get
		 * the wrong result without this logic.
		 */
		savedOrder = order;

		/*
		** Create a sort observer that are retained by the
		** sort.
		*/
		observer = new BasicSortObserver(true, distinct, sortTemplateRow, true);

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
	    	SanityManager.ASSERT( ! isOpen, "SortResultSet already open");

		/* NOTE: We need to save order to another variable
		 * in the constructor and reset it on every open.
		 * This is important because order can get reset in the
		 * guts of execution below.  Subsequent sorts could get
		 * the wrong result without this logic.
		 */
		order = savedOrder;

		sortResultRow = sortTemplateRow.getClone();

        source.openCore();

		/* If this is an in-order distinct then we do not need the sorter.
		 * (We filter out the duplicate rows ourselves.)
		 * We save a clone of the first row so that subsequent next()s
		 * do not overwrite the saved row.
		 */
		if (isInSortedOrder && distinct)
		{
			currSortedRow = getNextRowFromRS();
			if (currSortedRow != null)
			{
				currSortedRow = (ExecRow) currSortedRow.getClone();
			}
		}
		else
		{
			/*
			** Load up the sorter.
			*/
			scanController = loadSorter();
			sorted = true;
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
		long 					sortId;
		ExecRow 				sourceRow;
		ExecRow					inputRow;
		boolean					inOrder = (order.length == 0 || isInSortedOrder);
		int						inputRowCountEstimate = (int) optimizerEstimatedRowCount;

		// find the language context and
        // Get the current transaction controller
		TransactionController tc = getTransactionController();
		sortId = tc.createSort((Properties)null, 
						sortTemplateRow.getRowArray(),
						order,
						observer,
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
			/* The sorter is responsible for doing the cloning */
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

		// In order distinct
		if (isInSortedOrder && distinct)
		{
			// No rows, no work to do
			if (currSortedRow == null)
			{
				nextTime += getElapsedMillis(beginTime);
				return null;
			}

			/* If this is the 1st next, then simply return the 1st row
			 * (which we got on the open()).
			 */
			if (! nextCalled)
			{
				nextCalled = true;
				numColumns = currSortedRow.getRowArray().length;
				nextTime += getElapsedMillis(beginTime);
				rowsReturned++;
				setCurrentRow(currSortedRow);
				return currSortedRow;
			}

		    ExecRow sortResult = getNextRowFromRS();

			/* Drain and throw away rows until we find a new distinct row. */
			while (sortResult != null)
			{
				/* We found a new row.  Update the current row and return this one. */
				if (! filterRow(currSortedRow, sortResult))
				{
					/* Save a clone of the new row so that it doesn't get overwritten */
					currSortedRow = (ExecRow) sortResult.getClone();
					setCurrentRow(currSortedRow);
					nextTime += getElapsedMillis(beginTime);
					rowsReturned++;
					return currSortedRow;
				}

				// Get the next row
				sortResult = getNextRowFromRS();
			}

			// We've drained the source, so no more rows to return
			currSortedRow = null;
			nextTime += getElapsedMillis(beginTime);
			return null;
		}
		else
		{
		    ExecRow sortResult = getNextRowFromRS();

			if (sortResult != null)
			{
				setCurrentRow(sortResult);
				rowsReturned++;
			}
			nextTime += getElapsedMillis(beginTime);
		    return sortResult;
		}
	}

	/**
	 * Filter out the new row if it has the same contents as
	 * the current row.  (This allows us to process in-order
	 * distincts without a sorter.)
	 *
	 * @param currRow	The current row.
	 * @param newRow	The new row.
	 *
	 * @return	Whether or not to filter out the new row.
	 *
	 * @exception StandardException thrown on failure to get row location
	 */
	private boolean filterRow(ExecRow currRow, ExecRow newRow)
		throws StandardException
	{
		for (int index = 1; index <= numColumns; index++)
		{
			DataValueDescriptor currOrderable = currRow.getColumn(index);
			DataValueDescriptor newOrderable = newRow.getColumn(index);
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
			closeSource();

			if (dropGenericSort)
			{
				getTransactionController().dropSort(genericSortId);
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

	public void	finish() throws StandardException
	{
		source.finish();
		finishAndRTS();
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

		/*
			DISTINCT assumes the currentRow is good, since it
			is the only one with access to its sort scan result
		 */
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
	private ExecRow getNextRowFromRS()
		throws StandardException
	{
		return (scanController == null) ?
			getRowFromResultSet() :
			getRowFromSorter();
	}

	/**
	 * Get a row from the input result set.  
	 */	
	private ExecRow getRowFromResultSet()
		throws StandardException
	{
		ExecRow				sourceRow;
		ExecRow			inputRow = null;	

		if ((sourceRow = source.getNextRowCore()) != null)
		{
			rowsInput++;
			inputRow = sourceRow;
		}

		return inputRow;
	}


	/**
	 * Get a row from the sorter.  Side effects:
	 * sets currentRow.
	 */
	private ExecRow getRowFromSorter()
		throws StandardException
	{
		ExecRow			inputRow = null;	
		
		if (scanController.next())
		{
			// REMIND: HACKALERT we are assuming that result will
			// point to what sortResult is manipulating when
			// we complete the fetch.
			currentRow = sortResultRow;

			inputRow = sortResultRow;

			scanController.fetch(inputRow.getRowArray());
		}
		return inputRow;
	}

	/**
	 * Close the source of whatever we have been scanning.
	 */
	private void closeSource() throws StandardException
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
}
