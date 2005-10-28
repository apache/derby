/*

   Derby - Class org.apache.derby.impl.store.access.sort.MergeSort

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

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.ScanControllerRowSource;
import org.apache.derby.iapi.store.access.conglomerate.Sort;
import org.apache.derby.iapi.store.access.conglomerate.SortFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.types.CloneableObject;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

/**

  A sort implementation which does the sort in-memory if it can,
  but which can do an external merge sort so that it can sort an
  arbitrary number of rows.

**/

public final class MergeSort implements Sort
{

	/*
	 * Fields
	 */

	/**
	**/
	static final int STATE_CLOSED = 0;

	/**
	**/
	static final int STATE_INITIALIZED = 1;

	/**
	**/
	static final int STATE_INSERTING = 2;

	/**
	**/
	static final int STATE_DONE_INSERTING = 3;

	/**
	**/
	static final int STATE_SCANNING = 4;

	/**
	**/
	static final int STATE_DONE_SCANNING = 5;

	/**
	Maintains the current state of the sort as defined in
	the preceding values.  Sorts start off and end up closed.
	**/
	protected int state = STATE_CLOSED;

	/**
	The template as passed in on create.  Valid when the state
	is INITIALIZED through SCANNING, null otherwise.
	**/
	protected DataValueDescriptor[] template;

	/**
	The column ordering as passed in on create.  Valid when
	the state is INITIALIZED through SCANNING, null otherwise.
	May be null if there is no column ordering - this means
	that all rows are considered to be duplicates, and the
	sort will only emit a single row.
	**/
	protected ColumnOrdering columnOrdering[];

	/**
    A lookup table to speed up lookup of a column associated with the i'th
    column to compare.  To find the column id to compare as the i'th column
    look in columnOrderingMap[i].
	**/
	protected int columnOrderingMap[];

	/**
    A lookup table to speed up lookup of Ascending state of a column, 
	**/
	protected boolean columnOrderingAscendingMap[];

	/**
	The sort observer.  May be null.  Used as a callback.
	**/
	protected SortObserver sortObserver;

	/**
	Whether the rows are expected to be in order on insert,
	as passed in on create.
	**/
	protected boolean alreadyInOrder;

	/**
	The inserter that's being used to insert rows into the sort.
	This field is only valid when the state is INSERTING.
	**/
	protected MergeInserter inserter = null;

	/**
	The scan that's being used to return rows from the sort.
	This field is only valid when the state is SCANNING.
	**/
	protected Scan scan = null;

	/**
	A vector of merge runs, produced by the MergeInserter.
	Might be null if no merge runs were produced.
	It is a vector of container ids.
	**/
	protected Vector mergeRuns = null;

	/**
	An ordered set of the leftover rows that didn't go
	in the last merge run (might be all the rows if there
	are no merge runs).
	**/
	protected SortBuffer sortBuffer = null;

	/**
	The maximum number of entries a sort buffer can hold.
	**/
	protected int sortBufferMax;

	/**
	The minimum number of entries a sort buffer can hold.
	**/
	protected int sortBufferMin;

	/**
	Properties for mergeSort
	**/
	static Properties properties = null;

    /**
    Static initializer for MergeSort, to initialize once the properties
	for the sortBuffer.  
	**/
    static
    {
		properties = new Properties();
		properties.put(RawStoreFactory.STREAM_FILE_BUFFER_SIZE_PARAMETER, "16384");
    }

	/*
	 * Methods of Sort
	 */

	/**
	Open a sort controller.
	<p>
	This implementation only supports a single sort controller
	per sort.
	@see Sort#open
	**/
	public SortController open(TransactionManager tran)
		throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(state == STATE_INITIALIZED);

		// Ready to start inserting rows.
		state = STATE_INSERTING;

		// Create and initialize an inserter.  When the caller
		// closes it, it will call back to inserterIsClosed().
		this.inserter = new MergeInserter();
		if (this.inserter.initialize(this, tran) == false)
        {
			throw StandardException.newException(SQLState.SORT_COULD_NOT_INIT);
        }

		return this.inserter;
	}

	/**
	Open a scan controller.
	@see Sort#openSortScan
	**/

	public ScanController openSortScan(
    TransactionManager tran,
    boolean            hold)
			throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(state == STATE_DONE_INSERTING);

		if (mergeRuns == null || mergeRuns.size() == 0)
		{
			// There were no merge runs so we can just return
			// the rows from the sort buffer.
			scan = new SortBufferScan(this, tran, sortBuffer, hold);

			// The scan now owns the sort buffer
			sortBuffer = null;
		}
		else
		{
			// Dump the rows in the sort buffer to a merge run.
			long containerId = createMergeRun(tran, sortBuffer);
			mergeRuns.addElement(new Long(containerId));

			// If there are more merge runs than we can sort
			// at once with our sort buffer, we have to reduce
			// the number of merge runs
			if (mergeRuns.size() > ExternalSortFactory.DEFAULT_MAX_MERGE_RUN ||
				mergeRuns.size() > sortBuffer.capacity())
					multiStageMerge(tran);

			// There are now few enough merge runs to sort
			// at once, so create a scan for them.
			MergeScan mscan = 
                new MergeScan(
                    this, tran, sortBuffer, mergeRuns, sortObserver, hold);

			if (!mscan.init(tran))
            {
                throw StandardException.newException(
                        SQLState.SORT_COULD_NOT_INIT);
            }
			scan = mscan;

			// The scan now owns the sort buffer and merge runs.
			sortBuffer = null;
			mergeRuns = null;
		}

		// Ready to start retrieving rows.
		this.state = STATE_SCANNING;

		return scan;
	}

	/**
	Open a row source to get rows out of the sorter.
	@see Sort#openSortRowSource
	**/
	public ScanControllerRowSource openSortRowSource(TransactionManager tran)
			throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(state == STATE_DONE_INSERTING);

		ScanControllerRowSource rowSource = null;

		if (mergeRuns == null || mergeRuns.size() == 0)
		{
			// There were no merge runs so we can just return
			// the rows from the sort buffer.
			scan = new SortBufferRowSource(sortBuffer, tran, sortObserver, false, sortBufferMax);
			rowSource = (ScanControllerRowSource)scan;

			// The scan now owns the sort buffer
			sortBuffer = null;
		}
		else
		{
			// Dump the rows in the sort buffer to a merge run.
			long containerId = createMergeRun(tran, sortBuffer);
			mergeRuns.addElement(new Long(containerId));

			// If there are more merge runs than we can sort
			// at once with our sort buffer, we have to reduce
			// the number of merge runs
			if (mergeRuns.size() > ExternalSortFactory.DEFAULT_MAX_MERGE_RUN ||
				mergeRuns.size() > sortBuffer.capacity())	
				multiStageMerge(tran);

			// There are now few enough merge runs to sort
			// at once, so create a rowSource for them.
			MergeScanRowSource msRowSource = 
				new MergeScanRowSource(this, tran, sortBuffer, mergeRuns, sortObserver, false);
			if (!msRowSource.init(tran))
            {
                throw StandardException.newException(
                        SQLState.SORT_COULD_NOT_INIT);
            }
			scan = msRowSource;
			rowSource = msRowSource;

			// The scan now owns the sort buffer and merge runs.
			sortBuffer = null;
			mergeRuns = null;
		}

		// Ready to start retrieving rows.
		this.state = STATE_SCANNING;

		return rowSource;
	}



	/**
	Drop the sort.
	@see Sort#drop
	**/
	public void drop(TransactionController tran)
        throws StandardException
	{
		// Make sure the inserter is closed.  Note this
		// will cause the callback to doneInserting()
		// which will give us any in-progress merge
		// runs, if there are any.
		if (inserter != null)
			inserter.close();
		inserter = null;

		// Make sure the scan is closed, if there is one.
		// This will cause the callback to doneScanning().
		if (scan != null)
		{
			scan.close();
			scan = null;
		}

		// If we have a row set, get rid of it.
		if (sortBuffer != null)
		{
			sortBuffer.close();
			sortBuffer = null;
		}

		// Clean out the rest of the objects.
		template = null;
		columnOrdering = null;
		sortObserver = null;

		// If there are any merge runs, drop them.
		dropMergeRuns((TransactionManager)tran);

		// Whew!
		state = STATE_CLOSED;
	}


	/*
	 * Methods of MergeSort.  Arranged alphabetically.
	 */

	/**
	Check the column ordering against the template, making
	sure that each column is present in the template,
	implements Orderable, and is not mentioned more than
	once.  Intended to be called as part of a sanity check.
	**/
	protected boolean checkColumnOrdering(
    DataValueDescriptor[]   template, 
    ColumnOrdering          columnOrdering[])
	{
		// Allocate an array to check that each column mentioned only once.
		int templateNColumns = template.length;
		boolean seen[] = new boolean[templateNColumns];

		// Check each column ordering.
		for (int i = 0; i < columnOrdering.length; i++)
		{
			int colid = columnOrdering[i].getColumnId();

			// Check that the column id is valid.
			if (colid < 0 || colid >= templateNColumns)
				return false;
			
			// Check that the column isn't mentioned more than once.
			if (seen[colid])
				return false;
			seen[colid] = true;

			Object columnVal = 
                RowUtil.getColumn(template, (FormatableBitSet) null, colid);

			if (!(columnVal instanceof Orderable))
				return false;
		}

		return true;
	}

	/**
	Check that the columns in the row agree with the columns
	in the template, both in number and in type.
	<p>
	XXX (nat) Currently checks that the classes implementing
	each column are the same -- is this right?
	**/
	void checkColumnTypes(DataValueDescriptor[] row)
		throws StandardException
	{
		int nCols = row.length;
		if (template.length != nCols)
		{
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "template.length (" + template.length +
                    ") expected to be = to nCols (" +
                    nCols + ")");
            }
            throw StandardException.newException(
                    SQLState.SORT_TYPE_MISMATCH);
		}

        if (SanityManager.DEBUG)
        {
            for (int colid = 0; colid < nCols; colid++)
            {
                Object col1 = row[colid];
                Object col2 = template[colid];
                if (col1 == null)
				{
					SanityManager.THROWASSERT(
						"col[" + colid + "]  is null");
				}
						
                if (!(col1 instanceof CloneableObject))
				{
					SanityManager.THROWASSERT(
						"col[" + colid + "] (" +col1.getClass().getName()+
						") is not a CloneableObject.");
				}

                if (col1.getClass() != col2.getClass())
                {
                    SanityManager.THROWASSERT(
                        "col1.getClass() (" + col1.getClass() +
                        ") expected to be the same as col2.getClass() (" +
                        col2.getClass() + ")");
                }
            }
        }
	}

	int compare(
    DataValueDescriptor[] r1, 
    DataValueDescriptor[] r2)
		throws StandardException
	{
		// Get the number of columns we have to compare.
		int colsToCompare = this.columnOrdering.length;
        int r;

		// Compare the columns specified in the column
		// ordering array.
        for (int i = 0; i < colsToCompare; i++)
        {
			// Get columns to compare.
            int colid = this.columnOrderingMap[i];

			// If the columns don't compare equal, we're done.
			// Return the sense of the comparison.
			if ((r = r1[colid].compare(r2[colid])) 
                    != 0)
			{
				if (this.columnOrderingAscendingMap[i])
					return r;
				else
					return -r;
			}
		}

		// We made it through all the columns, and they must have
		// all compared equal.  So return that the rows compare equal.
		return 0;
	}

	/**
	Go from the CLOSED to the INITIALIZED state.
	**/
	public void initialize(
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    SortObserver            sortObserver,
    boolean                 alreadyInOrder,
    long                    estimatedRows,
    int                     sortBufferMax)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(state == STATE_CLOSED);
    	}

		// Make sure the column ordering makes sense
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(checkColumnOrdering(template, columnOrdering),
	    		"column ordering error");
	    }

		// Set user-defined parameters.
		this.template = template;
		this.columnOrdering = columnOrdering;
		this.sortObserver = sortObserver;
		this.alreadyInOrder = alreadyInOrder;

        // Cache results of columnOrdering calls, results are not allowed
        // to change throughout a sort.
        columnOrderingMap          = new int[columnOrdering.length];
        columnOrderingAscendingMap = new boolean[columnOrdering.length];
        for (int i = 0; i < columnOrdering.length; i++)
        {
            columnOrderingMap[i] = columnOrdering[i].getColumnId();
            columnOrderingAscendingMap[i] = columnOrdering[i].getIsAscending();
        }

		// No inserter or scan yet.
		this.inserter = null;
		this.scan = null;

		// We don't have any merge runs.
		this.mergeRuns = null;
		this.sortBuffer = null;
		this.sortBufferMax = sortBufferMax;

        if (estimatedRows > sortBufferMax)
			sortBufferMin = sortBufferMax;
		else
			sortBufferMin = (int)estimatedRows;
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("testSort"))
                sortBufferMin = sortBufferMax;
        }

		this.state = STATE_INITIALIZED;
	}

	/**
	An inserter is closing.
	**/
	void doneInserting(MergeInserter inserter,
		SortBuffer sortBuffer, Vector mergeRuns)
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(state == STATE_INSERTING);
    	}

		this.sortBuffer = sortBuffer;
		this.mergeRuns = mergeRuns;
		this.inserter = null;

		this.state = STATE_DONE_INSERTING;
	}

	void doneScanning(Scan scan, SortBuffer sortBuffer)
	{
		if (SanityManager.DEBUG)
		{
			// Make sure the scan we're getting back is the one we gave out

			if (this.scan != scan)
    			SanityManager.THROWASSERT("this.scan = " + this.scan 
										  + " scan = " + scan);
		}

		this.sortBuffer = sortBuffer;
		this.scan = null;

		this.state = STATE_DONE_SCANNING;
	}

	void doneScanning(Scan scan, SortBuffer sortBuffer,
		Vector mergeRuns)
	{
		this.mergeRuns = mergeRuns;

		doneScanning(scan, sortBuffer);
	}


	/**
	Get rid of the merge runs, if there are any.
	Must not cause any errors because it's called
	during error processing.
	**/
	void dropMergeRuns(TransactionManager tran)
	{
		if (mergeRuns != null)
		{
			Enumeration e = mergeRuns.elements();

			try 
			{
				Transaction rawTran = tran.getRawStoreXact();
				long segmentId = StreamContainerHandle.TEMPORARY_SEGMENT;

				while (e.hasMoreElements())
				{
					long containerId = ((Long) e.nextElement()).longValue();
					rawTran.dropStreamContainer(segmentId, containerId);
				}
			}
			catch (StandardException se)
			{
				// Ignore problems with dropping, worst case
				// the raw store will clean up at reboot.
			}
			mergeRuns = null;
		}
	}

	/* DEBUG (nat)
	void printRunInfo(TransactionController tran)
		throws StandardException
	{
		java.util.Enumeration e = mergeRuns.elements();
		while (e.hasMoreElements())
		{
			long conglomid = ((Long) e.nextElement()).longValue();
			ScanController sc = tran.openScan(conglomid, false,
									false, null, null, 0, null,
									null, 0);
			System.out.println("Merge run: conglomid=" + conglomid);
			while (sc.next())
			{
				sc.fetch(template);
				System.out.println(template);
			}
			sc.close();
		}
	}
	*/

	private void multiStageMerge(TransactionManager tran)
		throws StandardException
	{
		Enumeration e;
		//int iterations = 0; // DEBUG (nat)
		int maxMergeRuns = sortBuffer.capacity();

		if (maxMergeRuns > ExternalSortFactory.DEFAULT_MAX_MERGE_RUN)
			maxMergeRuns = ExternalSortFactory.DEFAULT_MAX_MERGE_RUN;

		Vector subset;
		Vector leftovers;

		while (mergeRuns.size() > maxMergeRuns)
		{
			// Move maxMergeRuns elements from the merge runs
			// vector into a subset, leaving the rest.
			subset = new Vector(maxMergeRuns);
			leftovers = new Vector(mergeRuns.size() - maxMergeRuns);
			e = mergeRuns.elements();
			while (e.hasMoreElements())
			{
				Long containerId = (Long) e.nextElement();
				if (subset.size() < maxMergeRuns)
					subset.addElement(containerId);
				else
					leftovers.addElement(containerId);
			}

			/* DEBUG (nat)
			iterations++;
				System.out.println(subset.size() + " elements in subset");
				System.out.println(leftovers.size() + " elements in leftovers");
				System.out.println(mergeRuns.size() + " elements in mergeRuns");
				System.out.println("maxMergeRuns is " + maxMergeRuns);
				System.out.println("iterations = " + iterations);
			if (subset.size() == 0)
			{
				System.exit(1);
			}
			*/

			mergeRuns = leftovers;

			// Open a merge scan on the subset.
			MergeScanRowSource msRowSource = 
				new MergeScanRowSource(this, tran, sortBuffer, subset, sortObserver, false);

			if (!msRowSource.init(tran))
            {
                throw StandardException.newException(
                        SQLState.SORT_COULD_NOT_INIT);
            }

			// Create and open another temporary stream conglomerate
			// which will become
			// a merge run made up with the merged runs from the subset.
			Transaction rawTran = tran.getRawStoreXact();
			int segmentId = StreamContainerHandle.TEMPORARY_SEGMENT;
			long id = rawTran.addAndLoadStreamContainer(segmentId,
				properties, msRowSource);

			mergeRuns.addElement(new Long(id));

			// Drop the conglomerates in the merge subset
			e = subset.elements();
			while (e.hasMoreElements())
			{
				Long containerId = (Long) e.nextElement();
				rawTran.dropStreamContainer(segmentId, containerId.longValue());
			}
		}
	}

	/**
	Remove all the rows from the sort buffer and store them
	in a temporary conglomerate.  The temporary conglomerate
	is a "merge run".  Returns the container id of the
	merge run.
	**/
	long createMergeRun(TransactionManager tran, SortBuffer sortBuffer)
		throws StandardException
	{
		// this sort buffer is not a scan and is not tracked by any
		// TransactionManager. 
		SortBufferRowSource rowSource =
			new SortBufferRowSource(sortBuffer, (TransactionManager)null, sortObserver, true, sortBufferMax); 

		// Create a temporary stream conglomerate...
		Transaction rawTran = tran.getRawStoreXact();  // get raw transaction
		int segmentId = StreamContainerHandle.TEMPORARY_SEGMENT;
		long id = rawTran.addAndLoadStreamContainer(segmentId,
			properties, rowSource);

		// Don't close the sortBuffer, we just emptied it, the caller may reuse
		// that sortBuffer for the next run.
		rowSource = null;

		return id;
	}
}
