/*

   Derby - Class org.apache.derby.impl.store.access.sort.MergeInserter

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

import java.util.Vector;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.SortInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

/**


**/

public final class MergeInserter implements SortController
{
	/**
	The sort this inserter is for.
	**/
	protected MergeSort sort = null;

	/**
	The transaction this inserter is in.
	**/
	protected TransactionManager tran;

	/**
	A vector of the conglomerate ids of the merge runs.
	**/
	Vector mergeRuns = null;

	/**
	An in-memory ordered set that is used to sort rows
	before they're sent to merge runs.
	**/
	SortBuffer sortBuffer = null;

	/**
	Information about memory usage to dynamically tune the
	in-memory sort buffer size.
	*/
	long beginFreeMemory;
	long beginTotalMemory;
	long estimatedMemoryUsed;
	boolean avoidMergeRun;		// try to avoid merge run if possible
    int runSize;
    int totalRunSize;

    protected String  stat_sortType;
    protected int     stat_numRowsInput;
    protected int     stat_numRowsOutput;
    protected int     stat_numMergeRuns;
    protected Vector  stat_mergeRunsSize;


	/*
	 * Methods of SortController
	 */

	/**
    Insert a row into the sort.
	@see SortController#insert
    **/
    public void insert(DataValueDescriptor[] row)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			// If the sort is null, probably the caller forgot
			// to call initialize.
			SanityManager.ASSERT(sort != null);
		}

		// Check that the inserted row is of the correct type
		sort.checkColumnTypes(row);

		// Insert the row into the sort buffer, which will
		// sort it into the right order with the rest of the
		// rows and remove any duplicates.
        int insertResult = sortBuffer.insert(row);
        stat_numRowsInput++;
        if (insertResult != SortBuffer.INSERT_DUPLICATE)
            stat_numRowsOutput++;
        if (insertResult == SortBuffer.INSERT_FULL)
		{
			if (avoidMergeRun)
			{
				Runtime jvm = Runtime.getRuntime();
				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON("SortTuning"))
                    {
                        jvm.gc();
                        jvm.gc();
                        jvm.gc();
                    }
                }

                long currentFreeMemory = jvm.freeMemory();
                long currentTotalMemory = jvm.totalMemory();

				// before we create an external sort, which is expensive, see if
                // we can use up more in-memory sort buffer
				// we see how much memory has been used between now and the
				// beginning of the sort.  Not all of this memory is used by
				// the sort and GC may have kicked in and release some memory.
				// But it is a rough guess.
        		estimatedMemoryUsed = (currentTotalMemory-currentFreeMemory) -
		   			(beginTotalMemory-beginFreeMemory);

 				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON("SortTuning"))
                    {
						SanityManager.DEBUG("SortTuning",
							"Growing sortBuffer dynamically, " +
							"current sortBuffer capacity= " + 
                                sortBuffer.capacity() +
							" estimatedMemoryUsed = " + estimatedMemoryUsed +
							" currentTotalMemory = " + currentTotalMemory +
							" currentFreeMemory = " + currentFreeMemory +
							" numcolumn = " + row.length +
							" real per row memory = " + 
                                (estimatedMemoryUsed / sortBuffer.capacity()));
                    }
                }

				// we want to double the sort buffer size if that will result
				// in the sort to use up no more than 1/2 of all the free
				// memory (including the sort memory)
				// or if GC is so effective we are now using less memory than before
				// or if we are using less than 1Meg of memory and the jvm is
				// using < 5 meg of memory (this indicates that the JVM can
				// afford to be more bloated ?)
				if (estimatedMemoryUsed < 0 ||
					((2*estimatedMemoryUsed) < (estimatedMemoryUsed+currentFreeMemory)/2) ||
					(2*estimatedMemoryUsed < ExternalSortFactory.DEFAULT_MEM_USE &&
					 currentTotalMemory < (5*1024*1024)))
				{
					// ok, double the sort buffer size
					sortBuffer.grow(100);

					if (sortBuffer.insert(row) != SortBuffer.INSERT_FULL)
						return;
				}

				avoidMergeRun = false; // once we did it, too late to do in
									   // memory sort
			}

			// The sort buffer became full.  Empty it into a
			// merge run, and add the merge run to the vector
			// of merge runs.
            stat_sortType = "external";
			long conglomid = sort.createMergeRun(tran, sortBuffer);
			if (mergeRuns == null)
				mergeRuns = new Vector();
			mergeRuns.addElement(new Long(conglomid));

            stat_numMergeRuns++;
            // calculate size of this merge run
            // buffer was too full for last row
            runSize = stat_numRowsInput - totalRunSize - 1;
            totalRunSize += runSize;
            stat_mergeRunsSize.addElement(new Integer(runSize));

			// Re-insert the row into the sort buffer.
			// This is guaranteed to work since the sort
			// buffer has just been emptied.
			sortBuffer.insert(row);
		}
	}

	/**
	Close this sort controller.	Closing the sort controller
	means the caller is done inserting rows.  This method
	must not throw any exceptions since it's called during
	error processing.

	@see SortController#close
	**/

	public void close()
	{
		// Tell the sort that we're closed, and hand off
		// the sort buffer and the vector of merge runs.
		if (sort != null)
			sort.doneInserting(this, sortBuffer, mergeRuns);

        // if this is an external sort, there will actually
        // be one last merge run with the contents of the
        // current sortBuffer. It will be created when the user
        // reads the result of the sort using openSortScan
        if (stat_sortType == "external")
        {
            stat_numMergeRuns++;
            stat_mergeRunsSize.addElement(new Integer(stat_numRowsInput - totalRunSize));
        }

        // close the SortController in the transaction.
        tran.closeMe(this);

		// Clean up.
		sort = null;
		tran = null;
		mergeRuns = null;
		sortBuffer = null;
	}

	/*
	 * Methods of MergeInserter.  Arranged alphabetically.
	 */

    /**
     * Return SortInfo object which contains information about the current
     * sort.
     * <p>
     *
     * @see SortInfo
     *
	 * @return The SortInfo object which contains info about current sort.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public SortInfo getSortInfo()
		throws StandardException
    {
        return(new MergeSortInfo(this));
    }


	/**
	Initialize this inserter.
	@return true if initialization was successful
	**/
	boolean initialize(MergeSort sort, TransactionManager tran)
	{
		Runtime jvm = Runtime.getRuntime();
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("SortTuning"))
            {
                jvm.gc();
                jvm.gc();
                jvm.gc();
            }
        }

		beginFreeMemory = jvm.freeMemory();
		beginTotalMemory = jvm.totalMemory();
		estimatedMemoryUsed = 0;
		avoidMergeRun = true;		// not an external sort
        stat_sortType = "internal";
        stat_numMergeRuns = 0;
        stat_numRowsInput = 0;
        stat_numRowsOutput = 0;
        stat_mergeRunsSize = new Vector();
        runSize = 0;
        totalRunSize = 0;


		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("testSort"))
            {
                avoidMergeRun = false;
            }
        }

		this.sort = sort;
		this.tran = tran;
		sortBuffer = new SortBuffer(sort);
		if (sortBuffer.init() == false)
			return false;
		return true;
	}

}
