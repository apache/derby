/*

   Derby - Class org.apache.derby.impl.store.access.sort.MergeScan

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

import java.util.Enumeration;
import java.util.Vector;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
	A sort scan that is capable of merging as many merge runs
	as will fit in the passed-in sort buffer.
**/

public class MergeScan extends SortScan
{
	/**
	The sort buffer we will use.
	**/
	protected SortBuffer sortBuffer;

	/**
	The merge runs.
	**/
	protected Vector mergeRuns;

	/**
	Array of scan controllers for the merge runs.
	Entries in the array become null as the last
	row is pulled out and the scan is closed.
	**/
	protected StreamContainerHandle openScans[];

	private SortObserver sortObserver;

	/*
	 * Constructors.
	 */

	MergeScan(
    MergeSort           sort, 
    TransactionManager  tran,
    SortBuffer          sortBuffer, 
    Vector              mergeRuns,
	SortObserver		sortObserver,
    boolean             hold)
	{
		super(sort, tran, hold);
		this.sortBuffer = sortBuffer;
		this.mergeRuns  = mergeRuns;
        this.tran       = tran;
		this.sortObserver = sortObserver;
	}

	/*
	 * Methods of MergeSortScan
	 */

    /**
    Move to the next position in the scan.
	@see ScanController#next
    **/
    public boolean next()
		throws StandardException
	{
		current = sortBuffer.removeFirst();
		if (current != null)
			mergeARow(sortBuffer.getLastAux());
		return (current != null);
	}

    /**
    Close the scan.
	@see ScanController#close
    **/
    public void close()
	{
		if (openScans != null)
		{
			for (int i = 0; i < openScans.length; i++)
			{
				if (openScans[i] != null)
                {
					openScans[i].close();
                }
				openScans[i] = null;
			}
			openScans = null;
		}

		// Hand sort buffer and remaining merge runs to sort.
		if (super.sort != null)
		{
			sort.doneScanning(this, sortBuffer, mergeRuns);
			sortBuffer = null;
			mergeRuns = null;
		}

		// Sets sort to null
		super.close();
	}

    /**
    Close the scan.
	@see ScanManager#closeForEndTransaction
    **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
	{
        if (!hold || closeHeldScan)
        {
            close();
            return(true);
        }
        else
        {
            return(false);
        }
    }

	/*
	 * Methods of MergeScan
	 */

	/**
	Initialize the scan, returning false if there
	was some error.
	**/
	public boolean init(TransactionManager tran)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			// We really expect to have at least one
			// merge run.
			SanityManager.ASSERT(mergeRuns != null);
			SanityManager.ASSERT(mergeRuns.size() > 0);

			// This sort scan also expects that the
			// caller has ensured that the sort buffer
			// capacity will hold a row from all the
			// merge runs.
			SanityManager.ASSERT(sortBuffer.capacity() >= mergeRuns.size());
		}

		// Clear the sort buffer.
		sortBuffer.reset();

		// Create an array to hold a scan controller
		// for each merge run.
		openScans = new StreamContainerHandle[mergeRuns.size()];
		if (openScans == null)
			return false;

		// Open a scan on each merge run.
		int scanindex = 0;
		Enumeration e = mergeRuns.elements();
		while (e.hasMoreElements())
		{
			// get the container id
			long id = ((Long) e.nextElement()).longValue();

			Transaction rawTran = tran.getRawStoreXact();  // get raw transaction
			int segmentId = StreamContainerHandle.TEMPORARY_SEGMENT;
			openScans[scanindex++] = 
                rawTran.openStreamContainer(segmentId, id, hold);
		}

		// Load the initial rows.
		for (scanindex = 0; scanindex < openScans.length; scanindex++)
			mergeARow(scanindex);

		// Success!
		return true;
	}

	/**
	Insert rows while we keep getting duplicates 
	from the merge run whose scan is in the
	open scan array entry indexed by scanindex.
	**/
	void mergeARow(int scanindex)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			// Unless there's a bug, the scan index will refer
			// to an open scan.  That's because we never put
			// a scan index for a closed scan into the sort
			// buffer (via setNextAux).
			SanityManager.ASSERT(openScans[scanindex] != null);
		}

		DataValueDescriptor[] row;

		// Read rows from the merge run and stuff them into the
		// sort buffer for as long as we encounter duplicates.
		do
		{
			row = sortObserver.getArrayClone();

			// Fetch the row from the merge run.
			if (!openScans[scanindex].fetchNext(row))
			{
                // If we're out of rows in the merge run, close the scan.
                
				openScans[scanindex].close();
				openScans[scanindex] = null;
				return;
			}

			// Save the index of this merge run with
			// the row we're putting in the sort buffer.
			sortBuffer.setNextAux(scanindex);
		}
		while (sortBuffer.insert(row) == SortBuffer.INSERT_DUPLICATE);
	}
}
