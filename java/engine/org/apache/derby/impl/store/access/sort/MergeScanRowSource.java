/*

   Derby - Class org.apache.derby.impl.store.access.sort.MergeScanRowSource

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

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.ScanControllerRowSource;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import java.util.Vector;

/**
	Wrapping the output of a MergeScan in a RowSource for the benefit of the
	createAndLoadConglomerate and loadConglomerate interface.  The output of a

	MergeScan is written to a file when we need more than one level of merge
	runs. 

	MergeScan implements ScanController, this class just implements the
	RowSource interface. 
*/
public class MergeScanRowSource extends MergeScan implements ScanControllerRowSource
{

    /* Constructors for This class: */
	MergeScanRowSource(
    MergeSort           sort, 
    TransactionManager  tran,
    SortBuffer          sortBuffer, 
    Vector              mergeRuns,
	SortObserver		sortObserver,
    boolean             hold)
    {
		super(sort, tran, sortBuffer, mergeRuns, sortObserver, hold);
    }

	/*
	 * Disable illegal and dangerous scan controller interface call
	 * @exception StandardException This is an illegal operation
	 */
	public boolean next() throws StandardException
	{
		throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    /* Private/Protected methods of This class: */
    /* Public Methods of This class: */
    /* Public Methods of RowSource class: */


    public DataValueDescriptor[] getNextRowFromRowSource() 
        throws StandardException
    {
		DataValueDescriptor[] row = sortBuffer.removeFirst();

		if (row != null)
		{
			mergeARow(sortBuffer.getLastAux());
		}

		return row;
	}

	/**
	 * @see RowLocationRetRowSource#needsRowLocation
	 */
	public boolean needsRowLocation()
	{
		return false;
	}

	/**
	 * @see org.apache.derby.iapi.store.access.RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return false;
	}


	/**
	 * @see RowLocationRetRowSource#rowLocation
	 */
	public void rowLocation(RowLocation rl)
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("unexpected call to RowSource.rowLocation");
	}


	/**
		All columns are always set from a sorter
	*/
	public FormatableBitSet getValidColumns()
	{
		return null;
	}

	/**
		Close the row source - implemented by MergeScan already
	 */
	public void closeRowSource()
	{
		close();
	}

}

