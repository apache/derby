/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.sort
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.conglomerate.ScanControllerRowSource;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
	Wrapping the output of a SortBuffer in a RowSource for the benefit of the
	createAndLoadConglomerate and loadConglomerate interface.

	Scan implements ScanController, this class just implements the
	RowSource interface. 

*/
public class SortBufferRowSource extends Scan
		implements ScanControllerRowSource
{
	/**
	The Sort buffer where rows come from
	**/
	SortBuffer sortBuffer = null;

	/**
	The TransactionManager that opened this scan.
	**/
	protected TransactionManager tran = null;

	private int			 maxFreeListSize;
	private boolean		 writingToDisk;
	private SortObserver sortObserver;

	/*
	 * Constructors.
	 */

	SortBufferRowSource(
    SortBuffer          sortBuffer, 
    TransactionManager  tran, 
    SortObserver        sortObserver,
    boolean             writingToDisk, 
    int                 maxFreeListSize)
	{
		super();
		this.sortBuffer = sortBuffer;
		this.tran = tran;
		this.sortObserver = sortObserver;
		this.writingToDisk = writingToDisk;
		this.maxFreeListSize = maxFreeListSize;
	}

	/* Private/Protected methods of This class: */
    /* Public Methods of This class: */
    /* Public Methods of RowSource class: */

    public DataValueDescriptor[] getNextRowFromRowSource()
    {
		if (sortBuffer == null)	// has been closed
			return null;

		DataValueDescriptor[] retval = sortBuffer.removeFirst();

		// Return the removed object to the free DataValueDescriptor[]
		if (retval != null && writingToDisk)
		{
			sortObserver.addToFreeList(retval, maxFreeListSize);
		}
		return retval;
	  }

	public boolean needsRowLocation()
	{
		return false;
	}

	/**
	 * @see RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return false;
	}

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
		Close the scan
	 */
	public void close()
	{
		if (sortBuffer != null)
		{
			sortBuffer.close();
			sortBuffer = null;
		}
		tran.closeMe(this);
	}

	/**
		Close the scan
	 */
	public boolean closeForEndTransaction(boolean closeHeldScan)
	{
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(
                closeHeldScan, 
                "Sort scan should not be held open across commit.");

        close();
        return(true);
    }

	/**
		Close the rowSource
	 */
	public void closeRowSource()
	{
		close();
	}

	/*
	 * Disable illegal and dangerous scan controller interface call
	 */
	public boolean next() throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    public void fetch(DataValueDescriptor[] result) throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    public final boolean fetchNext(DataValueDescriptor[] row) 
        throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

}
