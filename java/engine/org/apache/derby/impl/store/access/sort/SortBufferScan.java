/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.sort
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

/**

  A sort scan that just reads rows out of a sorter.

**/

public class SortBufferScan extends SortScan
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	The sorter we're returning rows from.
	**/
	protected SortBuffer sortBuffer;

	/*
	 * Constructors.
	 */

	SortBufferScan(
    MergeSort           sort, 
    TransactionManager  tran, 
    SortBuffer          sortBuffer,
    boolean             hold)
	{
		super(sort, tran, hold);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sortBuffer != null);

		this.sortBuffer = sortBuffer;
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
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                sortBuffer != null, 
                "next() called on scan after scan was closed.");
        }

		super.current = sortBuffer.removeFirst();
		return (super.current != null);
	}

    /**
    Close the scan.
    **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
    {
        if (closeHeldScan || !hold)
        {
            close();
            return(true);
        }
        else
        {
            return(false);
        }

    }

    /**
    Close the scan.
	@see ScanController#close
    **/
    public void close()
	{
		if (super.sort != null)
		{
			sort.doneScanning(this, sortBuffer);
			sortBuffer = null;
		}
		super.close();
	}

}
