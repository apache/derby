/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.sort
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.types.DataValueDescriptor;


/**

  Abstract base class for merge sort scans.

**/

public abstract class SortScan extends Scan
{
	/**
		IBM Copyright &copy notice.
	*/
 
    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	The sort that this class is scanning.
	**/
	protected MergeSort sort = null;

	/**
	The transactionManager that opened this scan.
	**/
	protected TransactionManager tran = null;

	/**
	The row at the current position of the scan, from which
	fetch will return values.
	**/
	protected DataValueDescriptor[] current;

	/**
	The row at the current position of the scan, from which
	fetch will return values.
	**/
	protected boolean hold;

	/*
	 * Constructors
	 */
	SortScan(MergeSort sort, TransactionManager tran, boolean hold) 
	{
		super();
		this.sort = sort;
		this.tran = tran;
		this.hold = hold;
	}

	/*
	 * Abstract methods of Scan
	 */

    /**
    Fetch the row at the next position of the Scan.

    If there is a valid next position in the scan then
	the value in the template storable row is replaced
	with the value of the row at the current scan
	position.  The columns of the template row must
	be of the same type as the actual columns in the
	underlying conglomerate.

    The resulting contents of templateRow after a fetchNext() 
    which returns false is undefined.

    The result of calling fetchNext(row) is exactly logically
    equivalent to making a next() call followed by a fetch(row)
    call.  This interface allows implementations to optimize 
    the 2 calls if possible.

    RESOLVE (mikem - 2/24/98) - come back to this and see if 
    coding this differently saves in sort scans, as did the
    heap recoding.

    @param template The template row into which the value
	of the next position in the scan is to be stored.

    @return True if there is a next position in the scan,
	false if there isn't.

	@exception StandardException Standard exception policy.
    **/
    public final boolean fetchNext(DataValueDescriptor[] row)
		throws StandardException
	{
        boolean ret_val = next();

        if (ret_val)
            fetch(row);

        return(ret_val);
    }

    /**
    Fetch the row at the current position of the Scan.
	@see ScanController#fetch
    **/
    public final void fetch(DataValueDescriptor[] result)
		throws StandardException
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(sort != null);
    	}

		if (current == null)
        {
            throw StandardException.newException(
                    SQLState.SORT_SCAN_NOT_POSITIONED);
        }

		// Make sure the passed in template row is of the correct type.
		sort.checkColumnTypes(result);

		// RESOLVE
        // Note that fetch() basically throws away the object's passed in.
        // We should figure out how to allow callers in this situation to
        // not go through the work of allocating objects in the first place.

		// Sort has allocated objects for this row, and will not 
        // reference them any more.  So just pass the objects out
        // to the caller instead of copying them into the provided
        // objects.
        System.arraycopy(current, 0, result, 0, result.length);
	}

    /**
    Close the scan.	@see ScanController#close
    **/
    public void close()
	{
		sort = null;
		current = null;

        tran.closeMe(this);
	}

	/*
	 * Methods of SortScan.  Arranged alphabetically.
	 */
}
