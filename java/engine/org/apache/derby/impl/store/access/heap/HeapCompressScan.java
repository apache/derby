/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapScan

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

package org.apache.derby.impl.store.access.heap;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.SpaceInfo;

import org.apache.derby.impl.store.access.conglomerate.RowPosition;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * A heap scan object represents an instance of a scan on a heap conglomerate.
 */
class HeapCompressScan 
    extends HeapScan
{

    /**************************************************************************
     * Constants of HeapScan
     **************************************************************************
     */

    /**************************************************************************
     * Fields of HeapScan
     **************************************************************************
     */
    private long pagenum_to_start_moving_rows = -1;



    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

	/**
	 ** The only constructor for a HeapCompressScan returns a scan in the
	 ** closed state, the caller must call open.
	 **/
	
	public HeapCompressScan()
	{
	}

    /**************************************************************************
     * Protected override implementation of routines in
     *     GenericController class:
     **************************************************************************
     */

    public int fetchNextGroup(
    DataValueDescriptor[][] row_array,
    RowLocation[]           old_rowloc_array,
    RowLocation[]           new_rowloc_array)
        throws StandardException
	{
        return(fetchRowsForCompress(
                    row_array, old_rowloc_array, new_rowloc_array));
    }

    /**
     * Fetch the next N rows from the table.
     * <p>
     * Utility routine used by both fetchSet() and fetchNextGroup().
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private int fetchRowsForCompress(
    DataValueDescriptor[][] row_array,
    RowLocation[]           oldrowloc_array,
    RowLocation[]           newrowloc_array)
        throws StandardException
	{
        int                     ret_row_count           = 0;
        DataValueDescriptor[]   fetch_row               = null;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row_array != null);
            SanityManager.ASSERT(row_array[0] != null,
                    "first array slot in fetchNextGroup() must be non-null.");
        }

        if (getScanState() == SCAN_INPROGRESS)
        {
            positionAtResumeScan(scan_position);
        }
        else if (getScanState() == SCAN_INIT)
        {
            // For first implementation of defragment use a conservative
            // approach, only move rows from the last "number of free pages"
            // of the container.  Should always at least be able to empty
            // that number of pages.
            SpaceInfo info = 
                open_conglom.getContainer().getSpaceInfo();

            pagenum_to_start_moving_rows = info.getNumAllocatedPages();

            positionAtStartForForwardScan(scan_position);
        }
        else if (getScanState() == SCAN_HOLD_INPROGRESS)
        {
            reopenAfterEndTransaction();

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    scan_position.current_rh != null, this.toString()); 
            }

            // reposition the scan at the row just before the next one to 
            // return.
            // This routine handles the mess of repositioning if the row or 
            // the page has disappeared. This can happen if a lock was not 
            // held on the row while not holding the latch.
            open_conglom.latchPageAndRepositionScan(scan_position);

            setScanState(SCAN_INPROGRESS);
        }
        else if (getScanState() == SCAN_HOLD_INIT)
        {
            reopenAfterEndTransaction();

            positionAtStartForForwardScan(scan_position);

        }
        else
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(getScanState() == SCAN_DONE);

            return(0);
        }

        // At this point:
        // scan_position.current_page is latched.  
        // scan_position.current_slot is the slot on scan_position.current_page
        // just before the "next" record this routine should process.

        // loop through successive pages and successive slots on those
        // pages.  Stop when either the last page is reached 
        // (scan_position.current_page will be null).  
        // Along the way apply qualifiers to skip rows which don't qualify.

		while (scan_position.current_page != null)
		{
			while ((scan_position.current_slot + 1) < 
                    scan_position.current_page.recordCount())
			{
                // Allocate a new row to read the row into.
                if (fetch_row == null)
                {
                     // point at allocated row in array if one exists.
                    if (row_array[ret_row_count] == null)
                    {
                        row_array[ret_row_count] = 
                          open_conglom.getRuntimeMem().get_row_for_export(
                              open_conglom.getRawTran());
                    }

                    fetch_row = row_array[ret_row_count];
                }

                // move scan current position forward.
                scan_position.positionAtNextSlot();

                this.stat_numrows_visited++;

                if (scan_position.current_page.isDeletedAtSlot(
                        scan_position.current_slot))
                {
                    // At this point assume table level lock, and that this
                    // transcation did not delete the row, so any
                    // deleted row must be a committed deleted row which can
                    // be purged.
                    scan_position.current_page.purgeAtSlot(
                        scan_position.current_slot, 1, false);

                    // raw store shuffles following rows down, so 
                    // postion the scan at previous slot, so next trip
                    // through loop will pick up correct row.
                    scan_position.positionAtPrevSlot();
                    continue;
                }

                if (scan_position.current_page.getPageNumber() > 
                        pagenum_to_start_moving_rows)
                {
                    // Give raw store a chance to move the row for compression
                    RecordHandle[] old_handle = new RecordHandle[1];
                    RecordHandle[] new_handle = new RecordHandle[1];
                    long[]         new_pageno = new long[1];

                    if (scan_position.current_page.moveRecordForCompressAtSlot(
                            scan_position.current_slot,
                            fetch_row,
                            old_handle,
                            new_handle) == 1)
                    {
                        // raw store moved the row, so bump the row count but 
                        // postion the scan at previous slot, so next trip
                        // through loop will pick up correct row.
                        // The subsequent rows will have been moved forward
                        // to take place of moved row.
                        scan_position.positionAtPrevSlot();

                        ret_row_count++;
                        stat_numrows_qualified++;


                        setRowLocationArray(
                            oldrowloc_array, ret_row_count - 1, old_handle[0]);
                        setRowLocationArray(
                            newrowloc_array, ret_row_count - 1, new_handle[0]);

                        fetch_row = null;

                    }
                }
			}

            this.stat_numpages_visited++;

            if (scan_position.current_page.recordCount() == 0)
            {
                // need to set the scan position before removing page
                scan_position.current_pageno = 
                    scan_position.current_page.getPageNumber();

                open_conglom.getContainer().removePage(
                    scan_position.current_page);

                // removePage unlatches the page, and page not available
                // again until after commit.
                scan_position.current_page = null;
            }
            else
            {
                positionAfterThisPage(scan_position);
                scan_position.unlatch();
            }


            if (ret_row_count > 0)
            {
                // rows were moved on this page, give caller a chance to
                // process those and free up access to the table.
                return(ret_row_count);
            }
            else
            {
                // no rows were moved so go ahead and commit the transaction
                // to allow other threads a chance at table.  Compress does
                // need to sync as long as transaction either completely 
                // commits or backs out, either is fine.
                /*
                open_conglom.getXactMgr().commitNoSync(
                    TransactionController.RELEASE_LOCKS);
                open_conglom.reopen();
                */
                positionAtResumeScan(scan_position);

            }
		}

        // Reached last page of scan.
        positionAtDoneScan(scan_position);

        // we need to decrement when we stop scan at the end of the table.
        this.stat_numpages_visited--;

		return(ret_row_count);
    }

    /**
     * Reposition the scan upon entering the fetchRows loop.
     * <p>
     * Called upon entering fetchRows() while in the SCAN_INPROGRESS state.
     * Do work necessary to look at rows in the current page of the scan.
     * <p>
     * The default implementation uses a record handle to maintain a scan
     * position.  It will get the latch again on the current
     * scan position and set the slot to the current record handle.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtResumeScan(
    RowPosition pos)
		throws StandardException
    {
        // reposition the scan at the row just before the next one to return.
        // This routine handles the mess of repositioning if the row or the
        // page has disappeared. This can happen if a lock was not held on the
        // row while not holding the latch.
        open_conglom.latchPageAndRepositionScan(scan_position);
    }

    /**
     * Move the scan from SCAN_INIT to SCAN_INPROGRESS.
     * <p>
     * This routine is called to move the scan from SCAN_INIT to 
     * SCAN_INPROGRESS.  Upon return from this routine it is expected
     * that scan_position is set such that calling the generic 
     * scan loop will reach the first row of the scan.  Note that this
     * usually means setting the scan_postion to one before the 1st 
     * row to be returned.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtStartForForwardScan(
    RowPosition pos)
        throws StandardException
    {
        if (pos.current_rh == null)
        {
            // 1st positioning of scan (delayed from openScan).  Do not
            // compress the first page, there is no previous page to move
            // rows to, and moving the special Heap metadata row from the
            // first page would cause problems.  Setting to next page is
            // why this scan overrides generic implementation.
            pos.current_page = 
                open_conglom.getContainer().getNextPage(
                    ContainerHandle.FIRST_PAGE_NUMBER);

            // set up for scan to continue at beginning of page following
            // the first page of the container.
            pos.current_slot = Page.FIRST_SLOT_NUMBER - 1;
        }
        else
        {
            // 1st positioning of scan following a reopenScanByRowLocation

            // reposition the scan at the row just before the next one to 
            // return.  This routine handles the mess of repositioning if the 
            // row or the page has disappeared. This can happen if a lock was 
            // not held on the row while not holding the latch.
            open_conglom.latchPageAndRepositionScan(pos);

            // set up for scan to at the specified record handle (position one
            // before it so that the loop increment and find it).
            pos.current_slot -= 1;
        }

        pos.current_rh              = null;
        this.stat_numpages_visited  = 1;
        this.setScanState(SCAN_INPROGRESS);
    }


    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Set scan position to just after current page.
     * <p>
     * Used to set the position of the scan if a record handle is not
     * avaliable.  In this case current_rh will be set to null, and 
     * current_pageno will be set to the current page number.
     * On resume of the scan, the scan will be set to just before the first
     * row returned form a getNextPage(current_pageno) call.
     * <p>
     * A positionAtResumeScan(scan_position) is necessary to continue the
     * scan after this call.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private void positionAfterThisPage(
    RowPosition pos)
        throws StandardException
    {
        pos.current_rh = null;
        pos.current_pageno = pos.current_page.getPageNumber();
    }

	/*
	** Methods of ScanManager
	*/

}
