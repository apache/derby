/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeMaxScan

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access.btree;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;


/**

  A b-tree scan controller corresponds to an instance of an open b-tree scan.
  <P>
  <B>Concurrency Notes<\B>
  <P>
  The concurrency rules are derived from OpenBTree.
  <P>
  @see OpenBTree

**/

/**

A BTreeScan implementation that provides the 90% solution to the max on
btree problem.  If the row is the last row in the btree it works very
efficiently.  This implementation will be removed once backward scan is
fully functional.

**/

public class BTreeMaxScan extends BTreeScan
{

    /**************************************************************************
     * Private methods of This class:
     **************************************************************************
     */

    /**
     * Fetch the maximum non-deleted row from the table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private boolean fetchMaxRowFromBeginning(
    BTreeRowPosition        pos,
    DataValueDescriptor[]   fetch_row)
        throws StandardException
	{
        int                 ret_row_count     = 0;
        RecordHandle        max_rh            = null;

        // we need to scan until we hit the end of the table or until we
        // run into a null.  Use this template to probe the "next" row so
        // that if we need to finish, fetch_row will have the right value.
        DataValueDescriptor[] check_row_template = new DataValueDescriptor[1];
        check_row_template[0] = fetch_row[0].getClone();
        FetchDescriptor check_row_desc = RowUtil.getFetchDescriptorConstant(1);

        // reopen the scan for reading from the beginning of the table.
        reopenScan(
            (DataValueDescriptor[]) null,
            ScanController.NA,
            (Qualifier[][]) null,
            (DataValueDescriptor[]) null,
            ScanController.NA);

        positionAtStartForForwardScan(pos);

        // At this point:
        // current_page is latched.  current_slot is the slot on current_page
        // just before the "next" record this routine should process.

        // loop through successive leaf pages and successive slots on those
        // leaf pages.  Stop when either the last leaf is reached. At any
        // time in the scan fetch_row will contain "last" non-deleted row
        // seen.

        boolean nulls_not_reached = true;
		while ((pos.current_leaf != null) && nulls_not_reached)
		{
			while ((pos.current_slot + 1) < pos.current_leaf.page.recordCount())
			{
                // unlock the previous row if doing read.
                if (pos.current_rh != null)
                {
                    this.getLockingPolicy().unlockScanRecordAfterRead(
                        pos, init_forUpdate);

                    // current_rh is used to track which row we need to unlock,
                    // at this point no row needs to be unlocked.
                    pos.current_rh = null;
                }

                // move scan current position forward.
                pos.current_slot++;
                this.stat_numrows_visited++;

                // get current record handle for positioning but don't read
                // data until we verify it is not deleted.  rh is needed
                // for repositioning if we lose the latch.
                RecordHandle rh = 
                    pos.current_leaf.page.fetchFromSlot(
                        (RecordHandle) null,
                        pos.current_slot, 
                        check_row_template,
                        null,
                        true);

                // lock the row.
                boolean latch_released =
                    !this.getLockingPolicy().lockScanRow(
                        this, this.getConglomerate(), pos,
                        false, 
                        init_lock_fetch_desc,
                        pos.current_lock_template,
                        pos.current_lock_row_loc,
                        false, init_forUpdate, lock_operation);

                // special test to see if latch release code works
                if (SanityManager.DEBUG)
                {
                    latch_released = 
                        test_errors(
                            this,
                            "BTreeMaxScan_fetchNextGroup", false, 
                            this.getLockingPolicy(),
                            pos.current_leaf, latch_released);
                }

                // At this point we have successfully locked this record, so
                // remember the record handle so that it can be unlocked if
                // necessary.  If the above lock deadlocks, we will not try
                // to unlock a lock we never got in close(), because current_rh
                // is null until after the lock is granted.
                pos.current_rh = rh;

                if (latch_released)
                {
                    // lost latch on page in order to wait for row lock.
                    // Because we have scan lock on page, we need only
                    // call reposition() which will use the saved record
                    // handle to reposition to the same spot on the page.
                    // We don't have to search the
                    // tree again, as we have the a scan lock on the page
                    // which means the current_rh is valid to reposition on.
                    if (!reposition(pos, false))
                    {
                        if (SanityManager.DEBUG)
                        {
                            // can't fail while with scan lock
                            SanityManager.THROWASSERT(
                                "can not fail holding scan lock.");
                        }
                    }
                }


                if (pos.current_leaf.page.isDeletedAtSlot(pos.current_slot))
                {
                    this.stat_numdeleted_rows_visited++;

                    if (check_row_template[0].isNull())
                    {
                        // nulls sort at high end and are not to be returned
                        // by max scan, so search is over, return whatever is
                        // in fetch_row.
                        nulls_not_reached = false;
                        break;
                    }
                }
                else if (check_row_template[0].isNull())
                {
                    nulls_not_reached = false;
                    break;
                }
                else 
                {

                    pos.current_leaf.page.fetchFromSlot(
                        pos.current_rh,
                        pos.current_slot, fetch_row, init_fetchDesc,
                        true);

                    stat_numrows_qualified++;
                    max_rh = pos.current_rh;
                }
			}

            // Move position of the scan to slot 0 of the next page.  If there
            // is no next page current_page will be null.
            positionAtNextPage(pos);

            this.stat_numpages_visited++;
		}


        // Reached last leaf of tree.
        positionAtDoneScan(pos);

        // we need to decrement when we stop scan at the end of the table.
        this.stat_numpages_visited--;

        return(max_rh != null);
	}

    /**************************************************************************
     * Protected implementation of abstract methods of BTreeScan class:
     **************************************************************************
     */

    /**
     * disallow fetchRows on this scan type.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    protected int fetchRows(
    BTreeRowPosition        pos,
    DataValueDescriptor[][] row_array,
    RowLocation[]           rowloc_array,
    BackingStoreHashtable   hash_table,
    long                    max_rowcnt,
    int[]                   key_column_numbers)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }


    /**
     * Position scan at "start" position of the scan.
     * <p>
     * Positions the scan to the slot just after the first record to be 
     * returned from the backward scan.  Returns the start page latched, and 
     * sets "current_slot" to the slot number just right of the first slot
     * to return.
     * <p>
     *
	 * @return The leaf page containing the start position, or null if no
     *         start position is found.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtStartPosition(
    BTreeRowPosition    pos)
        throws StandardException
	{
		boolean         exact;

        // This routine should only be called from first next() call //
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.scan_state          == SCAN_INIT);
            SanityManager.ASSERT(pos.current_rh          == null);
            SanityManager.ASSERT(pos.current_positionKey         == null);
            SanityManager.ASSERT(pos.current_scan_pageno == 0);
        }

        // Loop until you can lock the row previous to the first row to be
        // returned by the scan, while holding the page latched, without
        // waiting.  If you have to wait, drop the latch, wait for the lock -
        // which makes it likely if you wait for the lock you will loop just
        // once, find the same lock satisfies the search and since you already
        // have the lock it will be granted.
        while (true)
        {
            // Find the starting page and row slot, must start at root and
            // search either for leftmost leaf, or search for specific key.
            ControlRow root = ControlRow.Get(this, BTree.ROOTPAGEID); 

            // include search of tree in page visited stats.
            stat_numpages_visited += root.getLevel() + 1;

            if (init_startKeyValue == null)
            {
                // No start given, position at last slot + 1 of rightmost leaf 
                pos.current_leaf = (LeafControlRow) root.searchRight(this);

                pos.current_slot = pos.current_leaf.page.recordCount();
                exact     = false;
            }
            else
            {
                // only max needed, no start position supported.
                throw StandardException.newException(
                        SQLState.BTREE_UNIMPLEMENTED_FEATURE);
            }

            // backward scan initial positioning will request a previous
            // key lock for initial positioning.  The actual scan will have
            // to make 2 lock requests per row fetch, one for a previous key
            // and one for lock on row it is positioned on.  Optimizations
            // can be made depending on isolation level.
            // 
            // Note that this is not a "previous key" lock as the row we are
            // locking is the max row to return.  Get the scan lock at the
            // same time.

            pos.current_slot--;
            boolean latch_released = 
                !this.getLockingPolicy().lockScanRow(
                    this, this.getConglomerate(), pos,
                    true, 
                    init_lock_fetch_desc,
                    pos.current_lock_template,
                    pos.current_lock_row_loc,
                    false, init_forUpdate, lock_operation);
            pos.current_slot++;

            // special test to see if latch release code works
            if (SanityManager.DEBUG)
            {
                latch_released = 
                    test_errors(
                        this,
                        "BTreeMaxScan_positionAtStartPosition", true,
                        this.getLockingPolicy(), pos.current_leaf, latch_released);
            }

            if (latch_released)
            {
                // lost latch on pos.current_leaf, search the tree again.
                pos.current_leaf = null;
                continue;
            }
            else
            {
                // success! got all the locks, while holding the latch.
                break;
            }
        }

        this.scan_state          = SCAN_INPROGRESS;
        pos.current_scan_pageno = pos.current_leaf.page.getPageNumber();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(pos.current_leaf != null);
	}

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Fetch the maximum row in the table.
     * <p>
     * Utility routine used by both fetchSet() and fetchNextGroup().
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean fetchMax(
    DataValueDescriptor[]   fetch_row)
        throws StandardException
    {
        BTreeRowPosition    pos           = scan_position;
        int                 ret_row_count = 0;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null,
                "BTreeMaxScan.fetchMax() called on a closed scan.");
        }


        if (this.scan_state == BTreeScan.SCAN_INPROGRESS)
        {
            // Get current page of scan, with latch
            
            // reposition the scan at the row just before the next one to 
            // return.
            // This routine handles the mess of repositioning if the row or 
            // the page has disappeared. This can happen if a lock was not 
            // held on the row while not holding the latch (can happen if
            // this scan is read uncommitted).
            if (!reposition(scan_position, true))
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                        "can not fail with 2nd param true.");
                }
            }

        }
        else if (this.scan_state == SCAN_INIT)
        {
            // 1st positioning of scan (delayed from openScan).
            positionAtStartPosition(scan_position);
        }
        else
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(this.scan_state == SCAN_DONE);

            return(false);
        }


        // At this point:
        // current_page is latched.  current_slot is the slot on current_page
        // just "right" of the "next" record this routine should process.


        boolean max_found = false;

        // if we can find a non-deleted row on this page then it is easy.

        if ((pos.current_slot - 1) > 0)
        {
            // move scan current position forward.
            pos.current_slot--;

            while (pos.current_slot > 0)
            {
                this.stat_numrows_visited++;

                // get current record handle for positioning but don't read
                // data until we verify it is not deleted.  rh is needed
                // for repositioning if we lose the latch.
                RecordHandle rh = 
                    pos.current_leaf.page.fetchFromSlot(
                        (RecordHandle) null,
                        pos.current_slot, fetch_row, init_fetchDesc,
                        true);

                boolean latch_released =
                    !this.getLockingPolicy().lockScanRow(
                        this, this.getConglomerate(), pos, 
                        false, 
                        init_lock_fetch_desc,
                        pos.current_lock_template,
                        pos.current_lock_row_loc,
                        false, init_forUpdate, lock_operation);

                // At this point we have successfully locked this record, so
                // remember the record handle so that it can be unlocked if
                // necessary.  If the above lock deadlocks, we will not try
                // to unlock a lock we never got in close(), because current_rh
                // is null until after the lock is granted.
                pos.current_rh = rh;


                if (latch_released)
                {
                    // had to wait on lock while lost latch, now last page of
                    // index may have changed, give up on "easy/fast" max scan.
                    pos.current_leaf = null;
                    break;
                }

                if (pos.current_leaf.page.isDeletedAtSlot(pos.current_slot))
                {
                    this.stat_numdeleted_rows_visited++;
                    pos.current_rh_qualified = false;
                }
                else if (fetch_row[0].isNull())
                {
                    pos.current_rh_qualified = false;
                }
                else
                {
                    pos.current_rh_qualified = true;
                }

                if (pos.current_rh_qualified)
                {
                    // return the qualifying max row.

                    // Found qualifying row.  Are we done fetching rows for the
                    // group?
                    ret_row_count++;
                    stat_numrows_qualified++;

                    // current_slot is invalid after releasing latch
                    pos.current_slot = Page.INVALID_SLOT_NUMBER;

                    max_found = true;
                    break;
                }
                else
                {
                    pos.current_slot--;
                }
            }
		}

        if (pos.current_leaf != null)
        {
            // done with "last" page in table.
            pos.current_leaf.release();
            pos.current_leaf = null;
        }

        // Reached last leaf of tree.
        positionAtDoneScan(scan_position);

        if (!max_found)
        {
            // max row in table was not last row in table
            max_found = fetchMaxRowFromBeginning(scan_position, fetch_row);
        }

        return(max_found);
	}
}
