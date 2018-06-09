/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeMaxScan

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

package org.apache.derby.impl.store.access.btree;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException; 

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;


/**

  A b-tree scan controller corresponds to an instance of an open b-tree scan.
  <P>
  <B>Concurrency Notes</B>
  <P>
  The concurrency rules are derived from OpenBTree.
  <P>
  @see OpenBTree

**/

/**

A BTreeScan implementation that provides the 95% solution to the max on
btree problem.  If the row is the last row in the btree it works very
efficiently.  This implementation will be removed once backward scan is
fully functional.

The current implementation only exports to the user the ability to call
fetchMax() and get back one row, none of the generic scan ablities are
exported.  

To return the maximum row this implementation does the following:
1) calls positionAtStartPosition() which returns with the a latch on the
   rightmost leaf page and a lock on the rightmost leaf row on that page.
   It will loop until it can get the lock without waiting while holding
   the latch.  At this point the slot position is just right of the
   locked row.
2) in fetchMax() it loops backward on the last leaf page, locking rows
   as it does so, until it finds the first non-deleted, non-NULL row.
3) If it is not successful in this last page search it attempts to latch
   the left sibling page, without waiting to avoid deadlocks with forward
   scans, and continue the search on that page.
4) If the sibling page couldn't be latched without waiting, save the
   current position, release all latches, and restart the scan from the
   saved position.

**/

public class BTreeMaxScan extends BTreeScan
{

    /**************************************************************************
     * Private methods of This class:
     **************************************************************************
     */

    /**
     * Move the current position to the page to the left of the current page,
     * right after the last slot on that page. If we have to wait for a latch,
     * give up the latch on the current leaf and give up. The caller will have
     * to reposition and retry.
     *
     * @return true if the position was successfully moved, false if we had
     * to wait for a latch
     */
    private boolean moveToLeftSibling() throws StandardException {
        try {
            positionAtPreviousPage();
            return true;
        } catch (WaitError we) {
            // We couldn't get the latch without waiting. Let's save the
            // position and let the caller retry. But first, let's save the
            // page number of the left sibling so that we can try again to
            // get the latch after we've saved the position.
            long left = scan_position.current_leaf.getleftSiblingPageNumber();

            // If the page is empty, there is no position to save. Since
            // positionAtPreviousPage() skips empty pages mid-scan, we know
            // that an empty page seen here must be the rightmost leaf. In
            // that case, the caller can simply restart the scan, so there's
            // no need to save the position.
            if (isEmpty(scan_position.current_leaf.getPage())) {
                scan_position.current_leaf.release();
                scan_position.init();
            } else {
                // Save the leftmost record on the page. That's the record in
                // slot 1, since slot 0 is the control row.
                scan_position.current_slot = 1;
                savePositionAndReleasePage();
            }

            // There's no point in attempting to reposition too early, as we're
            // likely to hit the same WaitError again (and again). So instead
            // let's sleep here until the left sibling page has been released,
            // and then return to let the caller reposition and retry.
            Page leftPage = container.getPage(left);
            if (leftPage != null) {
                // Got the latch. Anything may have happened while we didn't
                // hold any latches, so we don't know if we're still supposed
                // to go to that page. Just release the latch and let the
                // caller reposition to the right spot in the B-tree.
                leftPage.unlatch();
                leftPage = null;
            }

            return false;
        }
    }


    /**************************************************************************
     * Protected implementation of abstract methods of BTreeScan class:
     **************************************************************************
     */

    /**
     * disallow fetchRows on this scan type, caller should only be able
     * to call fetchMax().
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
     * Position scan at "start" position of the MAX scan.
     * <p>
     * Positions the scan to the slot just after the last record on the
     * rightmost leaf of the index.  Returns the rightmost leaf page latched,  
     * the rightmost row on the page locked and 
     * sets "current_slot" to the slot number just right of the last row
     * on the page.
     * <p>
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
            SanityManager.ASSERT(this.scan_state         == SCAN_INIT);
            SanityManager.ASSERT(pos.current_rh          == null);
            SanityManager.ASSERT(pos.current_positionKey == null);
        }

        // Loop until you can lock the last row, on the rightmost leaf page
        // of the tree, while holding the page latched, without waiting.
        //
        // If you have to wait, drop the latch, and wait for the lock.
        // This makes it likely that the next search you will loop just
        // once, find the same lock satisfies the search and since you already
        // have the lock it will be granted.
        while (true)
        {
            // Find the starting page and row slot, must start at root and
            // search for rightmost leaf.
            ControlRow root = ControlRow.get(this, BTree.ROOTPAGEID); 

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

            // lock the last row on the rightmost leaf of the table, as this
            // is a max scan no previous key locking necessary.  Previous key
            // locking is used to protect a range of keys, but for max there
            // is only a single row returned.

            pos.current_slot--;
            boolean latch_released = 
                !this.getLockingPolicy().lockScanRow(
                    this, pos,
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
                        "BTreeMaxScan_positionAtStartPosition", pos,
                        this.getLockingPolicy(), pos.current_leaf, latch_released);
            }

            if (latch_released)
            {
                // lost latch on pos.current_leaf, search the tree again.
                // Forget the current position since we'll reposition on the
                // rightmost key, which is not necessarily the saved position.
                pos.init();
                continue;
            }
            else
            {
                // success! got all the locks, while holding the latch.
                break;
            }
        }

        this.scan_state          = SCAN_INPROGRESS;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(pos.current_leaf != null);
	}

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Fetch the maximum row in the table.
     *
     * Call positionAtStartPosition() to quickly position on rightmost row
     * of rightmost leaf of tree.
     *
     * Search last page for last non deleted row, and if one is found return
     * it as max.
     *
     * If no row found on last page, or could not find row without losing latch
     * then call fetchMaxRowFromBeginning() to search from left to right
     * for maximum value in index.
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

            // RESOLVE (mikem) - I don't think this code can be called.
            
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
        // In this case the "next" record is the last row on the rightmost
        // leaf page.


        boolean max_found = false;

        // Code is positioned on the rightmost leaf of the index, the rightmost
        // non-deleted row on this page is the maximum row to return.

        leaf_loop:
        while (!max_found && pos.current_leaf != null)
        {
            if (pos.current_slot <= 1)
            {
                // Reached beginning of this leaf page without finding a
                // value, so move position to the end of the left sibling and
                // resume the scan.
                boolean latch_released = !moveToLeftSibling();

                if (latch_released)
                {
                    // The previous page was latched by someone else, so we
                    // gave up the latch on this page to avoid running into a
                    // deadlock with someone scanning the leaves in the
                    // opposite direction.

                    if (SanityManager.DEBUG)
                    {
                        SanityManager.DEBUG(
                                "BTreeMaxScan.latchConflict",
                                "Couldn't get latch nowait, will retry");
                    }

                    if (pos.current_positionKey == null)
                    {
                        // We haven't seen any rows yet, so no position has
                        // been saved. See comment in moveToLeftSibling().
                        // Restart the scan from the rightmost leaf.
                        if (SanityManager.DEBUG)
                        {
                            SanityManager.DEBUG(
                                    "BTreeMaxScan.latchConflict",
                                    "Restart scan from rightmost leaf");
                        }
                        scan_state = SCAN_INIT;
                        positionAtStartPosition(pos);
                    }
                    else if (!reposition(pos, false))
                    {
                        if (SanityManager.DEBUG)
                        {
                            SanityManager.DEBUG(
                                    "BTreeMaxScan.latchConflict",
                                    "Saved position is gone");
                        }

                        // The row on the saved position doesn't exist anymore,
                        // so it must have been purged. Move to the position
                        // immediately to the left of where the row should
                        // have been.
                        if (!reposition(pos, true))
                        {
                            if (SanityManager.DEBUG)
                            {
                                SanityManager.THROWASSERT(
                                        "Cannot fail with 2nd param true");
                            }
                        }

                        // reposition() will position to the left of the purged
                        // row, whereas we want to be on the right side of it
                        // since we're moving backwards.
                        pos.current_slot++;
                    }
                }

                // We now have one of the following scenarios:
                //
                // 1) Current position is right after the last row on the left
                //    sibling page if we could move to the sibling page without
                //    waiting for a latch.
                // 2) Current position is on the same row as the last one we
                //    looked at if we had to wait for a latch on the sibling
                //    page and the row hadn't been purged before we
                //    repositioned.
                // 3) Current position is right after the position where we
                //    should have found the last row we looked at if we had
                //    to wait for a latch on the sibling page and the row was
                //    purged before we repositioned.
                // 4) There is no current position if we already were at the
                //    leftmost leaf page.
                //
                // For scenarios 1-3, we're positioned immediately to the right
                // of the next row we want to look at, so going to the top of
                // the loop will make us move to the next interesting row. For
                // scenario 4, we want to break out of the loop, which also
                // is handled by going to the top of the loop and reevaluating
                // the loop condition.
                continue;
            }

            // move scan backward in search of last non-deleted row on page.
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

                // lock current row in max scan, no previous key lock necessary.
                boolean latch_released =
                    !this.getLockingPolicy().lockScanRow(
                        this, pos,
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
                    // Had to wait on lock while not holding the latch, so now
                    // the current row may have been moved to another page and
                    // we need to reposition.
                    if (!reposition(pos, false))
                    {
                        // Could not position on the exact same row that was
                        // saved, which means that it has been purged.
                        // Reposition on the row immediately to the left of
                        // where the purged row should have been.
                        if (!reposition(pos, true))
                        {
                            if (SanityManager.DEBUG)
                            {
                                SanityManager.THROWASSERT(
                                        "Cannot fail with 2nd param true");
                            }
                        }

                        // Now we're positioned immediately to the left of our
                        // previous position. We want to be positioned to the
                        // right of that position so that we can restart the
                        // scan from where we were before we came to the row
                        // that disappeared for us. Adjust position one step
                        // to the right and continue from the top of the loop.
                        pos.current_slot++;
                        continue leaf_loop;
                    }
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

        // Clean up the scan based on searching through rightmost leaf of btree
        positionAtDoneScan(scan_position);

        return(max_found);
	}
}
