/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeForwardScan

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

import org.apache.derby.iapi.store.access.ScanController;

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

public class BTreeForwardScan extends BTreeScan
{
	/*
	** Private/Protected methods of This class, sorted alphabetically
	*/

	/**
	Position scan at "start" position.
	<p>
    Positions the scan to the slot just before the first record to be returned
    from the scan.  Returns the start page latched, and sets "current_slot" to
    the slot number.

	@exception  StandardException  Standard exception policy.
	**/
    protected void positionAtStartPosition(
    BTreeRowPosition    pos)
        throws StandardException
	{
        positionAtStartForForwardScan(pos);
	}

    /**
     * Fetch the next N rows from the table.
     * <p>
     * Utility routine used by both fetchSet() and fetchNextGroup().
     *
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
        if (SanityManager.DEBUG)
        {
            // RowLocations in the BTree itself are unstable and should
            // not be put in long-lived structures like persistent hash tables.
            SanityManager.ASSERT
                ( (hash_table == null) || !hash_table.includeRowLocations() );
        }

        int                     ret_row_count     = 0;
        DataValueDescriptor[]   fetch_row         = null;
        RecordHandle            rh;

        if (max_rowcnt == -1)
            max_rowcnt = Long.MAX_VALUE;


        if (this.scan_state == BTreeScan.SCAN_INPROGRESS)
        {
            // reposition the scan at the row just before the next one to 
            // return.
            // This routine handles the mess of repositioning if the row or 
            // the page has disappeared. This can happen if a lock was not 
            // held on the row while not holding the latch (can happen if
            // this scan is read uncommitted).
            //
            // code path tested by readUncommitted.sql:TEST 1
            //
            if (!reposition(pos, true))
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
            positionAtStartPosition(pos);
        }
        else if (this.scan_state == SCAN_HOLD_INPROGRESS)
        {
            reopen();

            this.scan_state = SCAN_INPROGRESS;

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(scan_position.current_positionKey != null);
            }

            // reposition the scan at the row just before the next one to 
            // return.
            // This routine handles the mess of repositioning if the row or 
            // the page has disappeared. This can happen if a lock was not 
            // held on the row while not holding the latch.
            //
            // code path tested by holdCursor.sql: TEST 9
            if (!reposition(pos, true))
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                        "can not fail with 2nd param true.");
                }
            }

        }
        else if (this.scan_state == SCAN_HOLD_INIT)
        {
            reopen();

            positionAtStartForForwardScan(scan_position);
        }
        else
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(this.scan_state == SCAN_DONE);

            return(0);
        }

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                init_template != null, "init_template is null");
		}

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null,
                "BTreeScan.next() called on a closed scan.");

            if (row_array != null)
                SanityManager.ASSERT(row_array[0] != null,
                    "first array slot in fetchNextGroup() must be non-null.");

            // Btree's don't support RowLocations yet.
            if (rowloc_array != null)
            {
                throw StandardException.newException(
                        SQLState.BTREE_UNIMPLEMENTED_FEATURE);
            }
        }

        // System.out.println("top of fetchRows, fetch_row = " + fetch_row);


        // At this point:
        // current_page is latched.  current_slot is the slot on current_page
        // just before the "next" record this routine should process.

        // loop through successive leaf pages and successive slots on those
        // leaf pages.  Stop when either the last leaf is reached (current_page
        // will be null), or when stopKeyValue is reached/passed.  Along the
        // way apply qualifiers to skip rows which don't qualify.

        leaf_loop:
		while (pos.current_leaf != null)
		{
            // System.out.println(
              //   "1 of fetchSet loop, ret_row_count = " + ret_row_count +
                // "fetch_row = " + fetch_row);

            slot_loop:
			while ((pos.current_slot + 1) < pos.current_leaf.page.recordCount())
			{

                // System.out.println(
                // "2 of fetchSet loop, ret_row_count = " + ret_row_count +
                // "fetch_row = " + fetch_row + 
                // "hash_table = " + hash_table);


                // unlock the previous row if doing read.
                if (pos.current_rh != null)
                {
                    this.getLockingPolicy().unlockScanRecordAfterRead(
                        pos, init_forUpdate);

                    // current_rh is used to track which row we need to unlock,
                    // at this point no row needs to be unlocked.
                    pos.current_rh = null;
                }

                // Allocate a new row to read the row into.
                if (fetch_row == null)
                {
                    if (hash_table == null)
                    {
                        // point at allocated row in array if one exists.
                        if (row_array[ret_row_count] == null)
                        {
                            row_array[ret_row_count] = 
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                                runtime_mem.get_row_for_export(getRawTran());
                        }

                        fetch_row = row_array[ret_row_count];
                    }
                    else
                    {
                        // get a brand new row.
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                        fetch_row = 
                            runtime_mem.get_row_for_export(getRawTran()); 
                    }
                }

                // move scan current position forward.
                pos.current_slot++;
                this.stat_numrows_visited++;

                rh =
                    pos.current_leaf.page.fetchFromSlot(
                        (RecordHandle) null,
                        pos.current_slot, fetch_row, 
                        init_fetchDesc,
                        true);


                pos.current_rh_qualified = true;

                // See if this is the stop row.
                if (init_stopKeyValue != null)
                {
                    // See if current row is the >= the stopKeyValue.
                    //
                    // ret >  0: key is greater than row on page.
                    // ret == 0: key is exactly the row on page if full key,
                    //           or partial match if partial key.
                    // ret <  0: key is less    than row on page.
                    //
                    int ret = ControlRow.compareIndexRowToKey(
                                fetch_row,
                                init_stopKeyValue,
                                fetch_row.length,
                                0, this.getConglomerate().ascDescInfo);

                    if ((ret == 0) && 
                        (init_stopSearchOperator == ScanController.GE))
                    {
                        // if (partial) matched and stop is GE, end the scan.
                        ret = 1;
                    }

                    if (ret > 0)
                    {
                        // This is the first non-qualifying row. We're done.

                        pos.current_leaf.release();
                        pos.current_leaf = null;
                        positionAtDoneScan(pos);

                        return(ret_row_count);
                    }
                }


                // Only lock rows that are < the stopKeyValue.  No need to
                // requalify against stop position after losing the latch
                // as the only change that could have happened is that the
                // row was marked deleted - the key value cannot change.
                boolean latch_released =
                    !this.getLockingPolicy().lockScanRow(
//IC see: https://issues.apache.org/jira/browse/DERBY-6041
                        this, pos,
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
                            "BTreeScan_fetchNextGroup", pos,
                            this.getLockingPolicy(),
                            pos.current_leaf, latch_released);
                }

                // At this point we have successfully locked this record, so
                // remember the record handle so that it can be unlocked if
                // necessary.  If the above lock deadlocks, we will not try
                // to unlock a lock we never got in close(), because current_rh
                // is null until after the lock is granted.
                pos.current_rh = rh;

                while (latch_released)
                {
                    // lost latch on page in order to wait for row lock.
                    // reposition() will take care of the complexity with
                    // finding the correct spot to position on if the row
                    // has been moved to another page.

                    if (!reposition(pos, false))
                    {
                        // Could not position on the exact same row that was
                        // saved, which means that it has been purged.
                        // Reposition on the row immediately to the left of
                        // the purged row instead.
                        if (!reposition(pos, true))
                        {
                            if (SanityManager.DEBUG)
                            {
                                SanityManager.THROWASSERT(
                                        "Cannot fail with 2nd param true");
                            }
                            // reposition will set pos.current_leaf to null if
                            // it returns false, so if this ever does fail in
                            // delivered code, expect a NullPointerException at
                            // the top of this loop when we call recordCount().
                        }

                        // Now we're positioned to the left of our saved
                        // position. Go to the top of the loop so that we move
                        // the scan to the next row and release the lock on
                        // the purged row.
                        continue slot_loop;
                    }

                    // At this point, the scan is positioned and the latch
                    // is held.
                    latch_released = false;

                    if (this.getConglomerate().isUnique())
                    {
                        // Handle row location changing since lock request was 
                        // initiated.
                        // In unique indexes, there is one case where an index 
                        // row can have it's data lock key change (this usually
                        // cannot happen because only inserts and deletes are 
                        // allowed - no updates).  This case is an insert of a 
                        // key, that exactly matches a committed deleted row, 
                        // in a unique index.  In that case the code updates 
                        // the RowLocation column and flips the deleted bit to
                        // mark the row valid.  The problem is that if this 
                        // happens while we are waiting on a lock on the old
                        // RowLocation then when we wake up we have the wrong 
                        // lock, and the row location we fetched earlier in
                        // this loop is invalid.

                            pos.current_leaf.page.fetchFromSlot(
                                (RecordHandle) null,
                                pos.current_slot, fetch_row, 
                                init_fetchDesc,
                                true);

                            latch_released =
                                !this.getLockingPolicy().lockScanRow(
                                    this, 
                                    pos, 
                                    init_lock_fetch_desc,
                                    pos.current_lock_template,
                                    pos.current_lock_row_loc,
                                    false, init_forUpdate, lock_operation);
                    }
                }


                if (pos.current_leaf.page.isDeletedAtSlot(pos.current_slot))
                {
                    this.stat_numdeleted_rows_visited++;
                    pos.current_rh_qualified = false;
                }
                else if (init_qualifier != null)
                {
                    // Apply qualifiers if there are any.
                    pos.current_rh_qualified = 
                        this.process_qualifier(fetch_row);
                }

                if (pos.current_rh_qualified)
                {
                    // qualifying row.  Save position, release latch and return.

                    // this.current_rh is save position of scan while latch is
                    // not held.  It currently points at the current_slot in
                    // search (while latch is held).
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT(
                            pos.current_leaf.page.getSlotNumber(pos.current_rh)
                                == pos.current_slot);
                    }

                    // Found qualifying row.  Are we done fetching rows for the
                    // group?
                    ret_row_count++;
                    stat_numrows_qualified++;

                    final boolean doneWithGroup = max_rowcnt <= ret_row_count;

                    if (doneWithGroup) {
                        if (SanityManager.DEBUG) {
                            SanityManager.ASSERT(pos == scan_position);
                        }
                        int[] vcols = init_fetchDesc.getValidColumnsArray();
                        savePositionAndReleasePage(fetch_row, vcols);
                    }

                    if (hash_table != null)
                    {
                        if (hash_table.putRow(false, fetch_row, null))
                            fetch_row = null;
                    }
                    else
                    {
                        fetch_row = null;
                    }

                    if (doneWithGroup)
                    {
                        return(ret_row_count);
                    }
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

		return(ret_row_count);
	}
}

