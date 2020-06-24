/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IRowLocking3

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

package org.apache.derby.impl.store.access.btree.index;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException; 
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.ControlRow;
import org.apache.derby.impl.store.access.btree.LeafControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;
import org.apache.derby.impl.store.access.btree.BTreeRowPosition;
import org.apache.derby.impl.store.access.btree.WaitError;
import org.apache.derby.impl.store.access.heap.HeapController;

/**

Implements the jdbc serializable isolation level using row locks.
<p>
Holds read and write locks until end of transaction.
Obtains previous key locks to protect from phantom reads.

**/
class B2IRowLocking3 implements BTreeLockingPolicy
{

    /**************************************************************************
     * Private/Protected fields of This class:
     **************************************************************************
     */

    /**
     * The container id of the base container for this index.  Used to build
     * record handles to make lock calls on.
     **/
    protected ConglomerateController        base_cc;

    /**
     * The OpenBtree to use if we have to lock anything in the btree vs.
     * base row locking.
     **/
    protected OpenBTree                     open_btree;

    /**
     * The transaction to associate lock requests with.
     **/
    private Transaction                     rawtran;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    B2IRowLocking3(
    Transaction             rawtran,
    int                     lock_level,
    LockingPolicy           locking_policy,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
    {
        this.rawtran             = rawtran;
        this.base_cc             = base_cc;
        this.open_btree          = open_btree;
    }

    /**************************************************************************
     * Private methods of This class:
     **************************************************************************
     */

    /**
     * Lock key previous to first key in btree.
     * <p>
     * In the previous key locking protocol repeatable read and phantom 
     * protection is guaranteed by locking a range of keys in the btree.
     * The range is defined by the key previous to the first key you look
     * at and all subsequent keys you look at.  The first key in the index
     * is a special case, as there are no keys previous to it.  In that
     * case a special key is declared the "previous key" to the first key
     * in the btree and is locked instead.
     * <p>
     * In this implementation that first key is defined to be in the base
     * container, page ContainerHandle.FIRST_PAGE_NUMBER, record id 
     * PREVIOUS_KEY_HANDLE.
     * <p>
     * Note that the previous key is the same for all indexes on a given
     * conglomerate.  It seemed better for all locks on a base table to have
     * the same containerid, rather than having some locks generated from 
     * a btree have a containerid from base table and some having a containerid
     * from the btree.  If this turns out to be a problem we could either
     * have 2 different containerid's, be more creative with the record id, or
     * even add more to the lock key.
     *
     * @param aux_leaf          If non-null, this leaf is unlatched if the 
     *                          routine has to wait on the lock.
     * @param lock_operation    Whether to lock exclusive or share.
     * @param lock_duration     For what duration should the lock be held,
     *                          if INSTANT_DURATION, then the routine will
     *                          guarantee that lock was acquired while holding
     *                          the latch, but then immediately release the
     *                          lock.  If COMMIT_DURATION or MANUAL_DURATION
     *                          then the lock be held when routine returns
     *                          successfully.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private boolean lockPreviousToFirstKey(
    LeafControlRow          current_leaf,
    LeafControlRow          aux_leaf,
    int                     lock_operation,
    int                     lock_duration)
		throws StandardException
    {
        // This is first row in table, lock the special key that 
        // represents the key previous to the first key of the table.

        // First try to get the lock NOWAIT, while latch is held.
        boolean ret_status = 
            base_cc.lockRow(
                BTree.ROOTPAGEID, 
                RecordHandle.PREVIOUS_KEY_HANDLE, 
                lock_operation,
                false /* NOWAIT */, 
                lock_duration);

        if (!ret_status)
        {
            current_leaf.release();
            current_leaf = null;

            if (aux_leaf != null)
            {
                aux_leaf.release();
                aux_leaf = null;
            }

            // Couldn't get the lock NOWAIT, release latch and wait for lock.
            base_cc.lockRow(
                BTree.ROOTPAGEID, 
                RecordHandle.PREVIOUS_KEY_HANDLE, 
                lock_operation,
                true /* WAIT */, 
                lock_duration);
        }

        return(ret_status);
    }


    /**
     * Lock a btree row (row is at given slot in page).
     * <p>
     * Lock the row at the given slot in the page.  Meant to be used if caller 
     * only has the slot on the page to be locked, and has not read the row
     * yet.  This routine fetches the row location field from the page, and then
     * locks that rowlocation in the base container.
     * <p>
     * Lock a btree row, enforcing the standard lock/latch protocol.  
     * On return the row is locked.  Return status indicates if the lock
     * was waited for, which will mean a latch was dropped while waiting.
     * In general a false status means that the caller will either have 
     * to research the tree unless some protocol has been implemented that
     * insures that the row will not have moved while the latch was dropped.
     * <p>
     * This routine request a row lock NOWAIT on the in-memory row 
     * "current_row.".  If the lock is granted the routine will return true.
     * If the lock cannot be granted NOWAIT, then the routine will release
     * the latch on "current_leaf" and "aux_leaf" (if aux_leaf is non-null),
     * and then it will request a WAIT lock on the row.  
     * <p>
     *
     * @param current_leaf      Latched current leaf where "current" key is.
     * @param aux_leaf          If non-null, this leaf is unlatched if the 
     *                          routine has to wait on the lock.
     * @param current_slot      Slot of row to lock.
     * @param lock_fetch_desc   Descriptor for fetching just the RowLocation,
     *                          used for locking.
     * @param position          The position to lock if the lock is requested
     *                          while performing a scan, null otherwise.
     * @param lock_operation    Whether lock is for key prev to insert or not.
     * @param lock_duration     For what duration should the lock be held,
     *                          if INSTANT_DURATION, then the routine will
     *                          guarantee that lock was acquired while holding
     *                          the latch, but then immediately release the
     *                          lock.  If COMMIT_DURATION or MANUAL_DURATION
     *                          then the lock be held when routine returns
     *                          successfully.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private boolean lockRowOnPage(
    LeafControlRow          current_leaf,
    LeafControlRow          aux_leaf,
    int                     current_slot,
    BTreeRowPosition        position,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    int                     lock_operation,
    int                     lock_duration)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(current_leaf != null);

            if (current_slot <= 0 || 
                current_slot >= current_leaf.getPage().recordCount())
            {
                SanityManager.THROWASSERT(
                    "current_slot = " + current_slot +
                    "; current_leaf.getPage().recordCount() = " +
                        current_leaf.getPage().recordCount());
            }

            SanityManager.ASSERT(lock_template != null, "template is null");

            // For now the RowLocation is expected to be the object located in
            // the last column of the lock_template, this may change if we
            // ever support rows with RowLocations somewhere else.
            SanityManager.ASSERT(
                lock_row_loc == lock_template[lock_template.length - 1], 
                "row_loc is not the object in last column of lock_template.");

            if (position != null) {
                SanityManager.ASSERT(current_leaf == position.current_leaf);
                SanityManager.ASSERT(current_slot == position.current_slot);
            }
        }

        // Fetch the row location to lock.
        RecordHandle rec_handle = 
            current_leaf.getPage().fetchFromSlot(
                (RecordHandle) null, current_slot, 
                lock_template, lock_fetch_desc, true);

        // First try to get the lock NOWAIT, while latch is held.
        boolean ret_status =
            base_cc.lockRow(
                lock_row_loc, 
                lock_operation,
                false /* NOWAIT */, lock_duration);

        if (!ret_status)
        {
            // Could not get the lock NOWAIT, release latch and wait for lock.

            if (position != null)
            {
                // since we're releasing the lock in the middle of a scan,
                // save the current position of the scan before releasing the
                // latch
                position.saveMeAndReleasePage();
            }
            else if (current_leaf != null)
            {
                // otherwise, just release the latch
                current_leaf.release();
                current_leaf = null;
            }
            if (aux_leaf != null)
            {
                aux_leaf.release();
                aux_leaf = null;
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-6419
            if ((((HeapController)base_cc).getOpenConglomerate().getOpenMode() &
                    TransactionManager.OPENMODE_LOCK_ROW_NOWAIT) != 0) {
                throw StandardException.newException(SQLState.LOCK_TIMEOUT);
            }

            base_cc.lockRow(
                lock_row_loc, 
                lock_operation,
                true /* WAIT */, lock_duration);
        }

        return(ret_status);
    }

    /**
     * move left in btree and lock previous key.
     * <p>
     * Enter routine with "current_leaf" latched.  This routine implements
     * the left travel ladder locking protocol to search the leaf pages from
     * right to left for the previous key to 1st key on current_leaf.
     *
     * There are 2 cases:
     * 1) the previous page has keys, in which case the last key on that
     *    page is locked, other wise search continues on the next page to
     *    the left.
     * 2) there are no keys on the current page and there is no page to the
     *    left.  In this case the special "leftmost key" lock is gotten by
     *    calling lockPreviousToFirstKey().
     *
     * Left laddar locking is used if all latches can be obtained immediately
     * with NOWAIT.  This means that current latch is held while asking for
     * left latch NOWAIT, and if left latch is granted then subsequently 
     * current latch can be released.  If this protocol is followed and 
     * all latches are granted then caller is guaranteed that the correct
     * previous key has been locked and current_page latch remains.  The
     * NOWAIT protocol is used to avoid latch/latch deadlocks.  The overall
     * protocol is that one never holds a latch while waiting on another unless
     * the direction of travel is down and to the right.
     * <p>
     * If along the search a latch has to be waited on then latches are
     * released and a wait is performed, and "false" status is returned to
     * caller.  In this case the routine can no longer be sure of it's current
     * position and may have to retry the whole operation.
     *
     * @return true if previous key found without ever waiting on a latch, 
     *         false if latch released in order to wait for other latch.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private boolean searchLeftAndLockPreviousKey(
    LeafControlRow          current_leaf,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    OpenBTree               open_btree,
    int                     lock_operation,
    int                     lock_duration)
		throws StandardException
    {
        boolean         latches_released = false;
        LeafControlRow  prev_leaf;
        LeafControlRow  prev_prev_leaf;

        try 
        {
            // Move left in tree, page latch will be requested nowait, 
            // and WaitError will be thrown if latch not granted.

            prev_leaf = 
                (LeafControlRow) current_leaf.getLeftSibling(open_btree);
        }
        catch (WaitError e)
        {
            // initial latch request on leaf left of current could not be
            // granted NOWAIT.

            long previous_pageno = current_leaf.getleftSiblingPageNumber();

            current_leaf.release();
            current_leaf = null;

            // wait on the left leaf, which we could not be granted NOWAIT.
            prev_leaf = (LeafControlRow) 
                ControlRow.get(open_btree, previous_pageno);

            latches_released = true;
        }
       
        while (true)
        {
            try
            {
                // loop searching left in the btree until you either find 
                // a record to lock, or you reach the leftmost empty leaf.

                if (prev_leaf.getPage().recordCount() > 1)
                {

                    // lock the last row on the page, which is the previous 
                    // record to the first row on the next page.
                    
                    boolean ret_status = 
                        lockRowOnPage(
                            prev_leaf, 
                            current_leaf, 
                            prev_leaf.getPage().recordCount() - 1, 
                            null,
                            lock_fetch_desc,
                            lock_template,
                            lock_row_loc,
                            lock_operation, 
                            lock_duration);

                    if (!ret_status)
                    {
                        // needed to wait on a row lock, so both prev_leaf and
                        // current_leaf latches have been released by 
                        // lockRowOnPage()
                        prev_leaf        = null;
                        current_leaf     = null;
                        latches_released = true;
                    }

                    break;
                }
                else if (prev_leaf.isLeftmostLeaf())
                {
                    // Table's first row, lock the key that represents the 
                    // key previous to first key of the table.
                    boolean ret_status = 
                        lockPreviousToFirstKey(
                            prev_leaf, current_leaf, 
                            lock_operation, lock_duration);

                    if (!ret_status)
                    {
                        // needed to wait on a row lock, so both prev_leaf and
                        // current_leaf latches have been released by 
                        // lockPreviousToFirstKey()

                        prev_leaf        = null;
                        current_leaf     = null;
                        latches_released = true;
                    }

                    break;
                }

                // Move left in tree, page latch will be requested nowait, 
                // and WaitError will be thrown if latch not granted.
                // Release latches on pages between "current_leaf" and 
                // where the search leads, so that at most 3 latched pages
                // (current_leaf, prev_leaf, prev_prev_leaf) are held during 
                // the search.  Do left ladder locking as you walk left.

                prev_prev_leaf = 
                    (LeafControlRow) prev_leaf.getLeftSibling(open_btree);
                prev_leaf.release();
                prev_leaf = prev_prev_leaf;
                prev_prev_leaf = null;

            }
            catch (WaitError e)
            {
                long previous_pageno = prev_leaf.getleftSiblingPageNumber();

                // error going left.  Release current page latch and 
                // original page latch continue the search.
//IC see: https://issues.apache.org/jira/browse/DERBY-3244
                if (current_leaf != null)
                {
                    // current_leaf may have already been released as part of
                    // previous calls, need to check null status.
                    current_leaf.release();
                    current_leaf = null;
                }

                // can only get here by above getLeftSibling() call so prev_leaf
                // should always be valid and latched at this point.  No null
                // check necessary.
                prev_leaf.release();
                prev_leaf = null;

                // wait on the left page, which we could not get before. 
                prev_leaf = (LeafControlRow) 
                    ControlRow.get(open_btree, previous_pageno);
//IC see: https://issues.apache.org/jira/browse/DERBY-2359
//IC see: https://issues.apache.org/jira/browse/DERBY-2359

                latches_released = true;
            }
        }

        if (prev_leaf != null)
            prev_leaf.release();

        return(!latches_released);
    }

    /**************************************************************************
     * Protected methods of This class:
     **************************************************************************
     */

    /**
     * Lock a row as part of doing the scan.
     * <p>
     * Lock the row at the given slot (or the previous row if slot is 0).
     * <p>
     * If this routine returns true all locks were acquired while maintaining
     * the latch on leaf.  If this routine returns false, locks may or may
     * not have been acquired, and the routine should be called again after
     * the client has researched the tree to reget the latch on the 
     * appropriate page.
     *
	 * @return Whether locks were acquired without releasing latch on leaf.
     *
     * @param open_btree        The open_btree to associate latches with - 
     *                          used if routine has to scan backward.
     * @param pos               The position of the row to lock.
     * @param request_row_lock  Whether to request the row lock, should
     *                          only be requested once per page in the scan.
     * @param lock_fetch_desc   The fetch descriptor to use to fetch the
     *                          row location for the lock request.
     * @param lock_template     A scratch area to use to read in rows.
     * @param previous_key_lock Is this a previous key lock call?
     * @param forUpdate         Is the scan for update or for read only.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected boolean _lockScanRow(
    OpenBTree               open_btree,
    BTreeRowPosition        pos,
    boolean                 request_row_lock,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    boolean                 previous_key_lock,
    boolean                 forUpdate,
    int                     lock_operation)
		throws StandardException
    {
        boolean latch_released = false;

        if (request_row_lock)
        {
            // In order to implement a serialized scan based on previous
            // key locking, this method acquires a row lock on
            // the base table's row from the index row at [startpage/startslot].
            // This will be the 'previous key'.

            if (pos.current_slot == 0)
            {
                // this call will take care of searching left in the btree
                // to find the previous row to lock, 0 is the control row and
                // not a valid thing to lock as a previous key.

                // it is ok to call the non-scan as this is just a special
                // case of a previous key lock call.  The only scan code that
                // will call this routine with slot == 0 will retry if this
                // routine returns that a latch was released.

                latch_released = 
                    !lockNonScanPreviousRow(
                        pos.current_leaf,
                        1 /* lock row previous to row at slot 1 */, 
                        lock_fetch_desc,
                        lock_template,
                        lock_row_loc,
                        open_btree, 
                        lock_operation,
                        TransactionManager.LOCK_COMMIT_DURATION);

                // special test to see if latch release code works
                if (SanityManager.DEBUG)
                {
                    latch_released = 
                        OpenBTree.test_errors(
                            open_btree,
                            "B2iRowLocking3_1_lockScanRow",
                            null, // Don't save position since the operation
                                  // will be retried if the latch was released.
                                  // See also comment above call to
                                  // lockNonScanPreviousRow().
                            this, pos.current_leaf, latch_released);
                }
            }
            else
            {
                // Just lock the row at "slot"

                latch_released = 
                    !lockRowOnPage(
                        pos.current_leaf, 
                        (LeafControlRow) null /* no other latch currently */,
                        pos.current_slot, 
                        pos,
                        lock_fetch_desc,
                        lock_template,
                        lock_row_loc,
                        lock_operation,
                        TransactionManager.LOCK_COMMIT_DURATION);

                // special test to see if latch release code works
                if (SanityManager.DEBUG)
                {
                    latch_released = 
                        OpenBTree.test_errors(
                            open_btree,
                            "B2iRowLocking3_2_lockScanRow", pos,
                            this, pos.current_leaf, latch_released);
                }
            }
        }

        return(!latch_released);
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */


    /**************************************************************************
     * Abstract Protected lockScan*() locking methods of BTree:
     *     lockScanRow              - lock row
     *     unlockScanRecordAfterRead- unlock the scan record
     **************************************************************************
     */

    /**
     * Lock a btree row to determine if it is a committed deleted row.
     * <p>
     * @see BTreeLockingPolicy#lockScanCommittedDeletedRow
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockScanCommittedDeletedRow(
    OpenBTree               open_btree,
    LeafControlRow          leaf,
    DataValueDescriptor[]   template,
    FetchDescriptor         lock_fetch_desc,
    int                     slot_no)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(leaf != null);

            if (slot_no <= 0 || slot_no >= leaf.getPage().recordCount())
            {
                SanityManager.THROWASSERT(
                    "slot_no = " + slot_no +
                    "; leaf.getPage().recordCount() = " +
                        leaf.getPage().recordCount());
            }

            SanityManager.ASSERT(template != null, "template is null");
        }

        RowLocation row_loc = (RowLocation) 
            template[((B2I) open_btree.getConglomerate()).rowLocationColumn];

        // Fetch the row location to lock.
        leaf.getPage().fetchFromSlot(
            (RecordHandle) null, slot_no, template, lock_fetch_desc, true);

        // Request the lock NOWAIT, return status
        return(
            base_cc.lockRow(row_loc, 
                ConglomerateController.LOCK_UPD,
                false /* NOWAIT */, 
                TransactionManager.LOCK_COMMIT_DURATION));
    }

    /**
     * Lock a row as part of doing the scan.
     * <p>
     * Lock the row at the given slot (or the previous row if slot is 0).
     * <p>
     * If this routine returns true all locks were acquired while maintaining
     * the latch on leaf.  If this routine returns false, locks may or may
     * not have been acquired, and the routine should be called again after
     * the client has researched the tree to reget the latch on the 
     * appropriate page.
     *
	 * @return Whether locks were acquired without releasing latch on leaf.
     *
     * @param open_btree        The open_btree to associate latches with - 
     *                          used if routine has to scan backward.
     * @param pos               The position of the row to lock.
     * @param lock_template     A scratch area to use to read in rows.
     * @param previous_key_lock Is this a previous key lock call?
     * @param forUpdate         Is the scan for update or for read only.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockScanRow(
    OpenBTree               open_btree,
    BTreeRowPosition        pos,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    boolean                 previous_key_lock,
    boolean                 forUpdate,
    int                     lock_operation)
		throws StandardException
    {
        return(
            _lockScanRow(
                open_btree,
                pos,
                true,  // request the row lock (always true for iso 3 )
                lock_fetch_desc,
                lock_template,
                lock_row_loc,
                previous_key_lock,
                forUpdate,
                lock_operation));
    }

    /**
     * Release read lock on a row.
     *
     * For serializable, there is no work to do.
     *
     *
     **/
    public void unlockScanRecordAfterRead(
    BTreeRowPosition        pos,
    boolean                 forUpdate)
		throws StandardException
    {
        return;
    }

    /**************************************************************************
     * Abstract Protected lockNonScan*() locking methods of BTree:
     *
     *     lockNonScanPreviousRow   - lock the row previous to the current
     *     lockNonScanRow           - lock the input row
     **************************************************************************
     */

    /**
     * Lock the row previous to the input row.
     * <p>
     * See BTreeLockingPolicy.lockNonScanPreviousRow
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockNonScanPreviousRow(
    LeafControlRow          current_leaf,
    int                     current_slot,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    OpenBTree               open_btree,
    int                     lock_operation,
    int                     lock_duration)
		throws StandardException
    {
        boolean          ret_status;

        if (current_slot > 1)
        {
            // Easy case, just lock the key previous to the current one.
            
            // Lock (current_slot - 1)

            ret_status = 
                lockRowOnPage(
                    current_leaf, (LeafControlRow) null, 
                    current_slot - 1,
                    null,
                    lock_fetch_desc,
                    lock_template,
                    lock_row_loc,
                    lock_operation, lock_duration);
        }
        else
        {
            // Should only be called while pointing at a valid location, 0 
            // is not a valid key slot - it is the control row.
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(current_slot == 1);

            if (current_leaf.isLeftmostLeaf())
            {
                // This is first row in table, lock the special key that 
                // represents the key previous to the first key of the table.
                ret_status = 
                    lockPreviousToFirstKey(
                        current_leaf, (LeafControlRow) null, 
                        lock_operation, lock_duration);
            }
            else
            {
                // The previous key is on a previous page, search left 
                // through the pages to find the key to latch.

                // If we need to release the latches while searching left,
                // a new key may have appeared in the range that we've already
                // searched, or the tree may have been rearranged, so the
                // caller must research, get new locks if this routine 
                // releases latches.
                ret_status = this.searchLeftAndLockPreviousKey(
//IC see: https://issues.apache.org/jira/browse/DERBY-6041
                    current_leaf,
                    lock_fetch_desc, lock_template, lock_row_loc,
                    open_btree, lock_operation, lock_duration);
            }
        }

        return(ret_status);
    }

    /**
     * Lock the in memory row.
     * <p>
     * See BTree.lockRow() for more info.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockNonScanRow(
    BTree                   btree,
    LeafControlRow          current_leaf,
    LeafControlRow          aux_leaf,
    DataValueDescriptor[]   current_row,
    int                     lock_operation)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(btree instanceof B2I);
        }
        B2I b2i = (B2I) btree;

        // First try to get the lock NOWAIT, while latch is held.
        boolean ret_status = 
            base_cc.lockRow(
                (RowLocation) current_row[b2i.rowLocationColumn], 
                lock_operation,
                false /* NOWAIT */, 
                TransactionManager.LOCK_COMMIT_DURATION);

        if (!ret_status)
        {
            // Could not get the lock NOWAIT, release latch and wait for lock.

            if (current_leaf != null)
            {
                current_leaf.release();
                current_leaf = null;
            }
            if (aux_leaf != null)
            {
                aux_leaf.release();
                aux_leaf = null;
            }

            base_cc.lockRow(
                (RowLocation) current_row[b2i.rowLocationColumn], 
                lock_operation,
                true /* WAIT */, 
                TransactionManager.LOCK_COMMIT_DURATION);
        }

        return(ret_status);
    }

    public boolean lockNonScanRowOnPage(
    LeafControlRow          current_leaf,
    int                     current_slot,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    int                     lock_operation)
		throws StandardException
    {
        return(
            lockRowOnPage(
                current_leaf,
                null,
                current_slot,
                null,
                lock_fetch_desc,
                lock_template,
                lock_row_loc,
                lock_operation,
                TransactionManager.LOCK_COMMIT_DURATION));
    }
}
