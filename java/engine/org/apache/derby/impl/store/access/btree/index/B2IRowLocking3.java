/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IRowLocking3

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

package org.apache.derby.impl.store.access.btree.index;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
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

/**

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
     * The locking policy to use to get and release the scan locks.  We could
     * cache this somewhere better.
     **/
    private LockingPolicy                   scan_locking_policy;

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
        this.scan_locking_policy = 
            rawtran.newLockingPolicy(
                LockingPolicy.MODE_RECORD, 
                TransactionController.ISOLATION_READ_COMMITTED, true);
    }

    /**************************************************************************
     * Private methods of This class:
     **************************************************************************
     */

    private boolean _lockScan(
    RecordHandle    rh,
    boolean         forUpdate,
    boolean         wait)
		throws StandardException
    {
        boolean ret_val = true;

        // only get the scan lock if we are record locking.
        
        if (!forUpdate)
        {
            ret_val = 
                scan_locking_policy.lockRecordForRead(
                    rawtran, open_btree.getContainerHandle(), 
                    rh, wait, false);
        }
        else
        {
            ret_val = 
                scan_locking_policy.lockRecordForWrite(
                    rawtran, rh, false, wait);
        }

        return(ret_val);
    }

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
     * @param open_btree        The open btree to associate this lock with.
     * @param aux_leaf          If non-null, this leaf is unlatched if the 
     *                          routine has to wait on the lock.
     * @param forUpdate         Whether to lock exclusive or share.
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
     * @param btree             The conglomerate we are locking.
     * @param current_leaf      Latched current leaf where "current" key is.
     * @param aux_leaf          If non-null, this leaf is unlatched if the 
     *                          routine has to wait on the lock.
     * @param current_slot      Slot of row to lock.
     * @param lock_fetch_desc   Descriptor for fetching just the RowLocation,
     *                          used for locking.
     * @param check_changed_rowloc
     *                          whether to check for the changed rowloc or not.
     * @param forUpdate         Whether to wait for lock.
     * @param forUpdatePrevKey  Whether lock is for key prev to insert or not.
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
    BTree                   btree,
    LeafControlRow          current_leaf,
    LeafControlRow          aux_leaf,
    int                     current_slot,
    boolean                 check_changed_rowloc,
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


            if (!(btree instanceof B2I))
            {
                SanityManager.THROWASSERT(
                    "btree not instance of B2I, it is " +
                    btree.getClass().getName());
            }

            SanityManager.ASSERT(lock_template != null, "template is null");

            // For now the RowLocation is expected to be the object located in
            // the last column of the lock_template, this may change if we
            // ever support rows with RowLocations somewhere else.
            SanityManager.ASSERT(
                lock_row_loc == lock_template[lock_template.length - 1], 
                "row_loc is not the object in last column of lock_template.");
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
                lock_row_loc, 
                lock_operation,
                true /* WAIT */, lock_duration);
        }

        return(ret_status);
    }

    private boolean searchLeftAndLockPreviousKey(
    B2I                     b2i,
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
            long previous_pageno = current_leaf.getleftSiblingPageNumber();

            // error going from mainpage to first left page.  Release 
            // current page latch and continue the search.
            current_leaf.release();
            current_leaf = null;

            // wait on the left page, which we could not get before. 
            prev_leaf = (LeafControlRow) 
                ControlRow.Get(open_btree, previous_pageno);

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
                            b2i,
                            prev_leaf, 
                            current_leaf, 
                            prev_leaf.getPage().recordCount() - 1, 
                            false, 
                            lock_fetch_desc,
                            lock_template,
                            lock_row_loc,
                            lock_operation, 
                            lock_duration);

                    if (!ret_status)
                    {
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
                // the search.  Do left ladder locking as you walk left, 
                // but be ready to release l

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
                current_leaf.release();
                current_leaf = null;
                prev_leaf.release();
                prev_leaf = null;

                // wait on the left page, which we could not get before. 
                prev_leaf = (LeafControlRow) 
                    ControlRow.Get(open_btree, previous_pageno);

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
     * Get the scan lock on the page if "request_scan_lock" is true.
     * <p>
     * If this routine returns true all locks were acquired while maintaining
     * the latch on leaf.  If this routine returns false, locks may or may
     * not have been acquired, and the routine should be called again after
     * the client has researched the tree to reget the latch on the 
     * appropriate page.
     * (p>
     * As a sided effect stores the value of the record handle of the current
     * scan lock.
     *
	 * @return Whether locks were acquired without releasing latch on leaf.
     *
     * @param open_btree        The open_btree to associate latches with - 
     *                          used if routine has to scan backward.
     * @param btree             the conglomerate info.
     * @param leaf              The control row of the current leaf to lock.
     * @param slot              The slot position of the row to lock.
     * @param request_row_lock  Whether to request the row lock, should
     *                          only be requested once per page in the scan.
     * @param request_scan_lock Whether to request the page scan lock, should
     *                          only be requested once per page in the scan.
     * @param lock_fetchDescriptor The fetch descriptor to use to fetch the
     *                          row location for the lock request.
     * @param scratch_template  A scratch area to use to read in rows.
     * @param previous_key_lock Is this a previous key lock call?
     * @param forUpdate         Is the scan for update or for read only.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected boolean _lockScanRow(
    OpenBTree               open_btree,
    BTree                   btree,
    BTreeRowPosition        pos,
    boolean                 request_row_lock,
    boolean                 request_scan_lock,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    boolean                 previous_key_lock,
    boolean                 forUpdate,
    int                     lock_operation)
		throws StandardException
    {
        boolean latch_released = false;
        B2I     b2i            = (B2I) btree;

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
                        btree,
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
                            "B2iRowLocking3_1_lockScanRow",  false,
                            this, pos.current_leaf, latch_released);
                }
            }
            else
            {
                // Just lock the row at "slot"

                latch_released = 
                    !lockRowOnPage(
                        btree,
                        pos.current_leaf, 
                        (LeafControlRow) null /* no other latch currently */,
                        pos.current_slot, 
                        true,
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
                            "B2iRowLocking3_2_lockScanRow", false,
                            this, pos.current_leaf, latch_released);
                }
            }
        }

        if (request_scan_lock && !latch_released)
        {
            // Get the scan lock on the start page.

            // Get shared RECORD_ID_PROTECTION_HANDLE lock to make sure that
            // we wait for scans in other transactions to move off of this page
            // before we split.


            latch_released = 
                !lockScan(
                    pos.current_leaf, 
                    (LeafControlRow) null, // no other latch currently
                    false,
                    ConglomerateController.LOCK_READ);// read scan lock position

            // special test to see if latch release code works
            if (SanityManager.DEBUG)
            {
                /* RESOLVE - need to get a container here */
                latch_released = 
                    OpenBTree.test_errors(
                        open_btree,
                        "B2iRowLocking3_3_lockScanRow", true, 
                        this, pos.current_leaf, latch_released);
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
     *     lockScan                 - lock the scan page
     *     lockScanForReclaimSpace  - lock page for reclaiming deleted rows.
     *     lockScanRow              - lock row and possibly the scan page
     *     unlockScan               - unlock the scan page
     *     unlockScanRecordAfterRead- unlock the scan record
     **************************************************************************
     */

    /**
     * Lock a control row page for scan.
     * <p>
     * Scanners get shared lock on the page while positioned on a row within
     * the page, splitter/purgers/mergers get exclusive lock on the page.
     *
     * See BTree.lockScan() for more info.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockScan(
    LeafControlRow          current_leaf,
    ControlRow              aux_control_row,
    boolean                 forUpdate,
    int                     lock_operation)
		throws StandardException
    {
        // The scan page lock is implemented as a row lock on the reserved
        // row id on the page (RecordHandle.RECORD_ID_PROTECTION_HANDLE).
        RecordHandle scan_lock_rh = 
            current_leaf.getPage().makeRecordHandle(
                RecordHandle.RECORD_ID_PROTECTION_HANDLE);

        // First try to get the lock NOWAIT, while latch is held.
        boolean ret_status = 
            _lockScan(scan_lock_rh, forUpdate, false /* NOWAIT */);

        if (!ret_status)
        {
            current_leaf.release();
            current_leaf = null;

            if (aux_control_row != null)
            {
                aux_control_row.release();
                aux_control_row = null;
            }

            // Could not get the lock NOWAIT, release latch and wait
            // for the lock.
            _lockScan(scan_lock_rh, forUpdate, true /* WAIT */);

            // once we get the lock, give it up as we need to get the lock
            // while we have the latch.  When the lock manager gives us the
            // ability to do instantaneous locks do that.  We just wait on the
            // lock to give the split a chance to finish before we interfere.

            if (!forUpdate)
            {
                scan_locking_policy.unlockRecordAfterRead(
                    rawtran, open_btree.getContainerHandle(), 
                    scan_lock_rh, false, true);
            }
            else
            {
                // RESOLVE - need instantaneous locks as there is no way 
                // currently to release a write lock.  This lock will only
                // be requested by split, and will be released by internal
                // transaction.
            }
        }

        return(ret_status);
    }

    /**
     * Lock a control row page for reclaiming deleted rows.
     * <p>
     * When reclaiming deleted rows during split need to get an exclusive
     * scan lock on the page, which will mean there are no other scans 
     * positioned on the page.  If there are other scans positioned, just
     * give up on reclaiming space now.
     *
	 * @return true if lock was granted nowait, else false and not lock was
     *         granted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockScanForReclaimSpace(
    LeafControlRow          current_leaf)
		throws StandardException
    {
        // The scan page lock is implemented as a row lock on the reserved
        // row id on the page (RecordHandle.RECORD_ID_PROTECTION_HANDLE).
        RecordHandle scan_lock_rh = 
            current_leaf.getPage().makeRecordHandle(
                RecordHandle.RECORD_ID_PROTECTION_HANDLE);

        // First try to get the lock NOWAIT, while latch is held.
        return(
            _lockScan(scan_lock_rh, true /* update */, false /* NOWAIT */));
    }

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
     * Get the scan lock on the page if "request_scan_lock" is true.
     * <p>
     * If this routine returns true all locks were acquired while maintaining
     * the latch on leaf.  If this routine returns false, locks may or may
     * not have been acquired, and the routine should be called again after
     * the client has researched the tree to reget the latch on the 
     * appropriate page.
     * (p>
     * As a sided effect stores the value of the record handle of the current
     * scan lock.
     *
	 * @return Whether locks were acquired without releasing latch on leaf.
     *
     * @param open_btree        The open_btree to associate latches with - 
     *                          used if routine has to scan backward.
     * @param btree             the conglomerate info.
     * @param leaf              The control row of the current leaf to lock.
     * @param slot              The slot position of the row to lock.
     * @param request_scan_lock Whether to request the page scan lock, should
     *                          only be requested once per page in the scan.
     * @param scratch_template  A scratch area to use to read in rows.
     * @param previous_key_lock Is this a previous key lock call?
     * @param forUpdate         Is the scan for update or for read only.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockScanRow(
    OpenBTree               open_btree,
    BTree                   btree,
    BTreeRowPosition        pos,
    boolean                 request_scan_lock,
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
                btree,
                pos,
                true,  // request the row lock (always true for iso 3 )
                request_scan_lock,
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
     * @param row_qualified     Did the row qualify to be returned to caller.
     *
     **/
    public void unlockScanRecordAfterRead(
    BTreeRowPosition        pos,
    boolean                 forUpdate)
		throws StandardException
    {
        return;
    }

    /**
     * Release the lock gotten by calling lockScan.  This call can only be
     * made to release read scan locks, write scan locks must be held until
     * end of transaction.
     * <p>
     * See BTree.unlockScan() for more info.
     *
     **/
    public void unlockScan(
    long            page_number)
    {
        // This is first row in table, lock the special key that 
        // represents the key previous to the first key of the table.
        try
        {
            RecordHandle scan_lock_rh = 
                open_btree.makeRecordHandle(
                    page_number, RecordHandle.RECORD_ID_PROTECTION_HANDLE);

            scan_locking_policy.unlockRecordAfterRead(
                rawtran, open_btree.getContainerHandle(), 
                scan_lock_rh, false, true);
        }
        catch (StandardException se)
        {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("error from make RecordHandle.");
        }

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
     * See BTree.lockPreviousRow() for more info.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockNonScanPreviousRow(
    BTree                   btree,
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

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(btree instanceof B2I);
        }

        if (current_slot > 1)
        {
            // Easy case, just lock the key previous to the current one.
            
            // Lock (current_slot - 1)

            ret_status = 
                lockRowOnPage(
                    btree,
                    current_leaf, (LeafControlRow) null, 
                    current_slot - 1,
                    false,
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

                // RESOLVE RLL (mikem) - do I need to do the 
                // RECORD_ID_PROTECTION_HANDLE lock.
                // First guarantee that record id's will not move off this
                // current page while searching for previous key, by getting
                // the RECORD_ID_PROTECTION_HANDLE lock on the current page.
                // Since we have a latch on the cur

                // RESOLVE RLL (mikem) - NO RECORD_ID PROTECTION IN EFFECT.
                // caller must research, get new locks if this routine 
                // releases latches.
                ret_status = this.searchLeftAndLockPreviousKey(
                    (B2I) btree,
                    current_leaf, current_slot,
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
    BTree                   btree,
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
                btree,
                current_leaf,
                null,
                current_slot,
                false,
                lock_fetch_desc,
                lock_template,
                lock_row_loc,
                lock_operation,
                TransactionManager.LOCK_COMMIT_DURATION));
    }
}
