/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2INoLocking

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

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.ControlRow;
import org.apache.derby.impl.store.access.btree.LeafControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;
import org.apache.derby.impl.store.access.btree.BTreeRowPosition;
import org.apache.derby.impl.store.access.btree.WaitError;

/**

**/

public class B2INoLocking implements BTreeLockingPolicy
{

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public B2INoLocking(
    Transaction             rawtran,
    int                     lock_level,
    LockingPolicy           locking_policy,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
    {
    }

    protected B2INoLocking()
    {
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
        return(true);
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
        // if doing no locking don't allow reclaiming space.
        return(false);
    }

    /**
     * Lock a btree row to determine if it is a committed deleted row.
     * <p>
     * Request an exclusive lock on the row located at the given slot, NOWAIT.
     * Return true if the lock is granted, otherwise false.
     * <p>
     *
     * @param open_btree        The conglomerate we are locking.
     * @param leaf              The leaf page with the row to lock.
     * @param template          A scratch area to use to read in RowLocation.
     * @param slot_no           The slot of row on "current_leaf" 
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
        return(true);
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
     * @param leaf              The control row of the current leaf to lock.
     * @param slot              The slot position of the row to lock.
     * @param request_scan_lock Whether to request the page scan lock, should
     *                          only be requested once per page in the scan.
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
        return(true);
    }

    /**
     * Release read lock on a row.
     *
     * @param forUpdate         Is the scan for update or for read only.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void unlockScanRecordAfterRead(
    BTreeRowPosition        pos,
    boolean                 forUpdate)
		throws StandardException
    {
        return;
    }


    /**
     * Unlock the lock gotten by lockScan().
     * <p>
     * See BTree.unlockScan() for more info.
     *
     **/
    public void unlockScan(
    long    page_number)
    {
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
        return(true);
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
        return(true);
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
        return(true);
    }
}
