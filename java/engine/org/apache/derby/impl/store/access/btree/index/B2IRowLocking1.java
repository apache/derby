/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IRowLocking1

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.impl.store.access.btree.BTreeRowPosition;
import org.apache.derby.impl.store.access.btree.ControlRow;
import org.apache.derby.impl.store.access.btree.LeafControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;
import org.apache.derby.impl.store.access.btree.WaitError;

/**

The btree locking policy which implements read uncommitted isolation level.

It inherits all functionality from B2IRowLocking2 except that it does not
get any read row locks (and thus does not release them).  Note that table
level and table level intent locking remains the same as B2IRowLocking2 as
this is currently the way we prevent concurrent ddl from happening while a
read uncommitted scanner is operating in the btree.

**/

class B2IRowLocking1 extends B2IRowLocking2 implements BTreeLockingPolicy
{ 
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    B2IRowLocking1(
    Transaction             rawtran,
    int                     lock_level,
    LockingPolicy           locking_policy,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
    {
        super(rawtran, lock_level, locking_policy, base_cc, open_btree);
    }


    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */


    /**************************************************************************
     * Abstract Protected lockScan*() locking methods of BTree:
     *     lockScan                 - lock the scan page
     *                                (inherit from B2IRowLocking2, we still
     *                                 get page control locks).
     *     lockScanForReclaimSpace  - lock page for reclaiming deleted rows.
     *                                (inherit from B2IRowLocking2, should never
     *                                 be called while in read uncommitted).
     *     lockScanRow              - lock row and possibly the scan page, only
     *                                if row is forUpdate and not a previous key
     *                                lock.
     *     unlockScan               - unlock the scan page
     *                                (inherit from B2IRowLocking2, should never
     *                                 be called while in read uncommitted).
     *     unlockScanRecordAfterRead- unlock the scan record if we locked it in
     *                                lockScanRow.
     *                                 
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
     * As a side effect stores the value of the record handle of the current
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
        // request the scan lock if necessary.
        // only get the row lock if it is not a previous key lock and iff
        // it is an update lock.
        return(
            _lockScanRow(
                 open_btree,
                 btree,
                 pos,
                 (forUpdate && !previous_key_lock), // only get update row lock
                 request_scan_lock,
                 lock_fetch_desc, lock_template, lock_row_loc,
                 previous_key_lock,
                 forUpdate,
                 lock_operation));
    }

    /**
     * Release read lock on a row.
     *
     * Because this is read uncommitted no S row locks will have been requested,
     * thus none need be released.  The only locks that need to be released
     * are U locks requested if the scan was opened for update.
     *
     * @param pos               The position of the row to unlock.
     * @param forUpdate         Is the scan for update or for read only.
     *
     **/
    public void unlockScanRecordAfterRead(
    BTreeRowPosition        pos,
    boolean                 forUpdate)
		throws StandardException
    {
        if (forUpdate)
        {
            super.unlockScanRecordAfterRead(pos, forUpdate);
        }
    }

    /**************************************************************************
     * Abstract Protected lockNonScan*() locking methods of BTree:
     *
     *     lockNonScanPreviousRow   - lock the row previous to the current
     *                                (inherit from B2IRowLocking2, we still 
     *                                 get page control locks) - only called
     *                                 by insert.
     *     lockNonScanRow           - lock the input row
     *                                (inherit from B2IRowLocking2, we still
     *                                 get page control locks) - only called
     *                                 by insert.
     **************************************************************************
     */

}
