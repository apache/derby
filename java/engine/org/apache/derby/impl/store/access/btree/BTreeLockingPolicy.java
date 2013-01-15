/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeLockingPolicy

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

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.raw.FetchDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**

The generic.BTree directory wants to know as little about locking as possible,
in order to make the code usuable by multiple implementations.  But the 
generic code will make calls to abstract lock calls implemented by concrete
btree implementations.  Concrete implementations like B2I understand locking,
and needs informatation specific to the implementation to make the lock calls.
<p>
This class is created and owned by the concrete application, but is passed
into and returned from the generic code when lock calls are made.
Concrete implementations which do not need lock calls can just pass a null
pointer where a BTreeLockingPolicy is requested.
<p>
There are 2 types of lock interfaces, lockScan*() and lockNonScan*().
<p>
The lockScan*() interfaces save the key for the current scan position before
giving up the latch on the page if they have to wait for a row lock. The
callers can reposition the scan using the saved key by calling
{@code BTreeScan.reposition()} if such a situation occurs. Then the latches
are reobtained, possibly on a different page if the current key has been moved
to another page in the meantime. Upon return from these interfaces
the row lock requested is guaranteed to have been obtained on the correct
key for the row requested.  These interfaces handle the special case of 
unique indexes where the RowLocation can change while waiting on the lock 
(see implementation for details), basically the lock is retryed after waiting
if the RowLocation has changed.
<p>
The lockNonScan*() interfaces do not save the current scan position. If these
routines return that the latch was released while waiting to obtain the
lock, then the caller must requeue the lock request after taking appropriate
action.  This action usually involves researching the tree to make sure 
that the correct key is locked with latches held. These interfaces
do not handle the special case of unique indexes where the RowLocation can 
change while waiting on the lock, as the row may disappear when the latch
is released to wait on the lock - thus it is necessary that the caller retry
the lock if the interface returns that the latch was released.


**/

public interface BTreeLockingPolicy
{
    /**************************************************************************
     * Abstract Protected lockScan*() locking methods of BTree:
     *     lockScanRow              - lock row
     *     unlockScanRecordAfterRead- unlock the scan record
     **************************************************************************
     */

    /**
     * Lock a btree row to determine if it is a committed deleted row.
     * <p>
     * Request an exclusive lock on the row located at the given slot, NOWAIT.
     * Return true if the lock is granted, otherwise false.
     * <p>
     *
     * @param open_btree        The conglomerate we are locking.
     * @param leaf              The leaf page with the row to lock.
     * @param template          Empty full template row, to read row into.
     * @param slot_no           The slot of row on "current_leaf" 
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public boolean lockScanCommittedDeletedRow(
    OpenBTree               open_btree,
    LeafControlRow          leaf,
    DataValueDescriptor[]   template,
    FetchDescriptor         lock_fetch_desc,
    int                     slot_no)
		throws StandardException;

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
     * @param pos               Description of position of row to lock.
     * @param lock_template     A scratch area to use to read in rows.
     * @param previous_key_lock Is this a previous key lock call?
     * @param forUpdate         Is the scan for update or for read only.
     * @param lock_operation    For what operation are we requesting the lock, 
     *                          this should be one of the following 4 options:
     *                          LOCK_READ [read lock], 
     *                          (LOCK_INS | LOCK_UPD) [ lock for insert], 
     *                          (LOCK_INSERT_PREVKEY | LOCK_UPD) [lock for 
     *                          previous key to insert],
     *                          (LOCK_UPD) [lock for delete or replace]
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public boolean lockScanRow(
    OpenBTree               open_btree,
    BTreeRowPosition        pos,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    boolean                 previous_key_lock,
    boolean                 forUpdate,
    int                     lock_operation)
		throws StandardException;

    /**
     * Release read lock on a row.
     *
     * @param pos               Data structure that defines the current position
     *                          in the scan to be unlocked.
     *
     * @param forUpdate         Is the scan for update or for read only.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public void unlockScanRecordAfterRead(
    BTreeRowPosition        pos,
    boolean                 forUpdate)
		throws StandardException;


    /**************************************************************************
     * Abstract Protected lockNonScan*() locking methods of BTree:
     *
     *     lockNonScanPreviousRow   - lock the row previous to the current
     *     lockNonScanRow           - lock the input row
     *     lockNonScanRowOnPage     - lock the given row on the page.
     **************************************************************************
     */

    /**
     * Lock the previous key.
     * <p>
     * Given the current latched page and slot number, lock the logically
     * previous key in the table.  There are 3 cases:
     * <p>
     * slotnumber > 1                       - just lock (slotnumber - 1)
     * (slotnumber == 1) && (leftmost leaf) - this is the first key in the
     *                                        table, so lock a "magic" FIRSTKEY.
     * (slotnumber == 1) && !(leftmost leaf)- traverse left in the tree looking
     *                                        for a previous key.
     * <p>
     * On successful return from this routine appropriate locking will have
     * been done.  All locks and latches are requested nowait, if any 
     * lock/latch cannot be granted this routine releases the current_leaf
     * latch and any latches it may have acquired and returns "false."
     * <p>
     * All extra latches that may have been gotten will have been released.
     * <p>
     * This routine will find the "previous row" to the (current_leaf,
     * current_slot), walking left in the tree as necessary, and first request
     * the lock on that row NOWAIT.  If that lock can not be granted,
     * then it will release all latches that it has acquired up to that point
     * including the latched current_leaf passed into the routine, and request 
     * the lock WAIT.  Once the lock has been granted the routine will return
     * and it is up to the caller to research the tree to find where the
     * current position may have ended up.  For instance in the case of insert
     * once the current latch is released, the correct page to do the insert
     * may no longer be where the original scan found it.
     * <p>
     * If routine returns true, lock was granted NOWAIT, current leaf
     * remains latched, and was never unlatched.  If routine returns false,
     * lock was granted WAIT, current leaf is not latched, row may have 
     * moved in the btree so caller must research to find the row.
     *
     *
     * @param current_leaf      Latched current leaf where "current" key is.
     * @param current_slot      The slot of row on "current_leaf" 
     * @param lock_template     Empty full template row, to read row into.
     * @param open_btree        The open_btree to associate latches with - 
     *                          used if routine has to scan backward.
     * @param lock_operation    For what operation are we requesting the lock, 
     *                          this should be one of the following 4 options:
     *                          LOCK_READ [read lock], 
     *                          (LOCK_INS | LOCK_UPD) [ lock for insert], 
     *                          (LOCK_INSERT_PREVKEY | LOCK_UPD) [lock for 
     *                          previous key to insert],
     *                          (LOCK_UPD) [lock for delete or replace]
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
    abstract public boolean lockNonScanPreviousRow(
    LeafControlRow          current_leaf,
    int                     current_slot,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    OpenBTree               open_btree,
    int                     lock_operation,
    int                     lock_duration)
		throws StandardException;

    /**
     * Lock a btree row (row in memory).  Meant to be used if caller 
     * has the entire row objectified.
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
     * the latch on "current_leaf" (if current_leaf is non-null) and 
     * "aux_leaf" (if aux_leaf is non-null), and then it will request a WAIT 
     * lock on the row.  
     *
     *
     * @param btree             The conglomerate we are locking.
     * @param current_leaf      If non-null, this leaf is unlatched if the 
     *                          routine has to wait on the lock.
     * @param aux_leaf          If non-null, this leaf is unlatched if the 
     *                          routine has to wait on the lock.
     * @param current_row       In memory, objectified "current" row.
     * @param lock_operation    For what operation are we requesting the lock, 
     *                          this should be one of the following 4 options:
     *                          LOCK_READ [read lock], 
     *                          (LOCK_INS | LOCK_UPD) [ lock for insert], 
     *                          (LOCK_INSERT_PREVKEY | LOCK_UPD) [lock for 
     *                          previous key to insert],
     *                          (LOCK_UPD) [lock for delete or replace]
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public boolean lockNonScanRow(
    BTree                   btree,
    LeafControlRow          current_leaf,
    LeafControlRow          aux_leaf,
    DataValueDescriptor[]   current_row,
    int                     lock_operation)
		throws StandardException;

    /**
     * Lock the row at the given slot.
     * <p>
     * If this routine returns true all locks were acquired while maintaining
     * the latch on leaf.  If this routine returns false, locks may or may
     * not have been acquired, and the routine should be called again after
     * the client has researched the tree to reget the latch on the 
     * appropriate page.
     *
	 * @return Whether locks were acquired without releasing latch on leaf.
     *
     * @param leaf              The control row of the current leaf to lock.
     * @param slot              The slot position of the row to lock.
     * @param lock_template     A scratch area to use to read in rows.
     * @param lock_operation    For what operation are we requesting the lock, 
     *                          this should be one of the following 4 options:
     *                          LOCK_READ [read lock], 
     *                          (LOCK_INS | LOCK_UPD) [ lock for insert], 
     *                          (LOCK_INSERT_PREVKEY | LOCK_UPD) [lock for 
     *                          previous key to insert],
     *                          (LOCK_UPD) [lock for delete or replace]
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public boolean lockNonScanRowOnPage(
    LeafControlRow          leaf,
    int                     slot,
    FetchDescriptor         lock_fetch_desc,
    DataValueDescriptor[]   lock_template,
    RowLocation             lock_row_loc,
    int                     lock_operation)
		throws StandardException;
}
