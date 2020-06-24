/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeController

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

import java.util.Properties;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;

/**

  A b-tree controller corresponds to an instance of an open b-tree conglomerate.
  <P>
  <B>Concurrency Notes</B>
  <P>
  The concurrency rules are derived from OpenBTree.
  <P>
  @see OpenBTree

**/

public class BTreeController extends OpenBTree implements ConglomerateController
{

    transient DataValueDescriptor[] scratch_template = null;

    /**
     * Whether to get lock on the row being inserted, usually this lock
     * has already been gotten when the row was inserted into the base table.
     **/
    boolean get_insert_row_lock;
    
    //constants for the status of dupicate checking
    private static final int NO_MATCH = 0;
    private static final int MATCH_FOUND = 1;
    private static final int RESCAN_REQUIRED = 2;

    /* Constructors: */

	public BTreeController()
	{
	}

	/*
	** private Methods of BTreeController
	*/

    /**
     * Attempt to reclaim committed deleted rows from the page.
     * <p>
     * Get exclusive latch on page, and then loop backward through
     * page searching for deleted rows which are committed.  The routine
     * assumes that it is called from a transaction which cannot have 
     * deleted any rows on the page.  For each deleted row on the page
     * it attempts to get an exclusive lock on the deleted row, NOWAIT.
     * If it succeeds, and since this row did not delete the row then the
     * row must have been deleted by a transaction which has committed, so
     * it is safe to purge the row.  It then purges the row from the page.
     * <p>
     * Note that this routine may remove all rows from the page, it will not
     * attempt a merge in this situation.  This is because this routine is
     * called from split which is attempting an insert on the given page, so
     * it would be a waste to merge the page only to split it again to allow
     * the insert of the row causing the split.
     *
	 * @return true if at least one row was purged.  If true, then the routine
     *              will leave the page latched, and the caller will release
     *              the latch by committing or aborting the transaction.  The
     *              latch must be held to end transaction to insure space on
     *              the page remains available for a undo of the purge.
     *
     * @param open_btree The already open btree to use to get latch on page.
     * @param pageno     The page number of the leaf to attempt the reclaim on.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private boolean reclaim_deleted_rows(
    OpenBTree   open_btree,
    long        pageno)
		throws StandardException
    {
        boolean     purged_at_least_one_row = false;
        ControlRow  controlRow              = null; 

        try
        {

//IC see: https://issues.apache.org/jira/browse/DERBY-2359
            if ((controlRow = ControlRow.get(open_btree, pageno)) == null)
                return(false);

            LeafControlRow leaf       = (LeafControlRow) controlRow;

            BTreeLockingPolicy  btree_locking_policy = 
                open_btree.getLockingPolicy();


            // The number records that can be reclaimed is:
            // total recs - control row - recs_not_deleted
            int num_possible_commit_delete = 
                leaf.page.recordCount() - 1 - leaf.page.nonDeletedRecordCount();

            if (num_possible_commit_delete > 0)
            {

                Page page   = leaf.page;


                // RowLocation column is in last column of template.
                FetchDescriptor lock_fetch_desc = 
                    RowUtil.getFetchDescriptorConstant(
                        scratch_template.length - 1);

                // loop backward so that purges which affect the slot table 
                // don't affect the loop (ie. they only move records we 
                // have already looked at).
                for (int slot_no = page.recordCount() - 1; 
                     slot_no > 0; 
                     slot_no--) 
                {
                    if (page.isDeletedAtSlot(slot_no))
                    {
                        // try to get an exclusive lock on the row, if we can 
                        // then the row is a committed deleted row and it is 
                        // safe to purge it.
                        if (btree_locking_policy.lockScanCommittedDeletedRow(
                                open_btree, leaf, scratch_template, 
                                lock_fetch_desc, slot_no))
                        {
                            // the row is a committed deleted row, purge it.
                            page.purgeAtSlot(slot_no, 1, true);

                            purged_at_least_one_row = true;
                        }
                    }
                }

            }
        }
        catch (java.lang.ClassCastException cce)
        {
            // because we give up the latch on the leaf before entering this
            // routine, the page might change from a leaf to branch.  If that
            // happens this routine will get a ClassCastException, and we
            // just give up trying to reclaim space.
        }
        finally
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5284
            if (controlRow != null) 
            {
                if (purged_at_least_one_row) 
                {
                    // Set a hint in the page that scans positioned on it
                    // need to reposition because rows have disappeared from
                    // the page.  If at least one row has been purged, then
                    // do not release the latch.  Purge requires latch to 
                    // be held until commit, where it will be released after
                    // the commit log record has been logged.
                    controlRow.page.setRepositionNeeded();
                }
                else
                {
                    // Ok to release latch if no purging has happened.
                    controlRow.release();
                }
            }
        }

        return(purged_at_least_one_row);
    }

    /**
     * Start an internal transaction and do the split.
     * <p>
     * This routine starts a new transaction, and handles any errors that
     * may come during the transaction.  This transation must not obtain any
     * locks as they are likely to conflict with the current user transaction.
     * <p>
     * If attempt_to_reclaim_deleted_rows is true this routine will 
     * attempt to reclaim space on the leaf page input, by purging 
     * committed deleted rows from the leaf.  If it succeeds in purging at
     * least one row, then it will commit the internal transaction and return
     * without actually performing a split.  
     *
     * @param scratch_template  A scratch template used to search a page.
     * @param rowToInsert       The row to insert, make sure during split to
     *                          make room for this row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private long 
    start_xact_and_dosplit(
    boolean                 attempt_to_reclaim_deleted_rows,
    long                    leaf_pageno,
    DataValueDescriptor[]   scratch_template, 
    DataValueDescriptor[]   rowToInsert,
    int                     flag)
        throws StandardException
    {
        TransactionManager split_xact       = null;
        OpenBTree          split_open_btree = null;
        ControlRow         root             = null;

        // Get an internal transaction to be used for the split.
        split_xact = this.init_open_user_scans.getInternalTransaction();

        // open the btree again so that actions on it take place in the
        // split_xact, don't get any locks in this transaction.

		if (SanityManager.DEBUG)
		{
            if (((getOpenMode() & ContainerHandle.MODE_FORUPDATE) !=
								   ContainerHandle.MODE_FORUPDATE))
            {
                SanityManager.THROWASSERT(
                    "Container not opened with update should not cause split");
            }
		}


        boolean do_split = true;
        if (attempt_to_reclaim_deleted_rows)
        {
            // Get lock on base table.

            ConglomerateController base_cc = null;

            try
            {
                base_cc = 
                    this.getConglomerate().lockTable(
                        split_xact, 
                        (ContainerHandle.MODE_FORUPDATE |
                         ContainerHandle.MODE_LOCK_NOWAIT), 
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_REPEATABLE_READ);
            }
            catch (StandardException se)
            {
                // any error just don't try to reclaim deleted rows.  The
                // expected error is that we can't get the lock, which the
                // current interface throws as a containerNotFound exception.
            }

            if (base_cc != null)
            {
                // we got IX lock on the base table, so can try reclaim space.


                // We can only reclaim space by opening the btree in row lock 
                // mode.  Table level lock row recovery is hard as we can't 
                // determine if the deleted rows we encounter have been 
                // deleted by our parent caller and have been committed or 
                // not.  We will have to get those rows offline.
                split_open_btree = new OpenBTree();
                split_open_btree.init(
                    this.init_open_user_scans, 
                    split_xact, 
                    null,                           // open the container.
                    split_xact.getRawStoreXact(), 
                    false,
                    (ContainerHandle.MODE_FORUPDATE | 
                     ContainerHandle.MODE_LOCK_NOWAIT),
                    TransactionManager.MODE_RECORD,
                    this.getConglomerate().getBtreeLockingPolicy(
                        split_xact.getRawStoreXact(), 
                        TransactionController.MODE_RECORD,
                        LockingPolicy.MODE_RECORD,
                        TransactionController.ISOLATION_REPEATABLE_READ, 
                        (ConglomerateController) base_cc, 
                        split_open_btree),
                    this.getConglomerate(), 
                    (LogicalUndo) null,
                    (DynamicCompiledOpenConglomInfo) null);

                // don't split if we reclaim any rows.
                do_split = !reclaim_deleted_rows(split_open_btree, leaf_pageno);

                // on return if !do_split then the latch on leaf_pageno is held
                // and will be released by the committing or aborting the 
                // transaction.  If a purge has been done, no other action on
                // the page should be attempted (ie. a split) before committing
                // the purges.

                split_open_btree.close();
            }
        }

        long new_leaf_pageno = leaf_pageno; 
        if (do_split)
        {
            // no space was reclaimed from deleted rows, so do split to allow
            // space for a subsequent insert.

            split_open_btree = new OpenBTree();
            split_open_btree.init(
                this.init_open_user_scans, 
                split_xact, 
                null,                           // open the container.
                split_xact.getRawStoreXact(), 
                false,
                getOpenMode(),                  // use same mode this controller
                                                // was opened with
                TransactionManager.MODE_NONE,
                this.getConglomerate().getBtreeLockingPolicy(
                    split_xact.getRawStoreXact(), 
                    this.init_lock_level,
                    LockingPolicy.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ, 
                    (ConglomerateController) null, // no base row locks during split
                    split_open_btree),
                this.getConglomerate(), 
                (LogicalUndo) null,
                (DynamicCompiledOpenConglomInfo) null);


            // Get the root page back, and perform a split following the
            // to-be-inserted key.  The split releases the root page latch.
            root = ControlRow.get(split_open_btree, BTree.ROOTPAGEID);
//IC see: https://issues.apache.org/jira/browse/DERBY-2359

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(root.page.isLatched());

            new_leaf_pageno = 
                root.splitFor(
                    split_open_btree, scratch_template, 
                    null, rowToInsert, flag);

            split_open_btree.close();
        }

        split_xact.commit();

        split_xact.destroy();

        return(new_leaf_pageno);
    }
    
    /**
     * Compares the oldrow with the one at 'slot' or the one left to it. 
     * If the slot is first slot it will move to the left sibiling of 
     * the 'leaf' and will compare with the record from the last slot.
     * @param slot slot number to start with
     * @param leaf LeafControlRow of the current page
     * @param rows DataValueDescriptot array to fill it with fetched values
     * @return  0 if no duplicate
     *          1 if duplicate 
     *          2 if rescan required
     * @throws StandardException
     */
    private int comparePreviousRecord (int slot, 
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
                                    LeafControlRow  leaf, 
                                    DataValueDescriptor [] rows,
                                    DataValueDescriptor [] oldRows) 
                                        throws StandardException {
        RecordHandle rh = null;
        boolean newLeaf = false;
        LeafControlRow originalLeaf = leaf;
        while (leaf != null) {
            if (slot == 0) {
                LeafControlRow oldLeaf = leaf;
                try {
                    //slot is pointing before the first slot
                    //get left sibiling
                    leaf = (LeafControlRow) leaf.getLeftSibling(this);
                    if (newLeaf) {
                        oldLeaf.release();
                    }
                    newLeaf = true;
                    //no left sibiling
                    if (leaf == null)
                        return NO_MATCH;
                    //set the slot to last slot number
                    slot = leaf.page.recordCount() - 1;
                    // DERBY-4027: We have moved to the previous page and need
                    // to recheck that the slot number is valid (it won't be
                    // if the page we moved to is empty). Restart from the top
                    // of the loop body to get the slot number rechecked.
                    continue;
                } catch (WaitError we) {
                    // DERBY-4097: Couldn't latch the left sibling without
                    // waiting. Release all latches and rescan from top of
                    // B-tree to prevent deadlock.
                    if (newLeaf) {
                        oldLeaf.release();
                    }
                    originalLeaf.release();
                    return RESCAN_REQUIRED;
                }
            }
            rh = leaf.page.fetchFromSlot(null, slot, rows, null, true);
            if (rh != null) {
                int ret = compareRowsForInsert(rows, oldRows, leaf, slot);

                // If we found a deleted row, we don't know whether there
                // is a duplicate, so we need to continue the search.
                final boolean continueSearch =
                        (ret == MATCH_FOUND && leaf.page.isDeletedAtSlot(slot));

                if (!continueSearch) {
                    if (newLeaf) {
                        // Since we have moved away from the original leaf,
                        // we need some logic to make sure we don't hold
                        // latches that we're not supposed to hold.
                        if (ret == RESCAN_REQUIRED) {
                            // When a rescan is required, we must release the
                            // original leaf, since the callers expect all
                            // latches to have been released (and so they
                            // should have been, so this is probably a bug -
                            // see DERBY-4080).
                            originalLeaf.release();
                        }
                        if (ret != RESCAN_REQUIRED) {
                            // Since a rescan is not required, we still hold
                            // the latch on the non-original leaf. No other
                            // leaves than the original one should be latched
                            // when we return, so release the current leaf.
                            leaf.release();
                        }
                    }
                    return ret;
                }
            }
            slot--;
        }
        return NO_MATCH;
    }
    
    /**
     * Compares the new record with the one at slot or the one 
     * right to it. If the slot is last slot in the page it will move to 
     * the right to sibling of the leaf and will compare with the record 
     * from the last slot. 
     * @param slot slot number to start with
     * @param leaf LeafControlRow of the current page
     * @param rows DataValueDescriptot array to fill it with fetched values
     * @return  0 if no duplicate
     *          1 if duplicate 
     *          2 if rescan required
     * @throws StandardException
     */
    private int compareNextRecord (int slot, 
                                    LeafControlRow  leaf, 
                                    DataValueDescriptor [] rows,
                                    DataValueDescriptor [] oldRows) 
                                        throws StandardException {
        RecordHandle rh = null;
        boolean newLeaf = false;
        LeafControlRow originalLeaf = leaf;
        while (leaf != null) {
            if (slot >= leaf.page.recordCount()) {
                //slot is pointing to last slot
                //get next sibling
                LeafControlRow oldLeaf = leaf;
                leaf = (LeafControlRow) leaf.getRightSibling(this);
                if (newLeaf) {
                    oldLeaf.release();
                }
                newLeaf = true;
                //this was right most leaf
                //no record at the right
                if (leaf == null)
                    return NO_MATCH;
                //point slot to the first record of new leaf
                slot = 1;
                // DERBY-4027: We have moved to the next page and need
                // to recheck that the slot number is valid (it won't be
                // if the page we moved to is empty). Restart from the top
                // of the loop body to get the slot number rechecked.
                continue;
            }
            rh = leaf.page.fetchFromSlot(null, slot, rows, null, true);
            if (rh != null) {
                int ret =  compareRowsForInsert(rows, oldRows, leaf, slot);

                // If we found a deleted row, we don't know whether there
                // is a duplicate, so we need to continue the search.
                final boolean continueSearch =
                        (ret == MATCH_FOUND && leaf.page.isDeletedAtSlot(slot));

                if (!continueSearch) {
                    if (newLeaf) {
                        // Since we have moved away from the original leaf,
                        // we need some logic to make sure we don't hold
                        // latches that we're not supposed to hold.
                        if (ret == RESCAN_REQUIRED) {
                            // When a rescan is required, we must release the
                            // original leaf, since the callers expect all
                            // latches to have been released (and so they
                            // should have been, so this is probably a bug -
                            // see DERBY-4080).
                            originalLeaf.release();
                        }
                        if (ret != RESCAN_REQUIRED) {
                            // Since a rescan is not required, we still hold
                            // the latch on the non-original leaf. No other
                            // leaves than the original one should be latched
                            // when we return, so release the current leaf.
                            leaf.release();
                        }
                    }
                    return ret;
                }
            }
            slot++;
        }
        return NO_MATCH;
    }
    
    /**
     * Compares two rows for insert. If the two rows are not equal,
     * {@link #NO_MATCH} is returned. Otherwise, it tries to get a lock on
     * the row in the tree. If the lock is obtained without waiting,
     * {@link #MATCH_FOUND} is returned (even if the row has been deleted).
     * Otherwise, {@link #RESCAN_REQUIRED} is returned to indicate that the
     * latches have been released and the B-tree must be rescanned.
     *
     * If {@code MATCH_FOUND} is returned, the caller should check whether
     * the row has been deleted. If so, it may have to move to check the
     * adjacent rows to be sure that there is no non-deleted duplicate row.
     *
     * If {@code MATCH_FOUND} or {@code RESCAN_REQUIRED} is returned, the
     * transaction will hold an update lock on the specified record when
     * the method returns.
     *
     * <b>Note!</b> This method should only be called when the index is almost
     * unique (that is, a non-unique index backing a unique constraint).
     *
     * @param originalRow row from the tree
     * @param newRow row to be inserted
     * @param leaf leaf where originalRow resides
     * @param slot slot where originalRow
     * @return  {@code NO_MATCH} if no duplicate is found,
     *          {@code MATCH_FOUND} if a duplicate is found, or
     *          {@code RESCAN_REQUIRED} if the B-tree must be rescanned
     */
    private int compareRowsForInsert (DataValueDescriptor [] originalRow,
                                      DataValueDescriptor [] newRow,
                                      LeafControlRow leaf, int slot) 
                                            throws StandardException {
        for (int i = 0; i < originalRow.length - 1; i++) {
            if (!originalRow [i].equals(newRow [i]))
                return NO_MATCH;
        }
        //It might be a deleted record try getting a lock on it
        DataValueDescriptor[] template = runtime_mem.get_template(getRawTran());
        FetchDescriptor lock_fetch_desc = RowUtil.getFetchDescriptorConstant(
                                                    template.length - 1);
        RowLocation lock_row_loc = 
            (RowLocation) scratch_template[scratch_template.length - 1];
        boolean latch_released = !getLockingPolicy().lockNonScanRowOnPage(
//IC see: https://issues.apache.org/jira/browse/DERBY-6041
                leaf, slot, lock_fetch_desc, template,
                lock_row_loc, ConglomerateController.LOCK_UPD);
        //if latch was released some other transaction was operating on this
        //record and might have changed the tree by now
        if (latch_released)
            return RESCAN_REQUIRED;

        return MATCH_FOUND;
    }
    
    /**
     * Compares immidiate left and right records to check for duplicates.
     * This methods compares new record (being inserted) with the record 
     * in immidate left and right postion to see if its duplicate (only for
     * almost unique index and for non null keys)
     * @param rowToInsert row being inserted
     * @param insert_slot slot where rowToInsert is being inserted
     * @param targetleaf page where rowToInsert
     * @return  0 if no duplicate
     *          1 if duplicate 
     *          2 if rescan required
     * @throws StandardException
     */
    private int compareLeftAndRightSiblings (
                            DataValueDescriptor[] rowToInsert, 
                            int insert_slot, 
                            LeafControlRow  targetleaf) throws StandardException {
        //proceed only if almost unique index
        if (this.getConglomerate().isUniqueWithDuplicateNulls()) {
            int keyParts = rowToInsert.length - 1;
            boolean hasnull = false;
            for (int i = 0; i < keyParts; i++) {
                //keys with null in it are unique
                //no need to compare
                if (rowToInsert [i].isNull()) {
                    return NO_MATCH;
                }
            }
            if (!hasnull) {
                DataValueDescriptor index [] =  
                        runtime_mem.get_template(getRawTran());
                int ret = comparePreviousRecord(insert_slot - 1, 
                        targetleaf, index, rowToInsert);
                if (ret > 0) {
                    return ret;                        
                }
                return compareNextRecord(insert_slot, targetleaf, index, rowToInsert);
            }
        }
        return NO_MATCH;
    }

	/**
    Insert a row into the conglomerate.

    @param rowToInsert The row to insert into the conglomerate.  The stored
	representations of the row's columns are copied into a new row
	somewhere in the conglomerate.

	@return Returns 0 if insert succeeded.  Returns 
    ConglomerateController.ROWISDUPLICATE if conglomerate supports uniqueness
    checks and has been created to disallow duplicates, and the row inserted
    had key columns which were duplicate of a row already in the table.  Other
    insert failures will raise StandardException's.

	@exception StandardException Standard exception policy.
    **/
	private int doIns(DataValueDescriptor[] rowToInsert)
        throws StandardException
	{
		LeafControlRow  targetleaf                      = null;
		LeafControlRow  save_targetleaf                 = null;
        int             insert_slot                     = 0;
        int             result_slot                     = 0;
        int             ret_val                         = 0;
        boolean         reclaim_deleted_rows_attempted  = false;

        if (scratch_template == null)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
            scratch_template = runtime_mem.get_template(getRawTran());
        }

        if (SanityManager.DEBUG)
            this.isIndexableRowConsistent(rowToInsert);

        // Create the objects needed for the insert.
        // RESOLVE (mikem) - should we cache this in the controller?
        SearchParameters sp = 
            new SearchParameters(
                rowToInsert,
                SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH,
                scratch_template, this, false);

        // RowLocation column is in last column of template.
        FetchDescriptor lock_fetch_desc = 
            RowUtil.getFetchDescriptorConstant(
                scratch_template.length - 1);
        RowLocation lock_row_loc = 
            (RowLocation) scratch_template[scratch_template.length - 1];

        // Row locking - lock the row being inserted.

        if (get_insert_row_lock)
        {
            // I don't hold any latch yet so I can wait on this lock, so I
            // don't care about return value from this call.  This
            // lock can only wait if the base table row was inserted in a
            // separate transaction which never happens in sql tables, but
            // does happen in the sparse indexes that synchronization builds.
        
            this.getLockingPolicy().lockNonScanRow(
                this.getConglomerate(),
                (LeafControlRow) null,
                (LeafControlRow) null,
                rowToInsert, 
                (ConglomerateController.LOCK_INS | 
                 ConglomerateController.LOCK_UPD));
        }

        while (true)
        {
            // Search the location at which the new row should be inserted.
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(this.container != null);

            targetleaf = (LeafControlRow)
                ControlRow.get(this, BTree.ROOTPAGEID).search(sp);

//IC see: https://issues.apache.org/jira/browse/DERBY-2359

            // Row locking - first lock row previous to row being inserted:
            //     o if (sp.resultExact) then the row must be deleted and
            //           we will be replacing it with the new row, lock
            //           the row before the slot as the previous key.
            //     o else 
            //           we will be inserting after the current slot so
            //           lock the current slot as the previous key.
            //
            int slot_after_previous = 
                (sp.resultExact ? sp.resultSlot : sp.resultSlot + 1);

            boolean latch_released = false;

            latch_released = 
                !this.getLockingPolicy().lockNonScanPreviousRow(
                    targetleaf, 
                    slot_after_previous, 
                    lock_fetch_desc,
                    scratch_template,
                    lock_row_loc,
                    this, 
                    (ConglomerateController.LOCK_INS_PREVKEY |
                     ConglomerateController.LOCK_UPD),
                    TransactionManager.LOCK_INSTANT_DURATION);

            // special test to see if latch release code works
            if (SanityManager.DEBUG)
            {
                latch_released = 
                    test_errors(
                        this,
                        "BTreeController_doIns", null,
                        this.getLockingPolicy(), 
                        targetleaf, latch_released);
            }

            if (latch_released)
            {
                // Had to release latch in order to get the lock, probably 
                // because of a forward scanner, research tree, and try again.
                targetleaf = null;
                continue;
            }

            // If the row is there already, simply undelete it.
            // The rationale for this is, since the index does
            // not support duplicates, the only way we could
            // find a duplicate is if we found a deleted row.
            // If we could lock it, then no other transaction
            // is deleting it; either this transaction deleted
            // it earlier, or it's simply a row that the space
            // reclaimer hasn't reclaimed yet.
            // Since inserts are done directly (i.e., not to a
            // location provided by a scan, we will see the 
            // deleted row).
            if (sp.resultExact)
            {
                result_slot = insert_slot = sp.resultSlot;

                if (this.getConglomerate().nKeyFields != 
                        this.getConglomerate().nUniqueColumns)
                {
                    // The key fields match, but not the row location.  We
                    // must wait on the lock on the other row location before
                    // preceding, so as to serialize behind any work being done
                    // to the row as part of another transaction.

                    latch_released = 
                        !this.getLockingPolicy().lockNonScanRowOnPage(
//IC see: https://issues.apache.org/jira/browse/DERBY-6041
                            targetleaf, insert_slot,
                            lock_fetch_desc, scratch_template, lock_row_loc,
                            ConglomerateController.LOCK_UPD);

                    if (latch_released)
                    {
                        // Had to release latch in order to get the lock, 
                        // probably to wait for deleting xact to commit or 
                        // abort.  Research tree, and try again.
                        targetleaf = null;
                        continue;
                    }
                }

                // The row better be deleted, or something is very wrong.

                if (!(targetleaf.page.isDeletedAtSlot(insert_slot)))
                {
                    // attempt to insert a duplicate into the index.
                    ret_val = ConglomerateController.ROWISDUPLICATE;
                    break;
                }
                else
                {
                    if (this.getConglomerate().nKeyFields == 
                        this.getConglomerate().nUniqueColumns)
                    {
                        // The row that we found deleted is exactly the new row.
                        targetleaf.page.deleteAtSlot(
                            insert_slot, false, this.btree_undo);

                        break;
                    }
                    else if (this.getConglomerate().nUniqueColumns == 
                             (this.getConglomerate().nKeyFields - 1))
                    {
                        // The row that we found deleted has matching keys
                        // which form the unique key fields,
                        // but the nonkey fields may differ (for now the
                        // heap rowlocation is the only nonkey field 
                        // allowed).
                        
                        // RESOLVE BT39 (mikem) - when/if heap row location
                        // is not fixed we must handle update failing for
                        // out of space and split if it does.  For now
                        // if the update fails because of lack of space
                        // an exception is thrown and the statement is 
                        // backed out.  Should not happen very often.
                        targetleaf.page.deleteAtSlot(
                            insert_slot, false, this.btree_undo);

                        boolean update_succeeded = true;
                        try 
                        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5367
                            if (runtime_mem.hasCollatedTypes())
                            {
                                // See DERBY-5367.
                                // There are types in the BTree with a 
                                // collation different than UCS BASIC, we
                                // update all fields to make sure they hold
                                // the correct values.
                                // NOTE: We could optimize here by only
                                // updating the fields that actually hold
                                // collated types.
                                int rowsToUpdate = getConglomerate().nKeyFields;
                                for (int i=0; i < rowsToUpdate; i++) {
                                targetleaf.page.updateFieldAtSlot(
                                    insert_slot, i, 
                                    (DataValueDescriptor) RowUtil.getColumn(
                                        rowToInsert, 
                                        (FormatableBitSet) null, i),
                                    this.btree_undo);
                                }
                            }
                            else
                            {
                                // There are no collated types in the BTree,
                                // which means that the values currently
                                // stored in the undeleted row are correct.
                                // We simply update the row location to point
                                // to the correct row in the heap.
                                int rowloc_index =
                                        this.getConglomerate().nKeyFields - 1;
                                targetleaf.page.updateFieldAtSlot(
                                    insert_slot, rowloc_index, 
                                    (DataValueDescriptor) RowUtil.getColumn(
                                        rowToInsert, 
                                        (FormatableBitSet) null, rowloc_index),
                                    this.btree_undo);
                            }
                        }
                        catch (StandardException se)
                        {
                            // check if the exception is for out of space
                            if (!se.getMessageId().equals(SQLState.DATA_NO_SPACE_FOR_RECORD))
                            {
                                throw se;
                            }

                            // The statement exception is
                            // because the update failed for out of
                            // space (ie. the field got longer and there
                            // is no room on the page for the expanded
                            // field).  Address this error by falling
                            // through the code and doing a split.
                            update_succeeded = false;                          // update failed.
                            targetleaf.page.deleteAtSlot(
                                insert_slot, true, this.btree_undo);
                        }

                        if (update_succeeded)
                            break;
                    }
                    else
                    {
                        // Can only happen with non key fields in the btree.
                        throw(
                            StandardException.newException(
                                SQLState.BTREE_UNIMPLEMENTED_FEATURE));
                    }
                }
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-5604
            else if (targetleaf.page.recordCount() - 1 < BTree.maxRowsPerPage)
            {
                // The row wasn't there, so try to insert it
                // on the page returned by the search.
                insert_slot = sp.resultSlot + 1;
                result_slot = insert_slot + 1;
                if (getConglomerate().isUniqueWithDuplicateNulls()) 
                {
                    int ret = compareLeftAndRightSiblings(rowToInsert, 
                            insert_slot, targetleaf);
                    if (ret == MATCH_FOUND) 
                    {
                        ret_val = ConglomerateController.ROWISDUPLICATE;
                        break;
                    }
                    if (ret == RESCAN_REQUIRED)
                        continue;
                }
                // By default maxRowsPerPage is set to MAXINT, some tests
                // set it small to cause splitting to happen quicker with
                // less data.

                if (targetleaf.page.insertAtSlot(
                        insert_slot, 
                        rowToInsert, (FormatableBitSet) null,
                        this.btree_undo,
                        Page.INSERT_DEFAULT,
						AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD) != null)
                {
                    // Insert succeeded, so we're done.

                    break;
                }

                // RESOLVE (mikem) - another long row issue.
                // For now if a row does not fit on a page and there 
                // is only the control row on the page and at most one
                // other row on the page, throw an exception

                if (targetleaf.page.recordCount() <= 2)
                {
                    throw StandardException.newException(
                            SQLState.BTREE_NO_SPACE_FOR_KEY);
                }

                // start splitting ...
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
            if (getConglomerate().isUniqueWithDuplicateNulls()) 
            {
                int ret = compareLeftAndRightSiblings(rowToInsert, 
                        insert_slot, targetleaf);
                if (ret == MATCH_FOUND) 
                {
                    ret_val = ConglomerateController.ROWISDUPLICATE;
                    break;
                }
                if (ret == RESCAN_REQUIRED)
                    continue;
            }
            
            // Create some space by splitting pages.

            // determine where in page/table row causing split would go
            int flag = 0;
            if (insert_slot == 1)
            {
                flag |= ControlRow.SPLIT_FLAG_FIRST_ON_PAGE;
                if (targetleaf.isLeftmostLeaf())
                    flag |= ControlRow.SPLIT_FLAG_FIRST_IN_TABLE;
            }
            else if (insert_slot == targetleaf.page.recordCount())
            {
                flag |= ControlRow.SPLIT_FLAG_LAST_ON_PAGE;
                if (targetleaf.isRightmostLeaf())
                    flag |= ControlRow.SPLIT_FLAG_LAST_IN_TABLE;
            }

            long targetleaf_pageno = targetleaf.page.getPageNumber();

            if ((targetleaf.page.recordCount() - 
                 targetleaf.page.nonDeletedRecordCount()) <= 0)
            {
                // Don't do reclaim work if there are no deleted records.
                reclaim_deleted_rows_attempted = true;
            }

            BranchRow branchrow = 
                BranchRow.createBranchRowFromOldLeafRow(
                    rowToInsert, targetleaf_pageno);

            // Release the target page because (a) it may change as a 
            // result of the split, (b) the latch ordering requires us 
            // to acquire latches from top to bottom, and (c) this 
            // loop should be done in a system transaction.
            targetleaf.release();
            targetleaf = null;

            start_xact_and_dosplit(
                !reclaim_deleted_rows_attempted, targetleaf_pageno, 
                scratch_template, branchrow.getRow(), flag);

            // only attempt to reclaim deleted rows once, otherwise the
            // split loop could loop forever, trying to reclaim a deleted
            // row that was not committed.
            reclaim_deleted_rows_attempted = true;

            // RESOLVE (mikem) possible optimization could be to save
            // split location and look there first, if this has 
            // already caused a split.  Or even return a latched page
            // from splitFor().  For now just execute the loop again
            // searching the tree for somewhere to put the row.
        }

        // set in-memory hint of where last row on page was inserted.
        targetleaf.last_search_result = result_slot;

        // Check that page just updated is consistent.
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
            {
                targetleaf.checkConsistency(this, null, true);
            }
        }

        // Done with the target page.
        targetleaf.release();
        targetleaf = null;

        // return the status about insert - 0 is ok, or duplicate status.
        return(ret_val);
	}

    /**
     * Just insert the row on the current page/slot if it fits.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
	private boolean do_load_insert(
    DataValueDescriptor[]   rowToInsert,
    LeafControlRow          leaf,
    int                     insert_slot)
        throws StandardException
	{
		LeafControlRow old_leaf         = null;
        boolean        row_inserted     = false;
        int            num_rows_on_page = leaf.page.recordCount() - 1;


        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(insert_slot == leaf.page.recordCount());
            SanityManager.ASSERT(
                leaf.getrightSiblingPageNumber() == 
                    ContainerHandle.INVALID_PAGE_NUMBER);
            this.isIndexableRowConsistent(rowToInsert);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5604
        if (num_rows_on_page < BTree.maxRowsPerPage)
        {
            // By default maxRowsPerPage is set to MAXINT, some tests
            // set it small to cause splitting to happen quicker with
            // less data.

            if (SanityManager.DEBUG)
            {
                // Caller should have sorted and done duplicate checking.

                if (insert_slot > 1)
                {
                    // verify that the row inserted is >= than previous row.
                    int compare_result =
//IC see: https://issues.apache.org/jira/browse/DERBY-2359
                        ControlRow.compareIndexRowFromPageToKey(
                            leaf,
                            insert_slot - 1,
                            scratch_template,
                            rowToInsert,
                            this.getConglomerate().nUniqueColumns,
                            0,
							this.getConglomerate().ascDescInfo);
                    
                    if (compare_result >= 0)
                    {
                        // Rows must be presented in order, so the row we are
                        // inserting must always be greater than the previous 
                        // row on the page.
                        SanityManager.THROWASSERT("result = " + compare_result);
                    }
                }
            }


            if (leaf.page.insertAtSlot(
                    insert_slot, 
                    rowToInsert, 
                    (FormatableBitSet) null, 
                    this.btree_undo,
                    Page.INSERT_DEFAULT,
					AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD) != null)
            {
                // Insert succeeded, so we're done.
                row_inserted = true;
            }
            else
            {
                // RESOLVE (mikem) - another long row issue.
                // For now if a row does not fit on a page and there 
                // is only the control row on the page and at most one
                // other row on the page, throw an exception

                if (leaf.page.recordCount() <= 2)
                {
                    throw StandardException.newException(
                            SQLState.BTREE_NO_SPACE_FOR_KEY);
                }
            }
        }

        // Check that page just updated is consistent.
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
            {
                leaf.checkConsistency(this, null, true);
            }
        }

        return(row_inserted);
	}

    /**
     * Create room to insert a row to the right of the largest key in table.
     * <p>
     * Perform a split pass on the tree which will move the largest key in
     * leaf right to a new leaf, splitting parent branch pages as necessary.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private LeafControlRow do_load_split(
    DataValueDescriptor[]   rowToInsert,
    LeafControlRow          leaf)
        throws StandardException
	{
		LeafControlRow new_leaf = null;

        BranchRow branchrow = 
            BranchRow.createBranchRowFromOldLeafRow(
                rowToInsert, leaf.page.getPageNumber());

        // Release the target page because (a) it may change as a 
        // result of the split, (b) the latch ordering requires us 
        // to acquire latches from top to bottom, and (c) this 
        // loop should be done in a system transaction.
        long old_leafpage = leaf.page.getPageNumber();

        leaf.release();
        leaf = null;
        
        long new_leaf_pageno = 
            start_xact_and_dosplit(
                false /* don't try to reclaim deleted rows */,
                old_leafpage,
                scratch_template, 
                branchrow.getRow(), 
                (ControlRow.SPLIT_FLAG_LAST_ON_PAGE | 
                    ControlRow.SPLIT_FLAG_LAST_IN_TABLE));

        new_leaf = (LeafControlRow) ControlRow.get(this, new_leaf_pageno);
//IC see: https://issues.apache.org/jira/browse/DERBY-2359

        // The leaf must be the rightmost leaf in the table, the first time
        // the root grows from leaf to branch it will be a leaf with many
        // rows which will probably have to be split soon, after that it will
        // be a leaf with only one row.  The current algorithm requires that
        // there be at least one row for duplicate checking (the duplicate
        // checking code does not handle going left to the previous leaf) - 
        // this is the way the split at rightmost leaf row works currently.
        if (SanityManager.DEBUG)
        {
            if (new_leaf.getrightSiblingPageNumber() != 
                    ContainerHandle.INVALID_PAGE_NUMBER)
            {
                SanityManager.THROWASSERT(
                    "new_leaf.getrightSiblingPageNumber() = " + 
                        new_leaf.getrightSiblingPageNumber());
            }
            if (new_leaf.page.recordCount() <= 1)
            {
                SanityManager.THROWASSERT(
                    "new_leaf.page.recordCount() = " + 
                    new_leaf.page.recordCount());
            }
        }

        return(new_leaf);
	}



	/*
	** public Methods of BTreeController
	*/

	/**
	Initialize the controller for use.
	<p>
	Any changes to this method will probably have to be reflected in close as 
    well.
	<p>
	Currently delegates to OpenBTree.  If the btree controller ends up not 
    having any state of its own, we can remove this method (the VM will 
    dispatch to OpenBTree), gaining some small efficiency.  For now, this 
    method remains for clarity.  

    @exception StandardException Standard exception policy.
	**/
	public void init(
    TransactionManager              xact_manager,
    boolean                         hold,
    ContainerHandle                 container,
    Transaction                     rawtran, 
	int					            open_mode,
    int                             lock_level,
    BTreeLockingPolicy              btree_locking_policy,
    BTree                           conglomerate,
    LogicalUndo                     undo,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
        get_insert_row_lock = 
            ((open_mode & 
              TransactionController.OPENMODE_BASEROW_INSERT_LOCKED) == 0);

		super.init(
            xact_manager, xact_manager, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1058
            container, rawtran, hold, open_mode,
            lock_level, btree_locking_policy,
            conglomerate, undo, dynamic_info);
	}

	/*
	** Methods of ConglomerateController
	*/

    /**
    Close the conglomerate controller.
	<p>
	Any changes to this method will probably have to be reflected in close as 
    well.
	<p>
	Currently delegates to OpenBTree.  If the btree controller ends up not 
    having any state of its own, we can remove this method (the VM will 
    dispatch to OpenBTree), gaining some small efficiency.  For now, this 
    method remains for clarity.  

	@see ConglomerateController#close
    **/
    public void close()
        throws StandardException
	{
		super.close();

		// If we are closed due to catching an error in the middle of init,
		// xact_manager may not be set yet. 
		if (getXactMgr() != null)
			getXactMgr().closeMe(this);
	}

    /**
     * Close conglomerate controller as part of terminating a transaction.
     * <p>
     * Use this call to close the conglomerate controller resources as part of
     * committing or aborting a transaction.  The normal close() routine may 
     * do some cleanup that is either unnecessary, or not correct due to the 
     * unknown condition of the controller following a transaction ending error.
     * Use this call when closing all controllers as part of an abort of a 
     * transaction.
     * <p>
     * This call is meant to only be used internally by the Storage system,
     * clients of the storage system should use the simple close() interface.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
     * @param closeHeldScan     If true, means to close controller even if
     *                          it has been opened to be kept opened 
     *                          across commit.  This is
     *                          used to close these controllers on abort.
     *
	 * @return boolean indicating that the close has resulted in a real close
     *                 of the controller.  A held scan will return false if 
     *                 called by closeForEndTransaction(false), otherwise it 
     *                 will return true.  A non-held scan will always return 
     *                 true.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
		throws StandardException
    {
        super.close();

        if ((!getHold()) || closeHeldScan) 
        {
            // If we are closed due to catching an error in the middle of init,
            // xact_manager may not be set yet. 
            if (getXactMgr() != null)
                getXactMgr().closeMe(this);

            return(true);
        }
        else
        {
            return(false);
        }
    }

	/**
    Insert a row into the conglomerate.
	@see ConglomerateController#insert

    @param row The row to insert into the conglomerate.  The stored
	representations of the row's columns are copied into a new row
	somewhere in the conglomerate.

	@return Returns 0 if insert succeeded.  Returns 
    ConglomerateController.ROWISDUPLICATE if conglomerate supports uniqueness
    checks and has been created to disallow duplicates, and the row inserted
    had key columns which were duplicate of a row already in the table.  Other
    insert failures will raise StandardException's.

	@exception StandardException Standard exception policy.
    **/
	public int insert(DataValueDescriptor[] row) 
         throws StandardException
    {

//IC see: https://issues.apache.org/jira/browse/DERBY-132
		if (isClosed())
        {
            if (getHold())
            {
                reopen();
            }
            else
            {
                throw StandardException.newException(
                            SQLState.BTREE_IS_CLOSED,
                            err_containerid);
            } 
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null);
        }

		return doIns(row);
	}

    /**
	Return whether this is a keyed conglomerate.
	<p>
	All b-trees are keyed.
	@see ConglomerateController#isKeyed
	**/
	public boolean isKeyed()
	{
		return(true);
	}

    /**
     * Request the system properties associated with a table. 
     * <p>
     * Request the value of properties that are associated with a table.  The
     * following properties can be requested:
     *     derby.storage.pageSize 
     *     derby.storage.pageReservedSpace
     *     derby.storage.minimumRecordSize
     *     derby.storage.initialPages
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     cc.getTableProperties(prop);
     *
     *     System.out.println(
     *         "table's page size = " + 
     *         prop.getProperty("derby.storage.pageSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void getTableProperties(Properties prop)
		throws StandardException
    {
		if (this.container == null)
        {
            throw StandardException.newException(
                        SQLState.BTREE_IS_CLOSED,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                        err_containerid);
        }

        container.getContainerProperties(prop);

        return;
    }

    /**
     * Request set of properties associated with a table. 
     * <p>
     * Returns a property object containing all properties that the store
     * knows about, which are stored persistently by the store.  This set
     * of properties may vary from implementation to implementation of the
     * store.
     * <p>
     * This call is meant to be used only for internal query of the properties
     * by jbms, for instance by language during bulk insert so that it can
     * create a new conglomerate which exactly matches the properties that
     * the original container was created with.  This call should not be used
     * by the user interface to present properties to users as it may contain
     * properties that are meant to be internal to jbms.  Some properties are 
     * meant only to be specified by jbms code and not by users on the command
     * line.
     * <p>
     * Note that not all properties passed into createConglomerate() are stored
     * persistently, and that set may vary by store implementation.
     *
     * @param prop   Property list to add properties to.  If null, routine will
     *               create a new Properties object, fill it in and return it.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Properties getInternalTablePropertySet(Properties prop)
		throws StandardException
    {
        Properties  ret_properties = 
            ConglomerateUtil.createRawStorePropertySet(prop);

        getTableProperties(ret_properties);

        return(ret_properties);
    }

    /**
     * Load rows from rowSource into the opened btree.
     * <p>
     * Efficiently load rows into the already opened btree.  The btree must
     * be table locked, as no row locks will be requested by this routine.  
     * On exit from this routine the conglomerate will be closed (on both
     * error or success).
     * <p>
     * This routine does an almost bottom up build of a btree.  It assumes
     * all rows arrive in sorted order, and inserts them directly into the
     * next (to the right) spot in the current leaf until there is no space.
     * Then it calls the generic split code to add the next leaf (RESOLVE - 
     * in the future we could optimize this to split bottom up rather than
     * top down for create index).
     *
     * @exception StandardException Standard exception policy.  If conglomerate
	 *                              supports uniqueness checks and has been 
     *                              created to disallow duplicates, and one of 
     *                              the rows being loaded had key columns which
     *                              were duplicate of a row already in the 
     *                              conglomerate, then raise 
     *                              SQLState.STORE_CONGLOMERATE_DUPLICATE_KEY_EXCEPTION.
     *
	 * @see org.apache.derby.iapi.store.access.conglomerate.Conglomerate#load
     **/
	public long load(
    TransactionManager      xact_manager,
    boolean                 createConglom,
    RowLocationRetRowSource rowSource)
		 throws StandardException
	{
        long num_rows_loaded = 0;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(createConglom,
				"Cannot load a btree incrementally - it must either be entirely logged, or entirely not logged.  Doesn't make sense to log only the allocation when one cannot guarantee to not touch any pre-existing pages");
		}

        if (scratch_template == null)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
            scratch_template = runtime_mem.get_template(getRawTran());
        }

        LeafControlRow current_leaf = null;

        try 
        {
            // Btree must just have been created and empty, so there must
            // be one root leaf page which is empty except for the control row.
            current_leaf = 
//IC see: https://issues.apache.org/jira/browse/DERBY-2359
                (LeafControlRow) ControlRow.get(this, BTree.ROOTPAGEID);
            int current_insert_slot = 1;

            if (SanityManager.DEBUG)
            {
                // root must be empty except for the control row.
                SanityManager.ASSERT(current_leaf.page.recordCount() == 1);
            }
           
            // now loop thru the row source and insert into the btree
            FormatableBitSet  validColumns = rowSource.getValidColumns();
            
			// get the next row and its valid columns from the rowSource
			DataValueDescriptor[] row;
            while ((row = rowSource.getNextRowFromRowSource()) != null)
            {
                num_rows_loaded++;

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        validColumns == null, "Does not support partial row");
                }

                while (true)
                {
                    if (do_load_insert(row, current_leaf, current_insert_slot))
                    {
                        // row inserted successfully.
                        break;
                    }
                    else
                    {
                        // if insert fails, do a split pass. There is an edge
                        // case where multiple split passes are necessary if
                        // branch splits are necessary, thus the loop.  It is
                        // most likely that only a single split pass will be
                        // necessary.
                        current_leaf = do_load_split(row, current_leaf);

                        current_insert_slot = current_leaf.page.recordCount();
                    }
                }
                current_insert_slot++;
            }

            current_leaf.release();
            current_leaf = null;

            // Loading done, must flush all pages to disk since it is unlogged.
            if (!this.getConglomerate().isTemporary())
                container.flushContainer();
        }
        finally
        {
            this.close();
        }

        return(num_rows_loaded);
	}

	/*
	** Methods of ConglomerateController which are not supported.
	*/

    /**
    Delete a row from the conglomerate.  
	@see ConglomerateController#delete

    @exception StandardException Standard exception policy.
    **/
    public boolean delete(RowLocation loc)
		throws StandardException
	{
        throw(StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE));
	}

    /**
    Fetch the row at the given location.
	@see ConglomerateController#fetch

    @exception StandardException Standard exception policy.
    **/
    public boolean fetch(
    RowLocation loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns) 
		throws StandardException
	{
        throw(StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE));
	}

    /**
    Fetch the row at the given location.
	@see ConglomerateController#fetch

    @exception StandardException Standard exception policy.
    **/
    public boolean fetch(
    RowLocation             loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns,
    boolean                 waitForLock) 
		throws StandardException
	{
        throw(StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE));
	}

	/**
	Insert a row into the conglomerate, and store its location in the
	provided template row location.

    Unimplemented by btree.

	@see ConglomerateController#insertAndFetchLocation

    @exception StandardException Standard exception policy.
	**/
	public void insertAndFetchLocation(
    DataValueDescriptor[]	row,
    RowLocation             templateRowLocation)
        throws StandardException
	{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}

	/**
	Return a row location object of the correct type to be
	used in calls to insertAndFetchLocation.

	@see ConglomerateController#newRowLocationTemplate

    @exception StandardException Standard exception policy.
	**/
	public RowLocation newRowLocationTemplate()
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}

    /**
     * Lock the given row location.
     * <p>
     * Should only be called by access.
     * <p>
     * This call can be made on a ConglomerateController that was opened
     * for locking only.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
	 * @return true if lock was granted, only can be false if wait was false.
     *
	 * @param loc    The "RowLocation" which describes the exact row to lock.
     * @param wait   Should the lock call wait to be granted?
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockRow(
    RowLocation loc,
    int         lock_operation,
    boolean     wait,
    int         lock_duration)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

    public boolean lockRow(
    long        page_num,
    int         record_id,
    int         lock_operation,
    boolean     wait,
    int         lock_duration)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

    public void unlockRowAfterRead(
    RowLocation     loc,
    boolean         forUpdate,
    boolean         row_qualifies)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

	/**
    Replace the entire row at the given location.  
	@see ConglomerateController#replace

    @exception StandardException Standard exception policy.
    **/
    public boolean replace(
    RowLocation             loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}
}
