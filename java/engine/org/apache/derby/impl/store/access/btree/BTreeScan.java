/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeScan

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.GenericScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ScanInfo;
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

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.impl.store.access.conglomerate.TemplateRow;

import org.apache.derby.iapi.services.io.FormatableBitSet;
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

public abstract class BTreeScan extends OpenBTree implements ScanManager
{

	/*
	** Fields of BTreeScan
	*/

    /**
     * init_startKeyValue, init_qualifier, and init_stopKeyValue all are used 
     * to store * references to the values passed in when ScanController.init()
     * is called.  It is assumed that these are not altered by the client
     * while the scan is active.
     */
    protected Transaction           init_rawtran             = null;
    protected boolean               init_forUpdate;
    protected FormatableBitSet               init_scanColumnList;
    protected DataValueDescriptor[] init_template;
    protected DataValueDescriptor[] init_startKeyValue;
    protected int                   init_startSearchOperator = 0;
    protected Qualifier             init_qualifier[][]       = null;
    protected DataValueDescriptor[] init_stopKeyValue;
    protected int                   init_stopSearchOperator  = 0;
    protected boolean               init_hold;


    /**
     * The fetch descriptor which describes the row to be returned by the scan.
     **/
    protected FetchDescriptor       init_fetchDesc;


    /**
     * A constant FetchDescriptor which describes the position of the 
     * RowLocation field within the btree (ie. the last column).  Used by 
     * lock/unlock to fetch the RowLocation.  Only needs to be allocated once
     * per scan.
     **/
    protected FetchDescriptor       init_lock_fetch_desc;


    BTreeRowPosition                scan_position;


     /**
      * Whether the scan should requests UPDATE locks which then will be 
      * converted to X locks when the actual operation is performed.
     **/
     protected boolean init_useUpdateLocks = false;

    /*
     * There are 5 states a scan can be in.
     *     SCAN_INIT - A scan has started but no positioning has been done.
     *                 The scan will be positioned when the first next() call
     *                 has been made.  None of the positioning state variables
     *                 are valid in this state.
     *     SCAN_INPROGRESS -
     *                 A scan is in this state after the first next() call.
     *                 On exit from any BTreeScan method, while in this state,
     *                 the scan "points" at a row which qualifies for the 
     *                 scan.  While not maintaining latches on a page the 
     *                 current position of the scan is either kept by record
     *                 handle or key.  To tell which use the following:
     *                 if (record key == null)
     *                    record handle has current position
     *                 else
     *                    record key has current position
     *
     *     SCAN_DONE - Once the end of the table or the stop condition is met
     *                 then the scan is placed in this state.  Only valid 
     *                 ScanController method at this point is close().
     *
     *     SCAN_HOLD_INIT -
     *                 The scan has been opened and held open across a commit,
     *                 at the last commit the state was SCAN_INIT.
     *                 The scan has never progressed from the SCAN_INIT state
     *                 during a transaction.  When a next is done the state
     *                 will either progress to SCAN_INPROGRESS or SCAN_DONE.
     *
     *     SCAN_HOLD_INPROGRESS -
     *                 The scan has been opened and held open across a commit,
     *                 at the last commit the state was in SCAN_INPROGRESS.
     *                 The transaction which opened the scan has committed,
     *                 but the scan was opened with the "hold" option true.
     *                 At commit the locks were released and the "current"
     *                 position is remembered.  In this state only two calls
     *                 are valid, either next() or close().  When next() is
     *                 called the scan is reopened, the underlying container
     *                 is opened thus associating all new locks with the current
     *                 transaction, and the scan continues at the "next" row.
     */
    protected static final int    SCAN_INIT             = 1;
    protected static final int    SCAN_INPROGRESS       = 2;
    protected static final int    SCAN_DONE             = 3;
    protected static final int    SCAN_HOLD_INIT        = 4;
    protected static final int    SCAN_HOLD_INPROGRESS  = 5;

    /**
     * Delay positioning the  table at the start position until the first
     * next() call.  The initial position is done in positionAtStartPosition().
     */
    protected int         scan_state      = SCAN_INIT;

    /**
     * Performance counters ...
     */
    protected int stat_numpages_visited         = 0;
    protected int stat_numrows_visited          = 0;
    protected int stat_numrows_qualified        = 0;
    protected int stat_numdeleted_rows_visited  = 0;

    /**
	 * What kind of row locks to get during the scan.
     **/
    protected int lock_operation;


    /**
     * A 1 element array to turn fetchNext and fetch calls into 
     * fetchNextGroup calls.
     **/
    protected DataValueDescriptor[][] fetchNext_one_slot_array = 
                                            new DataValueDescriptor[1][];

    /* Constructors for This class: */

	public BTreeScan()
	{
	}

	/*
	** Private/Protected methods of This class, sorted alphabetically
	*/

    /**
     * Fetch the next N rows from the table.
     * <p>
     * Utility routine used by both fetchSet() and fetchNextGroup().
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract protected int fetchRows(
    BTreeRowPosition        pos,
    DataValueDescriptor[][] row_array,
    RowLocation[]           rowloc_array,
    BackingStoreHashtable   hash_table,
    long                    max_rowcnt,
    int[]                   key_column_numbers)
        throws StandardException;


    /**
     * Shared initialization code between init() and reopenScan().
     * <p>
     * Basically save away input parameters describing qualifications for
     * the scan, and do some error checking.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void initScanParams(
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    Qualifier               qualifier[][],
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator)
        throws StandardException
	{
        // startKeyValue init.
	    this.init_startKeyValue         = startKeyValue;
		if (RowUtil.isRowEmpty(this.init_startKeyValue, (FormatableBitSet) null))
			this.init_startKeyValue = null;

        // startSearchOperator init.
	    this.init_startSearchOperator   = startSearchOperator;

        // qualifier init.
        if ((qualifier != null) && (qualifier .length == 0))
            qualifier = null;
        this.init_qualifier             = qualifier;

        // stopKeyValue init.
	    this.init_stopKeyValue          = stopKeyValue;
        if (RowUtil.isRowEmpty(this.init_stopKeyValue, (FormatableBitSet) null))
            this.init_stopKeyValue = null;

        // stopSearchOperator init.
	    this.init_stopSearchOperator    = stopSearchOperator;

        // reset the "current" position to starting condition.
        // RESOLVE (mmm) - "compile" this.
        scan_position = new BTreeRowPosition();

        scan_position.init();

        scan_position.current_lock_template = 
            new DataValueDescriptor[this.init_template.length];

        scan_position.current_lock_template[this.init_template.length - 1] = 
            scan_position.current_lock_row_loc = 
                (RowLocation) ((RowLocation) 
                     init_template[init_template.length - 1]).cloneObject(); 

        // Verify that all columns in start key value, stop key value, and
        // qualifiers are present in the list of columns described by the
        // scanColumnList.
        if (SanityManager.DEBUG)
        {
            if (init_scanColumnList != null)
            {
                // verify that all columns specified in qualifiers, start
                // and stop positions are specified in the scanColumnList.  
                
                FormatableBitSet required_cols;

                if (qualifier != null)
                    required_cols = RowUtil.getQualifierBitSet(qualifier);
                else
                    required_cols = new FormatableBitSet(0);

                // add in start columns
                if (this.init_startKeyValue != null)
                {
					required_cols.grow(this.init_startKeyValue.length);
                    for (int i = 0; i < this.init_startKeyValue.length; i++)
                        required_cols.set(i);
                }

                if (this.init_stopKeyValue != null)
                {
					required_cols.grow(this.init_stopKeyValue.length);
                    for (int i = 0; i < this.init_stopKeyValue.length; i++)
                        required_cols.set(i);
                }

                FormatableBitSet required_cols_and_scan_list = 
                    (FormatableBitSet) required_cols.clone();

                required_cols_and_scan_list.and(init_scanColumnList);

				// FormatableBitSet equals requires the two FormatableBitSets to be of same
				// length.
				required_cols.grow(init_scanColumnList.size());

                if (!required_cols_and_scan_list.equals(required_cols))
                {
                    SanityManager.THROWASSERT(
                        "Some column specified in a Btree " +
                        " qualifier/start/stop list is " +
                        "not represented in the scanColumnList." +
                        "\n:required_cols_and_scan_list = " + 
                            required_cols_and_scan_list + 
                        "\n;required_cols = " + required_cols +
                        "\n;init_scanColumnList = " + init_scanColumnList);
                }
            }
		} 
	}

    /**
     * Position scan at "start" position for a forward scan.
     * <p> 
     * Positions the scan to the slot just before the first record to be 
     * returned from the scan.  Returns the start page latched, and 
     * sets "current_slot" to the slot number.
     * <p>
     *
	 * @return The leaf on which scan is positioned.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtStartForForwardScan(
    BTreeRowPosition    pos)
		throws StandardException
	{
		boolean         exact;

        // This routine should only be called from first next() call //
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                (scan_state == SCAN_INIT) || (scan_state == SCAN_HOLD_INIT));
            SanityManager.ASSERT(pos.current_rh          == null);
            SanityManager.ASSERT(pos.current_positionKey == null);
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

            boolean need_previous_lock = true;

            if (init_startKeyValue == null)
            {
                // No start given, so position at 0 slot of leftmost leaf page
                pos.current_leaf = (LeafControlRow) root.searchLeft(this);

                pos.current_slot = ControlRow.CR_SLOT;
                exact     = false;
            }
            else
            {
                // Search for the starting row.

                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(
                        (init_startSearchOperator == ScanController.GE) ||
                        (init_startSearchOperator == ScanController.GT));

                SearchParameters sp = new SearchParameters(
                    init_startKeyValue, 
                    ((init_startSearchOperator == ScanController.GE) ? 
                        SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH : 
                        SearchParameters.POSITION_RIGHT_OF_PARTIAL_KEY_MATCH),
                    init_template, this, false);

                pos.current_leaf = (LeafControlRow) root.search(sp);

                pos.current_slot = sp.resultSlot;
                exact     = sp.resultExact;

                // The way that scans are used, the caller calls next()
                // to position on the first row.  If the result of the
                // search that found the starting page and slot was not
                // exact, then the page/slot will refer to the row before
                // the first qualifying row.  The first call to next()
                // will therefore move to the first (potentially) qualifying
                // row.  However, if the search was exact, then we don't
                // want to move the position on the first call to next.
                // In that case, by decrementing the slot, the first call
                // to next will put us back	on the starting row.

                if (exact && init_startSearchOperator == ScanController.GE)
                {
                    pos.current_slot--;

                    // A scan on a unique index, with a start position of
                    // GE, need not get a previous key lock to protect the
                    // range.  Since it is unique no other key can go before
                    // the first row returned from the scan.
                    //
                    // RESOLVE - currently btree's only support allowDuplicates
                    // of "false", so no need to do the extra check, current
                    // btree implementation depends on RowLocation field
                    // making every key unique (duplicate indexes are supported
                    // by the nUniqueColumns and nKeyFields). 
                    if (getConglomerate().nUniqueColumns < getConglomerate().nKeyFields)
                    {
                        // this implies unique index, thus no prev key.
                        need_previous_lock = false;
                    }
                }
            }

            boolean latch_released = false;
            if (need_previous_lock)
            {
                latch_released = 
                    !this.getLockingPolicy().lockScanRow(
                        this, this.getConglomerate(), pos,
                        true, 
                        init_lock_fetch_desc,
                        pos.current_lock_template,
                        pos.current_lock_row_loc,
                        true, init_forUpdate, 
                        lock_operation);
            }
            else
            {
                // Don't need to lock the "previous key" but still need to get
                // the scan lock to protect the position in the btree.

                latch_released =
                    !this.getLockingPolicy().lockScan(
                        pos.current_leaf,   // the page we are positioned on.
                        (ControlRow) null,  // no other page to unlatch
                        false,              // lock for read.
                        lock_operation);    // not used.
            }

            // special test to see if latch release code works
            if (SanityManager.DEBUG)
            {
                latch_released = 
                    test_errors(
                        this,
                        "BTreeScan_positionAtStartPosition", true,
                        this.getLockingPolicy(), 
                        pos.current_leaf, latch_released);
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

        this.scan_state         = SCAN_INPROGRESS;
        pos.current_scan_pageno = pos.current_leaf.page.getPageNumber();
		pos.current_slot        = pos.current_slot;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(pos.current_leaf != null);
	}

    /**
     * Position scan at "start" position for a backward scan.
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
    protected void positionAtStartForBackwardScan(
    BTreeRowPosition    pos)
        throws StandardException
	{
		boolean         exact;

        // This routine should only be called from first next() call //
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                (this.scan_state == SCAN_INIT) || 
                (this.scan_state == SCAN_HOLD_INIT));

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
                /*
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT(
                        "Code not ready yet for positioned backward scans.");
                        */


                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(
                        (init_startSearchOperator == ScanController.GE) ||
                        (init_startSearchOperator == ScanController.GT));

                // Search for the starting row.

                SearchParameters sp = new SearchParameters(
                    init_startKeyValue, 
                    ((init_startSearchOperator == ScanController.GE) ? 
                        SearchParameters.POSITION_RIGHT_OF_PARTIAL_KEY_MATCH : 
                        SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH),
                    init_template, this, false);

                pos.current_leaf = (LeafControlRow) root.search(sp);

                pos.current_slot = sp.resultSlot;
                exact     = sp.resultExact;

                // The way that backward scans are used, the caller calls next()
                // to position on the first row.  If the result of the
                // search that found the starting page and slot was not
                // exact, then the page/slot will refer to the row before
                // the first qualifying row.  The first call to next()
                // will therefore move to the first (potentially) qualifying
                // row.  However, if the search was exact, then we don't
                // want to move the position on the first call to next.
                // In that case, by decrementing the slot, the first call
                // to next will put us back	on the starting row.


                if (exact)
                {
                    // the search has found exactly the start position key 
                    if (init_startSearchOperator == ScanController.GE)
                    {
                        // insure backward scan returns this row by moving
                        // slot to one after this row.
                        pos.current_slot++;
                    }
                    else
                    {
                        // no work necessary leave startslot positioned on the
                        // row, we will skip this record
                        if (SanityManager.DEBUG)
                            SanityManager.ASSERT(
                                init_startSearchOperator == ScanController.GT);
                    }
                }
                else
                {
                    // the search positioned one before the start position key,
                    // move it to one "after"
                    pos.current_slot++;
                }
            }

            boolean latch_released = 
                !this.getLockingPolicy().lockScanRow(
                    this, this.getConglomerate(), pos,
                    true, 
                    init_lock_fetch_desc,
                    pos.current_lock_template,
                    pos.current_lock_row_loc,
                    true, init_forUpdate, lock_operation);

            // special test to see if latch release code works
            if (SanityManager.DEBUG)
            {
                latch_released = 
                    test_errors(
                        this,
                        "BTreeScan_positionAtStartPosition", true,
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

        // System.out.println("backward scan end start position: " +
        //       " current_slot = " + this.current_slot );
	}

    /**
     * Position scan to 0 slot on next page.
     * <p>
     * Position to next page, keeping latch on previous page until we have 
     * latch on next page.  This routine releases the latch on current_page
     * once it has successfully gotten both the latch on the next page and
     * the scan lock on the next page.
     *
	 * @return The next leaf page, null if scan is done.
     *
     * @param pos           current row position of the scan.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtNextPage(
    BTreeRowPosition    pos)
        throws StandardException
    {
        // RESOLVE (mikem) - not sure but someday in the future this
        // assert may not be true, but for now we always have the scan
        // lock when we call this routine.
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(pos.current_scan_pageno != 0);

        while (true)
        {
            if ((pos.next_leaf = 
                 (LeafControlRow) pos.current_leaf.getRightSibling(this)) == null)
            {
                break;
            }

            boolean latch_released = 
                !this.getLockingPolicy().lockScan(
                    pos.next_leaf,
                    (LeafControlRow) null, // no other latch currently
                    false /* not for update */,
                    ConglomerateController.LOCK_READ); // get read scan lock.

            // TESTING CODE:
            if (SanityManager.DEBUG)
            {
                latch_released = 
                    test_errors(
                        this,
                        "BTreeScan_positionAtNextPage", true,
                        this.getLockingPolicy(), pos.next_leaf, latch_released);
            }

            if (!latch_released)
            {
                break;
            }
        }

        // Now that we either have both latch and scan lock on next leaf, or 
        // there is no next leaf we can release scan and latch on current page.
        if (SanityManager.DEBUG)
        {
			if (pos.current_scan_pageno != pos.current_leaf.page.getPageNumber())
				SanityManager.THROWASSERT(
                "pos.current_scan_pageno = " + pos.current_scan_pageno +
                "pos.current_leaf = " + pos.current_leaf);
        }

        // unlock the previous row if doing read.
        if (pos.current_rh != null)
        {
            this.getLockingPolicy().unlockScanRecordAfterRead(
                pos, init_forUpdate);
        }

        this.getLockingPolicy().unlockScan(
            pos.current_leaf.page.getPageNumber());
        pos.current_leaf.release();
        pos.current_leaf        = pos.next_leaf;

        pos.current_scan_pageno = 
            (pos.next_leaf == null) ? 0 : pos.next_leaf.page.getPageNumber();

        // set up for scan to continue at beginning of next page.
        pos.current_slot        = Page.FIRST_SLOT_NUMBER;
        pos.current_rh          = null;
    }

	/**
	Position scan at "start" position.
	<p>
    Positions the scan to the slot just before the first record to be returned
    from the scan.  Returns the start page latched, and sets "current_slot" to
    the slot number.

	@exception  StandardException  Standard exception policy.
	**/
    abstract void positionAtStartPosition(
    BTreeRowPosition    pos)
        throws StandardException;


    /**
     * Do any necessary work to complete the scan.
     *
     * @param pos           current row position of the scan.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtDoneScanFromClose(
    BTreeRowPosition    pos)
        throws StandardException
    {
        // call unlockScanRecordAfterRead() before closing, currently
        // this is only important for releasing RR locks on non-qualified
        // rows.   
        //
        // Otherwise the correct behavior happens as part of the close, ie.:
        //
        //     for READ_UNCOMMITTED there is no lock to release, 
        //     for READ_COMMITTED   all read locks will be released, 
        //     for REPEATABLE_READ or SERIALIZABLE no locks are released.

        if ((pos.current_rh != null) && !pos.current_rh_qualified)
        {
            if (pos.current_leaf == null || pos.current_leaf.page == null)
            {
                // If we are being called from a "normal" close then there
                // will be no latch on current_leaf, get it and do the the
                // unlock.  We may be called sometimes, after an error where
                // we may have the latch, in this case the transaction is about
                // to be backed out anyway so don't worry about doing this 
                // unlock (thus why we only do the following code if we
                // "don't" have lock, ie. pos.current_leaf== null).

                if (!reposition(pos, false))
                {
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.THROWASSERT(
                            "can not fail while holding update row lock.");
                    }
                }

                this.getLockingPolicy().unlockScanRecordAfterRead(
                    pos, init_forUpdate);

                pos.current_rh   = null;
                pos.current_leaf.release();
                pos.current_leaf = null;
            }
        }


        // Need to do this unlock in any case, until lock manager provides
        // a way to release locks associated with a compatibility space.  This
        // scan lock is special, as it is a lock on the btree container rather
        // than the heap container.  The open container on the btree actually
        // has a null locking policy so the close of that container does not
        // release this lock, need to explicitly unlock it here or when the
        // scan is closed as part of the abort the lock will not be released.
        if (pos.current_scan_pageno != 0)
        {
            this.getLockingPolicy().unlockScan(pos.current_scan_pageno);
            pos.current_scan_pageno = 0;
        }

        pos.current_slot = Page.INVALID_SLOT_NUMBER;
        pos.current_rh   = null;
        pos.current_positionKey  = null;
        this.scan_state   = SCAN_DONE;

        return;
    }

    /**
     * Do work necessary to close a scan.
     * <p>
     * This routine can only be called "inline" from other btree routines,
     * as it counts on the state of the pos to be correct.
     * <p>
     * Closing a scan from close() must handle long jumps from exceptions
     * where the state of pos may not be correct.  The easiest case is
     * a lock timeout which has caused us not to have a latch on a page,
     * but pos still thinks there is a latch.  This is the easiest but
     * other exceptions can also caused the same state at close() time.
     **/
    protected void positionAtDoneScan(
    BTreeRowPosition    pos)
        throws StandardException
    {

        // Need to do this unlock in any case, until lock manager provides
        // a way to release locks associated with a compatibility space.  This
        // scan lock is special, as it is a lock on the btree container rather
        // than the heap container.  The open container on the btree actually
        // has a null locking policy so the close of that container does not
        // release this lock, need to explicitly unlock it here or when the
        // scan is closed as part of the abort the lock will not be released.
        if (pos.current_scan_pageno != 0)
        {
            this.getLockingPolicy().unlockScan(pos.current_scan_pageno);
            pos.current_scan_pageno = 0;
        }

        pos.current_slot        = Page.INVALID_SLOT_NUMBER;
        pos.current_rh          = null;
        pos.current_positionKey = null;
        this.scan_state         = SCAN_DONE;

        return;
    }


    /**
     * process_qualifier - Determine if a row meets all qualifier conditions.
     * <p>
     * Check all qualifiers in the qualifier array against row.  Return true
     * if all compares specified by the qualifier array return true, else
     * return false.
     * <p>
     * It is up to caller to make sure qualifier list is non-null.
     *
     * @param row      The row with the same partial column list as the
     *                 row returned by the current scan.
     *
	 * @exception  StandardException  Standard exception policy.
     */
    protected boolean process_qualifier(
    DataValueDescriptor[]     row) 
        throws StandardException
    {
        boolean     row_qualifies = true;
        Qualifier   q;

        // Process the 2-d qualifier which is structured as follows:
        //
        // A two dimensional array is to be used to pass around a AND's and OR's
        // in conjunctive normal form (CNF).  The top slot of the 2 dimensional
        // array is optimized for the more frequent where no OR's are present.  
        // The first array slot is always a list of AND's to be treated as 
        // described above for single dimensional AND qualifier arrays.  The 
        // subsequent slots are to be treated as AND'd arrays or OR's.  Thus 
        // the 2 dimensional array qual[][] argument is to be treated as the 
        // following, note if qual.length = 1 then only the first array is 
        // valid and // it is and an array of and clauses:
        //
        // (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
        // and
        // (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
        // and
        // (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
        // ...
        // and
        // (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])

        // First do the qual[0] which is an array of qualifer terms.

        if (SanityManager.DEBUG)
        {
            // routine should not be called if there is no qualifier
            SanityManager.ASSERT(this.init_qualifier != null);
            SanityManager.ASSERT(this.init_qualifier.length > 0);
        }

        for (int i = 0; i < this.init_qualifier[0].length; i++)
        {
            // process each AND clause 

            row_qualifies = false;

            // process each OR clause.

            q = this.init_qualifier[0][i];

            // Get the column from the possibly partial row, of the 
            // q.getColumnId()'th column in the full row.
            DataValueDescriptor columnValue = row[q.getColumnId()];

            row_qualifies =
                columnValue.compare(
                    q.getOperator(),
                    q.getOrderable(),
                    q.getOrderedNulls(),
                    q.getUnknownRV());

            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;

            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // all the qual[0] and terms passed, now process the OR clauses

        for (int and_idx = 1; and_idx < this.init_qualifier.length; and_idx++)
        {
            // process each AND clause 

            row_qualifies = false;

            if (SanityManager.DEBUG)
            {
                // Each OR clause must be non-empty.
                SanityManager.ASSERT(this.init_qualifier[and_idx].length > 0);
            }

            for (int or_idx = 0; 
                 or_idx < this.init_qualifier[and_idx].length; or_idx++)
            {
                // process each OR clause.

                q = this.init_qualifier[and_idx][or_idx];

                // Get the column from the possibly partial row, of the 
                // q.getColumnId()'th column in the full row.
                DataValueDescriptor columnValue = row[q.getColumnId()];

                row_qualifies =
                    columnValue.compare(
                        q.getOperator(),
                        q.getOrderable(),
                        q.getOrderedNulls(),
                        q.getUnknownRV());

                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;

                // once one OR qualifies the entire clause is TRUE
                if (row_qualifies)
                    break;
            }

            if (!row_qualifies)
                break;
        }

        return(row_qualifies);
    }


    /**
     * Reposition the scan leaving and reentering the access layer.
     * <p>
     * When a scan leaves access it saves the RecordHandle of the record
     * on the page.  There are 2 cases to consider when trying to reposition
     * the scan when re-entering access:
     *     o ROW has not moved off the page.
     *       If the row has not moved then the RecordHandle we have saved
     *       away is valid, and we just call RawStore to reposition on that
     *       RecordHandle (RawStore takes care of the row moving within
     *       the page).
     *     o ROW has moved off the page.
     *       This can only happen in the case of a btree split.  In that
     *       case the splitter will have caused all scans positioned on 
     *       this page within the same transaction to save a copy of the
     *       row that the scan was positioned on.  Then to reposition the
     *       scan it is necessary to research the tree from the top using
     *       the copy of the row.
     *
     * If the scan has saved it's position by key (and thus has given up the
     * scan lock on the page), there are a few cases where it is possible that
     * the key no longer exists in the table.  In the case of a scan held 
     * open across commit it is easy to imagine that the row the scan was 
     * positioned on could be deleted and subsequently purged from the table 
     * all before the scan resumes.  Also in the case of read uncommitted 
     * the scan holds no lock on the current row, so it could be purged -
     * in the following scenario for instance:  read uncommitted transaction 1
     * opens scan and positions on row (1,2), transaction 2 deletes (1,2) and
     * commits, transaction 1 inserts (1,3) which goes to same page as (1,2)
     * and is going to cause a split, transaction 1 saves scan position as
     * key, gives up scan lock and then purges row (1, 2), when transaction 
     * 1 resumes scan (1, 2) no longer exists.  missing_row_for_key_ok 
     * parameter is added as a sanity check to make sure it ok that 
     * repositioning does not go to same row that we were repositioned on.
     *
     *
     *
     * @param   pos                     position to set the scan to.
     *
     * @param   missing_row_for_key_ok  if true and exact key is not found then
     *                                  scan is just set to key just left of
     *                                  the key (thus a next will move to the
     *                                  key just after "pos")
     *
     * @return  returns true if scan has been repositioned successfully, else
     *          returns false if the position key could not be found and
     *          missing_row_for_key_ok was false indicating that scan could
     *          only be positioned on the exact key match.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected boolean reposition(
    BTreeRowPosition pos,
    boolean          missing_row_for_key_ok)
        throws StandardException
	{
        // RESOLVE (mikem) - performance - we need to do a buffer manager
        // get for every row returned from the scan.  It may be better to
        // allow a reference to the page with no latch (ie. a fixed bit).

        if (this.scan_state != SCAN_INPROGRESS)
        {
            throw StandardException.newException(
                SQLState.BTREE_SCAN_NOT_POSITIONED, 
                new Integer(this.scan_state));
        }

        // Either current_rh or positionKey is valid - the other is null.
        if (SanityManager.DEBUG)
        {
			if ((pos.current_rh == null) != (pos.current_positionKey != null))
            	SanityManager.THROWASSERT(
                	"pos.current_rh  = (" + pos.current_rh + "), " +
                	"pos.current_positionKey = (" + 
                    pos.current_positionKey + ").");
        }

        if (!((pos.current_rh == null) == (pos.current_positionKey != null)))
        {
            throw StandardException.newException(
                    SQLState.BTREE_SCAN_INTERNAL_ERROR, 
                    new Boolean(pos.current_rh == null), 
                    new Boolean(pos.current_positionKey == null));
        }

        if (pos.current_positionKey == null)
        {
            // Reposition to remembered spot on page.
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(pos.current_scan_pageno != 0);

            pos.current_leaf = (LeafControlRow)
                ControlRow.Get(this, pos.current_rh.getPageNumber());
            pos.current_slot =
                pos.current_leaf.page.getSlotNumber(pos.current_rh);
        }
        else
        {
            // RESOLVE (mikem) - not sure but someday in the future this
            // assert may not be true, but for now we always release the 
            // scan lock when we save the row away as the current position.
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(pos.current_scan_pageno == 0);

            SearchParameters sp =
                new SearchParameters(
                    pos.current_positionKey, 
                    // this is a full key search, so this arg is not used.
                    SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH,
                    init_template, this, false);

            // latch/lock loop, continue until you can get scan lock on page
            // while holding page latched without waiting.


            boolean latch_released;
            do
            {
                pos.current_leaf = (LeafControlRow)
                    ControlRow.Get(this, BTree.ROOTPAGEID).search(sp);

                if (sp.resultExact || missing_row_for_key_ok)
                {
                    // RESOLVE (mikem) - we could have a scan which always 
                    // maintained it's position by key value, or we could 
                    // optimize and delay this lock until we were about to 
                    // give up the latch.  But it is VERY likely we will get 
                    // the lock since we have the latch on the page.
                    //
                    // In order to be successfully positioned we must get the 
                    // scan lock again.
                    latch_released = 
                        !this.getLockingPolicy().lockScan(
                            pos.current_leaf, 
                            (LeafControlRow) null, // no other latch currently
                            false /* not for update */,
                            ConglomerateController.LOCK_READ); // read lock on scan position

                    // TESTING CODE:
                    if (SanityManager.DEBUG)
                    {
                        latch_released = 
                            test_errors(
                                this,
                                "BTreeScan_reposition", true, 
                                this.getLockingPolicy(),
                                pos.current_leaf, latch_released);
                    }
                }
                else
                {
                    // Did not find key to exactly position on.

                    pos.current_leaf.release();
                    pos.current_leaf = null;
                    return(false);
                }

            } while (latch_released);

            pos.current_scan_pageno = pos.current_leaf.page.getPageNumber();
            pos.current_slot        = sp.resultSlot;
            pos.current_positionKey = null;
        }

        return(true);
	}

	/*
	** Public Methods of BTreeScan
	*/


	/**
	Initialize the scan for use.
	<p>
	Any changes to this method may have to be reflected in close as well.
    <p>
    The btree init opens the container (super.init), and stores away the
    state of the qualifiers.  The actual searching for the first position
    is delayed until the first next() call.

	@exception  StandardException  Standard exception policy.
	**/
	public void init(
    TransactionManager              xact_manager,
    Transaction                     rawtran,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    BTreeLockingPolicy              btree_locking_policy,
    FormatableBitSet                         scanColumnList,
    DataValueDescriptor[]		    startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]		    stopKeyValue,
    int                             stopSearchOperator,
    BTree                           conglomerate,
    LogicalUndo                     undo,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
        throws StandardException
	{
		super.init(
            xact_manager, xact_manager, (ContainerHandle) null, rawtran,
            hold,
            open_mode, lock_level, btree_locking_policy, 
            conglomerate, undo, dynamic_info);


        this.init_rawtran               = rawtran;
        this.init_forUpdate             = 
            ((open_mode & ContainerHandle.MODE_FORUPDATE) == 
                 ContainerHandle.MODE_FORUPDATE);

		// Keep track of whether this scan should use update locks.
		this.init_useUpdateLocks = 
            ((open_mode &
                ContainerHandle.MODE_USE_UPDATE_LOCKS) != 0);

		this.init_hold                  = hold;

		this.init_template              = runtime_mem.get_template();

        this.init_scanColumnList        = scanColumnList;

        this.init_lock_fetch_desc        = 
            RowUtil.getFetchDescriptorConstant(init_template.length - 1);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                init_lock_fetch_desc.getMaxFetchColumnId() == 
                    (init_template.length - 1));
            SanityManager.ASSERT(
                (init_lock_fetch_desc.getValidColumnsArray())[init_template.length - 1] == 1); 
        }

        // note that we don't process qualifiers in btree fetch's
        this.init_fetchDesc             = 
            new FetchDescriptor(
                init_template.length, init_scanColumnList,(Qualifier[][]) null);

        initScanParams( 
            startKeyValue, startSearchOperator, 
            qualifier, stopKeyValue, stopSearchOperator);

        
        if (SanityManager.DEBUG)
        {
            // RESOLVE - (mikem) we should we require a template, need to 
            // clean up some of the old tests which did not provide one?
            if (init_template != null)
            {
                SanityManager.ASSERT(
                    TemplateRow.checkColumnTypes(
                        this.getConglomerate().format_ids, init_template));
            }
        }

        // System.out.println("initializing scan:" + this);

        // initialize default locking operation for the scan.
        this.lock_operation = 
            (init_forUpdate ? 
                ConglomerateController.LOCK_UPD : 
                ConglomerateController.LOCK_READ);

        if (init_useUpdateLocks)
            this.lock_operation |= ConglomerateController.LOCK_UPDATE_LOCKS;

        // System.out.println("Btree scan: " + this);
	}



	/*
	** Methods of ScanController
	*/

    /**
    Close the scan.
    **/
    public void close()
        throws StandardException
	{
        // Scan is closed, make sure no access to any state variables
        positionAtDoneScanFromClose(scan_position);

		super.close();

        // null out so that these object's can get GC'd earlier.
        this.init_rawtran       = null;
        this.init_template      = null;
        this.init_startKeyValue = null;
        this.init_qualifier     = null;
        this.init_stopKeyValue  = null;

        this.getXactMgr().closeMe(this);
	}

    /**
    Delete the row at the current position of the scan.
	@see ScanController#delete

	@exception  StandardException  Standard exception policy.
    **/
    public boolean delete()
		throws StandardException
	{
        boolean     ret_val      = false;

        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                SQLState.AM_SCAN_NOT_POSITIONED);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null,
                "BTreeScan.delete() called on a closed scan.");
            SanityManager.ASSERT(init_forUpdate);
        }

        try
        {
            // Get current page of scan, with latch.
            if (!reposition(scan_position, false))
            {
                throw StandardException.newException(
                        SQLState.AM_RECORD_NOT_FOUND,
                        new Long(err_containerid),
                        new Long(scan_position.current_rh.getId()));
            }


            if (init_useUpdateLocks)
            {
                // RESOLVE (mikem) - I don't think lockScanRow() is the right
                // thing to call.

                // if we are doing update locking, then we got an U lock on
                // this row when the scan positioned on it, but now that we
                // are doing a delete on the current position we need to upgrade
                // the lock to X.
                boolean latch_released =
                    !this.getLockingPolicy().lockScanRow(
                        this, this.getConglomerate(), scan_position,
                        false, 
                        init_lock_fetch_desc,
                        scan_position.current_lock_template,
                        scan_position.current_lock_row_loc,
                        false, init_forUpdate, lock_operation);

                if (latch_released)
                {
                    // lost latch on page in order to wait for row lock.
                    // Because we have scan lock on page, we need only
                    // call reposition() which will use the saved record
                    // handle to reposition to the same spot on the page.
                    // We don't have to search the
                    // tree again, as we have the a scan lock on the page
                    // which means the current_rh is valid to reposition on.
                    if (reposition(scan_position, false))
                    {
                        throw StandardException.newException(
                                SQLState.AM_RECORD_NOT_FOUND,
                                new Long(err_containerid),
                                new Long(scan_position.current_rh.getId()));
                    }
                }
            }


            // Do a fetch just to get the RecordHandle for the delete call,
            // don't fetch any columns.
            RecordHandle delete_rh = 
                scan_position.current_leaf.page.fetchFromSlot(
                    (RecordHandle) null, scan_position.current_slot, 
                    RowUtil.EMPTY_ROW, (FetchDescriptor) null, true);

            ret_val =
                scan_position.current_leaf.page.delete(
                    delete_rh, this.btree_undo);

            // See if we just deleted the last row on the page, in a btree a
            // page with all rows still has 1 left - the control row.
  	    // Beetle 5750: we do not reclaim the root page of the btree if 
            // there are no children since we were
    	    // doing too many post commit actions in a benchmark which does an
    	    // insert/commit/delete/commit operations in a single user system. now ,
    	    // with this change the work will move to the user
       	    // thread which does the insert 

            if (scan_position.current_leaf.page.nonDeletedRecordCount() == 1 &&
		!(scan_position.current_leaf.getIsRoot() && 
		  scan_position.current_leaf.getLevel() == 0 )) 
            {
                this.getXactMgr().addPostCommitWork(new BTreePostCommit(
                    this.getXactMgr().getAccessManager(),
                    this.getConglomerate(),
                    scan_position.current_leaf.page.getPageNumber()));
            }
        }
        finally
        {
            if (scan_position.current_leaf != null)
            {
                // release latch on page
                scan_position.current_leaf.release();
                scan_position.current_leaf = null;
            }
        }

        return(ret_val);
	}

    /**
     * A call to allow client to indicate that current row does not qualify.
     * <p>
     * Indicates to the ScanController that the current row does not
     * qualify for the scan.  If the isolation level of the scan allows, 
     * this may result in the scan releasing the lock on this row.
     * <p>
     * Note that some scan implimentations may not support releasing locks on 
     * non-qualifying rows, or may delay releasing the lock until sometime
     * later in the scan (ie. it may be necessary to keep the lock until 
     * either the scan is repositioned on the next row or page).
     * <p>
     * This call should only be made while the scan is positioned on a current
     * valid row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void didNotQualify()
        throws StandardException
    {
    }


    /**
     * Returns true if the current position of the scan still qualifies
     * under the set of qualifiers passed to the openScan().  When called
     * this routine will reapply all qualifiers against the row currently
     * positioned and return true if the row still qualifies.  If the row
     * has been deleted or no longer passes the qualifiers then this routine
     * will return false.
     * <p>
     * This case can come about if the current scan
     * or another scan on the same table in the same transaction
     * deleted the row or changed columns referenced by the qualifier after
     * the next() call which positioned the scan at this row.
     * <p>
     * Note that for comglomerates which don't support update, like btree's,
     * there is no need to recheck the qualifiers.
     * <p>
     * The results of a fetch() performed on a scan positioned on
     * a deleted row are undefined.
     * <p>
	 * @exception StandardException Standard exception policy.
    **/
    public boolean doesCurrentPositionQualify()
		throws StandardException
    {
        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                SQLState.AM_SCAN_NOT_POSITIONED);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null,
            "BTreeScan.doesCurrentPositionQualify() called on a closed scan.");
        }

        try
        {
            // Get current page of scan, with latch
            if (!reposition(scan_position, false))
            {
                // TODO - write unit test to get here, language always calls
                // isCurrentPositionDeleted() right before calling this, so
                // hard to write .sql test to exercise this.

                // if reposition fails it means the position of the scan
                // has been purged from the table - for example if this is
                // a uncommitted read scan and somehow the row was purged 
                // since the last positioning.

                return(false);
            }

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    scan_position.current_leaf.page.fetchNumFieldsAtSlot(
                        scan_position.current_slot) > 1);
            }

            // Since btree row don't get updated, the only way a current
            // position may not qualify is if it got deleted.
            return(
                !scan_position.current_leaf.page.isDeletedAtSlot(
                    scan_position.current_slot));
        }
        finally
        {

            if (scan_position.current_leaf != null)
            {
                // release latch on page.
                scan_position.current_leaf.release();
                scan_position.current_leaf = null;
            }
        }
    }


    /**
    Fetch the row at the current position of the Scan.
	@see ScanController#fetch

	@exception  StandardException  Standard exception policy.
    **/
	public void fetch(DataValueDescriptor[] row)
		throws StandardException
	{
        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                SQLState.AM_SCAN_NOT_POSITIONED);
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null,
                "BTreeScan.fetch() called on a closed scan.");
            
            TemplateRow.checkPartialColumnTypes(
                this.getConglomerate().format_ids, 
                init_scanColumnList, (int []) null, row);
        }

        try
        {
            // Get current page of scan, with latch
            if (!reposition(scan_position, false))
            {
                // TODO - write unit test to get here, language always calls
                // isCurrentPositionDeleted() right before calling this, so
                // hard to write .sql test to exercise this.

                throw StandardException.newException(
                        SQLState.AM_RECORD_NOT_FOUND,
                        new Long(err_containerid),
                        new Long(scan_position.current_rh.getId()));
            }

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    scan_position.current_leaf.page.fetchNumFieldsAtSlot(
                        scan_position.current_slot) > 1);
            }

            scan_position.current_rh = 
                scan_position.current_leaf.page.fetchFromSlot(
                (RecordHandle) null, 
                scan_position.current_slot, row, init_fetchDesc,
                true);

            // The possibility is that the row at the current position
            // has been marked as deleted (it cannot have been purged
            // since the scan maintains a lock on the row, and purges
            // are always done from system transactions).  I'm not sure
            // what the desired behavior is in this case.  For now,
            // just return null.

            // RESOLVE (mikem) - what should be done here?
            if (scan_position.current_leaf.page.isDeletedAtSlot(
                    scan_position.current_slot))
            {
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(false, "positioned on deleted row");
            }
        }
        finally
        {
            if (scan_position.current_leaf != null)
            {
                // release latch on page.
                scan_position.current_leaf.release();
                scan_position.current_leaf = null;
            }
        }

		return;
	}

    /**
     * Return ScanInfo object which describes performance of scan.
     * <p>
     * Return ScanInfo object which contains information about the current
     * scan.
     * <p>
     *
     * @see ScanInfo
     *
	 * @return The ScanInfo object which contains info about current scan.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public ScanInfo getScanInfo()
		throws StandardException
    {
        return(new BTreeScanInfo(this));
    }

    /**
    Returns true if the current position of the scan is at a
    deleted row.  This case can come about if the current scan
    or another scan on the same table in the same transaction
    deleted the row after the next() call which positioned the
    scan at this row.

    The results of a fetch() performed on a scan positioned on
    a deleted row are undefined.

	@exception StandardException Standard exception policy.
    **/
    public boolean isCurrentPositionDeleted()
		throws StandardException
    {
        boolean     ret_val;

        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                SQLState.AM_SCAN_NOT_POSITIONED);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.container != null,
                "BTreeScan.isCurrentPositionDeleted() called on closed scan.");
        }
        try
        {
            // Get current page of scan, with latch

            if (reposition(scan_position, false))
            {

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        scan_position.current_leaf.page.fetchNumFieldsAtSlot(
                            scan_position.current_slot) > 1);
                }

                ret_val = 
                    scan_position.current_leaf.page.isDeletedAtSlot(
                        scan_position.current_slot);
            }
            else
            {
                ret_val = false;
            }
        }
        finally
        {
            if (scan_position.current_leaf != null)
            {
                // release latch on page.
                scan_position.current_leaf.release();
                scan_position.current_leaf = null;
            }
        }

		return(ret_val);
    }

    /**
     * Return whether this is a keyed conglomerate.
     * <p>
     *
	 * @return whether this is a keyed conglomerate.
     **/
	public boolean isKeyed()
    {
        return(true);
    }


    /**
    Move to the next position in the scan.
	@see ScanController#next

	@exception  StandardException  Standard exception policy.
    **/
    public boolean next()
		throws StandardException
	{
        // Turn this call into a group fetch of a 1 element group.
        fetchNext_one_slot_array[0] = runtime_mem.get_scratch_row();
        boolean ret_val = 
            fetchRows(
                scan_position,
                fetchNext_one_slot_array, 
                (RowLocation[]) null,
                (BackingStoreHashtable) null,
                1,
                (int[]) null) == 1;


        return(ret_val);
    }

    /**
    Fetch the row at the next position of the Scan.

    If there is a valid next position in the scan then
	the value in the template storable row is replaced
	with the value of the row at the current scan
	position.  The columns of the template row must
	be of the same type as the actual columns in the
	underlying conglomerate.

    The resulting contents of templateRow after a fetchNext() 
    which returns false is undefined.

    The result of calling fetchNext(row) is exactly logically
    equivalent to making a next() call followed by a fetch(row)
    call.  This interface allows implementations to optimize 
    the 2 calls if possible.

    @param template The template row into which the value
	of the next position in the scan is to be stored.

    @return True if there is a next position in the scan,
	false if there isn't.

	@exception StandardException Standard exception policy.
    **/
	public boolean fetchNext(DataValueDescriptor[] row)
		throws StandardException
	{
        boolean ret_val;

        if (SanityManager.DEBUG)
        {
            TemplateRow.checkPartialColumnTypes(
                this.getConglomerate().format_ids, 
                init_scanColumnList, (int[]) null, row);
        }

        // Turn this call into a group fetch of a 1 element group.
        fetchNext_one_slot_array[0] = row;
        ret_val = 
            fetchRows(
                scan_position,
                fetchNext_one_slot_array, 
                (RowLocation[]) null,
                (BackingStoreHashtable) null,
                1,
                (int[]) null) == 1;

        return(ret_val);
    }

    /**
     * Fetch the next N rows from the table.
     * <p>
     * The client allocates an array of N rows and passes it into the
     * fetchNextSet() call.  This routine does the equivalent of N 
     * fetchNext() calls, filling in each of the rows in the array.
     * Locking is performed exactly as if the N fetchNext() calls had
     * been made.
     * <p>
     * It is up to Access how many rows to return.  fetchNextSet() will
     * return how many rows were filled in.  If fetchNextSet() returns 0
     * then the scan is complete, (ie. the scan is in the same state as if
     * fetchNext() had returned false).  If the scan is not complete then
     * fetchNext() will return (1 <= row_count <= N).
     * <p>
     * The current position of the scan is undefined if fetchNextSet()
     * is used (ie. mixing fetch()/fetchNext() and fetchNextSet() calls
     * in a single scan does not work).  This is because a fetchNextSet()
     * request for 5 rows from a heap where the first 2 rows qualify, but
     * no other rows qualify will result in the scan being positioned at
     * the end of the table, while if 5 rows did qualify the scan will be
     * positioned on the 5th row.
     * <p>
     * Qualifiers, start and stop positioning of the openscan are applied
     * just as in a normal scan. 
     * <p>
     * The columns of the row will be the standard columns returned as
     * part of a scan, as described by the validColumns - see openScan for
     * description.
     * <p>
     * Expected usage:
     *
     * // allocate an array of 5 empty row templates
     * DataValueDescriptor[][] row_array = allocate_row_array(5);
     * int row_cnt = 0;
     *
     * scan = openScan();
     *
     * while ((row_cnt = scan.fetchNextSet(row_array) != 0)
     * {
     *     // I got "row_cnt" rows from the scan.  These rows will be
     *     // found in row_array[0] through row_array[row_cnt - 1]
     * }
     *
     * <p>
     *
     * RESOLVE - This interface is being provided so that we can prototype
     *           the performance results it can achieve.  If it looks like
     *           this interface is useful, it is very likely we will look
     *           into a better way to tie together the now 4 different
     *           fetch interfaces: fetch, fetchNext(), fetchNextGroup(),
     *           and fetchSet().
     *
	 * @return The number of qualifying rows found and copied into the 
     *         provided array of rows.  If 0 then the scan is complete, 
     *         otherwise the return value will be: 
     *         1 <= row_count <= row_array.length
     *
     * @param row_array         The array of rows to copy rows into.  
     *                          row_array[].length must >= 1.  This routine
     *                          assumes that all entries in the array 
     *                          contain complete template rows.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int fetchNextGroup(
    DataValueDescriptor[][] row_array,
    RowLocation[]           rowloc_array)
        throws StandardException
	{
        return(
            fetchRows(
                scan_position,
                row_array, 
                rowloc_array,
                (BackingStoreHashtable) null,
                row_array.length,
                (int[]) null));
    }

    /**
     * Insert all rows that qualify for the current scan into the input
     * Hash table.  
     * <p>
     * This routine scans executes the entire scan as described in the 
     * openScan call.  For every qualifying unique row value an entry is
     * placed into the HashTable. For unique row values the entry in the
     * BackingStoreHashtable has a key value of the object stored in 
     * row[key_column_number], and the value of the data is row.  For row 
     * values with duplicates, the key value is also row[key_column_number], 
     * but the value of the data is a Vector of
     * rows.  The caller will have to call "instanceof" on the data value
     * object if duplicates are expected, to determine if the data value
     * of the Hashtable entry is a row or is a Vector of rows.
     * <p>
     * Note, that for this routine to work efficiently the caller must 
     * ensure that the object in row[key_column_number] implements 
     * the hashCode and equals method as appropriate for it's datatype.
     * <p>
     * It is expected that this call will be the first and only call made in
     * an openscan.  Qualifiers and stop position of the openscan are applied
     * just as in a normal scan.  This call is logically equivalent to the 
     * caller performing the following:
     *
     * import java.util.Hashtable;
     *
     * hash_table = new Hashtable();
     *
     * while (next())
     * {
     *     row = create_new_row();
     *     fetch(row);
     *     if ((duplicate_value = 
     *         hash_table.put(row[key_column_number], row)) != null)
     *     {
     *         Vector row_vec;
     *
     *         // inserted a duplicate
     *         if ((duplicate_value instanceof vector))
     *         {
     *             row_vec = (Vector) duplicate_value;
     *         }
     *         else
     *         {
     *             // allocate vector to hold duplicates
     *             row_vec = new Vector(2);
     *
     *             // insert original row into vector
     *             row_vec.addElement(duplicate_value);
     *
     *             // put the vector as the data rather than the row
     *             hash_table.put(row[key_column_number], row_vec);
     *         }
     *         
     *         // insert new row into vector
     *         row_vec.addElement(row);
     *     }
     * }
     * <p>
     * The columns of the row will be the standard columns returned as
     * part of a scan, as described by the validColumns - see openScan for
     * description.
     * RESOLVE - is this ok?  or should I hard code somehow the row to
     *           be the first column and the row location?
     * <p>
     * Currently it is only possible to hash on the first column in the
     * conglomerate, in the future we may change the interface to allow
     * hashing either on a different column or maybe on a combination of
     * columns.
     * <p>
     * No overflow to external storage is provided, so calling this routine
     * on a 1 gigabyte conglomerate will incur at least 1 gigabyte of memory
     * (probably failing with a java out of memory condition).  If this
     * routine gets an out of memory condition, or if "max_rowcnt" is 
     * exceeded then then the routine will give up, empty the Hashtable, 
     * and return "false."
     * <p>
     * On exit from this routine, whether the fetchSet() succeeded or not
     * the scan is complete, it is positioned just the same as if the scan
     * had been drained by calling "next()" until it returns false (ie. 
     * fetchNext() and next() calls will return false).  
     * reopenScan() can be called to restart the scan.
     * <p>
     *
     * RESOLVE - until we get row counts what should we do for sizing the
     *           the size, capasity, and load factor of the hash table.
     *           For now it is up to the caller to create the Hashtable,
     *           Access does not reset any parameters.
     * <p>
     * RESOLVE - I am not sure if access should be in charge of allocating
     *           the new row objects.  I know that I can do this in the
     *           case of btree's, but I don't think I can do this in heaps.
     *           Maybe this is solved by work to be done on the sort 
     *           interface.
     *
     *
	 * @return boolean indicating that the fetch set succeeded.  If it failed
     *                 Hashtable.clear() will be called leaving an empty 
     *                 table.
     *
     * @param max_rowcnt        The maximum number of rows to insert into the 
     *                          Hash table.  Pass in -1 if there is no maximum.
     * @param key_column_numbers The column numbers of the columns in the
     *                          scan result row to be the key to the Hashtable.
     *                          "0" is the first column in the scan result
     *                          row (which may be different than the first
     *                          column in the row in the table of the scan).
     * @param hash_table        The java HashTable to load into.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void fetchSet(
    long                    max_rowcnt,
    int[]                   key_column_numbers,
    BackingStoreHashtable   hash_table)
        throws StandardException
    {
        // System.out.println("fetchSet");
        
        fetchRows(
            scan_position,
            (DataValueDescriptor[][]) null,
            (RowLocation[]) null,
            (BackingStoreHashtable) hash_table,
            max_rowcnt,
            key_column_numbers);

        return;
    }


    /**
    Reposition the current scan.  This call is semantically the same as if
    the current scan had been closed and a openScan() had been called instead.
    The scan is reopened with against the same conglomerate, and the scan
    is reopened with the same "hold" and "forUpdate" parameters passed in
    the original openScan.  The previous template row continues to be used.

	@param startKeyValue  An indexable row which holds a
	(partial) key value which, in combination with the
	startSearchOperator, defines the starting position of
	the scan.  If null, the starting position of the scan
	is the first row of the conglomerate.

	@param startSearchOperation an operator which defines
	how the startKeyValue is to be searched for.  If
    startSearchOperation is ScanController.GE, the scan starts on
	the first row which is greater than or equal to the
	startKeyValue.  If startSearchOperation is ScanController.GT,
	the scan starts on the first row whose key is greater than
	startKeyValue.  The startSearchOperation parameter is
	ignored if the startKeyValue parameter is null.

	@param qualifier An array of qualifiers which, applied
	to each key, restrict the rows returned by the scan.  Rows
	for which any one of the qualifiers returns false are not
	returned by the scan. If null, all rows are returned.

	@param stopKeyValue  An indexable row which holds a
	(partial) key value which, in combination with the
	stopSearchOperator, defines the ending position of
	the scan.  If null, the ending position of the scan
	is the last row of the conglomerate.

	@param stopSearchOperation an operator which defines
	how the stopKeyValue is used to determine the scan stopping
	position. If stopSearchOperation is ScanController.GE, the scan
	stops just before the first row which is greater than or
	equal to the stopKeyValue.  If stopSearchOperation is
	ScanController.GT, the scan stops just before the first row whose
	key is greater than	startKeyValue.  The stopSearchOperation
	parameter is ignored if the stopKeyValue parameter is null.

	@exception StandardException Standard exception policy.
    **/
	public final void reopenScan(
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    Qualifier               qualifier[][],
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            if (!init_hold)
                SanityManager.ASSERT(this.container != null,
                    "BTreeScan.reopenScan() called on non-held closed scan.");

            // should only be called by clients outside of store, so should
            // not be possible for a latch to held.
            SanityManager.ASSERT(scan_position.current_leaf == null);
        }

        // call unlockScanRecordAfterRead() before setting the scan back
        // to init state, so that we release the last lock if necessary (ie.
        // for read committed).
        //
        
        if (scan_position.current_rh != null)
        {
            // reposition to get record handle if we don't have it.

            if (!reposition(scan_position, false))
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                        "can not fail while holding update row lock.");
                }
            }

            this.getLockingPolicy().unlockScanRecordAfterRead(
                scan_position, init_forUpdate);

            scan_position.current_rh   = null;
            scan_position.current_leaf.release();
            scan_position.current_leaf = null;
        }


        // Need to do this unlock in any case, until lock manager provides
        // a way to release locks associated with a compatibility space.  This
        // scan lock is special, as it is a lock on the btree container rather
        // than the heap container.  The open container on the btree actually
        // has a null locking policy so the close of that container does not
        // release this lock, need to explicitly unlock it here or when the
        // scan is closed as part of the abort the lock will not be released.
        if (scan_position.current_scan_pageno != 0)
        {
            this.getLockingPolicy().unlockScan(
                scan_position.current_scan_pageno);
            scan_position.current_scan_pageno = 0;
        }

        scan_position.current_slot = Page.INVALID_SLOT_NUMBER;
        scan_position.current_rh   = null;
        scan_position.current_positionKey  = null;

        initScanParams(
            startKeyValue, startSearchOperator, 
            qualifier, stopKeyValue, stopSearchOperator);

        if (!init_hold)
            this.scan_state = SCAN_INIT;
        else
            this.scan_state = 
                (this.container != null ? SCAN_INIT : SCAN_HOLD_INIT);
    }

    /**
    Reposition the current scan.  This call is semantically the same as if
    the current scan had been closed and a openScan() had been called instead.
    The scan is reopened against the same conglomerate, and the scan
    is reopened with the same "scan column list", "hold" and "forUpdate"
    parameters passed in the original openScan.  
    <p>
    The statistics gathered by the scan are not reset to 0 by a reopenScan(),
    rather they continue to accumulate.
    <p>
    Note that this operation is currently only supported on Heap conglomerates.
    Also note that order of rows within are heap are not guaranteed, so for
    instance positioning at a RowLocation in the "middle" of a heap, then
    inserting more data, then continuing the scan is not guaranteed to see
    the new rows - they may be put in the "beginning" of the heap.

	@param startRowLocation  An existing RowLocation within the conglomerate,
    at which to position the start of the scan.  The scan will begin at this
    location and continue forward until the end of the conglomerate.  
    Positioning at a non-existent RowLocation (ie. an invalid one or one that
    had been deleted), will result in an exception being thrown when the 
    first next operation is attempted.

	@param qualifier An array of qualifiers which, applied
	to each key, restrict the rows returned by the scan.  Rows
	for which any one of the qualifiers returns false are not
	returned by the scan. If null, all rows are returned.

	@exception StandardException Standard exception policy.
    **/
	public void reopenScanByRowLocation(
    RowLocation startRowLocation,
    Qualifier   qualifier[][])
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

	/*
	** Methods of ScanController, which are not supported by btree.
	*/

	/**
	Fetch the location of the current position in the scan.
	@see ScanController#fetchLocation

	@exception  StandardException  Standard exception policy.
	**/
	public void fetchLocation(RowLocation templateLocation)
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}

	/**
	Return a row location object of the correct type to be
	used in calls to fetchLocation.
	@see GenericScanController#newRowLocationTemplate

	@exception  StandardException  Standard exception policy.
	**/
	public RowLocation newRowLocationTemplate()
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}

    /**
    Replace the entire row at the current position of the scan.

    Unimplemented interface by btree, will throw an exception.

	@see ScanController#replace
	@exception  StandardException  Standard exception policy.
    **/
    public boolean replace(DataValueDescriptor[] row, FormatableBitSet validColumns)
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}

	/*
	** Methods of ScanManager
	*/


    /**
    Close the scan, a commit or abort is about to happen.
    **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
        throws StandardException
	{
        if (!init_hold || closeHeldScan)
        {
            // Scan is closed, make sure no access to any state variables
            positionAtDoneScan(scan_position);

            super.close();

            // null out so that these object's can get GC'd earlier.
            this.init_rawtran       = null;
            this.init_template      = null;
            this.init_startKeyValue = null;
            this.init_qualifier     = null;
            this.init_stopKeyValue  = null;

            this.getXactMgr().closeMe(this);

            return(true);
        }
        else
        {

            if (this.scan_state == SCAN_INPROGRESS)
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(scan_position != null);
                }

                if (scan_position.current_positionKey == null)
                {
                    // save position of scan by key rather than location so 
                    // that we can recover if the page with the position 
                    // disappears while we don't have a scan lock.

                    savePosition();
                }
                this.scan_state = SCAN_HOLD_INPROGRESS;
            }
            else if (this.scan_state == SCAN_INIT)
            {
                this.scan_state = SCAN_HOLD_INIT;
            }

            super.close();

            return(false);
        }
	}

    /**
     * Do work necessary to maintain the current position in the scan.
     * <p>
     * Save the current position of the scan as a key.
     * Do whatever is necessary to maintain the current position of the scan.
     * For some conglomerates this may be a no-op.
     *
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    private void savePosition()
		throws StandardException
    {
        if (this.scan_state == SCAN_INPROGRESS)
        {
            // Either current_rh or positionKey is valid - the other is null.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    (scan_position.current_rh == null) == 
                    (scan_position.current_positionKey != null));
            }

            try
            {
                if (scan_position.current_rh != null)
                {
                    // if scan position is not saved by key, then make it so.

                    // must reposition to get the page latched.

                    if (reposition(scan_position, false))
                    {
                        scan_position.current_positionKey = 
                            runtime_mem.get_row_for_export();


                        Page page = scan_position.current_leaf.getPage();


                        RecordHandle rh =
                            page.fetchFromSlot(
                                (RecordHandle) null,
                                page.getSlotNumber(scan_position.current_rh), 
                                scan_position.current_positionKey, 
                                (FetchDescriptor) null,
                                true);

                        if (SanityManager.DEBUG)
                        {
                            SanityManager.ASSERT(rh != null);
                        }

                        scan_position.current_rh = null;
                        scan_position.current_slot = Page.INVALID_SLOT_NUMBER;

                        // release scan lock now that the row is saved away.

                        if (scan_position.current_scan_pageno != 0)
                        {
                            this.getLockingPolicy().unlockScan(
                                scan_position.current_scan_pageno);
                            scan_position.current_scan_pageno = 0;
                        }

                    }
                    else
                    {
                        // this should never happen as we hold the scan lock
                        // on the page while maintaining the position by 
                        // recordhandle - reposition should always work in this
                        // case.

                        if (SanityManager.DEBUG)
                            SanityManager.THROWASSERT(
                                "Must always be able to reposition.");
                    }
                }

            }
            finally
            {

                if (scan_position.current_leaf != null)
                {
                    // release latch on page
                    scan_position.current_leaf.release();
                    scan_position.current_leaf = null;
                }
            }
        }

    }

    /**
     * Do work necessary to maintain the current position in the scan.
     * <p>
     * The latched page in the conglomerate "congomid" is changing, do
     * whatever is necessary to maintain the current position of the scan.
     * For some conglomerates this may be a no-op.
     * <p>
     *
     * @param conglom  Conglomerate object of the conglomerate being changed.
     * @param page      Page in the conglomerate being changed.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void savePosition(Conglomerate conglom, Page page)
        throws StandardException
	{
        // page should be latched by split.  This scan is assuming that latch
        // and reading off it's key from the page under the split's latch.
        // A lock should have already been gotten on this row.


        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(page.isLatched());
        }

        /*
        System.out.println(
            "Saving position in btree at top: " +
            " this.conglomerate = " +  this.conglomerate        +
            " this.scan_state   = " +  this.scan_state);
        SanityManager.DEBUG_PRINT("savePosition()", 
            "Saving position in btree at top: " +
            " this.conglomerate = " +  this.conglomerate        +
            " this.scan_state   = " +  this.scan_state);
        */


        if ((this.getConglomerate() == conglom) &&
            (this.scan_state == SCAN_INPROGRESS))
        {
            // Either current_rh or positionKey is valid - the other is null.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    (scan_position.current_rh == null) == 
                    (scan_position.current_positionKey != null));
            }

            /*
            SanityManager.DEBUG_PRINT("savePosition()", 
                "Saving position in btree: " +
                ";current_scan_pageno = " + this.current_scan_pageno +
                "this.current_rh = " + this.current_rh +
                ";page.getPageNumber() = " + page.getPageNumber() +
                ((this.current_rh != null) ?
                    (";this.current_rh.getPageNumber() = " +
                     this.current_rh.getPageNumber()) : ""));
            */

            if (scan_position.current_rh != null &&
                page.getPageNumber() == 
                    scan_position.current_rh.getPageNumber())
            {
                scan_position.current_positionKey = 
                    runtime_mem.get_row_for_export();

                RecordHandle rh =
                    page.fetchFromSlot(
                        (RecordHandle) null,
                        page.getSlotNumber(scan_position.current_rh), 
                        scan_position.current_positionKey, 
                        (FetchDescriptor) null,
                        true);

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(rh != null);
                }

                scan_position.current_rh = null;
                scan_position.current_slot = Page.INVALID_SLOT_NUMBER;

                // release the scan lock now that we have saved away the row.

                if (scan_position.current_scan_pageno != 0)
                {
                    this.getLockingPolicy().unlockScan(
                        scan_position.current_scan_pageno);
                    scan_position.current_scan_pageno = 0;
                }
            }
        }
	}

    public RecordHandle getCurrentRecordHandleForDebugging()
    {
        return(scan_position.current_rh);
    }

    /*
    ** Standard toString() method.  Prints out current position in scan.
    */
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            String string =
                "\n\tbtree = " + this.getConglomerate() +
                "\n\tscan direction       = " +
                    (this instanceof BTreeForwardScan  ? "forward"  :
                    (this instanceof BTreeMaxScan ? "backward" : 
                                                         "illegal")) +
                "\n\t(scan_state:" +
                (this.scan_state == SCAN_INIT       ? "SCAN_INIT"       :
                 this.scan_state == SCAN_INPROGRESS ? "SCAN_INPROGRESS" :
                 this.scan_state == SCAN_DONE       ? "SCAN_DONE"       :
                 this.scan_state == SCAN_HOLD_INIT  ? "SCAN_HOLD_INIT"  :
                 this.scan_state == SCAN_HOLD_INPROGRESS ? "SCAN_HOLD_INPROGRESS" :
                                                      "BAD_SCAN_STATE") +
                "\n\trh:"  + scan_position.current_rh  +
                "\n\tkey:" + scan_position.current_positionKey + ")"                        +
                "\n\tinit_rawtran = "              + init_rawtran +
                "\n\tinit_hold = "                 + init_hold +
                "\n\tinit_forUpdate = "            + init_forUpdate +
                "\n\tinit_useUpdateLocks = "       + init_useUpdateLocks +
                "\n\tinit_scanColumnList = "       + init_scanColumnList +
                "\n\tinit_scanColumnList.size() = "+ (
                    (init_scanColumnList != null ? 
                        init_scanColumnList.size() : 0)) +
                "\n\tinit_template = "             + 
                    RowUtil.toString(init_template) +
                "\n\tinit_startKeyValue = "        + 
                    RowUtil.toString(init_startKeyValue) +
                "\n\tinit_startSearchOperator = "  + 
                    (init_startSearchOperator == ScanController.GE ? "GE" : 
                    (init_startSearchOperator == ScanController.GT ? "GT" : 
                     Integer.toString(init_startSearchOperator))) +
                "\n\tinit_qualifier[]         = "  + init_qualifier +
                "\n\tinit_stopKeyValue = "         + 
                    RowUtil.toString(init_stopKeyValue) +
                "\n\tinit_stopSearchOperator = "   + 
                    (init_stopSearchOperator == ScanController.GE ? "GE" : 
                    (init_stopSearchOperator == ScanController.GT ? "GT" : 
                     Integer.toString(init_stopSearchOperator)))  +
                "\n\tstat_numpages_visited         = " + 
                    stat_numpages_visited +
                "\n\tstat_numrows_visited          = " + 
                    stat_numrows_visited  +
                "\n\tstat_numrows_qualified        = " + 
                    stat_numrows_qualified +
                "\n\tstat_numdeleted_rows_visited  = " + 
                    stat_numdeleted_rows_visited ;

            return(string);
        }
        else
        {
            return(null);
        }
    }
}
