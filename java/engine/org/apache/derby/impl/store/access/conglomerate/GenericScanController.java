/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.GenericScanController

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ScanInfo;
import org.apache.derby.iapi.store.access.SpaceInfo;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;


import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Properties; 


/**
Generic class implementing shared ScanController methods.

Logically a scancontroller is used to scan a set of rows that meet some 
specified qualification.  Rows that meet the qualification may be operated
upon by the scan to fetch, delete, or replace.  The ScanController also
supports the notion or "repositioning" the scan, which simply resets the
beginning of the scan to a new place, and allows the user to continue from
there.

This class attempts to abstract out some of the parts of the scan such that
maybe multiple access methods can share code, even if they perform parts of
the scan wildly differently.  Here is how the scan has been broken apart:

scan_position - this variable holds the current scan position, it may be 
                extended
                to provide more information if necessary.

scan_state    - a scan has 3 possible states: 
                SCAN_INIT, SCAN_INPROGRESS, SCAN_DONE

positionAtInitScan()
              - This routine is called to move the scan to the SCAN_INIT state.
                It is used both for initialization of the ScanController and
                by reopenScan().

positionAtStartForForwardScan()
              - This routine is called to move the scan from SCAN_INIT to 
                SCAN_INPROGRESS.  Upon return from this routine it is expected
                that scan_position is set such that calling the generic 
                scan loop will reach the first row of the scan.  Note that this
                usually means setting the scan_postion to one before the 1st 
                row to be returned.

fetchRows()   - This routine is the meat of the scan, it moves the scan to the
                next row, applies necessary qualifiers, and handles group or
                non-group operations.  It moves through rows on a page in
                order and then moves to the "next" page.

positionAtNextPage()
              - This routine handles moving the scan from the current 
                scan_position to the next page.

positionAtDoneScan()
              - Handle all cleanup associated with moving the scan state from
                SCAN_INPROGRESS to SCAN_DONE.  This may include releasing locks,
                and setting the state of the scan.  This does not close the 
                scan, it allows for a reopenScan() to be called.
**/

public abstract class GenericScanController 
    extends GenericController implements ScanManager
{

    /**************************************************************************
     * Constants of the class
     **************************************************************************
     */

    /*
     * There are 5 states a scan can be in.
     *     SCAN_INIT - A scan has started but no positioning has been done.
     *                 The scan will be positioned when the first next() call
     *                 has been made.  None of the positioning state variables
     *                 are valid in this state.
     *     SCAN_INPROGRESS -
     *                 A scan is in this state after the first next() call.
     *                 On exit from any GenericScanController method, while in 
     *                 this state,
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
    public static final int    SCAN_INIT             = 1;
    public static final int    SCAN_INPROGRESS       = 2;
    public static final int    SCAN_DONE             = 3;
    public static final int    SCAN_HOLD_INIT        = 4;
    public static final int    SCAN_HOLD_INPROGRESS  = 5;

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**
     * The following group of fields are all basic input parameters which are
     * provided by the calling code when doing a scan.
     * These are just saved values from what was initially input.
     **/
	private FormatableBitSet                 init_scanColumnList;
    private DataValueDescriptor[]   init_startKeyValue;
    private int                     init_startSearchOperator;
    private Qualifier[][]           init_qualifier;
    private DataValueDescriptor[]   init_stopKeyValue;
    private int                     init_stopSearchOperator;

    private FetchDescriptor init_fetchDesc;

    /**
     * Delay positioning the table at the start position until the first
     * next() call.
     */
    private int         scan_state;

    
    /**
     * The position for the current scan.  The can be maintained in any
     * of the following ways:
     *     record handle - scan_position.current_rh:
     *         The scan maintains it's position using the record handle while
     *         it does not have a latch on the page, which is the case anytime
     *         control leaves access.  The access method must take appropriate
     *         steps to make sure the record handle will still be valid when
     *         the scan needs to reposition using the record handle.
     *     slot number   - scan_position.current_slot:
     *         While the scan has a latch on the page the scan is positioned
     *         using the slot number as the order of the rows cannot change
     *         while the latch is held (unless the holder of the latch causes
     *         them to move).  
     *     page number   - (RESOLVE - TODO)
     *         Sometimes it would be interesting to position a scan "between"
     *         pages, such that the next time the scan starts is starts at
     *         the next page.  This would allow us to efficiently do group
     *         scans returning page at atime results.  
     *         NOT IMPLEMENTED CURRENTLY.
     **/
    protected RowPosition         scan_position;

    /**
     * Performance counters ...
     */
    protected int stat_numpages_visited         = 0;
    protected int stat_numrows_visited          = 0;
    protected int stat_numrows_qualified        = 0;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private methods of This class:
     **************************************************************************
     */

    private final void repositionScanForUpateOper()
		throws StandardException
    {
        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                    SQLState.AM_SCAN_NOT_POSITIONED);


        if (!open_conglom.latchPage(scan_position))
        {
            throw StandardException.newException(
                    SQLState.AM_RECORD_NOT_FOUND, 
                    open_conglom.getContainer().getId(),
                    new Long(scan_position.current_rh.getId()));
        }

        if (open_conglom.isUseUpdateLocks())
        {
            // we only have an U lock at this point which was acquired when the
            // scan positioned on the row, need to request an
            // X lock before we can actually perform the delete

            open_conglom.lockPositionForWrite(
                scan_position, false /* not insert */, true);
        }
    }


    /**************************************************************************
     * Protected methods implementing mechanics of scanning rows:
     *
     *     positionAtInitScan()             - move scan state to SCAN_INIT
     *     positionAtStartForForwardScan()  - SCAN_INIT -> SCAN_INPROGRESS
     *     positionAtResumeScan()           - reposition after losing scan latch
     *     fetchRows()                      - move scan while in SCAN_INPROGRESS
     *     positionAtNextPage()             - move page while in SCAN_INPROGRESS
     *     positionAtDoneScan()             - SCAN_INPROGRESS -> SCAN_DONE
     *
     **************************************************************************
     */

    /**
     * Move scan to the the SCAN_INIT state.
     * <p>
     * This routine is called to move the scan to the SCAN_INIT state.
     * It is used both for initialization of the ScanController and
     * by reopenScan().
     **/
	protected void positionAtInitScan(
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    Qualifier               qualifier[][],
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator,
    RowPosition             pos)
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

        // TODO (mikem) - this could be more efficient, by writing
        // code to figure out length of row, but scratch row is cached
        // so allocating it here is probably not that bad.
        init_fetchDesc = 
            new FetchDescriptor(
              (open_conglom.getRuntimeMem().get_scratch_row()).length,
              init_scanColumnList,
              init_qualifier);

        // stopKeyValue init.
	    this.init_stopKeyValue          = stopKeyValue;
        if (RowUtil.isRowEmpty(this.init_stopKeyValue, (FormatableBitSet) null))
            this.init_stopKeyValue = null;

        // stopSearchOperator init.
	    this.init_stopSearchOperator    = stopSearchOperator;

        // reset the "current" position to starting condition.
        pos.init();


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

        // Scan is fully initialized and ready to go.
        scan_state = SCAN_INIT;
    }


    /**
     * Reposition the scan upon entering the fetchRows loop.
     * <p>
     * Called upon entering fetchRows() while in the SCAN_INPROGRESS state.
     * Do work necessary to look at rows in the current page of the scan.
     * <p>
     * The default implementation uses a record handle to maintain a scan
     * position.  It will get the latch again on the current
     * scan position and set the slot to the current record handle.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtResumeScan(
    RowPosition pos)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                scan_position.current_rh != null, this.toString()); 
        }

        // reposition the scan at the row just before the next one to return.
        // This routine handles the mess of repositioning if the row or the
        // page has disappeared. This can happen if a lock was not held on the
        // row while not holding the latch.
        open_conglom.latchPageAndRepositionScan(scan_position);
    }

    /**
     * Move the scan from SCAN_INIT to SCAN_INPROGRESS.
     * <p>
     * This routine is called to move the scan from SCAN_INIT to 
     * SCAN_INPROGRESS.  Upon return from this routine it is expected
     * that scan_position is set such that calling the generic 
     * scan loop will reach the first row of the scan.  Note that this
     * usually means setting the scan_postion to one before the 1st 
     * row to be returned.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtStartForForwardScan(
    RowPosition pos)
        throws StandardException
    {
        if (pos.current_rh == null)
        {
            // 1st positioning of scan (delayed from openScan).
            pos.current_page = 
                open_conglom.getContainer().getFirstPage();

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    pos.current_page.getPageNumber() == 
                    ContainerHandle.FIRST_PAGE_NUMBER);

                if (pos.current_page.recordCount() < 1)
                    SanityManager.THROWASSERT(
                        "record count = " + pos.current_page.recordCount());
            }

            // set up for scan to continue at beginning of first page just
            // after first first control row on first page.
            pos.current_slot = Page.FIRST_SLOT_NUMBER;
        }
        else
        {
            // 1st positioning of scan following a reopenScanByRowLocation

            // reposition the scan at the row just before the next one to 
            // return.  This routine handles the mess of repositioning if the 
            // row or the page has disappeared. This can happen if a lock was 
            // not held on the row while not holding the latch.
            open_conglom.latchPageAndRepositionScan(pos);

            // set up for scan to at the specified record handle (position one
            // before it so that the loop increment and find it).
            pos.current_slot -= 1;
        }

        pos.current_rh    = null;
        this.stat_numpages_visited  = 1;
        this.scan_state             = SCAN_INPROGRESS;
    }

    /**
     * Position scan to slot before first slot on next page.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtNextPage(
    RowPosition pos)
        throws StandardException
    {
        // The current_page can become null, in a rare multi-user case, where
        // all pages in the heap are deallocated, in the middle of the scan
        // loop, when no latches are held, and the scan is waiting on a lock.
        // In this case the lockPositionForRead code, has nowhere good to 
        // position the scan, so it just sets the page to null and returns.
        if (pos.current_page != null)
        {
            // save current page number.
            long pageid = pos.current_page.getPageNumber();

            // unlatch old page.
            pos.unlatch();

            // latch page after current page number.
            pos.current_page = 
                open_conglom.getContainer().getNextPage(pageid);

            // set up for scan to continue at beginning of this new page.
            pos.current_slot = Page.FIRST_SLOT_NUMBER - 1;
        }
    }

    /**
     * Do any necessary work to complete the scan.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void positionAtDoneScan(
    RowPosition pos)
        throws StandardException
    {
        // Unlatch current page if any.
        pos.unlatch();

        // unlock the previous row.
        if (scan_position.current_rh != null)
        {
            open_conglom.unlockPositionAfterRead(scan_position);
            scan_position.current_rh = null;
        }

        this.scan_state = SCAN_DONE;
    }

	public void reopenScanByRowLocation(
    RowLocation startRowLocation,
    Qualifier qualifier[][])
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

    /**************************************************************************
     * Protected methods of This class:
     **************************************************************************
     */

    /**
     * Create object which represents the scan position.
     * <p>
     * Designed so that extending classes can override and allocate 
     * implementation specific row position's.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected RowPosition allocateScanPosition()
        throws StandardException
    {
        return(new RowPosition());
    }

    /**
     * Fetch the next N rows from the table.
     * <p>
     * Utility routine used by both fetchSet() and fetchNextGroup().
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected int fetchRows(
    DataValueDescriptor[][] row_array,
    RowLocation[]           rowloc_array,
    BackingStoreHashtable   hash_table,
    long                    max_rowcnt,
    int[]                   key_column_numbers)
        throws StandardException
	{
        int                     ret_row_count           = 0;
        DataValueDescriptor[]   fetch_row               = null;

        if (max_rowcnt == -1)
            max_rowcnt = Long.MAX_VALUE;

        if (SanityManager.DEBUG)
        {
            if (row_array != null)
            {
                SanityManager.ASSERT(row_array[0] != null,
                    "first array slot in fetchNextGroup() must be non-null.");
                SanityManager.ASSERT(hash_table == null);
            }
            else
            {
                SanityManager.ASSERT(hash_table != null);
            }
        }

        if (this.scan_state == SCAN_INPROGRESS)
        {
            positionAtResumeScan(scan_position);
        }
        else if (this.scan_state == SCAN_INIT)
        {
            positionAtStartForForwardScan(scan_position);

        }
        else if (this.scan_state == SCAN_HOLD_INPROGRESS)
        {
            open_conglom.reopen();

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    scan_position.current_rh != null, this.toString()); 
            }

            // reposition the scan at the row just before the next one to 
            // return.
            // This routine handles the mess of repositioning if the row or 
            // the page has disappeared. This can happen if a lock was not 
            // held on the row while not holding the latch.
            open_conglom.latchPageAndRepositionScan(scan_position);

            this.scan_state = SCAN_INPROGRESS;
        }
        else if (this.scan_state == SCAN_HOLD_INIT)
        {
            open_conglom.reopen();

            positionAtStartForForwardScan(scan_position);

        }
        else
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(this.scan_state == SCAN_DONE);

            return(0);
        }

        // At this point:
        // scan_position.current_page is latched.  
        // scan_position.current_slot is the slot on scan_position.current_page
        // just before the "next" record this routine should process.

        // loop through successive pages and successive slots on those
        // pages.  Stop when either the last page is reached 
        // (scan_position.current_page will be null).  
        // Along the way apply qualifiers to skip rows which don't qualify.

		while (scan_position.current_page != null)
		{
			while ((scan_position.current_slot + 1) < 
                    scan_position.current_page.recordCount())
			{
                // unlock the previous row.
                if (scan_position.current_rh != null)
                {
                    open_conglom.unlockPositionAfterRead(scan_position);

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
                              open_conglom.getRuntimeMem().get_row_for_export();
                        }

                        fetch_row = row_array[ret_row_count];
                    }
                    else
                    {
                        fetch_row = 
                            open_conglom.getRuntimeMem().get_row_for_export();
                    }
                }

                // move scan current position forward.
                scan_position.positionAtNextSlot();

                // Lock the row.
                boolean lock_granted_while_latch_held = 
                    open_conglom.lockPositionForRead(
                        scan_position, (RowPosition) null, true, true);

                if (!lock_granted_while_latch_held)
                {
                    // if lock could not be granted while holding
                    // latch, then the row may either be on the same page 
                    // or it may no longer exist, this implementation does not
                    // handle rows which move to different pages.  
                    // 
                    // If the row moved on the same page then 
                    // lockPositionForRead() will have automatically updated
                    // the scan_postion argument to point to it, and we 
                    // wil now have a latch and a lock on that row.
                    //
                    // If the row no longer exists then the 
                    // "moveForwardIfRowDisappears" argument makes this routine
                    // find the "next" row in the heap and position on it.  If
                    // a valid row exists in the current page to position on,
                    // then lockPositionForRead() will position on it, get
                    // a lock on it, and return with a latch on the page.  
                    // Otherwise the routine will return with current_slot == -1
                    // and it is up to this routine to continue the scan as
                    // normal at the top of the loop.

                    if (scan_position.current_page == null)
                    {
                        // page has been unlatched and the scan is done, there
                        // are no more pages.  getNextPage() has been coded to
                        // handle a null current_page.

                        break;
                    }
                    else if (scan_position.current_slot == -1)
                    {
                        // This means that lockPositionForRead() had to 
                        // reposition the scan forward to a new page, because 
                        // the row the scan was locking was purged, when the 
                        // latch was released to wait on the lock.  In this 
                        // case just jump back to the top of loop and continue 
                        // scan.

                        if (SanityManager.DEBUG)
                        {
                            SanityManager.ASSERT(
                                scan_position.current_rh == null);
                        }

                        continue;
                    }
                }

                this.stat_numrows_visited++;

                // lockRowAtPosition set pos.current_rh as part of getting lock.
                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(scan_position.current_rh != null);

                    // make sure current_rh and current_slot are in sync
                    if (scan_position.current_slot !=
                            scan_position.current_page.getSlotNumber(
                                scan_position.current_rh))
                    {
                        SanityManager.THROWASSERT(
                            "current_slot = " + scan_position.current_slot +
                            "current_rh = " + scan_position.current_rh +
                            "current_rh.slot = " + 
                            scan_position.current_page.getSlotNumber(
                                scan_position.current_rh));
                    }
                }

                // fetchFromSlot returns null if row does not qualify.

                scan_position.current_rh_qualified =
                    (scan_position.current_page.fetchFromSlot(
                        scan_position.current_rh, 
                        scan_position.current_slot, 
                        fetch_row, 
                        init_fetchDesc,
                        false) != null);

                if (scan_position.current_rh_qualified)
                {
                    // qualifying row.  


                    // scan_position.current_rh is save position of scan while 
                    // latch is not held.  It currently points at the 
                    // scan_position.current_slot in search (while latch is 
                    // held).
                    if (SanityManager.DEBUG)
                    {
                        // make sure current_rh and current_slot are in sync
                        SanityManager.ASSERT(
                            scan_position.current_slot ==
                                scan_position.current_page.getSlotNumber(
                                    scan_position.current_rh));
                    }

                    // Found qualifying row.  Done fetching rows for the group?
                    ret_row_count++;
                    stat_numrows_qualified++;


                    if (hash_table == null)
                    {
                        if (rowloc_array != null)
                        {
                            // if requested return the associated row location.
                            setRowLocationArray(
                                rowloc_array, ret_row_count - 1, scan_position);
                        }

                        fetch_row = null;
                    }
                    else
                    {
                        if (hash_table.put(false, fetch_row))
                        {
                            // The row was inserted into the hash table so we
                            // need to create a new row next time through.
                            fetch_row = null;
                        }
                    }

                    if (max_rowcnt <= ret_row_count) 
                    {
                        // exit fetch row loop and return to the client.
                        scan_position.unlatch();

                        if (SanityManager.DEBUG)
                        {
                            SanityManager.ASSERT(
                                scan_position.current_rh != null);
                        }

                        return(ret_row_count);
                    }
                }
			}

            positionAtNextPage(scan_position);

            this.stat_numpages_visited++;
		}

        // Reached last page of scan.
        positionAtDoneScan(scan_position);

        // we need to decrement when we stop scan at the end of the table.
        this.stat_numpages_visited--;

		return(ret_row_count);
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
    protected void reopenScanByRecordHandle(
    RecordHandle startRecordHandle,
    Qualifier qualifier[][])
        throws StandardException
    {
        // initialize scan position parameters at beginning of scan
        this.scan_state = 
            (!open_conglom.getHold() ? SCAN_INIT : SCAN_HOLD_INIT);

        // position the scan at the row before the given record id, so that
        // the first "next" starts on the given row.
        scan_position.current_rh = startRecordHandle;
    }

    protected void setRowLocationArray(
    RowLocation[]   rowloc_array,
    int             index,
    RowPosition     pos)
        throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }


    /**************************************************************************
     * abstract protected Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */
	public void init(
    OpenConglomerate                open_conglom,
	FormatableBitSet				            scanColumnList,
    DataValueDescriptor[]	        startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]	        stopKeyValue,
    int                             stopSearchOperator)
        throws StandardException
    {
        super.init(open_conglom);

        // RESOLVE (mikem) - move this into runtime_mem
        scan_position = allocateScanPosition();

        // remember inputs
        init_scanColumnList         = scanColumnList;

        positionAtInitScan(
            startKeyValue,
            startSearchOperator,
            qualifier,
            stopKeyValue,
            stopSearchOperator,
            scan_position);
    }


    public final int getNumPagesVisited()
    {
        return(stat_numpages_visited);
    }
    public final int getNumRowsVisited()
    {
        return(stat_numrows_visited);
    }
    public final int getNumRowsQualified()
    {
        return(stat_numrows_qualified);
    }
    public final FormatableBitSet getScanColumnList()
    {
        return(init_scanColumnList);
    }
    public final DataValueDescriptor[] getStartKeyValue()
    {
        return(init_startKeyValue);
    }
    public final int getStartSearchOperator()
    {
        return(init_startSearchOperator);
    }
    public final DataValueDescriptor[] getStopKeyValue()
    {
        return(init_stopKeyValue);
    }
    public final int getStopSearchOperator()
    {
        return(init_stopSearchOperator);
    }
    public final Qualifier[][] getQualifier()
    {
        return(init_qualifier);
    }


    public final int getScanState()
    {
        return(scan_state);
    }
    public final void setScanState(int state)
    {
        scan_state = state;
    }
    public final RowPosition getScanPosition()
    {
        return(scan_position);
    }
    public final void setScanPosition(RowPosition   pos)
    {
        scan_position = pos;
    }

    /**************************************************************************
     * Public Methods implementing ScanController:
     **************************************************************************
     */
    private void closeScan()
        throws StandardException
    {
        super.close();

		// If we are closed due to catching an error in the middle of init,
		// xact_manager may not be set yet. 
		if (open_conglom.getXactMgr() != null)
			open_conglom.getXactMgr().closeMe(this);

        // help the garbage collector.
        this.init_qualifier         = null;
        init_scanColumnList         = null;
        init_startKeyValue          = null;
        init_stopKeyValue           = null;
    }

    public void close()
        throws StandardException
	{
        // Finish the scan - this may release locks if read committed and scan
        // still holds some locks, and close comes before scan.next() returned
        // that scan was done.
        positionAtDoneScan(scan_position);

        closeScan();
	}

    public boolean closeForEndTransaction(
    boolean closeHeldScan)
        throws StandardException
	{
        if ((!open_conglom.getHold()) || closeHeldScan) 
        {
            // close the scan as part of the commit/abort

            this.scan_state = SCAN_DONE;

            closeScan();

            return(true);
        }
        else
        {
            super.close();

            // allow the scan to continue after the commit.
            // locks and latches will be released as part of the commit, so
            // no need to release them by hand.

            if (this.scan_state == SCAN_INPROGRESS)
                this.scan_state = SCAN_HOLD_INPROGRESS;
            else if (this.scan_state == SCAN_INIT)
                this.scan_state = SCAN_HOLD_INIT;


            return(false);
        }
	}


    /**
	@see ScanController#delete
	**/
    public boolean delete()
		throws StandardException
	{
        repositionScanForUpateOper();

        boolean ret_val = true;

        // RESOLVE (mikem) - RECID - performance could be better if we did not
        // have to call isDeletedAtSlot().

        // RESOLVE (mikem) - share code below with conglomerateController.

        if (scan_position.current_page.isDeletedAtSlot(
                scan_position.current_slot))
        {
            ret_val = false;
        }
        else
        {
            // Delete the row 
            scan_position.current_page.deleteAtSlot(
                scan_position.current_slot, true, (LogicalUndo) null);

            if (scan_position.current_page.nonDeletedRecordCount() == 0)
            {
                queueDeletePostCommitWork(scan_position);
            }
        }

        scan_position.unlatch();

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
     * RESOLVE (mikem-05/29/98) - Implement this when we support levels of
     * concurrency less than serializable.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void didNotQualify()
        throws StandardException
    {
    }

    /**
     * Insert all rows that qualify for the current scan into the input
     * Hash table.  
     * <p>
     * This routine scans executes the entire scan as described in the 
     * openScan call.  For every qualifying unique row value an entry is
     * placed into the HashTable. For unique row values the entry in the
     * Hashtable has a key value of the object stored in 
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
        fetchRows(
            (DataValueDescriptor[][]) null,
            (RowLocation[]) null,
            hash_table,
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

    @param template A prototypical row which the scan may use ot
	maintain its position in the conglomerate.  Not all access method
	scan types will require this, if they don't it's ok to pass in null.
    In order to scan a conglomerate one must allocate 2 separate "row"
    templates.  The "row" template passed into openScan is for the private
    use of the scan itself, and no access to it should be made
    by the caller while the scan is still open.  Because of this the 
    scanner must allocate another "row" template to hold the values returned 
    from fetch().

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
	public void reopenScan(
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    Qualifier               qualifier[][],
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            if (!open_conglom.getHold())
            {
                SanityManager.ASSERT(
                    !open_conglom.isClosed(), 
                    "GenericScanController.reopenScan() called on a non-held closed scan.");
            }
        }

        // initialize scan position parameters at beginning of scan
        this.scan_state = 
            (!open_conglom.getHold() ? SCAN_INIT : SCAN_HOLD_INIT);

        scan_position.current_rh   = null;
    }

    /**
	@see ScanController#replace
	**/
    public boolean replace(
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
        repositionScanForUpateOper();

        boolean ret_val = 
            scan_position.current_page.update(
                scan_position.current_rh, row, validColumns);

        scan_position.unlatch();

        return(ret_val);
	}

    /**
    Returns true if the current position of the scan still qualifies
    under the set of qualifiers passed to the openScan().  When called
    this routine will reapply all qualifiers against the row currently
    positioned and return true if the row still qualifies.  If the row
    has been deleted or no longer passes the qualifiers then this routine
    will return false.
    
    This case can come about if the current scan
    or another scan on the same table in the same transaction 
    deleted the row or changed columns referenced by the qualifier after 
    the next() call which positioned the scan at this row.  

    Note that for comglomerates which don't support update, like btree's, 
    there is no need to recheck the qualifiers.

    The results of a fetch() performed on a scan positioned on 
    a deleted row are undefined.

	@exception StandardException Standard exception policy.
    **/
    public boolean doesCurrentPositionQualify()
		throws StandardException
    {
        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                    SQLState.AM_SCAN_NOT_POSITIONED);

        if (!open_conglom.latchPage(scan_position))
        {
            return(false);
        }

        DataValueDescriptor row[] = 
            open_conglom.getRuntimeMem().get_scratch_row();

        // If fetchFromSlot returns null it either means the row is deleted,
        // or the qualifier evaluates to false.
        
        boolean ret_val = 
            (scan_position.current_page.fetchFromSlot(
                scan_position.current_rh, 
                scan_position.current_slot, 
                row,
                init_fetchDesc,
                false) != null);

        scan_position.unlatch();

        return(ret_val);
    }

    /**
	@see ScanController#fetch
	**/
	public void fetch(DataValueDescriptor[] row)
		throws StandardException
	{
        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                    SQLState.AM_SCAN_NOT_POSITIONED);

        if (!open_conglom.latchPage(scan_position))
        {
            throw StandardException.newException(
                    SQLState.AM_RECORD_NOT_FOUND, 
                    open_conglom.getContainer().getId(),
                    new Long(scan_position.current_rh.getId()));
        }

        // RESOLVE (mikem) - should this call apply the qualifiers again?
        RecordHandle rh = 
            scan_position.current_page.fetchFromSlot(
                scan_position.current_rh, 
                scan_position.current_slot, 
                row, 
                init_fetchDesc, 
                false);

        scan_position.unlatch();

        if (rh == null)
        {
            /*
            if (SanityManager.DEBUG)
            {
                if (isCurrentPositionDeleted())
                    SanityManager.THROWASSERT(
                        "The record (" + 
                        open_conglom.getContainer().getId() +
                        ", " +
                        scan_position.current_rh.getPageNumber() + ", " +
                        scan_position.current_rh.getId() + ") " +
                        "being fetched is marked deleted on page.:\n");
            }
            */

            throw StandardException.newException(
                    SQLState.AM_RECORD_NOT_FOUND, 
                    open_conglom.getContainer().getId(),
                    new Long(scan_position.current_rh.getId()));
        }

        return;
	}

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
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
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
        if (scan_state != SCAN_INPROGRESS)
            throw StandardException.newException(
                    SQLState.AM_SCAN_NOT_POSITIONED);

        if (!open_conglom.latchPage(scan_position))
        {
            return(true);
        }

        boolean ret_val = 
            scan_position.current_page.isDeletedAtSlot(
                scan_position.current_slot);

        scan_position.unlatch();

        return(ret_val);
    }
}
