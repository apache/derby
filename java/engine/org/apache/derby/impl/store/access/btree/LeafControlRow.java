/*

   Derby - Class org.apache.derby.impl.store.access.btree.LeafControlRow

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
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.TypedFormat;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.PrintStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * @format_id ACCESS_BTREE_LEAFCONTROLROW_V1_ID
 *
 * @purpose   Btree pages all have a control row at the front of every page.  To
 *            determine the type of row, read the first column which is a format
 *            id and it tells what kind of control row it is.
 *
 * @upgrade   This format was made obsolete in the kimono release.
 *
 * @disk_layout 
 * column 1 - control row type         : StorableFormatId
 * column 2 - left sibling page number : SQLLongint
 * column 3 - right sibling page number: SQLLongint
 * column 4 - parent page number       : SQLLongint
 * column 5 - level number (0 is leaf) : SQLLongint
 * column 6 - isRoot                   : SQLLongint
 * column 7 - Conglomerate object      : null unless it is root else
 *                                       a Conglomerate object, matching
 *                                       that of current table.
 *                                       Currently this field
 *                                       is only used by logical undo and
 *                                       the type of object is inferred by
 *                                       the logical undo code.
 **/

public class LeafControlRow extends ControlRow
{
	/*
	** Constructors of BranchControlRow
	*/

    /**
     * No arg constructor.
     * <p>
     * Public no arg constructor is for the monitor to call for format
     * id implemenation, it should not be called for any other reason.
     **/
    public LeafControlRow()
    {
    }

    /**
     * Constructs a leaf-page control row, for a newly allocated leaf page.  
     *
     * @param btree     The open btree to allocate this page from.
     * @param page      The newly allocated page where the control row will
     *                  be inserted.
     * @param parent    The parent of the leaf page.  Set to null for root.
     *                  RESOLVE (mikem) - set to null otherwise?
     * @param isRoot    Is this page the root of the tree?
     *
     * @exception StandardException Standard exception policy.
     */
    LeafControlRow(
    OpenBTree         btree,
    Page		      page, 
    ControlRow	      parent,
    boolean           isRoot)
            throws StandardException
    {
        // All leaf pages are at level 0.
        super(btree, page, 0, parent, isRoot);
    }

    /* Private/Protected methods of This class: */

    /**
     * Allocate a new leaf page to the conglomerate.
     *
     * @param btree     The open conglomerate from which to get the leaf from
     * @param parent    The parent page of the newly allocated page, null if
     *                  allocating root page.
     * 
     * @exception StandardException Standard exception policy.
     */
    private static LeafControlRow Allocate(
    OpenBTree   btree, 
    ControlRow  parent)
        throws StandardException
    {
        Page      page      = btree.container.addPage();

        // Create a control row for the new page.
        LeafControlRow control_row = 
            new LeafControlRow(btree, page, parent, false);

        // Insert the control row on the page, in the first slot on the page.
        // This operation is only done as part of a new tree or split, which
        // which both will be undone physically so no logical undo record is
        // needed.
		byte insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_DEFAULT;
        RecordHandle rh = 
            page.insertAtSlot(Page.FIRST_SLOT_NUMBER,
                control_row.getRow(),
                (FormatableBitSet) null, 
                (LogicalUndo) null, insertFlag,
				AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD);

        if (SanityManager.DEBUG)
        {
            RecordHandle    rh2 = null;

            rh2 = page.fetchFromSlot(
                    (RecordHandle) null, page.FIRST_SLOT_NUMBER, 
                    new DataValueDescriptor[0], (FetchDescriptor) null, true); 

            SanityManager.ASSERT(rh.getId() == rh2.getId() &&
                                 rh.getPageNumber() == rh2.getPageNumber());
        }

        // Page is returned latched.
        return(control_row);
    }

    /**
     * Return the number of non-deleted rows from slot 1 through "startslot"
     * <p>
     * Return the number of non-deleted rows that exist on the page starting
     * at slot one through "startslot".
     * <p>
     * RESOLVE (mikem) - is the expense of this routine worth it, it is only
     * used for costing.  Could an estimate from the nonDeletedRecordCount()
     * be used instead?
     *
	 * @return The requested non_deleted_row_count.
     *
     * @param startslot  Count non deleted row up to and including this slot.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private float get_left_nondeleted_rowcnt(
    int startslot)
        throws StandardException
    {
        int non_deleted_row_count = 0;

        for (int slot = 1; slot <= startslot; slot++)
        {
            if (!this.page.isDeletedAtSlot(slot))
            {
                non_deleted_row_count++;
            }
        }
        return(non_deleted_row_count);
    }


    /* Public Methods of LeafControlRow class: */

    /**
     * Perform page specific initialization.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     **/
    protected final void ControlRowInit()
    {
    }

    /**
     * Initialize conglomerate with one page, to be a 1 page btree.
     *
     * Given a conglomerate which already has one page allocated to it, 
     * initialize the page to be a leaf-root page with no entries.  Allocate
     * the control row and store it on the page.
     *
     * @param open_btree The open btree to initialize (container is open).
     *
     * @exception StandardException Standard exception policy.
     */
    public static void initEmptyBtree(
    OpenBTree   open_btree)
        throws StandardException
    {
        Page page = 
            open_btree.container.getPage(ContainerHandle.FIRST_PAGE_NUMBER);

        // create a leaf control row for root page of a single page index //
        LeafControlRow control_row =
            new LeafControlRow(open_btree, page, null, true);

		byte insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_DEFAULT;
        RecordHandle rh = 
            page.insertAtSlot(
                Page.FIRST_SLOT_NUMBER,
                control_row.getRow(),
                (FormatableBitSet) null,
                (LogicalUndo) null, insertFlag,
				AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD);

        if (SanityManager.DEBUG)
        {
            RecordHandle    rh2 = null;

            rh2 = page.fetchFromSlot(
                    (RecordHandle) null, 
                    Page.FIRST_SLOT_NUMBER, 
                    new DataValueDescriptor[0], (FetchDescriptor) null, true); 

            SanityManager.ASSERT(rh.getId() == rh2.getId() &&
                                 rh.getPageNumber() == rh2.getPageNumber());
        }

        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
            {
                control_row.checkConsistency(
                    open_btree, (ControlRow) null, true);
            }
        }

        page.unlatch();

        return;
    }

	/*
	** Non - Debug/consistency check Methods of ControlRow:
	*/


    /**
     * Get the number of columns in the control row.  
     * <p>
     * Control rows all share the first columns as defined by this class and
     * then add columns to the end of the control row.  For instance a branch
     * control row add a child page pointer field.
     * <p>
     *
	 * @return The total number of columns in the control row.
     **/
    protected final int getNumberOfControlRowColumns()
    {
        return(this.CR_NCOLUMNS);
    }

    /**
     * Is the current page the leftmost leaf of tree?
     * <p>
     *
	 * @return true if the current page is the leftmost leaf of the tree,
     *              else return false.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean isLeftmostLeaf()
		throws StandardException
    {
        return(getleftSiblingPageNumber() == 
               ContainerHandle.INVALID_PAGE_NUMBER); 
    }

    /**
     * Is the current page the rightmost leaf of tree?
     * <p>
     *
	 * @return true if the current page is the rightmost leaf of the tree,
     *              else return false.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean isRightmostLeaf()
		throws StandardException
    {
        return(getrightSiblingPageNumber() == 
               ContainerHandle.INVALID_PAGE_NUMBER); 
    }


	/**
	 ** Perform a search of this leaf page, ultimately returning the latched
	 ** leaf page and row slot after which the given key belongs.
	 ** The slot is returned in the result structure.  If the key
	 ** exists on the page, the result.exact will be true.  Otherwise,
	 ** result.exact will be false, and the row slot returned will be
	 ** the one immediately preceding the position at which the key
	 ** belongs.
     *
     * @exception StandardException Standard exception policy.
	 **/
	public ControlRow search(
        SearchParameters    sp)
            throws StandardException
    {
        searchForEntry(sp);

        if (sp.searchForOptimizer)
        {
            // Update left_fraction to be used to estimate the number of
            // rows left of the current search location.

            // after the code below startslot will be the slot that is one
            // before the first slot to be returned by the scan positioning
            // for this key, including GT/GE positioning.  This is exactly
            // what the LeafControlRow.positionAtStartForForwardScan() does,
            // to position for the start of a scan.

            int startslot = sp.resultSlot;

            if (sp.resultExact)
            {
                // we found exactly the row we are looking for.

                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(sp.resultSlot > 0);

                // RESOLVE (mikem) - add in a search operator argument so that 
                //     below can be if (op == ScanController.GE)

                if (sp.partial_key_match_op == 
                        SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH)
                {
                    // This means the scan was positioned for GE rather than GT
                    startslot--;
                }
            }

            // non_deleted_left_row is the number of actual rows left of the 
            // first row to be returned by a scan positioned as requested.  
            // The 0th slot is a control row which is not counted.
            float non_deleted_left_rows = get_left_nondeleted_rowcnt(startslot);

            int   non_deleted_row_count = this.page.nonDeletedRecordCount();

            // System.out.println(
            //   "\n\t non_deleted_row_count = " + non_deleted_row_count +
            // "\n\t non_deleted_left_rows = " + non_deleted_left_rows +
            // "\n\t startslot = " + startslot);

            if (this.getIsRoot())
            {
                sp.current_fraction = 1;
                sp.left_fraction    = 0;
            }

            // calculate the fraction of rows in the table which are left of
            // the current slot in the search.  After the search is completed
            // (sp.left_fraction * number of rows), is the estimated number
            // of rows to the left of the current row.

            if (non_deleted_row_count > 1)
                sp.left_fraction    += 
                    (sp.current_fraction) * 
                    (non_deleted_left_rows / (non_deleted_row_count - 1));

            // no-one really uses current fraction after leaf is through with
            // it.  Set it to help diagnose algorithm.
            if (non_deleted_row_count > 1)
                sp.current_fraction = 
                    (sp.current_fraction) * 
                    (((float) 1) / (non_deleted_row_count - 1));
        }

        return(this);
    }
	
    /**
     * Search and return the left most leaf page.
     * <p>
	 * Perform a recursive search, ultimately returning the
     * leftmost leaf page which is the first leaf page in the
	 * leaf sibling chain.  (This method might better be called
	 * getFirstLeafPage()).
     *
	 * @return The leftmost leaf page.
     *
     * @param btree  The open btree to associate latches/locks with.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected ControlRow searchLeft(OpenBTree btree)
        throws StandardException
    {
        return(this);
    }

    /**
     * Search and return the right most leaf page.
     * <p>
	 * Perform a recursive search, ultimately returning the
	 * rightmost leaf page which is the last leaf page in the
	 * leaf sibling chain.  (This method might better be called
	 * getLastLeafPage()).
     *
	 * @return The rightmost leaf page.
     *
     * @param btree  The open btree to associate latches/locks with.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected ControlRow searchRight(OpenBTree btree)
        throws StandardException
    {
        return(this);
    }


	/**
	 **	Perform a recursive shrink operation for the key.
	 ** If this method returns true, the caller should
	 ** remove the corresponding entry for the page.
	 ** This routine is not guaranteed to successfully
	 ** shrink anything.  The page lead to by the key might
	 ** turn out not to be empty by the time shrink gets
	 ** there, and shrinks will give up if there is a deadlock.
	 ** <P>
	 ** The receiver page must be latched on entry and is
	 ** returned unlatched.
     *
     * @exception StandardException Standard exception policy.
	 **/
	protected boolean shrinkFor(
    OpenBTree               btree, 
    DataValueDescriptor[]   key)
        throws StandardException
    {
        boolean shrink_me = false;

        try
        {
            // If this page is empty (ie. only has a control row), and it's not 
            // the root page, unlink it.  An empty btree consists of
            // simply an empty leaf-root page.

            // RESOLVE (mikem) - may want this routine to try to purge 
            // committed delete rows here?
            
            if ((this.page.recordCount() == 1) && !getIsRoot())
            {
                 // See if we can unlink this page (might not be able to because
                 // unlinking can cause deadlocks).  A successful unlink 
                 // unlatches the page.
                 shrink_me = unlink(btree);
            }
        }
        finally
        {
            if (!shrink_me)
                this.release();
        }

		return(shrink_me);
    }


    /**
     * Perform a top down split pass making room for the the key in "row".
     * <p>
     * Perform a split such that a subsequent call to insert
	 * given the argument index row will likely find room for it.  Since 
     * latches are released the client must code for the case where another
     * user has grabbed the space made available by the split pass and be
     * ready to do another split.
     * <p>
     * On entry, the parent is either null or latched, and the
     * current page is latched.  On exit, all pages will have been
     * unlatched.  If the parent is null, then this page is a root
     * leaf page.
     *
	 * @return page number of the newly allocated leaf page created by split.
     *
     * @param btree      The open btree to associate latches with.
     * @param template   A scratch area to use while searching for split pass.
     * @param parentpage The parent page of the current page in the split pass.
     *                   starts at null for root.
     * @param row        The key to make room for during the split pass.
     * @param flag       A flag used to direct where point of split should be
     *                   chosen.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected long splitFor(
    OpenBTree               open_btree, 
    DataValueDescriptor[]   template,
    BranchControlRow        parent_page, 
    DataValueDescriptor[]	splitrow,
    int                     flag)
        throws StandardException
    {
        long current_leaf_pageno = this.page.getPageNumber();

        if (SanityManager.DEBUG)
        {
			if (parent_page == null && ( ! this.getIsRoot()))
            	SanityManager.THROWASSERT(
                	this + " splitFor null parent and non-root");
        }

        // See if this page has space.
        if ((this.page.recordCount() - 1 < 
                open_btree.getConglomerate().maxRowsPerPage) &&
            (this.page.spaceForInsert(splitrow, (FormatableBitSet) null,
				AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD)))
        {
            // The splitFor() operation is complete, commit the work done
            // before releasing the latches.
            open_btree.getXactMgr().commit();
             
            if (parent_page != null)
                 parent_page.release();

            this.release();

            return(current_leaf_pageno);
        }

        // RESOLVE (mikem) - for rows bigger than pages this assert may 
        // trigger until we have long rows.
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(this.page.recordCount() > 1);

        // Track.LeafSplit++;

        if (this.getIsRoot())
        {
            // Track.LeafSplitRoot++;

            growRoot(open_btree, template, this);

         
            // At this point, this page has been unlatched.  So code below this
            // point must not access this object's fields.
            
            ControlRow new_root = ControlRow.Get(open_btree, BTree.ROOTPAGEID);

            return(
                new_root.splitFor(open_btree, template, null, splitrow, flag));
        }

        // At this point we know that this page has to be split and
        // that it isn't a root page.

        int splitpoint = (this.page.recordCount() - 1) / 2 + 1;

        if ((flag & ControlRow.SPLIT_FLAG_FIRST_ON_PAGE) != 0)
        {
            // move all the row to the new page
            splitpoint = 1;
        }
        else if ((flag & ControlRow.SPLIT_FLAG_LAST_ON_PAGE) != 0)
        {
            // This is not optimal as we would rather move no rows to the
            // next page, but what should we use as a discriminator?
            splitpoint = this.page.recordCount() - 1;
        }

        if (SanityManager.DEBUG)
        {
			if (splitpoint <= 0)
            	SanityManager.THROWASSERT(this + " yikes! splitpoint of 0!");
        }

        // Save away current split point leaf row, and build a branch row
        // based on it.
        DataValueDescriptor[] split_leaf_row = 
            open_btree.getConglomerate().createTemplate();

        this.page.fetchFromSlot(
            (RecordHandle) null, splitpoint, split_leaf_row, 
            (FetchDescriptor) null, true); 

        // Create the branch row to insert onto the parent page.  For now
        // use a fake page number because we don't know the real page 
        // number until the allocate is done, but want to delay the 
        // allocate until we know the insert will succeed.
        BranchRow branchrow = BranchRow.createBranchRowFromOldLeafRow(
            split_leaf_row, BranchRow.DUMMY_PAGE_NUMBER);


        // At this point we have guaranteed there is space in the parent
        // page for splitrow, but it could be the case that the new
        // "branchrow" does not fit on the parent page.
        if (!parent_page.page.spaceForInsert(
                branchrow.getRow(), (FormatableBitSet) null,
				AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD))
        {
            // There is no room on the parent page to complete a split at
            // the current level, so restart the split at top with the 
            // branchrow that did not fit.  On return from this routine
            // there is no way to know the state of the tree, so the
            // current split pass recursion must end.
            return(
                ((BranchControlRow) parent_page).restartSplitFor(
                    open_btree, template, parent_page, this, 
                    branchrow.getRow(), splitrow, flag));

        }
        // Before moving the rows on the page, while having the latch on the
        // page, notify btree scans that the rows on this page may be moving
        // onto another page.
        //
        // RESOLVE (mikem) - need to pass conlgomid.
        // RESOLVE (mikem) - some optimization later, we only need to notify
        // the scans which are positioned on moving rows.
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(open_btree.init_open_user_scans != null);

        open_btree.init_open_user_scans.saveScanPositions(
                open_btree.getConglomerate(), this.page);

        // Get exclusive RECORD_ID_PROTECTION_HANDLE lock to make sure that
        // we wait for scans in other transactions to move off of this page
        // before we split.

        if (!open_btree.getLockingPolicy().lockScan(
                this, parent_page, true /* for update */, 
                ConglomerateController.LOCK_UPD))
        {
            // we had to give up latches on this and parent_page to get the
            // split lock.  Redo the whole split pass as we have lost our
            // latches.  Just returning is ok, as the caller can not assume
            // that split has succeeded in making space.  Note that at this
            // point in the split no write work has been done in the current
            // internal transaction, so giving up here is fairly cheap.

            // RESOLVE RLL PERFORMANCE - we could keep a stack of visited
            // pages so as to not have to redo the complete search.
            return(current_leaf_pageno);
        }

        // Create a new leaf page under the parent.
        LeafControlRow newleaf = 
            LeafControlRow.Allocate(open_btree, parent_page);

        // Now that we know the page number of the new child page update
        // the branch row to be inserted with the correct value.
        branchrow.setPageNumber(newleaf.page.getPageNumber());

        // Test fail after allocation
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_abort1"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // Link it to the right of the current page.
        newleaf.linkRight(open_btree, this);


		// Copy the index rows (from the splitpoint to the end of the page) 
        // from the old page to the new leaf, do not
        // copy the control row.  This routine will purge all the copied rows
        // and maintain the deleted status of the moved rows.
        int num_rows_to_move = this.page.recordCount() - splitpoint;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(num_rows_to_move >= 0);

        if (num_rows_to_move != 0)
        {
            this.page.copyAndPurge(
                newleaf.page, splitpoint, num_rows_to_move, 1);
        }

        // Test fail after new page has been updated.
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_abort2"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // Test fail after new page has been updated.
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_abort3"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // Find spot to insert branch row, and insert it.


        BranchRow branch_template = 
            BranchRow.createEmptyTemplate(open_btree.getConglomerate());
        SearchParameters sp = 
            new SearchParameters(
                branchrow.getRow(),
                SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH,
                branch_template.getRow(),
                open_btree, false);

        parent_page.searchForEntry(sp);

        // There must be space on the parent to insert the row!
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                parent_page.page.spaceForInsert(
                    branchrow.getRow(), (FormatableBitSet) null,
					AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD));
        }

		byte insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_DEFAULT;
		insertFlag |= Page.INSERT_UNDO_WITH_PURGE;
        if (parent_page.page.insertAtSlot(
            sp.resultSlot + 1,
            branchrow.getRow(),
            (FormatableBitSet) null,
			(LogicalUndo)null, 
            insertFlag,
			AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD) == null) {

            throw StandardException.newException(
                    SQLState.BTREE_NO_SPACE_FOR_KEY);
		}

        // branchrow is only valid while split_leaf_row remains unchanged.
        branchrow = null;

        // RESOLVE (mikem) - this case breaks the btree currently - as the
        // abort of the insert leaves a logical delete in the tree.
        //
        // Test fail after parent page has been updated.
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_abort4"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
            {
                this.checkConsistency(open_btree, parent_page, false);
                newleaf.checkConsistency(open_btree, parent_page, false);
                parent_page.checkConsistency(open_btree, null, false);
            }
        }

        // At this point a unit of work in the split down the tree has
        // been performed in an internal transaction.  This work must
        // be committed before any latches are released.
        open_btree.getXactMgr().commit();

        parent_page.release();
        this.release();  // XXX (nat) Not good form to unlatch self.

        long new_leaf_pageno = newleaf.page.getPageNumber();
        newleaf.release();

        // Because we are at the leaf level and have completed the split
        // there is no more work, no latches should be held, and control
        // is returned up the recursive stack, to the insert causing the
        // split.  Because latches are released, the inserter must recheck
        // that there is now space available as some other thread of control
        // could get in before he latches the page again.
        return(new_leaf_pageno);
    }

	/**
	 ** Grow a new root page from a leaf page.  Slightly
	 ** tricky because we want to retain page 0 as the root.
	 ** <P>
	 ** On entry, the current leaf root page is expected 
	 ** to be latched.  On exit, all latches will have been
	 ** released.
     ** <P>
     ** The caller cannot not assume success.  If we have to release latches
     ** this routine just returns and assumes the caller will retry the 
     ** grow root if necessary.
	 **/
	private static void growRoot(
    OpenBTree               open_btree, 
    DataValueDescriptor[]   template, 
    LeafControlRow          leafroot)
        throws StandardException
	{
		BranchControlRow branchroot =  null;
		LeafControlRow   newleaf    =  null; 


        // Before moving the rows on the page, while having the latch on the
        // page, notify btree scans that the rows on this page may be moving
        // onto another page.
        //
        open_btree.init_open_user_scans.saveScanPositions(
                open_btree.getConglomerate(), leafroot.page);

        // Get exclusive RECORD_ID_PROTECTION_HANDLE lock to make sure that
        // we wait for scans in other transactions to move off of this page
        // before we grow root.  If we don't wait, scanners in other 
        // transactions may be positioned on the leaf page which we are 
        // about to make into a branch page.

        if (!open_btree.getLockingPolicy().lockScan(
                leafroot, (ControlRow) null, 
                true /* for update */,
                ConglomerateController.LOCK_UPD))
        {
            // We had to give up latches on leafroot to get the
            // split lock.  Redo the whole split pass as we have lost our
            // latches - which may mean that the root has grown when we gave
            // up the latch.  Just returning is ok, as the caller can not assume
            // that grow root has succeeded in making space.  Note that at this
            // point in the split no write work has been done in the current
            // internal transaction, so giving up here is fairly cheap.

            return;
        }

        // Allocate a new leaf page under the existing leaf root.

        newleaf = LeafControlRow.Allocate(open_btree, leafroot);

        // Test fail after allocation
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_growRoot1"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // Copy all the index rows from the root to the new leaf, do not
        // copy the control row.  This routine will purge all the copied 
        // rows and maintain the deleted status of the moved rows.

        if (SanityManager.DEBUG)
            SanityManager.ASSERT((leafroot.page.recordCount() - 1) > 0);
        leafroot.page.copyAndPurge(
            newleaf.page, 1, leafroot.page.recordCount() - 1, 1);

        // Test fail after row copy
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_growRoot2"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // Test fail after purge 
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_growRoot3"))
            {
                // Make sure tree is very trashed and logical recovery will
                // not work.
                leafroot.setLevel(42);
                leafroot.setParent(42);
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // Put a branch control row on the root page, making the new leaf 
        // the left child.  All leaf splits result in level-1 branch pages.
        // This will be a branch-root page.

        // Construction of the BranchControlRow will set it as the aux 
        // object for the page, this in turn invalidates the previous aux 
        // object which is leafroot. Thus leafroot must not be used once 
        // the constructor returns.

        branchroot = new BranchControlRow(
            open_btree, leafroot.page, 1, null, true, 
            newleaf.page.getPageNumber());
        leafroot = null;

        // Replace the old leaf root control row with the new branch root 
        // control row.
        branchroot.page.updateAtSlot(
            0, branchroot.getRow(), (FormatableBitSet) null);

        // Test fail after purge 
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_growRoot4"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
            {
                newleaf.checkConsistency(open_btree, branchroot, false);
                branchroot.checkConsistency(open_btree, null, false);
            }
        }
        
        // At this point a unit of work in the split down the tree has
        // been performed in an internal transaction.  This work must
        // be committed before any latches are released.
        open_btree.getXactMgr().commit();

        // Test fail after commit of split
        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("leaf_split_growRoot5"))
            {
                throw StandardException.newException(
                        SQLState.BTREE_ABORT_THROUGH_TRACE);
            }
        }

        // The variable 'branchroot' refers to a page that was latched by 
        // leafroot.  After a growRoot() from a leaf there will be no pages 
        // latched.  It is up to the callers to reget the root page latched 
        // and continue their work.
        //
        if (branchroot != null)
            branchroot.release();
        if (leafroot != null)
            leafroot.release();
        if (newleaf != null)
            newleaf.release();
	}
	
    /**
     * Return the left child pointer for the page.
     * <p>
     * Leaf pages don't have children, so they override this and return null.
     *
	 * @return The page which is the leftmost child of this page.
     *
     * @param btree  The open btree to associate latches/locks with.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected ControlRow getLeftChild(OpenBTree btree)
        throws StandardException
    {
        return(null);
    }

    /**
     * Return the right child pointer for the page.
     * <p>
     * Leaf pages don't have children, so they override this and return null.
     *
	 * @return The page which is the rightmost child of this page.
     *
     * @param btree  The open btree to associate latches/locks with.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected ControlRow getRightChild(OpenBTree btree)
        throws StandardException
    {
        return(null);
    }

	/*
	** Debug/consistency check Methods of ControlRow:
	*/

	/**
	 ** Perform consistency checks on a leaf page.
     ** 
	 ** Check consistency of the page and its children,
	 ** returning the number of pages seen, and throwing
	 ** errors if inconsistencies are found.
     ** The checks specific to a leaf page are:
	 ** <menu>
	 ** <li> Page is at level 0.
	 ** <li> Version is a valid leaf page version.
	 ** <li> Control row has right number of columns for leaf.
	 ** </menu>
	 ** This method also performs the consistency checks that
	 ** are common to both leaf and branch pages.
     ** @see ControlRow#checkGeneric
     **
     ** @exception StandardException Standard exception policy.
	 **/  
	public int checkConsistency(
    OpenBTree  btree, 
    ControlRow parent,
    boolean    check_other_pages
    )
        throws StandardException
	{
		// Do the consistency checks that are common to all
		// types of pages.
		checkGeneric(btree, parent, check_other_pages);

        // Leaf specific, control row checks
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.getLevel() == 0, "leaf not at level 0");

            // RESOLVE (mikem) - how to sanity check correct version?
            /*
			if (this.getVersion() != CURRENT_LEAF_VERSION)
            	SanityManager.THROWASSERT(
                	"Expected leaf version:(" + 
                	CURRENT_LEAF_VERSION + ") but got (" +
                	this.getVersion());
            */
            SanityManager.ASSERT(
                this.page.fetchNumFieldsAtSlot(CR_SLOT) == 
                ControlRow.CR_NCOLUMNS);

            // The remaining checks are specific to leaf pages.

            // Check that every row has at least as many columns
            // as the number of key fields in the b-tree.
            int numslots = this.page.recordCount();
            for (int slot = 1; slot < numslots; slot++)
            {
				if (this.page.fetchNumFieldsAtSlot(slot) <
                     btree.getConglomerate().nKeyFields)
                	SanityManager.THROWASSERT(
                    	"row[" + slot + "]"
                        	+ " has " + this.page.fetchNumFieldsAtSlot(slot)
                        	+ " columns, should have at least" + 
                        	btree.getConglomerate().nKeyFields);
                
                // RESOLVE - the generic btree code should know nothing about
                // the secondaryindex row location column, but put this here for
                // now because I can't figure how to get a call out to the
                // secondary index code at the page level consistency checking
                // level.
            }

        }

		// We checked one page (this one).
		return 1;
	}

	/**
	 ** Recursively print the tree starting at current node in tree.
     ** This is a leaf so return.

    @exception StandardException Standard exception policy.
	 **/
	public void printTree(
    OpenBTree  btree) 
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.DEBUG_PRINT("p_tree", this.debugPage(btree));

            return;
        }
    }


	/*
	 * Methods of TypedFormat:
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() 
    {
		return StoredFormatIds.ACCESS_BTREE_LEAFCONTROLROW_V1_ID;
	}
}
