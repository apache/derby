/*

   Derby - Class org.apache.derby.impl.store.access.btree.BranchControlRow

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

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.SQLLongint;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * @format_id ACCESS_BTREE_BRANCHCONTROLROW_V1_ID
 *
 * @purpose    Btree pages all have a control row at the front of every page.
 *             To determine the type of row, read the first column which is a
 *             format id and it tells what kind of control row it is.
 *
 * @upgrade    RESOLVE.
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
 * column 8 - left child page number   : SQLLongint
 **/

/**
A branch row contains key fields and the pointer to the child page.
**/
public class BranchControlRow extends ControlRow
{
    protected SQLLongint    left_child_page = null;


    /**
     * Only allocate one child_pageno_buf to read the page pointer field into,
     * then cache to "empty" object for reuse by the page itself.
     **/
    transient SQLLongint     child_pageno_buf = null;

    /* Column assignments */
    private static final int CR_LEFTCHILD     = ControlRow.CR_COLID_LAST + 1;
    private static final int CR_COLID_LAST    = CR_LEFTCHILD;
    private static final int CR_NCOLUMNS      = CR_COLID_LAST + 1;

    /**
     * bit sets used to fetch single columns at a time.
     **/
    protected static final FormatableBitSet   CR_LEFTCHILD_BITMAP = 
        new FormatableBitSet(CR_LEFTCHILD + 1);

	/*
	** Constructors of BranchControlRow
	*/

    static 
    {
        CR_LEFTCHILD_BITMAP.set(CR_LEFTCHILD);
    }

    /**
     * No arg constructor.
     * <p>
     * Public no arg constructor is for the monitor to call for format
     * id implementation, it should not be called for any other reason.
     **/
    public BranchControlRow()
    {
    }

	public BranchControlRow(
    OpenBTree       open_btree,
    Page            page,
    int             level,
    ControlRow      parent,
    boolean         isRoot,
    long            left_child)
        throws StandardException
	{
		super(open_btree, page,
              level, parent, isRoot);

        this.left_child_page = new SQLLongint(left_child);

        // finish initializing the row to be used for interacting with
        // raw store to insert, fetch, and update the control row on the page.
        this.row[CR_LEFTCHILD] = left_child_page;

        // set up buffer to read a branch row's page number into.
        child_pageno_buf = new SQLLongint();
	}

	/*
	** Non - Debug/consistency check Methods of ControlRow:
	*/

    /**
     * Perform page specific initialization.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     **/
    protected final void ControlRowInit()
    {
        child_pageno_buf = new SQLLongint();
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
        return(false);
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
        return(false);
    }

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

	public static long restartSplitFor(
    OpenBTree               open_btree,
    DataValueDescriptor[]	template,
    BranchControlRow        parent,
    ControlRow              child,
    DataValueDescriptor[]	newbranchrow,
    DataValueDescriptor[]	splitrow,
    int                     flag)
        throws StandardException
	{
        // release parent and current latch
        parent.release();
        child.release();
        parent = null;
        child  = null;

        // Get the root page back, and perform a split following the
        // branch row which would not fit.
        ControlRow root = ControlRow.Get(open_btree, BTree.ROOTPAGEID);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(root.page.isLatched());

        return(root.splitFor(open_btree, template, null, newbranchrow, flag));
    }


	/**
	 ** Perform a recursive search, ultimately returning the latched
	 ** leaf page and row slot after which the given key belongs.
	 ** The slot is returned in the result structure.  If the key
	 ** exists on the page, the result.exact will be true.  Otherwise,
	 ** result.exact will be false, and the row slot returned will be
	 ** the one immediately preceding the position at which the key
	 ** belongs.
     **
     ** @exception StandardException Standard exception policy.
	 **/
	public ControlRow search(SearchParameters sp)
        throws StandardException
    {
        ControlRow childpage    = null;
        long       childpageid;
        boolean    got_error    = true;

        try
        {
            searchForEntry(sp);

            if (sp.searchForOptimizer)
            {
                // Update left_fraction to be used to esitimate the number of
                // rows left of the current search key.

                // Some search results leave the search positioned on the 0th
                // slot which is a control row, in branch pages this results
                // in following the left page pointer, there is no key 
                // associated with this slot.  Set left_rows to be the number
                // of leaf page pointers on the page which are left
                // of the current slot.
                float left_rows = sp.resultSlot;

                // include the control row count here, as it accounts for the
                // left page pointer which has no associated key.
                int   row_count = this.page.recordCount();

                if (this.getIsRoot())
                {
                    sp.current_fraction = 1;
                    sp.left_fraction    = 0;
                }

                // calculate the fraction of rows in the table which are left 
                // of the current slot in the search.  This number represents
                // the fraction of rows in the sub-tree which includes all 
                // rows left of rows pointed at by the sub-tree to be followed
                // by the code below which descends the child page pointer.
                // After the search is 
                // completed (sp.left_fraction * number of rows), is the 
                // estimated number of rows to the left of the current row.
                sp.left_fraction    += 
                    (sp.current_fraction) * (left_rows / row_count);

                sp.current_fraction = 
                    (sp.current_fraction) * (((float) 1) / row_count);
            }

            childpage =
                this.getChildPageAtSlot(sp.btree, sp.resultSlot);

            this.release();

            got_error = false;

            return childpage.search(sp);
        }
        finally
        {
            if (got_error)
            {
                if (childpage != null)
                    childpage.release();
                if (this.page.isLatched())
                    this.release();
            }
        }
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
        ControlRow childpage    = null;
        boolean    got_error    = true;

        try
        {
            childpage = this.getLeftChild(btree);
            this.release();

            got_error = false;
            return childpage.searchLeft(btree);
        }
        finally
        {
            if (got_error)
            {
                if (childpage != null)
                    childpage.release();
                if (this.page.isLatched())
                    this.release();
            }
        }
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
        ControlRow childpage    = null;
        boolean    got_error    = true;

        try
        {
            childpage = this.getRightChild(btree);
            this.release();

            got_error = false;
            return(childpage.searchRight(btree));
        }
        finally
        {
            if (got_error)
            {
                if (childpage != null)
                    childpage.release();
                if (this.page.isLatched())
                    this.release();
            }
        }
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
	 ** returned latched.
     **
     ** @exception StandardException Standard exception policy.
	 **/
	protected boolean shrinkFor(
    OpenBTree               open_btree, 
    DataValueDescriptor[]   shrink_key)
        throws StandardException
    {
        ControlRow childpage = null;
        boolean    shrinkme  = false;

        try
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(this.page.isLatched());

            // Find the child page for the shrink key.

            BranchRow branch_template =
                BranchRow.createEmptyTemplate(open_btree.getConglomerate());
            SearchParameters sp = new SearchParameters(
                shrink_key,
                SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH,
                branch_template.getRow(), open_btree, false);

            this.searchForEntry(sp);
            childpage = this.getChildPageAtSlot(sp.btree, sp.resultSlot);

            // Recursively shrink the child.  If this call returns
            // true, then the child page has been deleted from its
            // sibling chain, and we have to delete the entry for it
            // in this page.

            if (childpage.shrinkFor(open_btree, shrink_key))
            {
                // Child was deallocated.
                if (sp.resultSlot != 0)
                {
                    // Remove the corresponding branch row.  This call assumes
                    // that raw store will shift all higher slots down to fill
                    // the purged slot.
                    this.page.purgeAtSlot(sp.resultSlot, 1, true);
                }
                else
                {
                    // Shrunk slot is zero, which means the left child page was
                    // deallocated. If the current page is empty, then
                    // we have to deallocate it.  Otherwise, we "slide" the rows
                    // down, making the first index row into the left child,
                    // and the second index row into the first, etc.

                    if (this.page.recordCount() > 1)
                    {
                        // There is a branch row on this page (besides the
                        // control row).  Make the first branch row into the
                        // left child.

                        long leftchildpageid =
                            getChildPageIdAtSlot(open_btree, 1);

                        this.setLeftChildPageno(leftchildpageid);

                        // purge the row we just made the "left child", this
                        // will automatically shifta all other rows "left" in
                        // the tree.
                        this.page.purgeAtSlot(1, 1, true);
                    }
                    else
                    {
                        // We shrunk the left child which was the last child on
                        // the page.  This means that this entire subtree is
                        // empty.  Again, there are two cases: root vs.
                        // non-root.  Because this method waits till pages are
                        // completely empty before deallocating them from the
                        // index, an empty root page means an empty index.
                        // If this page is not the root, then simply
                        // deallocate it and return that fact to the caller.

                        if (this.getIsRoot())
                        {
                            // The root page has become empty.  If the root page
                            // is empty, then the index is empty.  What has to
                            // happen here is that this page has to be
                            // converted back to an empty leaf page.

                            // With the current interface, after this page has
                            // been converted to a leaf, the caller will be
                            // left with a branch control row object, although
                            // the page is a leaf page.  This same problem was
                            // addressed in splitFor by adjusting the interface
                            // - the two routines should at least have the same
                            // interface style.

                            if (SanityManager.DEBUG)
                            {
                                SanityManager.ASSERT(
                                    this.page.recordCount() == 1);
                            }

                            LeafControlRow newleafroot = new LeafControlRow(
                                open_btree, this.page, null, true);

                            newleafroot.page.updateAtSlot(
                                0, newleafroot.getRow(), 
                                (FormatableBitSet) null);

                            newleafroot.release();

                            shrinkme = true;
                        }
                        else
                        {
                            // This page is empty, but it's not the root.  We
                            // have to unlink this page from its siblings, and
                            // return to the parent branch page that its
                            // branch row should be removed.

                            // Unlink this page from its siblings.
                            if (this.unlink(open_btree))
                            {
                                // Tell the caller to remove entry.
                                shrinkme = true;
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            // If shrinkme then the page has been unlatched either by
            // page.removePage(), or by the process of changing the root branch
            // page to a root leaf page.
            if (!shrinkme)
                this.release();
        }

        return(shrinkme);
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
     * Latches:
     * o PARENT    : is latched on entry (unless the split is the root then
     *               there is no parent.
     * o THISBRANCH: the current page is latched on entry.
     * o CHILD     : latch the child page which will be pointed at by the
     *               left child pointer of the new page.
     *               RESOLVE (mikem) -see comments below
     * o NEWPAGE   : Allocate and latch new page.
     * o CHILD     : release. (RESOLVE)
     * o fixparents: latch pages and reset their parent pointers.
     *               Conditionally fix up the parent links on the pages
     *               pointed at by the newly allocated page.  First get latch
     *               and release on the left child page and then loop through
     *               slots on NEWPAGE, from left to right getting and
     *               releasing latches.
     *
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
    DataValueDescriptor[]	template,
    BranchControlRow        parent,
    DataValueDescriptor[]	splitrow,
    int                     flag)
        throws StandardException
	{
		int        childpageid;
		ControlRow childpage;

		// On entry, the parent page is either latched by the caller,
		// or it's null (which implies that this object is the root).

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(parent != null || this.getIsRoot());

            SanityManager.ASSERT(
                parent == null || parent.page.isLatched(),
                "parent page is not latched");

            SanityManager.ASSERT(this.page.isLatched(),
                "page is not latched:");
        }

        if ((this.page.recordCount() - 1 >=
                open_btree.getConglomerate().maxRowsPerPage) ||
            (!this.page.spaceForInsert(splitrow, (FormatableBitSet) null,
				AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD)))
        {

            if (this.page.recordCount() == 1)
            {
                // RESOLVE (mikem) long row issue.  For now it makes no sense
                // to split if there are no rows.  So if spaceForRecord() fails
                // on empty page, we throw exception.
                throw StandardException.newException(
                        SQLState.BTREE_NO_SPACE_FOR_KEY);
            }

			// Track.BranchSplit++;

			if (this.getIsRoot())
			{
				// Track.BranchSplitRoot++;
				growRoot(open_btree, template, this);

				parent = (BranchControlRow)
                    ControlRow.Get(open_btree, BTree.ROOTPAGEID);


				return(parent.splitFor(
                        open_btree, template, null, splitrow, flag));
			}

			// At this point we know that this page has to be split and
			// that it isn't a root page.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(!this.getIsRoot());
                SanityManager.ASSERT(parent != null);
            }

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
                	SanityManager.THROWASSERT(this + "yikes! splitpoint of 0!");
            }


            // Before any logged operation is done in the current internal
            // xact, make sure that there is room in the parent to insert
            // the new branch row.
            //
			// Create a new branch row which points to the new page,
            // and insert it on parent page.

            // Read in the branch row which is at the split point.
            BranchRow split_branch_row =
                BranchRow.createEmptyTemplate(open_btree.getConglomerate());

            this.page.fetchFromSlot(
                (RecordHandle) null, splitpoint, split_branch_row.getRow(), 
                (FetchDescriptor) null, true);

            // Create the branch row to insert onto the parent page.  For now
            // use a fake page number because we don't know the real page
            // number until the allocate is done, but want to delay the
            // allocate until we know the insert will succeed.
			BranchRow newbranchrow =
                split_branch_row.createBranchRowFromOldBranchRow(
                        BranchRow.DUMMY_PAGE_NUMBER);

            // At this point we have guaranteed there is space in the parent
            // page for splitrow, but it could be the case that the new
            // "newbranchrow" does not fit on the parent page.
            if (!parent.page.spaceForInsert(
                    newbranchrow.getRow(), (FormatableBitSet) null,
					AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD))
            {
                // There is no room on the parent page to complete a split at
                // the current level, so restart the split at top with the
                // branchrow that did not fit.  On return from this routine
                // there is no way to know the state of the tree, so the
                // current split pass recursion must end.
                return(
                    parent.restartSplitFor(
                        open_btree, template, parent, this,
                        newbranchrow.getRow(), splitrow, flag));
            }

			// Get the child page for the index row at the split point
			// This will be the left child for	the new page.  We're
			// getting the page because BranchControlRow.Allocate
			// sets the left child pointer from a BranchControlRow.
			// If there were a version which just took the pageid,
			// we wouldn't have to get the page (the latch on this
			// page is enough to ensure that the child page won't
			// disappear).

            childpage = this.getChildPageAtSlot(open_btree, splitpoint);

			// Allocate a new branch page and link it to the
			// right of the current page.
			BranchControlRow newbranch =
                BranchControlRow.Allocate(open_btree, childpage,
                    this.getLevel(), parent);
			newbranch.linkRight(open_btree, this);


            // Test fail after allocation
            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("branch_split_abort1"))
                {
                    throw StandardException.newException(
                            SQLState.BTREE_ABORT_THROUGH_TRACE);
                }
            }

			// Done with the child page.
			childpage.release();

            // Now that we know the page number of the new child page update
            // the branch row to be inserted with the correct value.
            newbranchrow.setPageNumber(newbranch.page.getPageNumber());

            BranchRow branch_template =
                BranchRow.createEmptyTemplate(open_btree.getConglomerate());
			SearchParameters sp = new SearchParameters(
                newbranchrow.getRow(),
                SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH,
                branch_template.getRow(),
                open_btree, false);

			parent.searchForEntry(sp);

			byte insertFlag = Page.INSERT_INITIAL;
			insertFlag |= Page.INSERT_DEFAULT;
			insertFlag |= Page.INSERT_UNDO_WITH_PURGE;
			if (parent.page.insertAtSlot(
                    sp.resultSlot + 1,
                    newbranchrow.getRow(),
                    (FormatableBitSet) null,
                    (LogicalUndo)null,
                    insertFlag, AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD)
					== null)
            {
                throw StandardException.newException(
                            SQLState.BTREE_NO_SPACE_FOR_KEY);
			}


            // Test fail after of row onto parent page.
            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("branch_split_abort2"))
                {
                    throw StandardException.newException(
                            SQLState.BTREE_ABORT_THROUGH_TRACE);
                }
            }

            // newbranchrow only valid while contents of split_branch_row
            // remain unchanged.
            newbranchrow = null;

			// Copy the rows from the split point, but not including it (since
            // the split point is turning into the left child of the new
            // branch), onto the new page.  Purge the rows including the split
			// point from the current page.
            int num_rows_to_move = this.page.recordCount() - (splitpoint + 1);

            if (num_rows_to_move > 0)
            {
                this.page.copyAndPurge(
                    newbranch.page, splitpoint + 1, num_rows_to_move, 1);
            }

            // remove the splitpoint row, we didn't copy it because it became
            // the "left child", but we do need to get rid of it.
			this.page.purgeAtSlot(splitpoint, 1, true);

            // Test fail after of copy of rows to new page.
            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("branch_split_abort3"))
                {
                    throw StandardException.newException(
                            SQLState.BTREE_ABORT_THROUGH_TRACE);
                }
            }

            // Test fail after purge of rows on old page.
            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("branch_split_abort4"))
                {
                    throw StandardException.newException(
                            SQLState.BTREE_ABORT_THROUGH_TRACE);
                }
            }

            // Check pages that have been altered by above split
            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
                {
                    parent.checkConsistency(open_btree, null, false);
                    newbranch.checkConsistency(open_btree, parent, false);
                    this.checkConsistency(open_btree, parent, false);
                }
            }

			// Fix up the parent links on the pages for the rows that moved to
            // the new branch.
			newbranch.fixChildrensParents(open_btree, null);

            // At this point a unit of work in the split down the tree has
            // been performed in an internal transaction (ie. writes have been
            // done to latched pages), and the resulting
            // tree is logically consistent, thus the work can be committed.
            // This work must be committed before any latches are released.
            open_btree.getXactMgr().commit();

			// Decide whether we're following the current page or the new page.
			BranchControlRow pagetofollow;

            if (CompareIndexRowToKey(
                    splitrow, 
                    split_branch_row.getRow(),
                    split_branch_row.getRow().length - 1, 0,
					open_btree.getConglomerate().ascDescInfo) >= 0)
            {
                // Follow the new branch
				pagetofollow = newbranch;
				this.release();
			}
			else
			{
				// Follow the current branch
				pagetofollow = this;
				newbranch.release();
			}

            // At this point we hold latches on the parent, and the current
            // child of the page that we are following.  Note that committing
            // the internal transaction did not release the latches.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(parent != null);
                SanityManager.ASSERT(parent.page.isLatched());
                SanityManager.ASSERT(
                        pagetofollow.page.isLatched());
            }

			// Recurse down the tree splitting if necessary.
			return(
                pagetofollow.splitFor(
                    open_btree, template, parent, splitrow, flag));
		}

        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
            {
                this.checkConsistency(open_btree, parent, false);
            }
        }

		// Don't need the parent any more.
		if (parent != null)
			parent.release();

        // RESOLVE (mikem) - should this be passed in?
        BranchRow branch_template =
            BranchRow.createEmptyTemplate(open_btree.getConglomerate());
		SearchParameters sp = new SearchParameters(
            splitrow, 
            SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH,
            branch_template.getRow(), 
            open_btree, false);

		searchForEntry(sp);

        childpage = this.getChildPageAtSlot(open_btree, sp.resultSlot);

        return(childpage.splitFor(open_btree, template, this, splitrow, flag));
    }

	/*
	** Debug/consistency check Methods of ControlRow:
	*/


	/**
	 ** Perform consistency checks for a branch page.  The checks
	 ** specific to a branch page are:
	 ** <menu>
	 ** <li> The rows on the page are indeed branch rows, and
	 **      they all have the correct number of fields (which
	 **      is the b-tree's key fields plus one for the child
	 **      page number.
	 ** <li> The child pages pointed to by the left child pointer
	 **      and the index rows are linked together in the same
	 **      order that they appear on the page.
	 ** <li> The child pages themselves are all consistent.
	 ** </menu>
	 ** This method also performs the consistency checks that
	 ** are common to both leaf and branch pages (see
	 ** ControlRow.checkGeneric).
     **
     ** @exception StandardException Standard exception policy.
	 **/
	public int checkConsistency(
    OpenBTree  btree,
    ControlRow parent,
    boolean    check_other_pages)
        throws StandardException
	{
		// Do the consistency checks that are common to all
		// types of pages.
		checkGeneric(btree, parent, check_other_pages);

        // Branch specific Control Row checks.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                this.getLevel() > 0, "branch not above level 0");

            // RESOLVE (mikem) - how to check right version?
            /*
			if (this.getVersion() != CURRENT_BRANCH_VERSION)
            	SanityManager.THROWASSERT(
                	"Expected branch version:(" + CURRENT_BRANCH_VERSION +
                	") but got (" + this.getVersion());
            */
            SanityManager.ASSERT(
                this.page.fetchNumFieldsAtSlot(CR_SLOT) ==
                BranchControlRow.CR_NCOLUMNS);
            SanityManager.ASSERT(getLeftChildPageno() !=
                                 ContainerHandle.INVALID_PAGE_NUMBER);

            // RESOLVE (mikem) - this makes an assumption about page numbering,
            // that may not be always valid in all implementations but has
            // been useful in finding bugs with uninitialized fields.
            SanityManager.ASSERT(getLeftChildPageno() >= BTree.ROOTPAGEID);
        }

		// The remaining checks are specific to branch pages.
        if (SanityManager.DEBUG)
        {

            // Check that all the branch rows are branch rows
            // (we'll get a case error otherwise), and have the right
            // number of columns.  Every branch row should have the
            // btree's	key columns plus one for the child link.
            int numslots = this.page.recordCount();
            for (int slot = 1; slot < numslots; slot++)
            {
				if ((this.page.fetchNumFieldsAtSlot(slot) !=
                     (btree.getConglomerate().nKeyFields + 1)))
                	SanityManager.THROWASSERT(
                    	"row[" + slot + "]"
                        + " has " + this.page.fetchNumFieldsAtSlot(slot)
                        + " columns, should have at least " +
                        (btree.getConglomerate().nKeyFields + 1));

                SanityManager.ASSERT(this.getChildPageIdAtSlot(btree, slot) !=
                        ContainerHandle.INVALID_PAGE_NUMBER);

                // Rows on branch pages are never deleted, they are only purged.
                SanityManager.ASSERT(!this.page.isDeletedAtSlot(slot));

                // RESOLVE (mikem) - this makes an assumption about page
                // numbering, that may not be always valid in all
                // implementations but has been useful in finding bugs with
                // uninitialized fields.
                SanityManager.ASSERT(getLeftChildPageno() >= BTree.ROOTPAGEID);
            }
        }

		// Check that the linkage of the children is in the
		// same order as the branch rows.
        // RESOLVE (mikem) enable when multiple latches work.
        if (check_other_pages)
		    checkChildOrderAgainstRowOrder(btree);

		// Check the children.
		int nchildren = 0;

        // RESOLVE (mikem) enable when multiple latches work.
        if (check_other_pages)
            nchildren = checkChildren(btree);

		// Return the number of children visited plus one for this page.
		return nchildren + 1;
	}

    private int checkChildren(OpenBTree btree)
        throws StandardException
    {
		int         nchildren = 0;
        ControlRow  childpage = null;

        try
        {
            // Check the left child.
            childpage = this.getLeftChild(btree);
            nchildren += childpage.checkConsistency(btree, this, true);
            childpage.release();
            childpage = null;

            // Check children from each index row.
            int numslots = this.page.recordCount();
            for (int slot = 1; slot < numslots; slot++)
            {
                childpage = this.getChildPageAtSlot(btree, slot);
                nchildren += childpage.checkConsistency(btree, this, true);
                childpage.release();
                childpage = null;
            }

            return(nchildren);
        }
        finally
        {
            if (childpage != null)
                childpage.release();
        }
    }

	private void checkChildOrderAgainstRowOrder(OpenBTree btree)
        throws StandardException
	{
		ControlRow cur  = null;
		ControlRow prev = null;

        try
        {
            prev = this.getLeftChild(btree);

            int numslots = this.page.recordCount();
            for (int slot = 1; slot < numslots; slot++)
            {
                cur = this.getChildPageAtSlot(btree, slot);

                long shouldbecur_pageno = prev.getrightSiblingPageNumber();
                if (SanityManager.DEBUG)
                {
					if (shouldbecur_pageno != cur.page.getPageNumber())
                    	SanityManager.THROWASSERT(
                        	"child linkage error going right.\n" +
                        	"cur page control row = " + cur + "\n" +
                        	"prev page control row = " + prev + "\n");
                }

                long shouldbeprev_pageno = cur.getleftSiblingPageNumber();

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        shouldbeprev_pageno == prev.page.getPageNumber(),
                        "child linkeage error going left");
                }

                prev.release();
                prev = cur;
                cur  = null;
            }

            prev.release();
            prev = null;
        }
        finally
        {
            if (prev != null)
                prev.release();
            if (cur != null)
                cur.release();
        }

        return;
	}

    /**
     * Recursively print the tree starting at current node in tree.
     *
     * @param btree the open btree to print.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void printTree(
    OpenBTree  btree)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.DEBUG_PRINT("p_tree", this.debugPage(btree));

            ControlRow      child = null;

            try
            {
                child = this.getLeftChild(btree);

                child.printTree(btree);
                child.release();
                child = null;

                int numslots = this.page.recordCount();
                for (int slot = 1; slot < numslots; slot++)
                {
                    child = this.getChildPageAtSlot(btree, slot);
                    child.printTree(btree);
                    child.release();
                    child = null;
                }
            }
            finally
            {
                if (child != null)
                    child.release();
            }

            return;
        }
    }

	/*
	 * Private methods of BranchControlRow
	 */

	/**
     ** Add a level to the tree by moving the current branch-root page up
     ** one level and adding a new page as it's left child.  On exit the
     ** current root page remains the root of the tree.
	 ** <P>
	 ** On entry, the current branch root page is expected to be latched.
     ** On exit, all latches will have been released.
     ** <P>
     ** Latch order:
     **    o ROOT: on entry current root is latched.
     **            No other latches should be held.
     **    o ROOT_OLDCHILD: Get and Latch root's left child page.
     **    o ROOT_NEWCHILD: Allocate a new branch page with latch.
     **    o Conditionally fix up the parent links on the pages pointed at
     **      by the newly allocated page.  Loop through slots on ROOT_NEWCHILD,
     **      from left to right getting and releasing latches.  Note that
     **      fixChildrensParents() must not latch the leftchild as ROOT_OLDCHILD
     **      is already latched.
     **    RESOLVE: (mikem) does order of release matter.
     **    o ROOT         : released.
     **    o ROOT_NEWCHILD: released.
     **    o ROOT_OLDCHILD: released.
	 **/
	private static void growRoot(
    OpenBTree               open_btree,
    DataValueDescriptor[]   template,
    BranchControlRow        root)
        throws StandardException
	{
        ControlRow       leftchild = null;
        BranchControlRow branch    = null;

        try
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(root.page.isLatched());
                SanityManager.ASSERT(root.getIsRoot());
            }

            // System.out.println("Growing root: control row = " + root);
            // System.out.println("Growing root: page = " + root.page);

            // Get and latch the current root's left child.  This will become
            // the left child on the new branch page (and the new
            // branch will become the left child of the root).
            leftchild = root.getLeftChild(open_btree);

            // Allocate a new branch page.	 This one will take the
            // rows from the root, and remain at the old root's level.
            // Its parent is the root.
            branch =
                BranchControlRow.Allocate(
                    open_btree, leftchild, root.getLevel(), root);

            // Copy all the index rows from the root to the new branch.
            // Purge the index rows from the root now that they're safely on the
            // new branch page.  Leave the branch control row on the page.
            root.page.copyAndPurge(branch.page, 1, root.page.recordCount() - 1, 1);

            // Set the root's left child to be the new branch.
            root.setLeftChild(branch);

            // Move the root up a level
            root.setLevel(root.getLevel() + 1);

            // The parent of the old root's children has changed.
            // It used to be page 0 (the old root, but now it's
            // the new branch page.  Fix this up.
            branch.fixChildrensParents(open_btree, leftchild);

            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
                {
                    root.checkConsistency(open_btree, null, false);
                    branch.checkConsistency(open_btree, root, false);
                    leftchild.checkConsistency(open_btree, branch, false);
                }
            }

            // At this point a unit of work in the split down the tree has
            // been performed in an internal transaction.  This work must
            // be committed before any latches are released.
            open_btree.getXactMgr().commit();

        }
        finally
        {
            // At the end of a growRoot() no latches are held, the caller must
            // restart at the root.
            //
            root.release();
            if (branch != null)
                branch.release();
            if (leftchild != null)
                leftchild.release();
        }
        return;
	}
    /**
     * Allocate a new leaf page to the conglomerate.
     *
     * @exception StandardException Standard exception policy.
     */
    private static BranchControlRow Allocate(
    OpenBTree         open_btree,
    ControlRow        leftchild,
    int               level,
    ControlRow        parent)
        throws StandardException
    {
        Page      page      = open_btree.container.addPage();

        // Create a control row for the new page.
        BranchControlRow control_row =
            new BranchControlRow(
                open_btree, page, level,
                parent, false, leftchild.page.getPageNumber());

        // Insert the control row on the page.
		byte insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_DEFAULT;
        page.insertAtSlot(
            Page.FIRST_SLOT_NUMBER,
            control_row.getRow(),
            (FormatableBitSet) null, 
            (LogicalUndo)null,
            insertFlag, AccessFactoryGlobals.BTREE_OVERFLOW_THRESHOLD);

        // Page is returned latched.
        return(control_row);
    }

	protected void setLeftChildPageno(long leftchild_pageno)
        throws StandardException
	{
		// Store the field.
		if (left_child_page == null)
			left_child_page = new SQLLongint(leftchild_pageno);
        else
            this.left_child_page.setValue(leftchild_pageno);

		// Write the field through to the underlying row
		this.page.updateFieldAtSlot(
            CR_SLOT, CR_LEFTCHILD, this.left_child_page, null);
	}

	protected void setLeftChild(ControlRow leftchild)
        throws StandardException
	{
        this.setLeftChildPageno(leftchild.page.getPageNumber());
	}

	/**
	 ** A branch page that has just been allocated as part
	 ** of a split has index rows and a left child pointer
	 ** that were copied from another page.  The parent
	 ** link on the corresponding pages will still point to
	 ** the original page.  This method fixes their parent
	 ** pointers so that they point to the curren page like
	 ** they're supposed to.
	 ** <P>
	 ** Note that maintaining the parent link is kind of a
	 ** pain, and will slow down applications.  It's only
	 ** needed for consistency checks, so we may want to
	 ** have implementations that don't bother to maintain it.
     ** <P)
     ** This
	 **/
	private void fixChildrensParents(
    OpenBTree       btree,
    ControlRow      leftchild)
        throws StandardException
	{
        ControlRow child = null;

        try
        {
            if (leftchild == null)
            {
                child = this.getLeftChild(btree);
                child.setParent(this.page.getPageNumber());

                if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
                    {
                        child.checkConsistency(btree, this, false);
                    }
                }

                child.release();
                child = null;
            }
            else
            {
                leftchild.setParent(this.page.getPageNumber());

                if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
                    {
                        leftchild.checkConsistency(btree, this, false);
                    }
                }
            }

            int numslots = this.page.recordCount();
            for (int slot = 1; slot < numslots; slot++)
            {
                child = getChildPageAtSlot(btree, slot);
                child.setParent(this.page.getPageNumber());
                if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON("enableBtreeConsistencyCheck"))
                    {
                        child.checkConsistency(btree, this, false);
                    }
                }

                child.release();
                child = null;
            }
        }
        finally
        {
            if (child != null)
                child.release();
        }
	}

    private long getChildPageIdAtSlot(
    OpenBTree       btree,
    int             slot)
        throws StandardException
    {
        long child_page_id;

        if (slot == 0)
        {
            child_page_id = this.getLeftChildPageno();
        }
        else
        {
            this.page.fetchFieldFromSlot(
                slot, btree.getConglomerate().nKeyFields, child_pageno_buf);
            child_page_id = child_pageno_buf.getLong();
        }

        return(child_page_id);
    }

    protected ControlRow getChildPageAtSlot(
    OpenBTree       open_btree,
    int             slot)
        throws StandardException
    {
        ControlRow  child_control_row;

        if (slot == 0)
        {
            child_control_row = this.getLeftChild(open_btree);
        }
        else
        {
            this.page.fetchFieldFromSlot(
                slot, open_btree.getConglomerate().nKeyFields, 
                child_pageno_buf);

            child_control_row =
                ControlRow.Get(open_btree, child_pageno_buf.getLong());
        }

        return(child_control_row);
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
    public ControlRow getLeftChild(OpenBTree open_btree)
            throws StandardException
    {
         return(ControlRow.Get(open_btree, this.getLeftChildPageno()));
    }

    /**
     * Return the right child pointer for the page.
     * <p>
     * Leaf pages don't have children, so they override this and return null.
     *
	 * @return The page which is the rightmost child of this page.
     *
     * @param open_btree  The open btree to associate latches/locks with.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected ControlRow getRightChild(OpenBTree open_btree)
        throws StandardException
    {
        ControlRow right_child;
        int        num_slots = this.page.recordCount();

        // if num_slots is 1 then there are no branch rows, so just follow
        // the left page pointer, else if num_slots is > 1 then follow the
        // last branch row to find the rightmost child.
        right_child = 
            (num_slots == 1 ? 
                ControlRow.Get(open_btree, this.getLeftChildPageno()) :
                getChildPageAtSlot(open_btree, (num_slots - 1)));

        return(right_child);
    }

	/**
	 ** Return the left child page number for the page.  Leaf pages
	 ** don't have left children, so they override this and return
	 ** null.
	 **/
	long getLeftChildPageno()
        throws StandardException
    {
        if (this.left_child_page == null)
        {
            this.left_child_page = new SQLLongint();

            scratch_row[CR_LEFTCHILD] = this.left_child_page;

            fetchDesc.setValidColumns(CR_LEFTCHILD_BITMAP);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
        }
        return(left_child_page.getLong());
    }

	/*
	 * TypedFormat:
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
    {
		return StoredFormatIds.ACCESS_BTREE_BRANCHCONTROLROW_V1_ID;
	}

    /**
     * Return a new template for reading a data row from the current page.
     * <p>
     * Default implementation for rows which are the same as the conglomerates
     * template, sub-classes can alter if underlying template is different
     * (for instance branch rows add an extra field at the end).
     *
	 * @return Newly allocated template.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DataValueDescriptor[] getRowTemplate(OpenBTree    open_btree)
		throws StandardException
    {
        return(BranchRow.createEmptyTemplate(
                    open_btree.getConglomerate()).getRow());
    }

	/**
	 ** The standard toString.
	 **/
	public String toString()
    {
        if (SanityManager.DEBUG)
        {
            String string = super.toString();

            try 
            {
                string += "left child page = " + getLeftChildPageno() + ";";
                
            }
            catch (Throwable t)
            {
                string += "error encountered while doing ControlRow.toString()";
            }

            return(string);
        }
        else
        {
            return(null);
        }
    }
}
