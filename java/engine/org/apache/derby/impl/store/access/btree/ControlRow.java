/*

   Derby - Class org.apache.derby.impl.store.access.btree.ControlRow

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

import java.io.PrintStream;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.TypedFormat;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.AuxObject;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.impl.store.access.StorableFormatId;


import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;

/**

Base class for leaf and branch control rows.
<P>
<B>Concurrency Notes</B>
<P>
All access through control rows is serialized by an exclusive latch on 
the page the control row is for.  The page is latched when the control
row is "gotten" (ControlRow#Get), and unlatched when the control row
is released (ControlRow#release).
<P>
<B>To Do List</B>
<UL>
<LI> <I>[NOTE1]</I>
The code is arranged to fault in fields from the row as necessary.
many of the fields of a control row are rarely used (left sibling, parent).
The accessors fault in the underlying column only when
requested by allocating the appropriate object and calling fetchFromSlot and
only fetching the requested field.
<LI> <I>[NOTE2]</I> 
Currently, all the fields of the control row are stored as StorableU8s
for simplicity.  This is too few bits to hold the long page numbers, and
too many to hold the version, level, and isRoot flag.  Some consideration
will have to be given to the appropriate storage format for these values.
<LI> <I>[NOTE3]</I>
The implementation relies on the existance of page "auxiliary" pointers 
which keep Object versions of the control row.
<P>
@see ControlRow#Get
@see ControlRow#release

**/

public abstract class ControlRow implements AuxObject, TypedFormat
{
    /**
     * Version indentifier of the control row within the page.  
     * <p>
     * This is the format id of the control row.  The format id is currently
     * one of either StoredFormatIds.ACCESS_BTREE_LEAFCONTROLROW_ID or
     * StoredFormatIds.ACCESS_BTREE_BRANCHCONTROLROW_ID.
     **/
    private StorableFormatId    version = null;

    /**
     * Pointer to page which is "left" at the current level.
     * <p>
     * Currently all pages at a level are doubly linked.  The leftmost page
     * in a level has a leftSiblingPageNumber == 
     * ContainerHandle.INVALID_PAGE_NUMBER.  All key values on the page which
     * is left must precede the first key value on the current page.
     **/
	private SQLLongint leftSiblingPageNumber;

    /**
     * Pointer to page which is "right" at the current level.
     * <p>
     * Currently all pages at a level are doubly linked.  The rightmost page
     * in a level has a rightSiblingPageNumber == 
     * ContainerHandle.INVALID_PAGE_NUMBER.  All key values on the page which
     * is right of the current page must follow the last key value on the 
     * current page.
     **/
	private SQLLongint rightSiblingPageNumber;

    /**
     * The parent page of the current page.
     * <p>
     * For consistency checking it is useful to maintain the parentPageNumber
     * field of the current page.  The root page has a value of 
     * ContainerHandle.INVALID_PAGE_NUMBER in it's parentPageNumber field.
     * <p>
     * RESOLVE (mikem) - we need to come up with some way to not maintain these,
     * maybe by providing a property on secondary index or a different 2nd 
     * index.
     *
     **/
	private SQLLongint parentPageNumber; // for consistency checking


    /**
     * The level of the btree.
     * <p>
     * The leaf level of the btree is 0.  The first branch level (parent level
     * of the leaf), is level 1.  The height of the btree is (level + 1).
     * <p>
     * The smallest btree is a one page btree with only a leaf, and no branch
     * pages. 
     **/
	private SQLLongint level;

    /**
     * Is this page the root of the btree?
     * <p>
     * Currently "1" if the page is the root page, else "0".
     * <p>
     * RESOLVE (mikem) When real datatype come about, this value should 
     * probably be just a bit in some status word.
     **/
	private SQLLongint isRoot = null;

    /**
     * A copy of the Conglomerate that describes the owning conglom.
     * <p>
     * This information is used during logical undo to get the type information
     * so that rows can be compared and searched for.  We may be able to get
     * away with a subset of the information stored in the conglomerate.
     * <p>
     * RESOLVE (mikem) - change this to only store the info on the root page.
     **/
    private BTree    btree = null;

    /**
     * The page that this control row describes.
     **/
	protected Page page;

    /**
     * The page that this control row describes.
     **/
	protected DataValueDescriptor row[];

    /**
     * row used to replace fetchFieldFromSlot() calls.
     **/
    protected DataValueDescriptor[] scratch_row;

    /**
     * FetchDescriptor used to replace fetchFieldFromSlot() calls.
     **/
    protected FetchDescriptor   fetchDesc;

    /**
     * In memory hint about whether to use the last_search_result hint during
     * search.
     **/
    transient protected boolean use_last_search_result_hint = false;

    /**
     * In memory hint about where to begin the binary search to find a key
     * on the the current control page.
     **/
    transient protected int last_search_result = 0;

    /**
     * Column number assignments for columns of the control row.
     * <p>
     * The control row is stored as the first row in a btree page.  The row
     * is an array of columns.  The Control row columns are the columns numbered
     * from ControlRow.CR_COLID_FIRST through ControlRow.CR_COLID_LAST.  The
     * classes which implement the concrete derived classes of ControlRow may
     * add columns to the control row, but they must be added after the 
     * ControlRow columns.
     **/
	protected static final int CR_COLID_FIRST		= 0;
	protected static final int CR_VERSION_COLID		= CR_COLID_FIRST + 0;
	protected static final int CR_LEFTSIB_COLID		= CR_COLID_FIRST + 1;
	protected static final int CR_RIGHTSIB_COLID	= CR_COLID_FIRST + 2;
	protected static final int CR_PARENT_COLID		= CR_COLID_FIRST + 3;
	protected static final int CR_LEVEL_COLID		= CR_COLID_FIRST + 4;
	protected static final int CR_ISROOT_COLID		= CR_COLID_FIRST + 5;
	protected static final int CR_CONGLOM_COLID	    = CR_COLID_FIRST + 6;
	protected static final int CR_COLID_LAST		= CR_CONGLOM_COLID;
	protected static final int CR_NCOLUMNS			= CR_COLID_LAST + 1;

    /**
     * bit sets used to fetch single columns at a time.
     **/
    protected static final FormatableBitSet   CR_VERSION_BITSET = 
        new FormatableBitSet(CR_VERSION_COLID + 1);
    protected static final FormatableBitSet   CR_LEFTSIB_BITSET = 
        new FormatableBitSet(CR_LEFTSIB_COLID + 1);
    protected static final FormatableBitSet   CR_RIGHTSIB_BITSET =
        new FormatableBitSet(CR_RIGHTSIB_COLID + 1);
    protected static final FormatableBitSet   CR_PARENT_BITSET =
        new FormatableBitSet(CR_PARENT_COLID + 1);
    protected static final FormatableBitSet   CR_LEVEL_BITSET =
        new FormatableBitSet(CR_LEVEL_COLID + 1);
    protected static final FormatableBitSet   CR_ISROOT_BITSET =
        new FormatableBitSet(CR_ISROOT_COLID + 1);
    protected static final FormatableBitSet   CR_CONGLOM_BITSET =
        new FormatableBitSet(CR_CONGLOM_COLID + 1);


    /**
     * Values passed in the flag argument to splitFor.
     **/
    /* row causing split would be last row on leaf page */
    public static final int SPLIT_FLAG_LAST_ON_PAGE      = 0x000000001;
    /* row causing split would be last row in table */
    public static final int SPLIT_FLAG_LAST_IN_TABLE     = 0x000000002;
    /* row causing split would be first row on page */
    public static final int SPLIT_FLAG_FIRST_ON_PAGE     = 0x000000004;
    /* row causing split would be first row in table */
    public static final int SPLIT_FLAG_FIRST_IN_TABLE    = 0x000000008;

    /**
     * The slot at which all control rows reside.
     **/
	protected static final int CR_SLOT = 0;

	/*
	** Constructors of ControlRow
	*/

    static 
    {
        CR_VERSION_BITSET.set(CR_VERSION_COLID);
        CR_LEFTSIB_BITSET.set(CR_LEFTSIB_COLID);
        CR_RIGHTSIB_BITSET.set(CR_RIGHTSIB_COLID);
        CR_PARENT_BITSET.set(CR_PARENT_COLID);
        CR_LEVEL_BITSET.set(CR_LEVEL_COLID);
        CR_ISROOT_BITSET.set(CR_ISROOT_COLID);
        CR_CONGLOM_BITSET.set(CR_CONGLOM_COLID);
    }

    /**
     * No arg constructor.
     * <p>
     * GetControlRowForPage() will call this constructor when it uses the 
     * monitor to create a control row dynamically given a given format id.
     **/
    protected ControlRow()
    {
        this.scratch_row = 
            new DataValueDescriptor[getNumberOfControlRowColumns()];

        this.fetchDesc   = 
            new FetchDescriptor(
                this.scratch_row.length, (FormatableBitSet) null, (Qualifier[][]) null);
    }

    /**
     * Constructor for making a new control row as part of allocating a new
	 * page.  Fills in all the fields but does not write them anywhere.
     * <p>
	 * <P>
	 * Changes to this constructor will probably require changes to the
	 * corresponding accessor(s).
     *
     * @param btree      Static information about the btree.
     * @param container  The container in which this btree resides.
     * @param page       The page described by this control row.
     * @param parent     The parent page of this page, "null" if this page is 
     *                   root or if not maintaining parent links.
     * @param isRoot     Is this page the root of the tree?
     *
     *
     * @exception StandardException Standard exception policy.
     **/
	protected ControlRow(
    OpenBTree         btree,
    Page		      page, 
    int			      level, 
    ControlRow	      parent,
    boolean           isRoot
    )
        throws StandardException
	{
		// The caller is expected to have latched the pages.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(page.isLatched());
            SanityManager.ASSERT(parent == null || parent.page.isLatched());
        }

		// Maintain which page this control row describes.
		this.page = page;

		// Page numbers start out "invalid".  Presumably the caller will
		// link the page into a page chain as one of its next steps.
		leftSiblingPageNumber  = 
            new SQLLongint(btree.container.INVALID_PAGE_NUMBER);
		rightSiblingPageNumber = 
            new SQLLongint(btree.container.INVALID_PAGE_NUMBER);

		// Remember the parent if there is one and we're remembering parents.
        parentPageNumber = new SQLLongint( 
            (parent == null ?  
                 btree.container.INVALID_PAGE_NUMBER : 
                 parent.page.getPageNumber()));

		// All pages start out not being root pages.  The caller will setIsRoot
		// if this is going to be a root page. Zero means false - see 
		// getIsRoot/setIsRoot.
		this.isRoot = new SQLLongint(isRoot ? 1 : 0);

        // set the rest of the state, as passed in.
		this.level   = new SQLLongint(level);
        this.version = new StorableFormatId(getTypeFormatId());

        // If it is a root page then store the real btree conglomerate, if it
        // is not a root page then set up an "empty" btree conglomerate which
        // will be stored as "null".
        this.btree = 
            (isRoot ? 
             btree.getConglomerate() : 
             (BTree) Monitor.newInstanceFromIdentifier(
                btree.getConglomerate().getTypeFormatId()));

        // Initialize the object array to be used for interacting with raw
        // store to insert, fetch, and update the control row.
        this.row = new DataValueDescriptor[getNumberOfControlRowColumns()];
	    this.row[CR_VERSION_COLID]	= this.version;
	    this.row[CR_LEFTSIB_COLID]	= this.leftSiblingPageNumber;
	    this.row[CR_RIGHTSIB_COLID]	= this.rightSiblingPageNumber;
	    this.row[CR_PARENT_COLID]	= this.parentPageNumber;
	    this.row[CR_LEVEL_COLID]	= this.level;
	    this.row[CR_ISROOT_COLID]	= this.isRoot;
	    this.row[CR_CONGLOM_COLID]  = this.btree;


		// Make the control row the aux object for the page so control row
		// getters end up with the same row.
		page.setAuxObject(this);
	}

    /**
     * Constructor for making a control row for an existing page.
     * <p>
     * Not all the fields are filled in; their values will get faulted in from 
     * the page as necessary.
     * <p>
	 * Classes which extend ControlRow must delegate to this constructor
	 * and may want to override it as well.
	 * Changes to this constructor will probably require changes to the
	 * corresponding accessor(s).
     *
     * @param container  Open container 
     * @param page       The page described by this control row.
     *
     * @exception StandardException Standard exception policy.
     **/
	protected ControlRow(ContainerHandle container, Page page)
        throws StandardException
	{
        System.out.println("ControlRow construct 2.");

		// The caller is expected to have latched the pages.
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(page.isLatched());

		// Remember the page.
		this.page = page;

		// The rest of the fields are left null; they'll get faulted
		// in if/when necessary.  See the accessors.
	}

    /* Private/Protected methods of ControlRow: */

    /**
     * Get version of the control row.
     * <p>
     * Returns the version of the control row, faulting it in from the page
     * if necessary.
     *
	 * @return version of the control row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected int getVersion()
		throws StandardException
	{
		if (this.version == null)
		{
			// Fault in the version.
			this.version = new StorableFormatId();

            scratch_row[CR_VERSION_COLID] = this.version;

            fetchDesc.setValidColumns(CR_VERSION_BITSET);

            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
		}
		return this.version.getValue();
	}

    /**
     * Set version of the control row.
     * <p>
     * Sets the version of the control row.  Updates both the in-memory 
     * control row and the disk copy.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void setVersion(int version)
		throws StandardException
	{
		// Store the field.
		if (this.version == null)
			this.version = new StorableFormatId();
		this.version.setValue(version);

		// Write the field through to the underlying row.
		this.page.updateFieldAtSlot(
            CR_SLOT, CR_VERSION_COLID, this.version, null);
	}

	/**
	 * Get the control row for this page's left sibling, or null if there is no
	 * left sibling (which probably means it's the leftmost page at its level).
	 * Since right-to-left traversal of an index level	is deadlock-prone, this 
	 * method will only get get the left sibling if it can latch it without
	 * waiting.
     *
	 * @exception WaitError if the latch request would have had to wait.
     *
     * @exception StandardException Standard exception policy.
	 **/
	public ControlRow getLeftSibling(OpenBTree btree)
		throws StandardException, WaitError
	{
		ControlRow cr;
		
		long pageno = this.getleftSiblingPageNumber();

		// Is there a left sibling?
		if (pageno == ContainerHandle.INVALID_PAGE_NUMBER)
			return null;

		// Try to get the control row without waiting
		cr = ControlRow.GetNoWait(btree, pageno);
		if (cr == null)
			throw new WaitError();

		return cr;
	}

	protected void setLeftSibling(ControlRow leftsib)
        throws StandardException
	{
        long left_sib_pageno = 
            (leftsib == null ? ContainerHandle.INVALID_PAGE_NUMBER : 
                               leftsib.page.getPageNumber());
        
		// Store the field.
		if (leftSiblingPageNumber == null)
			leftSiblingPageNumber = new SQLLongint(left_sib_pageno);
        else
            this.leftSiblingPageNumber.setValue(left_sib_pageno);

		// Write the field through to the underlying row
        try
        {
            this.page.updateFieldAtSlot(
                CR_SLOT, CR_LEFTSIB_COLID, this.leftSiblingPageNumber, null);
        }
        catch (StandardException se)
        {
            // Since this is an update of a fixed length field it should 
            // never fail, but it has happened enough that an assert helps
            // with debugging.
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "setLeftSibling got an exception: " + se +
                    "control_row = " + this +
                    "trying to update field number " + CR_LEFTSIB_COLID + 
                    "to new value " + this.leftSiblingPageNumber);
            }
            throw(se);
        }
	}

	/**
	Return the control row for this page's right sibling.  Unlike getting
	the left sibling, it's ok to wait for the right sibling latch since
	left-to-right is the deadlock-free ordering.

    @exception StandardException Standard exception policy.
	**/
	protected ControlRow getRightSibling(OpenBTree open_btree)
		throws StandardException
	{
		long pageno = this.getrightSiblingPageNumber();

		// Return the control row for the page.
		if (pageno == ContainerHandle.INVALID_PAGE_NUMBER)
			return null;
		else
			return ControlRow.Get(open_btree, pageno);
	}

	// This method will have to update the row.
	protected void setRightSibling(ControlRow rightsib)
        throws StandardException
	{
        long right_sib_pageno = 
            (rightsib == null ? ContainerHandle.INVALID_PAGE_NUMBER : 
                                rightsib.page.getPageNumber());
        
		// Store the field.
		if (rightSiblingPageNumber == null)
			rightSiblingPageNumber = new SQLLongint(right_sib_pageno);
        else
            this.rightSiblingPageNumber.setValue(right_sib_pageno);

		// Write the field through to the underlying row
        try
        {
		this.page.updateFieldAtSlot(
            CR_SLOT, CR_RIGHTSIB_COLID, this.rightSiblingPageNumber, null);
        }
        catch (StandardException se)
        {
            // Since this is an update of a fixed length field it should 
            // never fail, but it has happened enough that an assert helps
            // with debugging.

            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "setRightSibling got an exception: " + se +
                    "control_row = " + this +
                    "trying to update field number " + CR_RIGHTSIB_COLID + 
                    "to new value " + this.rightSiblingPageNumber);
            }
            throw(se);
        }
	}

	/**
	Get the page number of the left sibling. Fault it's value in if it
    hasn't been yet.

    @exception StandardException Standard exception policy.
	**/
	public long getleftSiblingPageNumber()
        throws StandardException
	{
		if (this.leftSiblingPageNumber == null)
		{
			// Fault in the page number.
			this.leftSiblingPageNumber = new SQLLongint();

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(scratch_row != null);

            scratch_row[CR_LEFTSIB_COLID] = this.leftSiblingPageNumber;

            fetchDesc.setValidColumns(CR_LEFTSIB_BITSET);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 

		}

        return(leftSiblingPageNumber.getLong());
	}

	/**
	Get the page number of the right sibling. Fault it's value in if it
    hasn't been yet.

    @exception StandardException Standard exception policy.
	**/
	protected long getrightSiblingPageNumber()
        throws StandardException
	{
		if (this.rightSiblingPageNumber == null)
		{
			// Fault in the page number.
			this.rightSiblingPageNumber = new SQLLongint();

            scratch_row[CR_RIGHTSIB_COLID] = this.rightSiblingPageNumber;

            fetchDesc.setValidColumns(CR_RIGHTSIB_BITSET);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
		}

        return(rightSiblingPageNumber.getLong());
	}

	/**
	Get the page number of the parent, if it's being maintained.
	Note that there is intentionally no way to get the control
	row for the parent page - the b-tree code NEVER traverses
	up the tree, even in consistency checks.

    @exception StandardException Standard exception policy.
	**/
	protected long getParentPageNumber()
        throws StandardException
	{
		if (this.parentPageNumber == null)
		{
			// Fault in the page number.
			this.parentPageNumber = new SQLLongint();

            scratch_row[CR_PARENT_COLID] = this.parentPageNumber;

            fetchDesc.setValidColumns(CR_PARENT_BITSET);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
		}

		// See NOTE3 about converting from int to long.
		long pageno = parentPageNumber.getLong();
		return pageno;
	}
	
	void setParent(long parent)
        throws StandardException
	{
		// Store the field.
		if (parentPageNumber == null)
			parentPageNumber = new SQLLongint();
		this.parentPageNumber.setValue(parent);

		// Write the field through to the underlying row
        try
        {
            this.page.updateFieldAtSlot(
                CR_SLOT, CR_PARENT_COLID, this.parentPageNumber, null);
        }
        catch (StandardException se)
        {
            // Since this is an update of a fixed length field it should 
            // never fail, but it has happened enough that an assert helps
            // with debugging.

            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "setParent got an exception: " + se +
                    "control_row = " + this +
                    "trying to update field number " + CR_PARENT_COLID + 
                    "to new value " + this.parentPageNumber);
            }
            throw(se);
        }

        return;
	}

	protected int getLevel()
        throws StandardException
	{
		if (this.level == null)
		{
			// Fault in the level
			this.level = new SQLLongint();

            scratch_row[CR_LEVEL_COLID] = this.level;

            fetchDesc.setValidColumns(CR_LEVEL_BITSET);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
		}

		return((int) this.level.getLong());
	}

	protected void setLevel(int newlevel)
        throws StandardException
	{
		// Store the field.
		if (this.level == null)
			this.level = new SQLLongint();
		this.level.setValue((long) newlevel);

		// Write the field through to the underlying row.
		this.page.updateFieldAtSlot(CR_SLOT, CR_LEVEL_COLID, this.level, null);
	}

	protected boolean getIsRoot()
        throws StandardException
	{
		// convert 1 to true, 0 to false;
        
		if (this.isRoot == null)
		{
			// Fault in the level
			this.isRoot = new SQLLongint();

            scratch_row[CR_ISROOT_COLID] = this.isRoot;

            fetchDesc.setValidColumns(CR_ISROOT_BITSET);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
		}

		return((this.isRoot.getLong() == 1));
	}
	
	protected void setIsRoot(boolean isRoot)
        throws StandardException
	{
        // RESOLVE (mmm) - need to store more efficiently //

		// Store the field.
		if (this.isRoot == null)
			this.isRoot = new SQLLongint();

		this.isRoot.setValue((isRoot) ? 1 : 0);

		// Write the field through to the underlying row.
		this.page.updateFieldAtSlot(
            CR_SLOT, CR_ISROOT_COLID, this.isRoot, null);
	}

    /**
     * Get format id information for row on page.
     * <p>
     * Returns the format id information for a row on the page. faulting it 
     * in from the page if necessary.
     *
	 * @return format id of a row on the page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public BTree getConglom(int format_id)
		throws StandardException
	{

        if (SanityManager.DEBUG)
        {
            // this call is only valid on root pages.  If called on non
            // root pages it will return a "null" conglom object.
            SanityManager.ASSERT(
                (this.page.getPageNumber() == BTree.ROOTPAGEID) && getIsRoot());
        }

		if (this.btree == null)
		{
            // use format id to create empty instance of Conglomerate class
            this.btree = (BTree) Monitor.newInstanceFromIdentifier(format_id);

            scratch_row[CR_CONGLOM_COLID] = this.btree;

            fetchDesc.setValidColumns(CR_CONGLOM_BITSET);
            this.page.fetchFromSlot(
               (RecordHandle) null, CR_SLOT, scratch_row, fetchDesc, false); 
		}
		return this.btree;
	}

    /**
     * Set the conglomerate field in the btree.
     * <p>
     * Sets the btree field of the control row.  Updates just the disk copy. 
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void setConglom(BTree btree)
		throws StandardException
	{
		// Store the field.  Delay faulting value into object until getConlgom()
        // call, which in general only happens during recovery.

		// Write the field through to the underlying row.
		this.page.updateFieldAtSlot(CR_SLOT, CR_CONGLOM_COLID, btree, null);
	}

	/*
	** Methods for getting control rows from pages.
	*/

	/**
	  Get the control row from the given page in the b-tree.
	  The returned control row will be of the correct type
	  for the page (i.e., either a LeafControlRow or a
	  BranchControlRow).

    @exception StandardException Standard exception policy.
	 **/
	public static ControlRow Get(OpenBTree open_btree, long pageNumber)
		throws StandardException
	{
        return(ControlRow.Get(open_btree.container, pageNumber));
	}

	public static ControlRow Get(ContainerHandle container, long pageNumber)
		throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(container != null, 
                "ControlRow.Get() is being called on a closed container.");
        }

		// Get the page, waiting if necessary.  The page is returned latched.
		Page page = container.getPage(pageNumber);

        if (SanityManager.DEBUG)
        {
			if (page == null)
            	SanityManager.THROWASSERT(
                	"No page at pagenumber: " + pageNumber +
                	"; ContainerHandle = "    + container);
        }

		// Return the corresponding control row.
		return GetControlRowForPage(container, page);
	}

	/**
	Get the control row for the given page if the latch on the
	page can be obtained without waiting, else return null.

    @exception StandardException Standard exception policy.
	**/
	public static ControlRow GetNoWait(
    OpenBTree       open_btree, 
    long            pageNumber)
		throws StandardException
	{
		// Try to get the page without waiting.  If we would have
		// to wait, return null.
		Page page = open_btree.container.getUserPageNoWait(pageNumber);
		if (page == null)
			return null; 

		// Got the page without waiting.  Return the corresponding
		// control row.
		return GetControlRowForPage(open_btree.container, page);
	}

	protected static ControlRow GetControlRowForPage(
    ContainerHandle container,
    Page            page)
        throws StandardException
	{
        ControlRow cr = null;

		// See if the control row is still cached with the page
		// If so, just use the cached control row.
		AuxObject auxobject = page.getAuxObject();
		if (auxobject != null)
			return (ControlRow) auxobject;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(page.recordCount() >= 1);

		// No cached control row, so create a new one.
        
        // Use the version field to determine the type of the row to
        // create.  This routine depends on the version field being the same
        // number column in all control rows.
        
        StorableFormatId version = new StorableFormatId();

        DataValueDescriptor[] version_ret = new DataValueDescriptor[1];
        
        version_ret[0] = version;

        // TODO (mikem) - get rid of this new.

        page.fetchFromSlot(
           (RecordHandle) null, CR_SLOT, version_ret,
           new FetchDescriptor(1, CR_VERSION_BITSET, (Qualifier[][]) null), 
           false); 

        // use format id to create empty instance of right Conglomerate class
        cr = (ControlRow) Monitor.newInstanceFromIdentifier(version.getValue());
        cr.page = page;

        // call page specific initialization.
        cr.ControlRowInit();

        // cache this Control row with the page in the cache.
		page.setAuxObject(cr);

        return(cr);
	}

	/**
	Release this control row's resources.
	**/
	public void release()
	{
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(page != null);

        if (page != null)
            page.unlatch();

		// It might be nice to set the page object to null, but
		// since there might be multiple control rows on this
		// page, we'd have to maintain a use count.  Rather than
		// doing that we'll let the garbage collector earn its
		// keep.  We are also expecting that the raw store will
		// throw errors if we attempt to use an unlatched page.
	}

    /**
    Search this index page.
    <P>
    This method is very performance sensitive.  It is the intention that no
    object allocations occur during the execution of this method.
    <P>
    This method performs a binary search on the page and finds the entry i on
    the page such that entry[i] <= key < entry[i+1].  The result of the search
    is filled into the passed in params structure.

    @param params the parameters of the search

    @exception StandardException could be thrown by underlying raw store 
    operations.

    @see SearchParameters
    **/
    protected void searchForEntry(SearchParameters params)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            // System.out.println("searchForEntry() enter: params:" + params);
            // System.out.println("searchForEntry() enter: this:"   + this);
            // System.out.println("searchForEntry() enter: this page:"   + debugPage(params.btree));
        }


        // leftrange and rightrange indicates the range of slots to search.
        // The range starts as all the slots on the page not including slot
        // 0 which is the control row.
        int leftrange  = 1;
        int rightrange = page.recordCount() - 1;

        // leftslot and rightslot if non-zero, mean that the key has been
        // compared to the row at that slot.  If non-zero the key must be
        // greater than the key at leftslot and the key must be lest than
        // the key at rightslot.
        int leftslot  = 0;
        int rightslot = rightrange + 1;

        int midslot;
        int compare_ret;

        // search until you either exactly find the key, or you have 
        // compared 2 adjacent rows and found the value must exist between
        // the 2.


        if (this.use_last_search_result_hint)
        {
            // make sure to set midslot to point to somwhere in the legal range.
            midslot = 
                ((this.last_search_result == 0) ? 1 : this.last_search_result);

            if (midslot > rightrange)
                midslot = rightrange;
        }
        else
        {
            // if we don't think we have a good hint where to start the search
            // just go to the middle.
            midslot = (leftrange + rightrange) / 2;
        }

        if (SanityManager.DEBUG)
        {
            if ((leftslot != (rightslot - 1)) &&
                !(midslot >= leftrange && midslot <= rightrange))
            {
                SanityManager.THROWASSERT(
                    "midslot = "     + midslot +
                    ";leftrange = "  + leftrange +
                    ";rightrange = " + rightrange);
            }
        }

        
        while (leftslot != (rightslot - 1))
        {
			// Compare the index row to the key.
			compare_ret = 
                CompareIndexRowFromPageToKey(
                    this,
                    midslot,
                    params.template, params.searchKey, 
                    params.btree.getConglomerate().nUniqueColumns, 
                    params.partial_key_match_op,
                    params.btree.getConglomerate().ascDescInfo);

            if (compare_ret == 0)
            {
                // Found exact match
				params.resultSlot = midslot;
				params.resultExact = true;

                // update the hints based on result of the search.
                use_last_search_result_hint = 
                    (midslot == this.last_search_result) ? true : false;
                this.last_search_result = midslot;

                return;
            }
            else if (compare_ret > 0)
            {
                // key falls to the left of midslot
                rightslot  = midslot;
                rightrange = midslot - 1;
            }
            else
            {
                // key falls to the right of midslot
                leftslot   = midslot;
                leftrange  = midslot + 1;
            }

            midslot = (leftrange + rightrange) / 2;
            //midslot = (leftrange + rightrange) >> 1;
        }

        // update the hints based on result of the search.
        this.use_last_search_result_hint = 
            (leftslot == this.last_search_result);
        this.last_search_result = leftslot;

        // no exact match found, leftslot will point at the slot on the
        // page just before where the row should be inserted.  In the case
        // where the key is before rows on the page then leftslot will be
        // 0 (an empty page is a special case of this).
        if (SanityManager.DEBUG)
        {
			if (leftslot != rightslot - 1)
            	SanityManager.THROWASSERT(
                	"leftslot = " + leftslot + "; rightslot = " + rightslot);
        }

        params.resultSlot  = leftslot;
        params.resultExact = false;

        if (SanityManager.DEBUG)
        {
            // System.out.println("searchForEntry() exit: params:" + params);
        }

        return;
    }

    protected void searchForEntryBackward(SearchParameters params)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            // System.out.println("searchForEntry() enter: params:" + params);
            // System.out.println("searchForEntry() enter: this:"   + this);
            // System.out.println("searchForEntry() enter: this page:"   + debugPage(params.btree));
        }


        // leftrange and rightrange indicates the range of slots to search.
        // The range starts as all the slots on the page not including slot
        // 0 which is the control row.
        int leftrange  = 1;
        int rightrange = page.recordCount() - 1;

        // leftslot and rightslot if non-zero, mean that the key has been
        // compared to the row at that slot.  If non-zero the key must be
        // greater than the key at leftslot and the key must be lest than
        // the key at rightslot.
        int leftslot  = 0;
        int rightslot = rightrange + 1;

        int midslot;
        int compare_ret;

        // search until you either exactly find the key, or you have 
        // compared 2 adjacent rows and found the value must exist between
        // the 2.


        if (this.use_last_search_result_hint)
        {
            // make sure to set midslot to point to somwhere in the legal range.
            midslot = 
                ((this.last_search_result == 0) ? 1 : this.last_search_result);

            if (midslot > rightrange)
                midslot = rightrange;
        }
        else
        {
            // if we don't think we have a good hint where to start the search
            // just go to the middle.
            midslot = (leftrange + rightrange) / 2;
        }

        if (SanityManager.DEBUG)
        {
            if ((leftslot != (rightslot - 1)) &&
                !(midslot >= leftrange && midslot <= rightrange))
            {
                SanityManager.THROWASSERT(
                    "midslot = "     + midslot +
                    ";leftrange = "  + leftrange +
                    ";rightrange = " + rightrange);
            }
        }

        
        while (leftslot != (rightslot - 1))
        {
			// Compare the index row to the key.
			compare_ret = 
                CompareIndexRowFromPageToKey(
                    this,
                    midslot,
                    params.template, params.searchKey, 
                    params.btree.getConglomerate().nUniqueColumns, 
                    params.partial_key_match_op,
                    params.btree.getConglomerate().ascDescInfo);

            if (compare_ret == 0)
            {
                // Found exact match
				params.resultSlot = midslot;
				params.resultExact = true;

                // update the hints based on result of the search.
                use_last_search_result_hint = 
                    (midslot == this.last_search_result) ? true : false;
                this.last_search_result = midslot;

                return;
            }
            else if (compare_ret > 0)
            {
                // key falls to the left of midslot
                rightslot  = midslot;
                rightrange = midslot - 1;
            }
            else
            {
                // key falls to the right of midslot
                leftslot   = midslot;
                leftrange  = midslot + 1;
            }

            midslot = (leftrange + rightrange) / 2;
            //midslot = (leftrange + rightrange) >> 1;
        }

        // update the hints based on result of the search.
        this.use_last_search_result_hint = 
            (leftslot == this.last_search_result);
        this.last_search_result = leftslot;

        // no exact match found, leftslot will point at the slot on the
        // page just before where the row should be inserted.  In the case
        // where the key is before rows on the page then leftslot will be
        // 0 (an empty page is a special case of this).
        if (SanityManager.DEBUG)
        {
			if (leftslot != rightslot - 1)
            	SanityManager.THROWASSERT(
                	"leftslot = " + leftslot + "; rightslot = " + rightslot);
        }

        params.resultSlot  = leftslot;
        params.resultExact = false;

        if (SanityManager.DEBUG)
        {
            // System.out.println("searchForEntry() exit: params:" + params);
        }

        return;
    }

	/**
	Compare two orderable rows, considering nCompareCols, and return -1, 0, or 1
	depending on whether the first row (indexrow) is less than, equal to, or 
    greater than the second (key).  The key may have fewer columns present 
    than nCompareCols.

	In such a case, if all the columns of the partial key match all of the 
    corresponding columns in the index row, then the value passed in in 
    partialKeyOrder is returned.  The caller should pass in partialKeyOrder=1 
    if the index rows which match a partial key should be considered to be 
    greater than the partial key, and -1 if they should be considered to be 
    less.

    This routine only reads objects off the page if it needs them, so if a 
    multi-part key differs in the first column the subsequent columns are not
    read.

    @param indexpage Controlrow of page to get target row from.
    @param slot      Slot to get control row from.
    @param indexrow template of the target row (the row in the index).
	@param key the (possibly partial) search key.
	@param nCompareCols the number of columns to compare.
	@param partialKeyOrder what to return on a partial key match.
	@param ascOrDesc column sort order information
	@throws StandardException if lower levels have a problem.

    @exception StandardException Standard exception policy.
	**/
	public static int CompareIndexRowFromPageToKey(
    ControlRow              indexpage,
    int                     slot,
    DataValueDescriptor[]   indexrow, 
    DataValueDescriptor[]	key,
    int                     nCompareCols, 
    int                     partialKeyOrder,
    boolean[]               ascOrDesc)
        throws StandardException
	{
        int compare_result;

		// Get the actual number of key columns present
		// in the partial key.
		int partialKeyCols = key.length;

        // Fetch entire index row from page.
        // RESOLVE (mikem) - it may be more efficient to fetch just the
        // columns you need, but there is overhead currently in raw
        // store, since to get to the n'th column you have to walk 
        // through the preceding n-1 columns.
        indexpage.page.fetchFromSlot(
            (RecordHandle) null, slot, indexrow, 
            (FetchDescriptor) null,
            true);

		// Compare corresponding columns in the index row and the key.
		for (int i = 0; i < nCompareCols; i++)
		{
			// See if we have run out of partial key columns.
			if (i >= partialKeyCols)
			{
				// All the columns of the partial key match, and 
				// there are more columns in the index row.  We
				// want to return -1 or 1, depending on whether the
				// caller wants to direct the search to the beginning
				// of this key range or the beginning of the next
				// one.  If the caller passes in -1, the index row
				// will appear less than the partial key, sending the
				// search to the next range ("to the right").  If the
				// caller passes in 1, the index row will appear
				// to be greater than the search key, sending the search
				// to the beginning of the range ("to the left").
				return partialKeyOrder;
			}

			// Get the corresponding columns to compare

			// Orderable indexcol = (Orderable) indexrow[i];
			// Orderable keycol = (Orderable) key[i];

			// Compare them.
			// int r = indexcol.compare(keycol);

            int r = indexrow[i].compare(key[i]);

			// If the columns don't compare equal, we're done.
			// Return the sense of the comparison.
			if (r != 0)
			{
				//coulmns could have been sorted in ascending or descending
				//order. depending on ascending/descending order search 
				//direction will change.

				if (ascOrDesc[i])  // true - Ascending order
					return r;
				else
					return -r;
		    }
		}

		// We made it through all the columns, and they must have
		// all compared equal.  So return that the rows compare equal.
		return 0;
	}

	public static int CompareIndexRowToKey(
    DataValueDescriptor[]   indexrow, 
    DataValueDescriptor[]   key,
    int                     nCompareCols, 
    int                     partialKeyOrder,
    boolean[]               ascOrDesc)
        throws StandardException
	{
		// Get the actual number of key columns present
		// in the partial key.
		int partialKeyCols = key.length;

		// Compare corresponding columns in the index row and the key.
		for (int i = 0; i < nCompareCols; i++)
		{
			// See if we have run out of partial key columns.
			if (i >= partialKeyCols)
			{
				// All the columns of the partial key match, and 
				// there are more columns in the index row.  We
				// want to return -1 or 1, depending on whether the
				// caller wants to direct the search to the beginning
				// of this key range or the beginning of the next
				// one.  If the caller passes in -1, the index row
				// will appear less than the partial key, sending the
				// search to the next range ("to the right").  If the
				// caller passes in 1, the index row will appear
				// to be greater than the search key, sending the search
				// to the beginning of the range ("to the left").
				return partialKeyOrder;
			}

			// Get the corresponding columns to compare
			DataValueDescriptor indexcol = indexrow[i];
			DataValueDescriptor keycol = key[i];

			// Compare them.
			int r = indexcol.compare(keycol);

			// If the columns don't compare equal, we're done.
			// Return the sense of the comparison.
			if (r != 0)
			{
				if (ascOrDesc[i])  // true - column in ascending order
					return r;
				else
					return -r;
		    }
		}

		// We made it through all the columns, and they must have
		// all compared equal.  So return that the rows compare equal.
		return 0;
	}

	/**
	 ** Perform consistency checks which are common to all
	 ** pages that derive from ControlRow (both leaf and 
	 ** branch pages).  The checks are:
	 ** <menu>
	 ** <li> This page thinks the parent argument is actually
	 **      its parent.
	 ** <li> The level of this page is 1 less than the level of
	 **      the parent.
	 ** <li> All the rows on the page are in order.
	 ** <li> Both left and right siblings, if they exist, are at
	 **      the same level of this page.
	 ** <li> This page is the left sibling of its right sibling,
	 **      and it's the right sibling of its left sibling.
	 ** <li> The last row on the left sibling is < the first
	 **      row on this page.
	 ** <li> The first row on the right sibling is > than the
	 **      the last row on this page.
	 ** </menu>
	 ** Note that these last two are really only true if there
	 ** are never duplicate keys.

    @exception StandardException Standard exception policy.
	 **/  
	protected void checkGeneric(
    OpenBTree  btree, 
    ControlRow parent,
    boolean    check_other_pages)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.page.recordCount() >= 1);
            SanityManager.ASSERT(!this.page.isDeletedAtSlot(0));

            // Make sure that we think we're a child of the parent.
			if (((parent != null) &&
                 (parent.page.getPageNumber() != this.getParentPageNumber())))
            	SanityManager.THROWASSERT(this + " not child of " + parent);

            // Check this page's level.
			if (((parent != null) &&
                 (parent.getLevel() != this.getLevel() + 1)))
            	SanityManager.THROWASSERT(this +
						" at wrong level when compared to parent:" + parent);
            
            // Check rows are in order.
            checkRowOrder(btree, parent);

            // Check siblings.
            if (check_other_pages)
                checkSiblings(btree);
        }
	}

	/**
	 ** Check that all rows on the page are in order.  This
	 ** means that each key is > than the previous key.

    @exception StandardException Standard exception policy.
	 **/
	protected boolean checkRowOrder(OpenBTree btree, ControlRow parent)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            RecordHandle            lesser_handle   = null;
            RecordHandle            greater_handle  = null;
            DataValueDescriptor[]   lesser          = getRowTemplate(btree);
            DataValueDescriptor[]   greater         = getRowTemplate(btree);
            boolean                 is_consistent   = true;
           
            
            int numslots = page.recordCount();
            for (int i = ControlRow.CR_SLOT + 1; (i + 1) < numslots; i++)
            {
               lesser_handle = 
                   page.fetchFromSlot(
                       (RecordHandle) null, i, lesser, 
                       (FetchDescriptor) null, true); 
               greater_handle = 
                   page.fetchFromSlot(
                       (RecordHandle) null, i + 1, greater, 
                       (FetchDescriptor) null, true); 

               SanityManager.ASSERT(btree.getConglomerate().nUniqueColumns <= 
                                    btree.getConglomerate().nKeyFields);
               int compare_result = 
                   CompareIndexRowToKey(
                       lesser, greater,
                       btree.getConglomerate().nUniqueColumns, 0,
                       btree.getConglomerate().ascDescInfo);

               // >= 0 means that lesser >= greater
			   if (compare_result >= 0)
               {
                   SanityManager.THROWASSERT(
                       "Bad order of rows found in conglomerate: " + btree +
                       "\n." +
                       "compare result = " + compare_result + ".  " + 
                       "nKeyFields = "     + btree.getConglomerate().nKeyFields +
                       ".\n" +  
                       this + " rows " + (i) + " and " + (i + 1) +
                       " out of order.\n" +
                       "row[" + i + "] + "  + RowUtil.toString(lesser) + "\n" + 
                       "row[" + (i + 1) + "] + "  + RowUtil.toString(greater) +
                       "\ndump of page = " + 
                           debugPage(btree) +
                       "\ndump of parent page = " + 
                           ((parent != null) ? 
                                parent.debugPage(btree) : "null parent") +
                       "\rawstore dump = " + this.page);

                   is_consistent = false;
               }
            }
            return(is_consistent);
        }
        else
        {
            return(true);
        }
	}

    protected boolean compareRowsOnSiblings(
        OpenBTree   btree,
        ControlRow  left_sib,
        ControlRow  right_sib)
            throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            boolean is_consistent = true;

            // Check that left last row is < right page's first row.
            if (left_sib.page.recordCount()  > 1 && 
                right_sib.page.recordCount() > 1)
            {
                DataValueDescriptor[] left_lastrow   = getRowTemplate(btree);
                DataValueDescriptor[] right_firstrow = getRowTemplate(btree);

                RecordHandle    left_lastrow_handle   = 
                    left_sib.page.fetchFromSlot(
                        (RecordHandle) null, left_sib.page.recordCount() - 1, 
                        left_lastrow, 
                        (FetchDescriptor) null, true); 

                RecordHandle    right_firstrow_handle  = 
                    right_sib.page.fetchFromSlot(
                        (RecordHandle) null, 1, right_firstrow, 
                        (FetchDescriptor) null, true); 

                int r = 
                    CompareIndexRowToKey(
                        left_lastrow, right_firstrow,
                        btree.getConglomerate().nUniqueColumns,
                        0, btree.getConglomerate().ascDescInfo);

				if (r >= 0)
                {
                	SanityManager.THROWASSERT(
                      "last row on left page " + 
                          left_sib.page.getPageNumber() + 
                      " > than first row on right page " + 
                          right_sib.page.getPageNumber() + "\n" + 
                      "left last row = " + RowUtil.toString(left_lastrow) +
                      "right first row = " + RowUtil.toString(right_firstrow)+
                      left_sib + " last > first of " + right_sib);

                    is_consistent = false;
                }
            }
            return(is_consistent);
        }
        else
        {
            return(true);
        }
    }

	/**
	 ** Perform checks on the siblings of this page: make sure
	 ** that they're at the same level as this page, that they're
	 ** mutually linked together, and that the first/last keys
	 ** on sibling pages are in order.

    @exception StandardException Standard exception policy.
	 **/
	protected void checkSiblings(OpenBTree btree)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            // Get this page's left sibling.
            ControlRow leftsib  = null;
            ControlRow rightsib = null;

            try
            {
                try
                {
                    leftsib = this.getLeftSibling(btree);
                }
                catch (WaitError e)
                {
                    // In a normal system it may be possible to not get
                    // the left sibling (some other thread either user
                    // or daemon cache cleaner thread) may already have
                    // the latch on the page, and waiting on it could cause
                    // a latch/latch deadlock.  So for now just give up
                    // doing the consistency check in this case.
                    //
                    // RESOLVE (mikem) - We could do fancier things than 
                    // ignore this error, but the only safe way to wait for
                    // a right to left latch is to release current latch which
                    // will complicate all the code, and this is only a sanity
                    // check.

                    // SanityManager.DEBUG_PRINT(
                    //   "ControlRow.checkSiblings",
                    //   this + " left sibling deadlock");

                    // give up on checking the left sibling.
                    leftsib = null;
                }

                // There may not be a left sibling; if there is, check it out.
                if (leftsib != null)
                {
                    // Check that it's at the same level as this page.
					if (leftsib.getLevel() != this.getLevel())
                    	SanityManager.THROWASSERT(
                        	(leftsib + "not at same level as " + this));

                    // Check that its right sibling is this page.
                    long hopefullythis_pageno = 
                        leftsib.getrightSiblingPageNumber();

					if (hopefullythis_pageno != this.page.getPageNumber())
                    	SanityManager.THROWASSERT(
                        	"right sibling of " + leftsib + " isn't "  + this);

                    // Check that its last row is < this page's first row.
                    compareRowsOnSiblings(btree, leftsib, this);

                    // Done looking at the left sibling.
                    leftsib.release();
                    leftsib = null;
                }

                // Get the right sibling page.
                rightsib = this.getRightSibling(btree);

                // There may not be a right sibling; if there is, check it out.
                if (rightsib != null)
                {
                    // Check that it's at the same level as this page.
					if (rightsib.getLevel() != this.getLevel())
                    	SanityManager.THROWASSERT(
                        	rightsib + "not at same level as " + this);

                    // Check that its left sibling is this page.
                    long hopefullythis_pageno = 
                        rightsib.getleftSiblingPageNumber();

					if (hopefullythis_pageno != this.page.getPageNumber())
                    	SanityManager.THROWASSERT(
                        	"left sibling of " + rightsib + " isn't "  + this);

                    // Check that its first row is > this page's last row.
                    compareRowsOnSiblings(btree, this, rightsib);

                    // Done looking at it.
                    rightsib.release();
                    rightsib = null;
                }
            }
            finally
            {
                if (leftsib != null)
                    leftsib.release();
                if (rightsib != null)
                    rightsib.release();
            }
        }
	}

	/**
	 ** Link this page to the right of the target page.
	 ** <P>
	 ** Upon entry, this page and the target must be
	 ** latched.  Upon exit, this page and the target
	 ** remain latched.
	 ** <P>
	 ** This method carefully acquires pages from left
	 ** to right in order to avoid deadlocks.

    @exception StandardException Standard exception policy.
	 */
	void linkRight(OpenBTree btree, ControlRow target)
        throws StandardException
	{
		ControlRow rightSibling = null;

        try
        {
            rightSibling = target.getRightSibling(btree);
            this.setRightSibling(rightSibling);
            this.setLeftSibling(target);
            if (rightSibling != null)
                rightSibling.setLeftSibling(this);
            target.setRightSibling(this);
        }
        finally
        {
            if (rightSibling != null)
                rightSibling.release();
        }
	}

	/**
	 ** Unlink this page from its siblings.  This method
	 ** will give up and return false rather than run the
	 ** risk of a deadlock.
	 ** <P>
	 ** On entry this page must be latched.  The siblings
	 ** are latched and unlatched during the operation.  Upon
	 ** exit, this page will remain latched, but unlinked from
	 ** its siblings and deallocated from the container.
	 ** <P>
	 ** The seemingly odd situation that this page will be
	 ** returned latched but deallocated is intentional.
	 ** The container will not be able to reuse this page
	 ** until the latch is released, and the caller may still
	 ** need to read information out of it.

    @exception StandardException Standard exception policy.
	 **/
	boolean unlink(OpenBTree btree)
        throws StandardException
	{
		ControlRow leftsib  = null;
		ControlRow rightsib = null;

		
        try 
        {
            // Try to get the left sibling, and give up if 
            // it can't be obtained without waiting.

            try
            {
                leftsib = this.getLeftSibling(btree);
            }
            catch (WaitError e)
            {
                return false;
            }

            // We can wait for the right sibling since it's
            // in the deadlock-free direction.

            rightsib = this.getRightSibling(btree);

            // Change the links that pointed to this page to
            // point to the appropriate sibling.

            if (leftsib != null)
                leftsib.setRightSibling(rightsib);
            if (rightsib != null)
                rightsib.setLeftSibling(leftsib);

            // Deallocate the page.
            // Would need to clear out aux object here.
            //
            // RESOLVE (mikem) - how to deallocate a page. //
            btree.container.removePage(this.page);

            // After removePage call the current page is unlatched, and should
            // not be referenced anymore.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(!this.page.isLatched());
            }

            return true;
        }
        finally
        {
            // Unlatch the siblings.
            if (leftsib != null)
                leftsib.release();
            if (rightsib != null)
                rightsib.release();
        }
	}

    public Page getPage()
    {
        return(page);
    }

    /**
     * Get the row.
     * <p>
     * Return the object array that represents the control row for use
     * in raw store fetch, insert, and/or update.
     *
	 * @return The row.
     *
     **/
    protected final DataValueDescriptor[] getRow()
    {
        return(row);
    }

	/*
	 * The following methods must be implemented by all
	 * control rows.
	 */

    /**
     * Check consistency of the page and its children, returning the number of 
     * pages seen, and throwing errors if inconsistencies are found.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param btree  The open btree to associate latches/locks with.
     * @param parent The parent page of this page, "null" if this page is 
     *               root or if not maintaining parent links.
     * @param check_other_pages
     *               Should the consistency check go to other pages (this 
     *               option breaks the latch protocol).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	abstract protected int checkConsistency(
    OpenBTree  btree, 
    ControlRow parent,
    boolean    check_other_pages)
        throws StandardException;

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
	protected abstract ControlRow getLeftChild(OpenBTree btree)
        throws StandardException;

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
	protected abstract ControlRow getRightChild(OpenBTree btree)
        throws StandardException;

    /**
     * Perform page specific initialization.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     **/
    protected abstract void ControlRowInit();

    /**
     * Is the current page the leftmost leaf of tree?
     * <p>
     *
	 * @return true if the current page is the leftmost leaf of the tree,
     *              else return false.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public abstract boolean isLeftmostLeaf()
		throws StandardException;

    /**
     * Is the current page the rightmost leaf of tree?
     * <p>
     *
	 * @return true if the current page is the rightmost leaf of the tree,
     *              else return false.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public abstract boolean isRightmostLeaf()
		throws StandardException;

	/**
	 ** Perform a recursive search, ultimately returning the latched
	 ** leaf page and row slot after which the given key belongs.
	 ** The slot is returned in the result structure.  If the key
	 ** exists on the page, the resultExact field will be true.  Otherwise,
	 ** resultExact field will be false, and the row slot returned will be
	 ** the one immediately preceding the position at which the key
	 ** belongs.

    @exception StandardException Standard exception policy.
	 **/
	public abstract ControlRow search(
        SearchParameters    search_params)
            throws StandardException;
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
    protected abstract int getNumberOfControlRowColumns();
	
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
	protected abstract ControlRow searchLeft(OpenBTree btree)
        throws StandardException;

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
	protected abstract ControlRow searchRight(OpenBTree btree)
        throws StandardException;

	/**
	 **	Perform a recursive shrink operation for the key.
	 ** If this method returns true, the caller should
	 ** remove the corresponding entry for the page.
	 ** This routine is not guaranteed to successfully
	 ** shrink anything.  The page lead to by the key might
	 ** turn out not to be empty by the time shrink gets
	 ** there, and shrinks will give up if there is a deadlock.
     ** <P>
     ** As currently implemented shrinkFor must be executed while holding
     ** an exclusive container lock on the entire table.  It is expected that
     ** this call is made within an internal transaction which has been called
     ** by a post commit thread.  Latches are released by the code.  The raw 
     ** store guarantees that deallocated pages are not seen by other xacts
     ** until the transaction has been committed.  
	 ** <P>
     ** Note that a non-table level lock implementation must hold latches on
     ** pages affected until end transaction.
     ** <p>
     ** On entry, the current page is latched.  On exit, all pages will have
     ** been unlatched. 
     ** 
     ** @exception StandardException Standard exception policy.
	 **/
	protected abstract boolean shrinkFor(
    OpenBTree               btree, 
    DataValueDescriptor[]   key)
        throws StandardException;

    /**
     * Perform a top down split pass making room for the the key in "row".
     * <p>
     * Perform a split such that a subsequent call to insert
	 * given the argument index row will likely find room for it.  Since 
     * latches are released the client must code for the case where another
     * user has grabbed the space made available by the split pass and be
     * ready to do another split.
     * <p>
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
	protected abstract long splitFor(
    OpenBTree               open_btree, 
    DataValueDescriptor[]   template, 
    BranchControlRow        parentpage, 
    DataValueDescriptor[]   row,
    int                     flag)
        throws StandardException;

	/**
	 ** Recursively print the tree starting at current node in tree.

    @exception StandardException Standard exception policy.
	 **/
	public abstract void printTree(
    OpenBTree  btree) 
        throws StandardException;
	


	/*
	** Methods of AuxObject
	*/

	/**
		Called when the page is being evicted from cache or when a rollback
		happened on the page and may possibly have changed the control row's 
        value

		@see AuxObject#auxObjectInvalidated
	**/
 	public void auxObjectInvalidated()
	{
		version = null;
		leftSiblingPageNumber = null;
		rightSiblingPageNumber = null;
		parentPageNumber = null;
		level = null;
		isRoot = null;
		page = null;
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
        return(open_btree.getConglomerate().createTemplate());
    }


	/**
	 ** Debug toString() method's.
	 **/

    /**
     * Dump complete information about control row and rows on the page.
     * <p>
     *
	 * @return string with all info.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public String debugPage(
    OpenBTree   open_btree)
        throws StandardException
	{
        String ret_str;

        if (SanityManager.DEBUG)
        {
            StringBuffer string = new StringBuffer(4096);
            string.append(this.toString());
            string.append("\n");

            DataValueDescriptor[] row = getRowTemplate(open_btree);

            string.append(
                ConglomerateUtil.debugPage(
                    page, ControlRow.CR_SLOT + 1, false, row));

            ret_str = string.toString();
        }
        else
        {
            ret_str = null;
        }

        return(ret_str);
	}

    /**
     * The standard toString().
     * <p>
     * This is a concise print out of the info in the control row, does not
     * include anything the page.
     * <p>
     * 
     **/
	public String toString()
    {
        if (SanityManager.DEBUG)
        {
            StringBuffer string = new StringBuffer(4096);

            try {


                // LEAF, BRANCH, LEAF-ROOT, BRANCH-ROOT
                string.append((getLevel() == 0) ? "\nLEAF" : "\nBRANCH");
                string.append((getIsRoot())     ? "-ROOT" : "");

                // (PAGE NUMBER)(LEVEL):num recs 
                //     example: (107)(lev=2):num recs = 16
                string.append("(");
                string.append(this.page.getPageNumber());
                string.append(")(lev=");
                string.append(level);
                string.append("): num recs = ");
                string.append(this.page.recordCount());
                string.append("\n");

                // rest of info
                string.append("\t");

                string.append("left = ");
                string.append(getleftSiblingPageNumber());
                string.append(";");

                string.append("right = ");
                string.append(getrightSiblingPageNumber());
                string.append(";");

                string.append("parent = ");
                string.append(getParentPageNumber());
                string.append(";");

                string.append("isRoot = ");
                string.append(getIsRoot());
                string.append(";");

            }
            catch (Throwable t)
            {
                string.append(
                    "error encountered while doing ControlRow.toString()");
            }

            return(string.toString());
        }
        else
        {
            return(null);
        }
    }
}
