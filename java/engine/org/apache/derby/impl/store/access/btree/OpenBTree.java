/*

   Derby - Class org.apache.derby.impl.store.access.btree.OpenBTree

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.store.access.conglomerate.OpenConglomerateScratchSpace;



/**

  An open b-tree contains fields and methods common to scans and controllers.
  <P>
  <B>Concurrency Notes<\B>
  <P>
  An instance of an open b-tree is owned by a single context.  The b-tree code
  assumes that the context ensures that only one thread at a time is using
  the open b-tree.  The open b-tree itself does not enforce or check this.

**/

public class OpenBTree 
{
	/*
	** Fields of OpenBTree
	*/

    /**
     * The following group of fields are all basic input parameters which are
     * provided by the calling code when doing any sort of operation requiring
     * an open conglomerate (openScan(), open(), openCostController(), ...).
     * These are just saved values from what was initially input.
     **/
    private BTree                           init_conglomerate;

    /**
    The TransactionManager that open'd this btree.  In the case of Internal
    transactions used by split this will be the internal transaction, and 
    init_open_user_scans will be the user transaction that began the internal
    transaction.
    **/
    private TransactionManager              init_xact_manager;

    private Transaction                     init_rawtran;

	/**
	The ContainerHandle mode the container is opened with.  Remember this so
	that if the BTree needs to do SMO with another transaction, it would open
	the container with the same mode.
	**/
    private int                             init_openmode;

    /**
    Table or page locking?
    **/
    protected int                           init_lock_level;

    private DynamicCompiledOpenConglomInfo  init_dynamic_info;
    private boolean                         init_hold;

    /**
    The Locking Policy to use for for access to this btree.
    **/
    private BTreeLockingPolicy              init_btree_locking_policy;

	
	/**
	The (open) container which contains the b-tree.
	**/
	protected ContainerHandle container;

    /**
    The conglomerate containerid for error reporting.
    **/
    protected long  err_containerid;

    /**
    In the case of splits, notify all scans in this transaction to save their
    current position by key, because the split may move the row they are 
    positioned on.  This is done by calling open_user_scans.saveScanPositions().
    Note that not all OpenBTree's will have a non-null open_user_scans.  For
    instance logical undo of btree operations will get a OpenBTree with a null
    open_user_scans, this is all right because this operation should never need
    to call saveScanPositions() (ie. it will never do a split).
    **/
    protected TransactionManager init_open_user_scans = null;


    protected LogicalUndo btree_undo = null;

    /**
     * scratch space used for stuff like templates, export rows, ...
     **/
    protected OpenConglomerateScratchSpace  runtime_mem;

    /**************************************************************************
     * Public Accessors of This class:
     **************************************************************************
     */
    public final TransactionManager getXactMgr()
    {
        return(init_xact_manager);
    }

    public final Transaction getRawTran()
    {
        return(init_rawtran);
    }
    public final int getLockLevel()
    {
        return(init_lock_level);
    }

    public final ContainerHandle getContainer()
    {
        return(container);
    }

    public final int getOpenMode()
    {
        return(init_openmode);
    }
    
    public final BTree getConglomerate()
    {
        return(init_conglomerate);
    }

    public final boolean getHold()
    {
        return(init_hold);
    }
    public final BTreeLockingPolicy getLockingPolicy()
    {
        return(init_btree_locking_policy);
    }
    public final void setLockingPolicy(BTreeLockingPolicy policy)
    {
        init_btree_locking_policy = policy;
    }


    public final boolean isClosed()
    {
        return(container == null);
    }

    public final OpenConglomerateScratchSpace getRuntimeMem()
    {
        return(runtime_mem);
    }

    /**************************************************************************
     * Public Methods of RowCountable class:
     **************************************************************************
     */

    /**
     * Get the total estimated number of rows in the container.
     * <p>
     * The number is a rough estimate and may be grossly off.  In general
     * the server will cache the row count and then occasionally write
     * the count unlogged to a backing store.  If the system happens to 
     * shutdown before the store gets a chance to update the row count it
     * may wander from reality.
     * <p>
     * This call is currently only supported on Heap conglomerates, it
     * will throw an exception if called on btree conglomerates.
     *
	 * @return The total estimated number of rows in the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public long getEstimatedRowCount()
		throws StandardException
    {
        if (container == null)
            reopen();

        // Don't return 0 rows (return 1 instead), as this often leads the 
        // optimizer to produce plans which don't use indexes because of the 0 
        // row edge case.
        //
        // Eventually the plan is recompiled when rows are added, but we
        // have seen multiple customer cases of deadlocks and timeouts 
        // because of these 0 row based plans.  
        long row_count = 
            this.container.getEstimatedRowCount(/* unused flag */ 0);

        return(row_count == 0 ? 1 : row_count);
    }

    /**
     * Set the total estimated number of rows in the container.
     * <p>
     * Often, after a scan, the client of RawStore has a much better estimate
     * of the number of rows in the container than what store has.  For 
     * instance if we implement some sort of update statistics command, or
     * just after a create index a complete scan will have been done of the
     * table.  In this case this interface allows the client to set the
     * estimated row count for the container, and store will use that number
     * for all future references.
     * <p>
     * This call is currently only supported on Heap conglomerates, it
     * will throw an exception if called on btree conglomerates.
     *
     * @param count the estimated number of rows in the container.
     *
	 * @return The total estimated number of rows in the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void setEstimatedRowCount(long count)
		throws StandardException
    {
        if (container == null)
            reopen();

        this.container.setEstimatedRowCount(count, /* unused flag */ 0);
    }


    /**************************************************************************
     * Public Methods of ConglomerateController interface:
     **************************************************************************
     */

    /**
     * Check consistency of a btree.
     * <p>
     * Read in root and check consistency of entire tree.  Currently raises
     * sanity check errors.
     * <p>
     * RESOLVE (mikem) if this is to be supported in non-sanity servers what 
     * should it do?
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void checkConsistency()
		throws StandardException
    {
		ControlRow root = null;

        try
        {
            if (this.container == null)
            {
                throw(StandardException.newException(
                        SQLState.BTREE_IS_CLOSED, new Long(err_containerid)));
            }

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(this.init_conglomerate.format_ids != null);

            root = ControlRow.Get(this, BTree.ROOTPAGEID);

            int actualpages = root.checkConsistency(this, null, true);

            // RESOLVE (mikem) - anything useful to assert about number of pages
            // in the tree?
        }
        finally
        {
            if (root != null)
                root.release();
        }
    }

    /**************************************************************************
     * Public Methods of ScanController interface:
     **************************************************************************
     */

    /**
     * is the open btree table locked?
     **/
    public boolean isTableLocked()
    {
        return(init_lock_level == TransactionController.MODE_TABLE);
    }


	/*
	** Methods of OpenBTree
	*/

	/**
	Initialize the open conglomerate.

    If container is null, open the container, otherwise use the container
    passed in.

    @exception StandardException standard exception policy.
	**/
    /**
     * Initialize the open conglomerate.
     * <p>
     * If container is null, open the container, otherwise use the container
     * passed in.  The container is always opened with no locking, it is up
     * to the caller to make the appropriate container locking call.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param open_user_scans The user transaction which opened this btree.
     * @param xact_manager    The current transaction, usually the same as
     *                        "open_user_scans", but in the case of split it
     *                        is the internal xact nested below the user xact.
     * @param input_container The open container holding the index, if it is
     *                        already open, else null which will mean this
     *                        routine will open it.
     * @param rawtran         The current raw store transaction.
     * @param open_mode       The opening mode for the ContainerHandle.
     * @param conglomerate    Readonly description of the conglomerate.
     * @param undo            Logical undo object to associate with all updates
     *                        done on this open btree.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void init(
    TransactionManager              open_user_scans,
    TransactionManager              xact_manager,
    ContainerHandle                 input_container,
    Transaction                     rawtran,
    boolean                         hold,
	int                             open_mode,
    int                             lock_level,
    BTreeLockingPolicy              btree_locking_policy,
    BTree                           conglomerate,
    LogicalUndo                     undo,
    DynamicCompiledOpenConglomInfo  dynamic_info)
        throws StandardException
	{
		// If the b-tree is already open, close it.
		if (this.container != null)
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(false, "why is the container open?");
			close();
        }
        err_containerid = conglomerate.id.getContainerId();

        // Locking policy to pass back to concrete implementation lock calls
        this.init_btree_locking_policy = btree_locking_policy;

		// if the conglomerate is temporary, open with IS_KEPT set.
		// RESOLVE(mikem): track 1825
		// don't want to open temp cantainer with IS_KEPT always.
		if (conglomerate.isTemporary())
			open_mode |= ContainerHandle.MODE_TEMP_IS_KEPT;

        // now open the container if it wasn't already opened by the client.
        // No locks will be requested by raw store on this open.
        if (input_container == null)
        {
            // Open the container. 
            this.container = 
                rawtran.openContainer(
                    conglomerate.id, 
                    (LockingPolicy) null /* get no locks on btree */,
					open_mode);
        }
        else
        {
            // Use the open container passed in.
            this.container = input_container;

			// RESOLVE (sku) - ContainerHandle should have an interface to
			// verify that it is opened with open_mode
        }

		if (this.container == null)
        {
            throw StandardException.newException(
                    SQLState.BTREE_CONTAINER_NOT_FOUND,
                    new Long(err_containerid));
        }

		// Remember the conglomerate so its properties can be found.
        init_conglomerate   = conglomerate;

        // Remember the transaction manager so commit() can be called
        init_xact_manager   = xact_manager;

        init_rawtran        = rawtran;

        init_openmode       = open_mode;

        // Isolation level of this btree.
        init_lock_level     = lock_level;

        init_dynamic_info   = dynamic_info;

        init_hold           = hold;


        // Remember the transaction manager so saveScanPositions() can be called
        this.init_open_user_scans = open_user_scans;

        // Logical undo class to pass to raw store, on inserts/deletes.
        this.btree_undo = undo;

        // either use passed in "compiled" runtime scratch space, or create
        // new space.
        this.runtime_mem    = 
            (dynamic_info != null ? 
             ((OpenConglomerateScratchSpace) dynamic_info) : 
             new OpenConglomerateScratchSpace(conglomerate.format_ids));

	}

    /**
     * Open the container after it has been closed previously.
     * <p>
     * Open the container, obtaining necessary locks.  Most work is actually
     * done by RawStore.openContainer().  Will only reopen() if the container
     * is not already open.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public ContainerHandle reopen()
        throws StandardException
    {
        // reget transaction from context manager, in the case of XA
        // transaction this may have changed.
        //
        /* TODO - XA transactions my change the current transaction on the 
         * context stack.  Will want to something like:
         *
         * init_rawtran = context_manager.getcurrenttransaction()
         */

		// If the b-tree is already open, close it.

        /*
		if (this.container != null)
        {
			close();
        }
        */

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(init_xact_manager != null);
            SanityManager.ASSERT(init_xact_manager.getRawStoreXact() != null);
            SanityManager.ASSERT(init_conglomerate != null);
        }
     
        if (container == null)
        {
            // Open the container. 
            this.container = 
                init_xact_manager.getRawStoreXact().openContainer(
                    init_conglomerate.id, 
                    (LockingPolicy) null /* get no locks on btree */,
                    init_openmode);
        }

        return(this.container);
    }

    /**
    Close the open conglomerate.
    **/
    public void close()
        throws StandardException
	{
		if (container != null)
			container.close();
		container = null;
	}

    /**
    Check if all the 
    columns are Indexable and Storable.  Eventually this routine could
    check whether all the types were right also.

    @exception StandardException Standard Exception Policy.
    **/
    void isIndexableRowConsistent(DataValueDescriptor[] row)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            DataValueDescriptor[] template = 
                this.init_conglomerate.createTemplate();

            // RESOLVE - could just compare format id's rather than allocate
            // objects.

            for (int i = 0; i < row.length; i++)
            {
                // RESOLVE (mikem) - use format id's for more efficient test.
				if (!row[i].getClass().equals(template[i].getClass()))
                {
                    SanityManager.THROWASSERT(
                        "type of inserted column[" + i + "] = " + 
                        row[i].getClass().getName()                +
                        "type of template column[" + i + "] = " +
                        template[i].getClass().getName());
                }
            }
        }
    }

    /**
     * Return the container handle.
     * <p>
	 * @return The open container handle of the btree.
     **/
    public ContainerHandle getContainerHandle()
    {
        return(container);
    }
	
	/**
     * get height of the tree.
     * <p>
     * Read in root and return the height (number of levels) of the tree.
     * The level of a tree is 0 in the leaf and increases by 1 for each
     * level of the tree as you go up the tree.  
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int getHeight()
		throws StandardException
    {
		// container.checkConsistency();

		ControlRow root = null;

        try
        {
            root = ControlRow.Get(this, BTree.ROOTPAGEID);

            int height = root.getLevel() + 1;

            return(height);
        }
        finally
        {
            if (root != null)
                root.release();
        }
    }

    public RecordHandle makeRecordHandle(
    long    page_number,
    int     rec_id)
        throws StandardException
    {
        return(
            container.makeRecordHandle(
                page_number, rec_id));
    }

    /**
     * Dump information about tree into the log.
     * <p>
     * Traverse the tree dumping info about tree into the log.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void debugConglomerate()
		throws StandardException
    {
		// container.checkConsistency();

		ControlRow root = null;

        try
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.DEBUG_PRINT(
                    "p_tree", "BTREE Dump: containerId " + container.getId());
                SanityManager.DEBUG_PRINT(
                    "p_tree", "BTREE Dump: btree " + this.init_conglomerate);
            }

            root = ControlRow.Get(this, BTree.ROOTPAGEID);
            root.printTree(this);
        }
        finally
        {
            if (root != null)
                root.release();
        }
    }

    /**
     * Testing infrastructure to cause unusual paths through the code.
     * <p>
     * Through the use of debug flags allow test code to cause otherwise
     * hard to cause paths through the code.  
     * <p>
     *
	 * @return whether the latch has been released by this routine.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static boolean test_errors(
    OpenBTree           open_btree,
    String              debug_string,
    boolean             release_scan_lock,
    BTreeLockingPolicy  btree_locking_policy,
    LeafControlRow      leaf,
    boolean             input_latch_released)
        throws StandardException
    {
        boolean latch_released = input_latch_released;

        // special test to see if latch release code works
        if (SanityManager.DEBUG)
        {
            String debug_lost_latch = debug_string + "1";

            if (SanityManager.DEBUG_ON(debug_lost_latch))
            {
                // Simulate a lost latch because of a wait for a lock.
                if (!latch_released)
                {
                    if (release_scan_lock)
                    {
                        btree_locking_policy.unlockScan(
                            leaf.page.getPageNumber());
                    }
                    leaf.release();

                    latch_released = true;
                    SanityManager.DEBUG_PRINT(
                        debug_lost_latch, debug_lost_latch);
                    SanityManager.DEBUG_CLEAR(debug_lost_latch);
                }
            }

            String debug_deadlock = debug_string + "2";

            if (SanityManager.DEBUG_ON(debug_deadlock))
            {
                SanityManager.DEBUG_PRINT(debug_deadlock, debug_deadlock);
                SanityManager.DEBUG_CLEAR(debug_deadlock);

                // Simulate a deadlock error.
                StandardException se = 
                    StandardException.newException(
                        SQLState.DEADLOCK, "fake deadlock", "fake victim");

				se.setReport(StandardException.REPORT_ALWAYS);
				throw se;
            }
        }

        return(latch_released);
    }

    public SpaceInfo getSpaceInfo()
        throws StandardException
    {
        return container.getSpaceInfo();
    }

	// return column Sort order information
	public boolean[] getColumnSortOrderInfo()
		throws	StandardException
	{
		return init_conglomerate.ascDescInfo;
	}
}
