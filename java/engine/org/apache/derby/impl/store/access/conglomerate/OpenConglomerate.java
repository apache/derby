/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.OpenConglomerate

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
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomPropertyQueryable;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import java.util.Properties; 


/**

A Generic class which implements the basic functionality needed to operate
on an "open" conglomerate.  This class assumes the following general things
about the access method.
<p>
The access method is page based and contained in a single container maintained
by raw store.  

**/

public abstract class OpenConglomerate
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**
     * The following group of fields are all basic input parameters which are
     * provided by the calling code when doing any sort of operation requiring
     * an open conglomerate (openScan(), open(), openCostController(), ...).
     * These are just saved values from what was initially input.
     **/
    private Conglomerate                    init_conglomerate;
    private TransactionManager              init_xact_manager;
    private Transaction                     init_rawtran;
    private int                             init_openmode;
    private int                             init_lock_level;
    private DynamicCompiledOpenConglomInfo  init_dynamic_info;
    private boolean                         init_hold;
    private LockingPolicy                   init_locking_policy;


    /**
     * convenience boolean's for various mode's
     **/
    private boolean useUpdateLocks;
    private boolean forUpdate;
    private boolean getBaseTableLocks;

    /**
     * scratch space used for stuff like templates, export rows, ...
     **/
    private OpenConglomerateScratchSpace  runtime_mem;


    /*
     * The open raw store container associated with this open conglomerate
     **/
    private ContainerHandle container;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private methods for This class:
     **************************************************************************
     */

    /**************************************************************************
     * abstract methods of This class:
     **************************************************************************
     */

    /**
     * Return an "empty" row location object of the correct type.
     * <p>
     *
	 * @return The empty Rowlocation.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected abstract RowLocation newRowLocationTemplate()
		throws StandardException;

    abstract public int[] getFormatIds();


    /**************************************************************************
     * Public Methods implementing standard store row locking interfaces:
     *     latchPage(RowPosition)
     *     latchPageAndRepositionScan(RowPosition)
     *     lockPositionForRead(RowPosition, aux_pos, moveForwardIfRowDisappears)
     *     lockPositionForWrite(RowPosition, forInsert, wait)
     *     unlockPositionAfterRead(RowPosition)
     **************************************************************************
     */

    /**
     * Latch the page containing the current RowPosition, and reposition scan.
     * <p>
     * Upon return the scan will hold a latch on the page to continue the
     * scan on.  The scan will positioned on the record, just before the
     * next record to return.
     *
     * Note that for both hold cursor and read uncommitted support this routine
     * handles all cases of either the current position "dissappearing" (either
     * the row and/or page).  The row and/or page can disappear by deleted 
     * space being reclaimed post commit of that delete, and for some reason 
     * the code requesting the reposition does not have locks which prevented
     * the space reclamation.  Both hold cursor and read uncommitted scans are 
     * examples of ways the caller will not prevent space reclamation from 
     * claiming the position.
     *
     * This implementation also automatically updates the RowPosition to
     * point at the slot containing the current RowPosition.  This slot 
     * value is only valid while the latch is held.
     *
	 * @return true if scan had to reposition because a row disappeared.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected boolean latchPageAndRepositionScan(RowPosition pos)
		throws StandardException
    {
        boolean scan_repositioned = false;

        // Get the page the record handle refers to.
        pos.current_page = null;

        try
        {
            pos.current_page = 
                container.getPage(pos.current_rh.getPageNumber());

        }
        catch (Throwable t)
        {
            // Assume all errors are caused by the page "disappearing", will
            // handle this by positioning on next page in code below.
            // Note that in most cases if the page does not exist, getPage()
            // will return null rather than throw an exception, so this path
            // is hard to reach.

            // just continue on first record of the next page.
            // This should only happen if the page on which the scan was
            // positioned had all of it's row deleted and the page was
            // purged.

            // This can happen in a cursor held across a commit, where the
            // scan needs to be repositioned after the first "next()" in the
            // subsequent reopen() of the held cursor.
        }

        if (pos.current_page != null)
        {
            try
            {
                // reposition scan at the old position, now that latch is held.
                pos.current_slot = 
                    pos.current_page.getSlotNumber(pos.current_rh);
            }
            catch (StandardException se)
            {
                scan_repositioned = true;

                // The record that the scan was positioned on, no longer exists.
                // The normal way this happens is if we were positioned on
                // a deleted row, without holding a lock on it, and while
                // the scan did not hold the latch on the page a post commit
                // job purged the row as part of space reclamation.   This can
                // happen in all ISOLATION level scans below serializable.
                pos.current_slot = 
                    pos.current_page.getNextSlotNumber(pos.current_rh);

                if (pos.current_slot == -1)
                {
                    // in this case we there are no more rows on this page
                    // to visit, so position on the next page.  In this case
                    // the row that the scan was positioned on was purged,
                    // and there exists no rows now which are greater than this
                    // record id.

                    pos.current_page.unlatch();
                    pos.current_page = null;
                }
                else
                {
                    // The way scans work, need to position on the row just
                    // before the one to return "next".  The first thing the
                    // next loop will do is move the scan forward one row.
                    pos.current_slot--;
                }
            }
        }

        if (pos.current_page == null)
        {
            // position on the next page.
            pos.current_page = 
                container.getNextPage(pos.current_rh.getPageNumber());

            pos.current_slot = Page.FIRST_SLOT_NUMBER - 1;

            scan_repositioned = true;
        }

        if (scan_repositioned)
        {
            pos.current_rh = null;
        }

        return(scan_repositioned);
    }

    /**
     * Latch the page containing the current RowPosition.
     * <p>
     * This implementation also automatically updates the RowPosition to
     * point at the slot containing the current RowPosition.  This slot 
     * value is only valid while the latch is held.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean latchPage(RowPosition pos)
		throws StandardException
    {
        pos.current_page = null; 

        try
        {
            pos.current_page = 
                container.getPage(pos.current_rh.getPageNumber());

        }
        catch (Throwable t)
        {
            // Assume all errors are caused by the page "disappearing", will
            // handle this by returning false indicating that row can't be 
            // found.  This can easily happen when using read uncommitted 
            // isolation level.
        }

        if (pos.current_page != null)
        {
            try
            {
                pos.current_slot = 
                    pos.current_page.getSlotNumber(pos.current_rh);
                
                return(true);
            }
            catch (Throwable t)
            {
                // Assume all errors are caused by the row "disappearing",
                // will handle this by returning false indicating that row
                // can't be found.  This can easily happen when using read
                // uncommitted isolation level.

                pos.current_page.unlatch();
                pos.current_page = null;
            }
        }

        return(false);
    }


    /**
     * Lock row at given row position for read.
     * <p>
     * This routine requests a row lock NOWAIT on the row located at the given
     * RowPosition.  If the lock is granted NOWAIT the 
     * routine will return true.  If the lock cannot be granted NOWAIT, then 
     * the routine will release the latch on "page" and then it will request 
     * a WAIT lock on the row.  
     * <p>
     * This implementation:
     * Assumes latch held on current_page.
     * If the current_rh field of RowPosition is non-null it is assumed that
     * we want to lock that record handle and that we don't have a slot number.
     * If the current_rh field of RowPosition is null, it is assumed the we
     * want to lock the indicated current_slot.  Upon return current_rh will
     * point to the record handle associated with current_slot.
     * <p>
     * After waiting and getting the lock on the row, this routine will fix up
     * RowPosition to point at the row locked.  This means it will get the
     * page latch again, and it will fix the current_slot to point at the 
     * waited for record handle - it may have moved while waiting on the lock.
     *
     * @param pos       Position to lock.
     * @param aux_pos   If you have to give up latch to get lock, then also 
     *                  unlock this position if it is non-null.
     * @param moveForwardIfRowDisappears
     *                  If true, then this routine must handle the case where
     *                  the row id we are waiting on disappears when the latch
     *                  is released.  If false an exception will be thrown if
     *                  the row disappears.
     * @param waitForLock
     *                  if true wait for lock, if lock can't be granted NOWAIT,
     *                  else if false, throw a lock timeout exception if the
     *                  lock can't be granted without waiting.
     *
	 * @return true if lock granted without releasing the latch, else return
     *              false.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockPositionForRead(
    RowPosition pos,
    RowPosition aux_pos,
    boolean     moveForwardIfRowDisappears,
    boolean     waitForLock)
        throws StandardException
    {
        if (pos.current_rh == null)
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    pos.current_page != null &&
                    pos.current_slot != Page.INVALID_SLOT_NUMBER);

            }

            // work around for lockmanager problem with lock/latch releasing.
            // Get RecordHandle to lock.
            pos.current_rh = 
                pos.current_page.getRecordHandleAtSlot(pos.current_slot);

            if (SanityManager.DEBUG)
            {
                // make sure current_rh and current_slot are in sync
                if (pos.current_slot !=
                        pos.current_page.getSlotNumber(pos.current_rh))
                {
                    SanityManager.THROWASSERT(
                        "current_slot = " + pos.current_slot +
                        "current_rh = " + pos.current_rh +
                        "current_rh.slot = " + 
                        pos.current_page.getSlotNumber(pos.current_rh));
                }
            }
        }

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(pos.current_rh != null);

        boolean lock_granted_with_latch_held =
            this.container.getLockingPolicy().lockRecordForRead(
                init_rawtran, container, pos.current_rh, 
                false /* NOWAIT */, forUpdate);

        if (!lock_granted_with_latch_held)
        {

            // Could not get the lock NOWAIT, release latch and wait for lock.
            pos.current_page.unlatch();
            pos.current_page = null;


            if (aux_pos != null)
            {
                aux_pos.current_page.unlatch();
                aux_pos.current_page = null;
            }

            if (!waitForLock)
            {
                // throw lock timeout error.
                throw StandardException.newException(SQLState.LOCK_TIMEOUT);
            }

            this.container.getLockingPolicy().lockRecordForRead(
                init_rawtran, container, pos.current_rh, 
                true /* WAIT */, forUpdate);

            if (moveForwardIfRowDisappears)
            {

                if (latchPageAndRepositionScan(pos))
                {
                    if (pos.current_slot != -1)
                    {
                        // If scan was repositioned to just before a valid row
                        // on the current page, then move forward and lock and
                        // return that row (slot != -1).  
                        // 
                        // Let the caller handle the "-1" 
                        // case, which may be one of 3 cases - need to go to 
                        // slot 1 on current page, need to go to next page, 
                        // need to end scan as there is no "next" page.  All
                        // 3 cases are handled by the generic scan loop in 
                        // GenericScanController.fetchRows().

                        pos.positionAtNextSlot();
                        lockPositionForRead(pos, aux_pos, true, true);

                    }
                }
            }
            else
            {
                latchPage(pos);
            }
        }

        return(lock_granted_with_latch_held);
    }

    public boolean lockPositionForWrite(
    RowPosition pos,
    boolean     forInsert,
    boolean     waitForLock)
        throws StandardException
    {
        if (pos.current_rh == null)
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(pos.current_page != null);
                SanityManager.ASSERT(
                    pos.current_slot != Page.INVALID_SLOT_NUMBER);

            }

            // work around for lockmanager problem with lock/latch releasing.
            // Get RecordHandle to lock.
            pos.current_rh = 
                pos.current_page.fetchFromSlot(
                    null, 
                    pos.current_slot, 
                    RowUtil.EMPTY_ROW, 
                    RowUtil.EMPTY_ROW_FETCH_DESCRIPTOR, 
                    true);

            if (SanityManager.DEBUG)
            {
                // make sure current_rh and current_slot are in sync
                if (pos.current_slot !=
                        pos.current_page.getSlotNumber(pos.current_rh))
                {
                    SanityManager.THROWASSERT(
                        "current_slot = " + pos.current_slot +
                        "current_rh = " + pos.current_rh +
                        "current_rh.slot = " + 
                        pos.current_page.getSlotNumber(pos.current_rh));
                }
            }
        }

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(pos.current_rh != null);

        boolean lock_granted_with_latch_held =
            this.container.getLockingPolicy().
                lockRecordForWrite(
                    init_rawtran, pos.current_rh, 
                    forInsert, false /* NOWAIT */);

        if (!lock_granted_with_latch_held)
        {
            if (!waitForLock)
            {
                // throw lock timeout error.
                throw StandardException.newException(SQLState.LOCK_TIMEOUT);
            }

            // Could not get the lock NOWAIT, release latch and wait for lock.
            pos.current_page.unlatch();
            pos.current_page = null;

            if (!waitForLock)
            {
                // throw lock timeout error.
                throw StandardException.newException(SQLState.LOCK_TIMEOUT);
            }

            this.container.getLockingPolicy().
                lockRecordForWrite(
                    init_rawtran, pos.current_rh, forInsert, true /* WAIT */);

            latchPage(pos);
        }

        return(lock_granted_with_latch_held);
    }


    /**
     * Unlock the record after a previous request to lock it.
     * <p>
     * Unlock the record after a previous call to lockRecordForRead().  It is
     * expected that RowPosition contains information used to lock the record,
     * Thus it is important if using a single RowPosition to track a scan to
     * call unlock before you move the position forward to the next record.
     * <p>
     * Note that this routine assumes that the row was locked forUpdate if
     * the OpenConglomerate is forUpdate, else it assumes the record was
     * locked for read.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void unlockPositionAfterRead(
    RowPosition pos) 
        throws StandardException
    {
        if (!isClosed())
            container.getLockingPolicy().
                unlockRecordAfterRead(
                    init_rawtran, container, pos.current_rh, forUpdate, 
                    pos.current_rh_qualified);
    }


    /**************************************************************************
     * Public Methods implementing ConglomPropertyQueryable Interface: 
     **************************************************************************
     */

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
        container.getContainerProperties(prop);

        return;
    }

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

    public final ContainerHandle getContainer()
    {
        return(container);
    }

    public final int getOpenMode()
    {
        return(init_openmode);
    }
    
    public final Conglomerate getConglomerate()
    {
        return(init_conglomerate);
    }

    public final boolean getHold()
    {
        return(init_hold);
    }


    public final boolean isForUpdate()
    {
        return(forUpdate);
    }

    public final boolean isClosed()
    {
        return(container == null);
    }

    public final boolean isUseUpdateLocks()
    {
        return(useUpdateLocks);
    }

    public final OpenConglomerateScratchSpace getRuntimeMem()
    {
        return(runtime_mem);
    }

    /**************************************************************************
     * Public Methods implementing some ConglomerateController Interfaces: 
     **************************************************************************
     */


    /**
     * Check consistency of a conglomerate.
     * <p>
     * Checks the consistency of the data within a given conglomerate, does not
     * check consistency external to the conglomerate (ie. does not check that 
     * base table row pointed at by a secondary index actually exists).
     * <p>
     * There is no checking in the default implementation, you must override
     * to get conglomerate specific consistency checking.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void checkConsistency()
		throws StandardException
    {
        return;
    }



    public void debugConglomerate()
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.DEBUG_PRINT(
                "p_heap", "\nHEAP DUMP:containerId " + container.getId());

            // get a template.

            DataValueDescriptor[] row = runtime_mem.get_row_for_export();

            // Print pages of the heap.
            Page page = container.getFirstPage();

            while (page != null)
            {
                SanityManager.DEBUG_PRINT(
                    "p_heap", ConglomerateUtil.debugPage(page, 0, false, row));

                long pageid = page.getPageNumber();
                page.unlatch();
                page = container.getNextPage(pageid);
            }
        }

        return;
    }


    /**
    Get information about space used by the conglomerate.
    **/
    public SpaceInfo getSpaceInfo()
        throws StandardException
    {
        return container.getSpaceInfo();
    }

	protected boolean isKeyed()
	{
		return false;
	}

    /**
     * is the open btree table locked?
     **/
    protected boolean isTableLocked()
    {
        return(init_lock_level == TransactionController.MODE_TABLE);
    }

    /**************************************************************************
     * Public Methods of this class:
     **************************************************************************
     */

    /**
     * Open the container.
     * <p>
     * Open the container, obtaining necessary locks.  Most work is actually
     * done by RawStore.openContainer().  
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public ContainerHandle init(
    ContainerHandle                 open_container,
    Conglomerate                    conglomerate,
    int[]                           format_ids,
    TransactionManager              xact_manager,
    Transaction                     rawtran,
    boolean                         hold,
    int                             openmode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    DynamicCompiledOpenConglomInfo  dynamic_info)
        throws StandardException
    {
        // save state of all inputs.
        init_conglomerate       = conglomerate;
        init_xact_manager       = xact_manager;
        init_rawtran            = rawtran;
        init_openmode           = openmode;
        init_lock_level         = lock_level;
        init_dynamic_info       = dynamic_info;
        init_hold               = hold;
        init_locking_policy     = locking_policy;


        // either use passed in "compiled" runtime scratch space, or create
        // new space.
        this.runtime_mem    = 
            (dynamic_info != null ? 
             ((OpenConglomerateScratchSpace) dynamic_info) : 
             new OpenConglomerateScratchSpace(format_ids));

        // Is this an open for update or read?  This will
		// be passed down to the raw store fetch methods, which allows
		// it to do the appropriate locking.
		this.forUpdate = 
            ((openmode & ContainerHandle.MODE_FORUPDATE) != 0); 

        // keep track of whether this open conglomerate should use update locks.
		this.useUpdateLocks = 
            ((openmode & ContainerHandle.MODE_USE_UPDATE_LOCKS) != 0);

        // If this flag is set, then the client has already locked the row
        // by accessing it through the secondary index and has already locked
        // the row, so the base conglomerate need not re-lock the row.
        this.getBaseTableLocks =
            ((openmode & ContainerHandle.MODE_SECONDARY_LOCKED) == 0);

		// if the conglomerate is temporary, open with IS_KEPT set.
		// RESOLVE(mikem): track 1825
		// don't want to open temp cantainer with IS_KEPT always.
        if (conglomerate.isTemporary())
        {
			init_openmode |= ContainerHandle.MODE_TEMP_IS_KEPT;
        }

        if (!getBaseTableLocks)
            init_locking_policy = null;

		// Open the container. 
        this.container = 
            (open_container != null ?  
                 open_container : 
                 rawtran.openContainer(
                    conglomerate.getId(), init_locking_policy, init_openmode));

        return(this.container);
    }

    /**
     * Open the container.
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
     
        if (this.container == null)
        {
            this.container = 
                 init_rawtran.openContainer(
                    init_conglomerate.getId(), 
                    init_locking_policy, 
                    init_openmode);
        }

        return(this.container);
    }

    /**
     * Close the container.
     * <p>
     * Handles being closed more than once.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void close()
        throws StandardException
	{
		if (container != null)
        {
			container.close();
            container = null;
        }
	}
}
