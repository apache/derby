/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapController

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

package org.apache.derby.impl.store.access.heap;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.store.access.conglomerate.OpenConglomerate;
import org.apache.derby.impl.store.access.conglomerate.GenericConglomerateController;
import org.apache.derby.impl.store.access.conglomerate.RowPosition;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**

**/

public class HeapController 
    extends GenericConglomerateController 
    implements ConglomerateController
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Protected concrete impl of abstract methods of 
     *     GenericCongloemrateController class:
     **************************************************************************
     */
    protected final void getRowPositionFromRowLocation(
    RowLocation row_loc,
    RowPosition pos)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row_loc instanceof HeapRowLocation);
        }
        pos.current_rh = 
            ((HeapRowLocation) row_loc).getRecordHandle(
                open_conglom.getContainer());
        pos.current_rh_qualified = true;
    }

    protected void queueDeletePostCommitWork(
    RowPosition pos)
        throws StandardException
    {
        TransactionManager xact_mgr = open_conglom.getXactMgr();

        xact_mgr.addPostCommitWork(
            new HeapPostCommit(
                xact_mgr.getAccessManager(), 
                (Heap) open_conglom.getConglomerate(),
                pos.current_page.getPageNumber()));
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Insert a new row into the heap.
     * <p>
     * Overflow policy:
     * The current heap access method implements an algorithm that optimizes
     * for fetch efficiency vs. space efficiency.  A row will not be over
     * flowed unless it is bigger than a page.  If it is bigger than a page
     * then it's initial part will be placed on a page and then subsequent
     * parts will be overflowed to other pages.
     * <p>
     *
	 * @return The record handle of the inserted row.
     *
     * @param row           The row to insert.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private RecordHandle doInsert(DataValueDescriptor[] row)
		throws StandardException
	{
		Page page = null;
        byte  insert_mode;
		
        RecordHandle rh;

        if (SanityManager.DEBUG)
        {
            Heap heap = (Heap) open_conglom.getConglomerate();
            // Make sure valid columns are in the list.  The RowUtil
            // call is too expensive to make in a released system for
            // every insert.

            int invalidColumn = 
                RowUtil.columnOutOfRange(
                    row, null, heap.format_ids.length);

            if (invalidColumn >= 0)
            {
                throw(StandardException.newException(
                        SQLState.HEAP_TEMPLATE_MISMATCH,
                        new Long(invalidColumn), 
                        new Long(heap.format_ids.length)));
            }
        }

        // Get the last page that was returned for insert or the last page
        // that was allocated.
        page = open_conglom.getContainer().getPageForInsert(0);

        if (page != null) {

            // if there are 0 rows on the page allow the insert to overflow.
            insert_mode = 
                (page.recordCount() == 0) ? 
                    Page.INSERT_OVERFLOW : Page.INSERT_DEFAULT;

            // Check to see if there is enough space on the page
            // for the row.
            rh = page.insert(row, null, insert_mode,
				AccessFactoryGlobals.HEAP_OVERFLOW_THRESHOLD);
            page.unlatch();
            page = null;

            // If we have found a page with enough space for the row,
            // insert it and release exclusive access to the page.
            if (rh != null)
            {
                return rh;

            }
        }

        // If the last inserted page is now full, or RawStore have
        // forgotten what it was, or the row cannot fit on the last
        // inserted page, try to have rawStore get a relatively unfilled
        // page.

        page = 
            open_conglom.getContainer().getPageForInsert(
                ContainerHandle.GET_PAGE_UNFILLED);

        if (page != null)
        {
            // Do the insert all over again hoping that it will fit into
            // this page, and if not, allocate a new page.

            // if there are 0 rows on the page allow the insert to overflow.
            insert_mode = 
                (page.recordCount() == 0) ? 
                    Page.INSERT_OVERFLOW : Page.INSERT_DEFAULT;
            
            rh = page.insert(row, null, insert_mode,
				AccessFactoryGlobals.HEAP_OVERFLOW_THRESHOLD);

            page.unlatch();
            page = null;

            // If we have found a page with enough space for the row,
            // insert it and release exclusive access to the page.
            if (rh != null)
            {
                return rh;
            }
        }

        page = open_conglom.getContainer().addPage();

        // At this point with long rows the raw store will guarantee
        // that any size row will fit on an empty page.

        rh = page.insert(row, null, Page.INSERT_OVERFLOW,
			AccessFactoryGlobals.HEAP_OVERFLOW_THRESHOLD);
        page.unlatch();
        page = null;

        if (SanityManager.DEBUG)
        {
            // a null will only be returned if this page is not empty
            SanityManager.ASSERT(rh != null);
        }

        return rh;
	}

	protected long load(
    TransactionManager      xact_manager,
    Heap                    heap,
    boolean                 createConglom,
    RowLocationRetRowSource rowSource)
		 throws StandardException
	{
        long    num_rows_loaded = 0;

		if (SanityManager.DEBUG)
        {
			SanityManager.ASSERT(open_conglom == null,
				"load expects container handle to be closed on entry.");
        }

		// The individual rows that are inserted are not logged.  To use a
		// logged interface, use insert.  RESOLVE: do we want to allow client
		// to use the load interface even for logged insert?
		int mode = 
            (ContainerHandle.MODE_FORUPDATE | ContainerHandle.MODE_UNLOGGED); 

		// If the container is being created in the same operation, don't log
		// page allocation.  
		if (createConglom)
			mode |= ContainerHandle.MODE_CREATE_UNLOGGED;

        OpenConglomerate open_conglom = new OpenHeap();

        if (open_conglom.init(
                (ContainerHandle) null,
                heap,
                heap.format_ids,
                xact_manager,
                xact_manager.getRawStoreXact(),
                false,
                mode,
                TransactionController.MODE_TABLE,
                xact_manager.getRawStoreXact().newLockingPolicy(
                    LockingPolicy.MODE_CONTAINER,
                    TransactionController.ISOLATION_SERIALIZABLE, true),
                (DynamicCompiledOpenConglomInfo) null) == null)
        {
            throw StandardException.newException(
                    SQLState.HEAP_CONTAINER_NOT_FOUND, 
                    new Long(heap.id.getContainerId()));
        }

        this.init(open_conglom);

		// For bulk loading, we always use only brand new page because the row
		// insertion itself is not logged.  We cannot pollute pages with
		// pre-existing data with unlogged rows because nobody is going to wipe
		// out these rows if the transaction rolls back.  We are counting on
		// the allocation page rollback to obliterate these rows if the
		// transaction fails, or, in the CREAT_UNLOGGED case, the whole
		// container to be removed.

		Page page = open_conglom.getContainer().addPage();

		boolean callbackWithRowLocation = rowSource.needsRowLocation();
		RecordHandle rh;
		HeapRowLocation rowlocation;

		if (callbackWithRowLocation)
			rowlocation = new HeapRowLocation();
		else
			rowlocation = null;

        FormatableBitSet validColumns = rowSource.getValidColumns();

		try
		{
 			// get the next row and its valid columns from the rowSource
			DataValueDescriptor[] row;
            while ((row = rowSource.getNextRowFromRowSource()) != null)
            {
                num_rows_loaded++;

                if (SanityManager.DEBUG)
                {
                    // Make sure valid columns are in the list.  The RowUtil
                    // call is too expensive to make in a released system for 
                    // every insert.
                    int invalidColumn = 
                        RowUtil.columnOutOfRange(
                            row, validColumns, heap.format_ids.length);

                    if (invalidColumn >= 0)
                    {
                        throw(StandardException.newException(
                                SQLState.HEAP_TEMPLATE_MISMATCH,
                                new Long(invalidColumn), 
                                new Long(heap.format_ids.length)));
                    }
                }


				// Insert it onto this page as long as it can fit more rows.
				if ((rh = page.insert(
                        row, validColumns, Page.INSERT_DEFAULT,
						AccessFactoryGlobals.HEAP_OVERFLOW_THRESHOLD)) 
                                == null)
				{
					// Insert faied, row did not fit.  Get a new page.  

					page.unlatch();
					page = null;

					page = open_conglom.getContainer().addPage();

					// RESOLVE (mikem) - no long rows yet so the following code
					// will get an exception from the raw store for a row that
					// does not fit on a page.
					//
					// Multi-thread considerations aside, the raw store will 
                    // guarantee that any size row will fit on an empty page.
					rh = page.insert(
                            row, validColumns, Page.INSERT_OVERFLOW,
							AccessFactoryGlobals.HEAP_OVERFLOW_THRESHOLD);

				}

				// Else, the row fit.  If we are expected to call back with the
				// row location, do so.  All the while keep the page latched
				// and go for the next row.
				if (callbackWithRowLocation)
				{
					rowlocation.setFrom(rh);
					rowSource.rowLocation(rowlocation);
				}
			}
			page.unlatch();
			page = null;

			// Done with the container, now we need to flush it to disk since
			// it is unlogged.
            if (!heap.isTemporary())
                open_conglom.getContainer().flushContainer();
		}
		finally
		{
            // If an error happened here, don't bother flushing the
            // container since the changes should be rolled back anyhow.
            close();
		}
        return(num_rows_loaded);
	}

    protected boolean lockRow(
    RecordHandle    rh,
    int             lock_oper,
    boolean         wait,
    int             lock_duration)
        throws StandardException
    {
        boolean ret_val;
        boolean forUpdate = 
            ((ConglomerateController.LOCK_UPD & lock_oper) != 0);
        boolean forUpdateLock = 
            ((ConglomerateController.LOCK_UPDATE_LOCKS & lock_oper) != 0);

        if (forUpdate && !forUpdateLock)
        {
            boolean forInsert = 
                ((ConglomerateController.LOCK_INS & lock_oper) != 0);
            boolean forInsertPrevKey = 
                ((ConglomerateController.LOCK_INS_PREVKEY & lock_oper) != 0);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(!(forInsertPrevKey && forInsert));
            }

            if (lock_duration == TransactionManager.LOCK_INSTANT_DURATION)
            {
                ret_val = 
                    open_conglom.getContainer().getLockingPolicy().
                        zeroDurationLockRecordForWrite(
                            open_conglom.getRawTran(), rh, forInsertPrevKey, wait);
            }
            else
            {
                ret_val = 
                    open_conglom.getContainer().getLockingPolicy().
                        lockRecordForWrite(
                            open_conglom.getRawTran(), rh, forInsert, wait);
            }
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    (ConglomerateController.LOCK_INS & lock_oper) == 0);
                SanityManager.ASSERT(
                    (ConglomerateController.LOCK_INS_PREVKEY & lock_oper) == 0);
            }

            ret_val = 
                open_conglom.getContainer().getLockingPolicy().lockRecordForRead(
                    open_conglom.getRawTran(), 
                    open_conglom.getContainer(), rh, wait, forUpdate);
        }

        return(ret_val);
    }

    protected Page getUserPageNoWait(long pageno)
        throws StandardException
    {
        return(open_conglom.getContainer().getUserPageNoWait(pageno));
    }
    protected Page getUserPageWait(long pageno)
        throws StandardException
    {
        return(open_conglom.getContainer().getUserPageWait(pageno));
    }
    protected boolean lockRowAtSlotNoWaitExclusive(RecordHandle rh)
        throws StandardException
    {
        return(
            open_conglom.getContainer().getLockingPolicy().
                lockRecordForWrite(
                    open_conglom.getRawTran(), rh, false, false));
    }
    protected void removePage(Page page)
        throws StandardException
    {
        open_conglom.getContainer().removePage(page);
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    public int insert(DataValueDescriptor[] row)
		throws StandardException
	{
		if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                open_conglom.reopen();
            }
            else
            {
                throw(StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            } 
        }

		doInsert(row);

        return(0);
	}

	public void insertAndFetchLocation(
    DataValueDescriptor[] row, 
    RowLocation           templateRowLocation)
		throws StandardException
	{
		if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                open_conglom.reopen();
            }
            else
            {
                throw(StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            } 
        }

		RecordHandle rh = doInsert(row);
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                templateRowLocation instanceof HeapRowLocation);
        }
		HeapRowLocation hrl = (HeapRowLocation) templateRowLocation;
		hrl.setFrom(rh);
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
	 * @param loc       The "RowLocation" which describes the exact row to lock.
     * @param wait      Should the lock call wait to be granted?
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockRow(
    RowLocation     loc,
    int             lock_operation,
    boolean         wait,
    int             lock_duration)
        throws StandardException
    {
        RecordHandle rh = 
            ((HeapRowLocation) loc).getRecordHandle(
                open_conglom.getContainer());

        return(lockRow(rh, lock_operation, wait, lock_duration));
    }

    /**
     * UnLock the given row location.
     * <p>
     * Should only be called by access.
     * <p>
     * This call can be made on a ConglomerateController that was opened
     * for locking only.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
	 * @param loc       The "RowLocation" which describes the row to unlock.
     * @param forUpdate Row was previously Locked the record for read or update.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void unlockRowAfterRead(
    RowLocation     loc,
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
    {

        RecordHandle rh = 
            ((HeapRowLocation) loc).getRecordHandle(
                open_conglom.getContainer());

        open_conglom.getContainer().getLockingPolicy().
            unlockRecordAfterRead(
                open_conglom.getRawTran(), 
                open_conglom.getContainer(),
                rh, 
                open_conglom.isForUpdate(),
                row_qualified);
    }


    /**
     * Lock the given record id/page num pair.
     * <p>
     * Should only be called by access, to lock "special" locks formed from
     * the Recordhandle.* reserved constants for page specific locks.
     * <p>
     * This call can be made on a ConglomerateController that was opened
     * for locking only.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
	 * @return true if lock was granted, only can be false if wait was false.
     *
	 * @param loc       The "RowLocation" which describes the exact row to lock.
     * @param forUpdate Lock the record for read or write.
     * @param forInsert is row Lock for insert?
     * @param wait      Should the lock call wait to be granted?
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean lockRow(
    long            page_num,
    int             record_id,
    int             lock_operation,
    boolean         wait,
    int             lock_duration)
        throws StandardException
    {
        boolean ret_val;

        RecordHandle rh = 
            open_conglom.getContainer().makeRecordHandle(page_num, record_id);

        return(lockRow(rh, lock_operation, wait, lock_duration));
    }

	public RowLocation newRowLocationTemplate()
		throws StandardException
	{
		if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                open_conglom.reopen();
            }
            else
            {
                throw(StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            } 
        }

		return new HeapRowLocation();
	}


    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
