/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapPostCommit

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

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.reference.SQLState;

/**

The HeapPostCommit class implements the Serviceable protocol.  

In it's role as a Serviceable object, it stores the state necessary to 
find a page in a heap that may have committed delete's to reclaim.

It looks up the page described, and reclaims space in the conglomerate.  
It first trys to clean up any deleted commits on the page.  It will then 
deallocate the page if no rows remain on the page.  All work is done while
holding the latch on the page, and locks are never "waited" on while holding
this latch.

This implementation uses record level locking to reclaim the space.  
For the protocols to work correctly all other heap methods must be 
prepared for a record or a page to "disappear" if they don't hold a latch and/or
a lock.  An example of the problem case is a scan which does not hold locks
on it's current position (group scan works this way), which is positioned
on a row deleted by another xact, it must be prepared to continue the 
scan after getting an error if the current page/row disapppears.

**/

class HeapPostCommit implements Serviceable
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    private AccessFactory access_factory  = null;
    private Heap          heap            = null;
    private long          page_number     = ContainerHandle.INVALID_PAGE_NUMBER;


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    HeapPostCommit(
    AccessFactory   access_factory,
    Heap            heap,
    long            input_page_number)
    {
        this.access_factory = access_factory; 
        this.heap           = heap; 
        this.page_number    = input_page_number; 
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Reclaim space taken up by committed deleted rows.
     * <p>
     * This routine assumes it has been called by an internal transaction which
     * has performed no work so far, and that it has an exclusive intent table 
     * lock.  It will attempt obtain exclusive row locks on deleted rows, where
     * successful those rows can be reclaimed as they must be "committed 
     * deleted" rows.
     * <p>
     * This routine will latch the page and hold the latch due to interface
     * requirement from Page.purgeAtSlot.
     *
     * @param open_btree The btree already opened.
     * @param pageno The page number of the page to look for committed deletes.
     *
     * @see Page#purgeAtSlot
     * @exception  StandardException  Standard exception policy.
     **/
    private final void purgeCommittedDeletes(
    HeapController      heap_control,
    long                pageno)
        throws StandardException
    {
        // The following can fail either if it can't get the latch or
        // somehow the page requested no longer exists. 
	
	//resolve - what will happen if the user page doesnt exist  

        // wait to get the latch on the page 
        Page page = heap_control.getUserPageWait(pageno);
        boolean purgingDone = false;

        if (page != null)
        {
            try
            {
                // The number records that can be reclaimed is:
                // total recs - recs_not_deleted
                int num_possible_commit_delete = 
                    page.recordCount() - page.nonDeletedRecordCount();

                if (num_possible_commit_delete > 0)
                {
                    // loop backward so that purges which affect the slot table 
                    // don't affect the loop (ie. they only move records we 
                    // have already looked at).
                    for (int slot_no = page.recordCount() - 1; 
                         slot_no >= 0; 
                         slot_no--) 
                    {
                        boolean row_is_committed_delete = 
                            page.isDeletedAtSlot(slot_no);

                        if (row_is_committed_delete)
                        {
                            // At this point we only know that the row is
                            // deleted, not whether it is committed.

                            // see if we can purge the row, by getting an
                            // exclusive lock on the row.  If it is marked
                            // deleted and we can get this lock, then it
                            // must be a committed delete and we can purge 
                            // it.

                            RecordHandle rh =
                                page.fetchFromSlot(
                                    (RecordHandle) null,
                                    slot_no,
                                    RowUtil.EMPTY_ROW,
                                    RowUtil.EMPTY_ROW_FETCH_DESCRIPTOR,
                                    true);

                            row_is_committed_delete =
                                heap_control.lockRowAtSlotNoWaitExclusive(rh);

                            if (row_is_committed_delete)
                            {
                                purgingDone = true;

                                page.purgeAtSlot(slot_no, 1, false);

                                if (SanityManager.DEBUG)
                                {
                                    if (SanityManager.DEBUG_ON(
                                            "verbose_heap_post_commit"))
                                    {
                                        SanityManager.DEBUG_PRINT(
                                            "HeapPostCommit", 
                                            "Purging row[" + slot_no + "]" + 
                                            "on page:" + pageno + ".\n");
                                    }
                                }
                            }
                        }
                    }
                }
                if (page.recordCount() == 0)
                {
                    purgingDone = true;

                    // Deallocate the current page with 0 rows on it.
                    heap_control.removePage(page);

                    // removePage guarantees to unlatch the page even if an
                    // exception is thrown. The page is protected against reuse
                    // because removePage locks it with a dealloc lock, so it
                    // is OK to release the latch even after a purgeAtSlot is
                    // called.
                    // @see ContainerHandle#removePage

                    if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON("verbose_heap_post_commit"))
                        {
                            SanityManager.DEBUG_PRINT(
                                "HeapPostCommit", 
                                "Calling Heap removePage().; pagenumber="+pageno+"\n");
                        }
                    }
                }
            }
            finally
            {
                // If no purge happened on the page and the page is not
                // removed, feel free to unlatch it.  Otherwise, let
                // transaction commit take care of it.
				if (!purgingDone)
                {
                    page.unlatch();
                    page = null;
                }
            }
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON("verbose_heap_post_commit"))
                {
                    SanityManager.DEBUG_PRINT(
                        "HeapPostCommit", 
                        "Get No Wait returned null. page num = " + 
                        pageno + "\n");

                    SanityManager.showTrace(new Throwable());
                }
            }
        }
        return;
    }

    /**************************************************************************
     * Public Methods implementing the Serviceable interface:
     **************************************************************************
     */

    /**
     * The urgency of this post commit work.
     * <p>
     * This determines where this Serviceable is put in the post commit 
     * queue.  Post commit work in the heap can be safely delayed until there
     * is not user work to do.
     *
     * @return false, this work should not be serviced ASAP
     **/
    public boolean serviceASAP()
    {
        return(true);
    }

	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	


    /**
     * perform the work described in the postcommit work.
     * <p>
     * In this implementation the only work that can be executed by this
     * post commit processor is this class itself.
     * <p>
     *
     * @return Returns Serviceable.DONE when work has completed, or
     *         returns Serviceable.REQUEUE if work needs to be requeued.
     *
     * @param contextMgr the context manager started by the post commit daemon
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public int performWork(ContextManager contextMgr)
        throws StandardException
    {
        TransactionManager  tc             = (TransactionManager)
            this.access_factory.getAndNameTransaction(
                contextMgr, AccessFactoryGlobals.SYS_TRANS_NAME);

        TransactionManager  internal_xact  = tc.getInternalTransaction();

        // only requeue if work was not completed in this try.
        boolean             requeue_work = false;

        HeapController      heapcontroller;

        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("verbose_heap_post_commit"))
                SanityManager.DEBUG_PRINT(
                    "HeapPostCommit", "starting internal xact\n");
        }

        try
        {
            // This call will attempt to open the heap table locked with 
            // table level IX mode, preparing to do record level locked space 
            // reclamation.  
            //
            // The call will either succeed immediately, or throw an exception
            // which could mean the container does not exist or that the lock
            // could not be granted immediately. 

			//Reversed the fix for 4255:
			//page reclaimation is done asynchronosly by raswstore daemon
			//not good to WAIT FOR LOCKS , as it can freeze the daemon
			//If we can not get the lock this reclamation request will 
			//requeued.

            heapcontroller = (HeapController)
                heap.open(
                    internal_xact,
                    internal_xact.getRawStoreXact(),
                    false,
                    ContainerHandle.MODE_FORUPDATE |
                    ContainerHandle.MODE_LOCK_NOWAIT,
                    TransactionController.MODE_RECORD,
                    internal_xact.getRawStoreXact().newLockingPolicy(
                        LockingPolicy.MODE_RECORD,
                        TransactionController.ISOLATION_REPEATABLE_READ, true),
                    heap,
                    (DynamicCompiledOpenConglomInfo) null);

            // We got a table intent lock, all deleted rows we encounter can
            // be reclaimed, once an "X" row lock is obtained on them.

            // Process all the rows on the page while holding the latch.
            purgeCommittedDeletes(heapcontroller, this.page_number);

        }
        catch (StandardException se)
        {
            // exception might have occured either container got dropper or lock not granted.
            // It is possible by the time this post commit work gets scheduled 
            // that the container has been dropped and that the open container 
            // call will return null - in this case just return assuming no 
            // work to be done.

			//If this expcetion is because lock could not be obtained , work is requeued.
			if (se.getMessageId().equals(SQLState.LOCK_TIMEOUT) || 
				se.getMessageId().equals(SQLState.DEADLOCK))
			{
				requeue_work = true;
			}

            // Do not close the controller because that will unlatch the
            // page.  Let the commit and destroy do release the latch and
            // close the controller.
            // heapcontroller.close();
        }
            
        // It is ok to not sync this post work.  If no subsequent log record
        // is sync'd to disk then it is ok that this transaction not make
        // it to the database.  If any subsequent transaction is sync'd to
        // the log file, then this transaction will be sync'd as part of that
        // work.

        internal_xact.commitNoSync(Transaction.RELEASE_LOCKS);
        internal_xact.destroy();


        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("verbose_heap_post_commit"))
            {
                if (requeue_work)
                    SanityManager.DEBUG_PRINT(
                        "HeapPostCommit", 
                        "requeueing on page num = " + page_number);
            }
        }

        return(requeue_work ? Serviceable.REQUEUE : Serviceable.DONE);
    }
}

