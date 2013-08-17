/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreePostCommit

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

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.reference.SQLState;

/**

The BTreePostCommit class implements the Serviceable protocol.  

In it's role as a Serviceable object, it stores the state necessary to 
find a page in a btree that may have committed delete's to reclaim.

In it's role as a PostCommitProcessor it looks up the page described, and
reclaims space in the btree.  It first trys to clean up any deleted commits
on the page.  It then will shrink the tree if it is going to delete all
rows from the page (RESOLVE - not done yet).

**/

class BTreePostCommit implements Serviceable
{
    private     AccessFactory access_factory  = null;
    private     long          page_number = ContainerHandle.INVALID_PAGE_NUMBER;

    protected   BTree         btree           = null;

    /* Constructors for This class: */
    BTreePostCommit(
    AccessFactory   access_factory,
    BTree           btree,
    long            input_page_number)
    {
        this.access_factory = access_factory; 
        this.btree          = btree; 
        this.page_number    = input_page_number; 
    }

    /* Private/Protected methods of This class: */

    /* Public Methods of This class: */

    /* Public Methods of Serviceable class: */

    /**
     * The urgency of this post commit work.
     * <p>
     * This determines where this Serviceable is put in the post commit 
     * queue.  Post commit work in the btree can be safely delayed until there
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

    private final void doShrink(
    OpenBTree               open_btree, 
    DataValueDescriptor[]	shrink_row)
        throws StandardException
    {
        ControlRow root = null;

        /*
        System.out.println(
            "Calling shrink on tree with levels = " + 
            open_btree.getHeight() + "\n");
        */

        // Get the root page back, and perform a split following the
        // to-be-inserted key.  The split releases the root page latch.
        root = ControlRow.get(open_btree, BTree.ROOTPAGEID);

        root.shrinkFor(open_btree, shrink_row);

        root = null;

        // on return from shrinkFor the root pointer is invalid.  The
        // latch has been released, the root page may have changed from
        // a branch to a leaf.

        return;
    }

    /**
     * Open index for either table level or row level update.
     * <p>
     * @param lock_level For table level use TransactionManager.MODE_TABLE,
     *                   for row   level use TransactionManager.MODE_RECORD
     * @param lock_mode  For table level use LockingPolicy.MODE_CONTAINER,
     *                   for row   level use LockingPolicy.MODE_RECORD
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private final OpenBTree openIndex(
    TransactionManager internal_xact,
    int                lock_level,
    int                lock_mode)
        throws StandardException
    {
        OpenBTree open_btree = new OpenBTree();

        ConglomerateController base_cc = 
            btree.lockTable(
                internal_xact, 
                (ContainerHandle.MODE_FORUPDATE |
                 ContainerHandle.MODE_LOCK_NOWAIT), 
                lock_level,
                TransactionController.ISOLATION_REPEATABLE_READ);

        open_btree.init(
            (TransactionManager) null, 
            internal_xact, 
            (ContainerHandle) null,           // open the container 
            internal_xact.getRawStoreXact(),
            false,
            (ContainerHandle.MODE_FORUPDATE | ContainerHandle.MODE_LOCK_NOWAIT),
            lock_level,
            btree.getBtreeLockingPolicy(
                internal_xact.getRawStoreXact(),
                lock_level,
                lock_mode,
                TransactionController.ISOLATION_REPEATABLE_READ, 
                base_cc,
                open_btree),
            btree, 
            (LogicalUndo) null,              // No logical undo necessry.
            (DynamicCompiledOpenConglomInfo) null);

        return(open_btree);
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
     * @param contextMgr the context manager started by the
	 *         post commit daemon
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int performWork(ContextManager contextMgr)
        throws StandardException
    {

        // requeue if work was not completed in this try because of locks
        boolean             requeue_work = false;

        TransactionManager tc             = (TransactionManager)
            this.access_factory.getAndNameTransaction(
                contextMgr, AccessFactoryGlobals.SYS_TRANS_NAME);

        TransactionManager  internal_xact  = tc.getInternalTransaction();

        if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("verbose_btree_post_commit"))
                System.out.println("starting internal xact\n");
        }

        OpenBTree           open_btree = null;

        try
        {
            // Get lock on base table.
            
            // First attempt to get a table lock on the btree.  This lock is
            // requested NOWAIT to not impede normal operation on the table.
            // If the lock were to wait then the current lock manager livelock 
            // algorithm would block all subsequent lock requests on this 
            // btree even if they are compatible with the current holder of 
            // the lock.
            //
            // If this lock is granted then:
            // 1) deleted rows on the page can automatically be purged as
            //    they must be committed, otherwise lock would not have been
            //    granted.
            // 2) if all rows from page are reclaimed then a structure shrink
            //    which requires table level lock can be executed.
            //
            open_btree = 
                openIndex(
                    internal_xact, 
                    TransactionController.MODE_TABLE, 
                    LockingPolicy.MODE_CONTAINER);

            DataValueDescriptor[] shrink_key = 
                purgeCommittedDeletes(open_btree, this.page_number);

            if (shrink_key != null)
                doShrink(open_btree, shrink_key);
        }
        catch (StandardException se)
        {
            // 2 kinds of errors here expected here.  Either container not 
            // found or could not obtain lock (LOCK_TIMEOUT or DEADLOCK).
            //
            // It is possible by the time this post commit work gets scheduled 
            // that the container has been dropped and that the open container 
            // call will return null - in this case just return assuming no 
            // work to be done.

            if (se.isLockTimeoutOrDeadlock())
			{
                // Could not get exclusive table lock, so try row level
                // reclaim of just the rows on this page.  No merge is 
                // attempted.

                try
                {
                    open_btree = 
                        openIndex(
                            internal_xact, 
                            TransactionController.MODE_RECORD, 
                            LockingPolicy.MODE_RECORD);

                    purgeRowLevelCommittedDeletes(open_btree);

                }
                catch (StandardException se2)
                {
                    if (se2.isLockTimeoutOrDeadlock())
                    {
                        // Could not get intended exclusive table lock, so 
                        // requeue and hope other user gives up table level
                        // lock soon.  This should not be normal case.
                        requeue_work = true;
                    }
                }
            }
        }
        finally
        {
            if (open_btree != null)
                open_btree.close();

            // counting on this commit to release latches associated with
            // row level purge, that have been left to prevent others from
            // getting to purged pages before the commit.  If latch is released
            // early, other transactions could insert on the page which could
            // prevent undo of the purges in case of a crash before the commit
            // gets to the disk.
            internal_xact.commit();
            internal_xact.destroy();
        }

        return(requeue_work ? Serviceable.REQUEUE : Serviceable.DONE);
    }

    private final DataValueDescriptor[] getShrinkKey(
    OpenBTree   open_btree, 
    ControlRow  control_row,
    int         slot_no)
        throws StandardException
    {
        DataValueDescriptor[] shrink_key = 
            open_btree.getConglomerate().createTemplate(
                    open_btree.getRawTran());

        control_row.page.fetchFromSlot(
            (RecordHandle) null,
            slot_no, shrink_key, 
            (FetchDescriptor) null,
			true);

        return(shrink_key);
    }

    /**
     * Reclaim space taken up by committed deleted rows.
     * <p>
     * This routine assumes it has been called by an internal transaction which
     * has performed no work so far, and that it has an exclusive table lock.  
     * These assumptions mean that any deleted rows encountered must be from
     * committed transactions (otherwise we could not have gotten the exclusive
     * table lock).
     * <p>
     * This routine handles purging committed deletes while holding a table
     * level exclusive lock.  See purgeRowLevelCommittedDeletes() for row level
     * purging.
     *
     * @param open_btree The btree already opened.
     * @param pageno The page number of the page to look for committed deletes.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private final DataValueDescriptor[] purgeCommittedDeletes(
    OpenBTree           open_btree,
    long                pageno)
        throws StandardException
    {
        ControlRow              control_row = null;
        DataValueDescriptor[]	shrink_key  = null; 

        try
        {
            // The following can fail either if it can't get the latch or
            // somehow the page requested no longer exists.  In either case
            // the post commit work will just skip it.
            control_row = ControlRow.getNoWait(open_btree, pageno);

            if (control_row != null)
            {
                Page page   = control_row.page;

                // The number records that can be reclaimed is:
                // total recs - control row - recs_not_deleted
                int num_possible_commit_delete = 
                    page.recordCount() - 1 - page.nonDeletedRecordCount();

                if (num_possible_commit_delete > 0)
                {
                    // loop backward so that purges which affect the slot table 
                    // don't affect the loop (ie. they only move records we 
                    // have already looked at).
                    for (int slot_no = page.recordCount() - 1; 
                         slot_no > 0; 
                         slot_no--) 
                    {
                        if (page.isDeletedAtSlot(slot_no))
                        {

                            if (page.recordCount() == 2)
                            {
                                // About to purge last row from page so 
                                // remember the key so we can shrink the 
                                // tree.
                                shrink_key = this.getShrinkKey(
                                    open_btree, control_row, slot_no);
                            }

                            page.purgeAtSlot(slot_no, 1, true);
                            // Tell scans positioned on this page to reposition
                            // because the row they are positioned on may have
                            // disappeared.
                            page.setRepositionNeeded();

                            if (SanityManager.DEBUG)
                            {
                                if (SanityManager.DEBUG_ON(
                                        "verbose_btree_post_commit"))
                                {
                                    System.out.println(
                                        "Purging row[" + slot_no + "]" + 
                                        "on page:" + pageno + ".\n");
                                }
                            }
                        }
                    }
                }

                if (page.recordCount() == 1)
                {
                    if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON("verbose_btree_post_commit"))
                        {
                            System.out.println("Chance to shrink.\n");
                        }
                    }
                }
            }
            else
            {
				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON("verbose_btree_post_commit"))
                    {
                        System.out.println(
                            "Get No Wait returned null. page num = " + pageno +
                            "\n");
                    }
                }
            }
        }
        finally
        {
            if (control_row != null)
                control_row.release();
        }

        return(shrink_key);
    }

    /**
     * Attempt to reclaim committed deleted rows from the page with row locking.
     * <p>
     * Get exclusive latch on page, and then loop backward through
     * page searching for deleted rows which are committed.  
     * This routine is called only from post commit processing so it will never
     * see rows deleted by the current transaction.
     * For each deleted row on the page
     * it attempts to get an exclusive lock on the deleted row, NOWAIT.
     * If it succeeds, and since this transaction did not delete the row then 
     * the row must have been deleted by a transaction which has committed, so
     * it is safe to purge the row.  It then purges the row from the page.
     * <p>
     * The latch on the leaf page containing the purged rows must be kept until
     * after the transaction has been committed or aborted in order to insure
     * proper undo of the purges can take place.  Otherwise another transaction
     * could use the space freed by the purge and then prevent the purge from
     * being able to undo.
     *
     * @param open_btree The already open btree, which has been locked with IX
     *                   table lock, to use to get latch on page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private final void purgeRowLevelCommittedDeletes(
    OpenBTree           open_btree)
        throws StandardException
    {
        LeafControlRow leaf = null;

        // The following can fail, returning null, either if it can't get
        // the latch or somehow the page requested no longer exists.  In 
        // either case the post commit work will just skip it.
        leaf = (LeafControlRow) 
            ControlRow.getNoWait(open_btree, page_number);
        if (leaf == null)
            return;

        BTreeLockingPolicy  btree_locking_policy = 
            open_btree.getLockingPolicy();

        // The number records that can be reclaimed is:
        // total recs - control row - recs_not_deleted
        int num_possible_commit_delete = 
            leaf.page.recordCount() - 1 - leaf.page.nonDeletedRecordCount();

        if (num_possible_commit_delete > 0)
        {
            DataValueDescriptor[] scratch_template = 
                open_btree.getRuntimeMem().get_template(
                    open_btree.getRawTran());

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
                        // Tell scans positioned on this page to reposition
                        // because the row they are positioned on may have
                        // disappeared.
                        page.setRepositionNeeded();
                    }
                }
            }

        }

        // need to maintain latch on leaf until xact is committed.  The
        // commit will clear the latch as part of releasing all 
        // locks/latches associated with a transaction.

        return;
    }
}
