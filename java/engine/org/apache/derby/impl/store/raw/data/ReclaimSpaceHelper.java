/*

   Derby - Class org.apache.derby.impl.store.raw.data.ReclaimSpaceHelper

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.impl.store.raw.data.BasePage;
import org.apache.derby.impl.store.raw.data.ReclaimSpace;


import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;


/**
	This class helps a BaseDataFactory reclaims unused space

Space needs to be reclaimed in the following cases:
<BR><NL>
<LI> Row with long columns or overflow row pieces is deleted
<LI> Insertion of a row that has long columns or overflows to other row pieces is rolled back
<LI> Row is updated and the head row or some row pieces shrunk
<LI> Row is updated and some long columns are orphaned because they are updated
<LI> Row is updated and some long columns are created but the update rolled back
<LI> Row is updated and some new row pieces are created but the update rolled back
</NL> <P>

We can implement a lot of optimization if we know that btree does not overflow.
However, since that is not the case and Raw Store cannot tell if it is dealing
with a btree page or a heap page, they all have to be treated gingerly.  E.g.,
in heap page, once a head row is deleted (via a delete operation or via a
rollback of insert), all the long rows and long columns can be reclaimed - in
fact, most of the head row can be removed and reclaimed, only a row stub needs
to remain for locking purposes.  But in the btree, a deleted row still needs to
contain the key values so it cannot be cleaned up until the row is purged.

<P><B>
Row with long columns or long row is deleted
</B><BR>

When Access purge a committed deleted row, the purge operation will see if the
row has overflowed row pieces or if it has long columns.  If it has, then all
the long columns and row pieces are purged before the head row piece can be
purged.  When a row is purged from an overflow page and it is the only row on
the page, then the page is deallocated in the same transaction. Note that
non-overflow pages are removed by Access but overflow pages are removed by Raw
Store.  Note that page removal is done in the same transaction and not post
commit.  This is, in general, dangerous because if the transaction does not
commit for a long time, uncommit deallocated page slows down page allocation
for this container.  However, we know that access only purges committed delete
row in access post commit processing so we know the transaction will tend to
commit relatively fast.  The alternative is to queue up a post commit
ReclaimSpace.PAGE to reclaim the page after the purge commits.  In order to do
that, the time stamp of the page must also be remembered because post commit
work may be queued more than once, but in this case, it can only be done once.
Also, doing the page deallocation post commit adds to the overall cost and
tends to fill up the post commit queue. <BR>

This approach is simple but has the drawback that the entire long row and all
the long columns are logged in the purge operation.  The alternative is more
complicated, we can remember all the long columns on the head row piece and
where the row chain starts and clean them up during post commit.  During post
commit, because the head row piece is already purged, there is no need to log
the long column or the long rows, just wipe the page or just reuse the page if
that is the only thing on the page.  The problem with this approach is that we
need to make sure the purging of the head row does indeed commit (the
transaction may commit but the purging may be rolled back due to savepoint).
So, we need to find the head row in the post commit and only when we cannot
find it can we be sure that the purge is committed.  However, in cases where
the page can reuse its record Id (namely in btree), a new row may reuse the
same recordId.  In that case, the post commit can purge the long columns or the
rest of the row piece only if the head piece no longer points to it.  Because
of the complexity of this latter approach, the first simple approach is used.
However, if the performance due to extra logging becomes unbearble, we can
consider implementing the second approach.  

<P><B>
Insertion of a row with long column or long row is rolled back.
</B><BR>

Insertion can be rolled back with either delete or purge.  If the row is rolled
back with purge, then all the overflow columns pieces and row pieces are also
rolled back with purge.  When a row is purged from an overflow page and it is
the only row on the page, then a post commit ReclaimSpace.PAGE work is queued
by Raw Store to reclaim that page.<BR>

If the row is rolled back with delete, then all the overflow columns pieces and
row pieces are also rolled back with delete.  Access will purge the deleted row
in due time, see above.

<P><B>
Row is updated and the head row or some row pieces shrunk
</B><BR>

Every page that an update operation touches will see if the record on that page
has any reserve space.  It it does, and if the reserve space plus the record
size exceed the mininum record size, then a post commit ROW_RESERVE work will
be queued to reclaim all unnecessary row reserved space for the entire row.

<P><B>
Row is updated and old long columns are orphaned
</B><BR>

The ground rule is, whether a column is a long column or not before an update
has nothing to do with whether a column will be a long column or not after the
update.  In other words, update can turn a non-long column into a long column,
or it can turn a long column into a non-long column, or a long column can be
updated to another long column and a non-long column can be updated to a
non-long column.  The last case - update of a non-long column to another
non-long column - is only of concern if it shrinks the row piece it is on (see
above).<BR>

So update can be looked at as 2 separate problems: A) a column is a long column
before the update and the update will "orphaned" it.  B) a column is a long
column after the update and the rollback of the update will "orphaned" it if it
is rolled back with a delete.  This section deals with problem A, next section
deals with problem B.<BR>

Update specifies a set of columns to be updated.  If a row piece contains one
or more columns to be updated, those columns are examined to see if they are
actually long column chains.  If they are, then after the update, those long
column chains will be orphaned.  So before the update happens, a post commit
ReclaimSpace.COLUMN_CHAIN work is queued which contains the head rows id, the
column number, the location of the first piece of the column chain, and the
time stamp of the first page of the column chain. <BR>

If the update transaction commits, the post commit work will walk the row until
it finds the column number (note that it may not be on the page where the
update happened because of subsequent row splitting), and if it doesn't point
to the head of the column chain, we know the update operation has indeed
committed (versus rolled back by a savepoint).  If a piece of the the column
chain takes up an entire page, then the entire page can be reclaimed without
first purging the row because the column chain is already orphaned.<BR>

We need to page time stamp of the first page of the column chain because if the
post commit ReclaimSpace.COLUMN_CHAIN is queued more than once, as can happen
in repeated rollback to savepoint, then after the first time the column is
reclaimed, the pages in the column chain can be reused.  Therefore, we cannot
reclaim the column chain again.  Since there is no back pointer from the column
chain to the head row, we need the timestamp to tell us if that column chain
has already been touched (reclaimed) or not.

<P><B> 
Row is updated with new long columns and update is rolled back.
</B><BR>

When the update is rolled back, the new long columns, which got there by
insertion, got rolled back either by delete or by purge.  If they were rolled
back with delete, then they will be orphaned and need to be cleaned up with
post abort work.  Therefore, insertion of long columns due to update must be
rolled back with purge.<BR>

This is safe because the moment the rollback of the head row piece happens, the
new long column is orphaned anyway and nobody will be able to get to it.  Since
we don't attempt to share long column pages, we know that nobody else could be
on the page and it is safe to deallocate the page.

<P><B>
Row is updated with new long row piece and update is rolled back.
</B><BR>

When the update is rolled back, the new long row piece, which got there by
insertion, got rolled back either by delete or by purge.  Like update with new
long row, they should be rolled back with purge.  However, there is a problem
in that the insert log record does not contain the head row handle.  It is
possible that another long row emanating from the same head page overflows to
this page.  That row may since have been deleted and is now in the middle of a
purge, but the purge has not commit.  To the code that is rolling back the
insert (caused by the update that split off a new row piece) the overflow page
looks empty.  If it went ahead and deallocate the page, then the transaction
which purged the row piece on this page won't be able to roll back.  For this
reason, the rollback to insert of a long row piece due to update must be rolled
back with delete.  Furthermore, there is no easy way to lodge a post
termination work to reclaim this deleted row piece so it will be lost forever.
<BR>

RESOLVE: need to log the head row's handle in the insert log record, i.e., any
insert due to update of long row or column piece should have the head row's
handle on it so that when the insert is rolled back with purge, and there is no
more row on the page, it can file a post commit to reclaim the page safely.
The post commit reclaim page needs to lock the head row and latch the head page
to make sure the entire row chain is stable.

<P><B>
*/
public class ReclaimSpaceHelper
{
	/**
		Reclaim space based on work.
	 */
	public static int reclaimSpace(BaseDataFileFactory dataFactory,
							RawTransaction tran,
							ReclaimSpace work) 
		 throws StandardException
	{
	
		if (work.reclaimWhat() == ReclaimSpace.CONTAINER)
			return reclaimContainer(dataFactory, tran, work);

		// Else, not reclaiming container. Get a no-wait shared lock on the 
		// container regardless of how the user transaction had the
		// container opened. 

		LockingPolicy container_rlock = 
			tran.newLockingPolicy(LockingPolicy.MODE_RECORD,
								  TransactionController.ISOLATION_SERIALIZABLE, 
								  true /* stricter OK */ );

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(container_rlock != null);

		ContainerHandle containerHdl = 
			openContainerNW(tran, container_rlock, work.getContainerId());

		if (containerHdl == null)
		{
			tran.abort();

			if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
                {
                    SanityManager.DEBUG(
                        DaemonService.DaemonTrace, " aborted " + work + 
                        " because container is locked or dropped");
                }
            }

			if (work.incrAttempts() < 3) // retry this for serveral times
				return Serviceable.REQUEUE;
			else
				return Serviceable.DONE;
		}	

		// At this point, container is opened with IX lock.

		if (work.reclaimWhat() == ReclaimSpace.PAGE)
		{
			// Reclaiming a page - called by undo of insert which purged the
			// last row off an overflow page. It is safe to reclaim the page
			// without first locking the head row because unlike post commit
			// work, this is post abort work.  Abort is guarenteed to happen
			// and to happen only once, if at all.
			Page p = containerHdl.getPageNoWait(work.getPageId().getPageNumber());
			if (p != null)
				containerHdl.removePage(p);

			tran.commit();
			return Serviceable.DONE;
		}

		// We are reclaiming row space or long column.  First get an xlock on the
		// head row piece.
		RecordHandle headRecord = work.getHeadRowHandle();

		if (!container_rlock.lockRecordForWrite(
                tran, headRecord, false /* not insert */, false /* nowait */))
		{
			// cannot get the row lock, retry
			tran.abort();
			if (work.incrAttempts() < 3)
				return Serviceable.REQUEUE;
			else
				return Serviceable.DONE;
		}

		// The exclusive lock on the head row has been gotten.

		if (work.reclaimWhat() == ReclaimSpace.ROW_RESERVE)
		{
			// This row may benefit from compaction.
			containerHdl.compactRecord(headRecord);

            // This work is being done - post commit, there is no user 
            // transaction that depends on the commit being sync'd.  It is safe
            // to commitNoSync() This do as one of 2 things will happen:
            //
            //     1) if any data page associated with this transaction is
            //        moved from cache to disk, then the transaction log
            //        must be sync'd to the log record for that change and
            //        all log records including the commit of this xact must
            //        be sync'd before returning.
            //
            //     2) if the data page is never written then the log record
            //        for the commit may never be written, and the xact will
            //        never make to disk.  This is ok as no subsequent action
            //        depends on this operation being committed.
            //
			tran.commitNoSync(Transaction.RELEASE_LOCKS);

			return Serviceable.DONE;
		}
		else
		{
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(work.reclaimWhat() == ReclaimSpace.COLUMN_CHAIN);

			// Reclaiming a long column chain due to update.  The long column
			// chain being reclaimed is the before image of the update
			// operation.  
			// 
			long headPageId = ((PageKey)headRecord.getPageId()).getPageNumber();
			StoredPage headRowPage = 
				(StoredPage)containerHdl.getPageNoWait(headPageId);

			if (headRowPage == null)
			{
				// Cannot get page no wait, try again later.
				tran.abort();
				if (work.incrAttempts() < 3)
					return Serviceable.REQUEUE;
				else
					return Serviceable.DONE;
			}

			try
			{
				headRowPage.removeOrphanedColumnChain(work, containerHdl);
			}
			finally
			{
				headRowPage.unlatch();
			}

            // This work is being done - post commit, there is no user 
            // transaction that depends on the commit being sync'd.  It is safe
            // to commitNoSync() This do as one of 2 things will happen:
            //
            //     1) if any data page associated with this transaction is
            //        moved from cache to disk, then the transaction log
            //        must be sync'd to the log record for that change and
            //        all log records including the commit of this xact must
            //        be sync'd before returning.
            //
            //     2) if the data page is never written then the log record
            //        for the commit may never be written, and the xact will
            //        never make to disk.  This is ok as no subsequent action
            //        depends on this operation being committed.
            //
			tran.commitNoSync(Transaction.RELEASE_LOCKS);

			return Serviceable.DONE;
		}
	}

	private static int reclaimContainer(BaseDataFileFactory dataFactory,
										RawTransaction tran,
										ReclaimSpace work) 
		 throws StandardException
	{
		// when we want to reclaim the whole container, gets an exclusive
		// XLock on the container, wait for the lock.

		LockingPolicy container_xlock = 
			tran.newLockingPolicy(LockingPolicy.MODE_CONTAINER,
								  TransactionController.ISOLATION_SERIALIZABLE, 
								  true /* stricter OK */ );

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(container_xlock != null);

		// Try to just get the container thru the transaction.
		// Need to do this to transition the transaction to active state. 
		RawContainerHandle containerHdl = tran.openDroppedContainer(
								work.getContainerId(), 
								container_xlock);

		// if it can get lock but it is not deleted or has already been
		// deleted, done work
		if (containerHdl == null || 
			containerHdl.getContainerStatus() == RawContainerHandle.NORMAL ||
			containerHdl.getContainerStatus() == RawContainerHandle.COMMITTED_DROP)
		{
			if (containerHdl != null)
				containerHdl.close();
			tran.abort();	// release xlock, if any

			if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
                {
                    SanityManager.DEBUG(
                        DaemonService.DaemonTrace, "  aborted " + work);
                }
            }
		}	
		else
		{
			// we got an xlock on a dropped container.  Must be committed.
			// Get rid of the container now.
			ContainerOperation lop = new
				ContainerOperation(containerHdl, ContainerOperation.REMOVE);

			// mark the container as pre-dirtied so that if a checkpoint
			// happens after the log record is sent to the log stream, the
			// cache cleaning will wait for this change.
			containerHdl.preDirty(true);
			try
			{
				tran.logAndDo(lop);
			}
			finally
			{
				// in case logAndDo fail, make sure the container is not
				// stuck in preDirty state.
				containerHdl.preDirty(false);
			}


			containerHdl.close();
			tran.commit();

			if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
                {
                    SanityManager.DEBUG(
                        DaemonService.DaemonTrace, "  committed " + work);
                }
            }
		}

		return Serviceable.DONE;

	}


	/**
		Open container shared no wait
	 */
	private static ContainerHandle openContainerNW(Transaction tran,
		LockingPolicy rlock, ContainerKey containerId)
		throws StandardException
	{
		ContainerHandle containerHdl = tran.openContainer
			(containerId, rlock,
			 ContainerHandle.MODE_FORUPDATE |
			 ContainerHandle.MODE_LOCK_NOWAIT); 

		return containerHdl;
	}

}
