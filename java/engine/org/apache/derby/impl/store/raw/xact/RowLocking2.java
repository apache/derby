/*

   Derby - Class org.apache.derby.impl.store.raw.xact.RowLocking2

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerLock;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.RowLock;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException;


/**
	A locking policy that implements row level locking with isolation degree 2.

    The approach is to place all "write" container and row locks on the 
    transaction group lock list.  Locks on this group will last until end
    of transaction.  All "read" container and row locks will be placed 
    on a group list, key'd by the ContainerId of the lock.  Locks on this
    list will either be released explicitly by the caller, or will be released
    as a group when the unlockContainer() call is made.

    Note that write operations extend from the RowLocking3 implementations.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLocking2 extends RowLockingRR
{
	// no locking has no state, so it's safe to hold
	// it as a static
	private static final LockingPolicy NO_LOCK = new NoLocking();

	protected RowLocking2(LockFactory lf) 
    {
		super(lf);
	}

    /**
     * Obtain container level intent lock.
     * <p>
     * This implementation of row locking is 2 level, ie. table and row locking.
     * It will interact correctly with tables opened with ContainerLocking3
     * locking mode.
     * <p>
     * Updater's will get table level IX locks, and X row locks.
     * <p>
     * Reader's will get table level IS locks, and S row locks.
     * <p>
     * Read locks are put in a separate "group" from the transaction, so that
     * when the container is closed it can release these read locks.
     *
     * @param t            Transaction to associate lock with.
     * @param container    Container to lock.
     * @param waitForLock  Should lock request wait until granted?
     * @param forUpdate    Should container be locked for update, or read?
     *
     * @return true if the lock was obtained, false if it wasn't. 
     *   False should only be returned if the waitForLock policy was set to
     *  "false," and the lock was unavailable.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean lockContainer(
    Transaction         t, 
    ContainerHandle     container, 
    boolean             waitForLock,
    boolean             forUpdate)
		throws StandardException 
    {
		Object qualifier = forUpdate ? ContainerLock.CIX : ContainerLock.CIS;

        // for cursor stability put read locks on a separate lock chain, which
        // will be released when the container is unlocked.
        Object group = 
            forUpdate ? ((Object) t) : ((Object) container.getUniqueId());

		boolean gotLock = 
            lf.lockObject(
                t.getCompatibilitySpace(), group, container.getId(), qualifier,
                waitForLock ? C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT);

		if (gotLock) 
        {
			// look for covering table locks
			// CIS and CIX is covered by CX 
            // In that case move the lock to the transaction list from the
            // container list, as the null locking policy will do nothing in
            // unlockContainer().
            //


			if (lf.isLockHeld(t.getCompatibilitySpace(), t, container.getId(), ContainerLock.CX) ||
				((!forUpdate) && 
                 lf.isLockHeld(t.getCompatibilitySpace(), t, container.getId(), ContainerLock.CS)))
            {
                // move lock from container group to transaction group.
                if (!forUpdate)
                    lf.transfer(t.getCompatibilitySpace(), group, t);
				container.setLockingPolicy(NO_LOCK);
			}
		}

		return gotLock;
	}


    /**
     * Obtain lock on record being read.
     * <p>
     * Assumes that a table level IS has been acquired.  Will acquire a Shared
     * or Update lock on the row, depending on the "forUpdate" parameter.
     * <p>
     * Read lock will be placed on separate group from transaction.
     *
     * @param t             The transaction to associate the lock with.
     * @param record        The record to be locked.
     * @param waitForLock   Should lock request wait until granted?
     * @param forUpdate     Whether to open for read or write access.
     *
     * @return true if the lock was granted, false if waitForLock was false 
     * and the lock could not be granted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean lockRecordForRead(
    Transaction     t, 
    ContainerHandle container_handle,
    RecordHandle    record, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException
	{
		Object qualifier = forUpdate ? RowLock.RU2 : RowLock.RS2;

        return( 
            lf.lockObject(
                t.getCompatibilitySpace(), 
                container_handle.getUniqueId(), 
                record, 
                qualifier,
                waitForLock ? 
                    C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT));
	}

    /**
     * Obtain lock on record being read while holding a latch.
     * <p>
     * Assumes that a table level IS has been acquired.  Will acquire a Shared
     * or Update lock on the row, depending on the "forUpdate" parameter.
     * <p>
     *
     * @param latch         The latch being held.
     * @param record        The record to be locked.
     * @param forUpdate     Whether to open for read or write access.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void lockRecordForRead(
    Latch			latch, 
    RecordHandle    record, 
    boolean         forUpdate)
		throws StandardException
	{
        // RESOLVE - Did I do the right thing with the "forUpdate" variable.
        // RESOLVE (mikem) - looks like it needs work for update locks, and
        //     compatibility spaces.

		Object qualifier = forUpdate ? RowLock.RU2 : RowLock.RS2;

        lf.lockObject(
            record.getContainerId(), record, qualifier, 
            C_LockFactory.TIMED_WAIT, latch);
	}

	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container_handle,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
	{
		Object qualifier = forUpdate ? RowLock.RU2 : RowLock.RS2;

        int count = 
            lf.unlock(
                t.getCompatibilitySpace(), container_handle.getUniqueId(), 
                record, qualifier);

        if (SanityManager.DEBUG)
        {
            // in the case of lock escalation the count could be 0.
            if (!(count == 1 || count == 0))
			{
				SanityManager.THROWASSERT(
                "count = " + count +
                "record.getContainerId() = " + record.getContainerId());
			}
        }
	}

    /**
     * Unlock read locks.
     * <p>
     * In Cursor stability release all read locks obtained.  unlockContainer()
     * will be called when the container is closed.
     * <p>
     *
     * @param t             The transaction to associate the lock with.
     * @param container     Container to unlock.
     **/
	public void unlockContainer(
    Transaction     t, 
    ContainerHandle container_handle)
    {
        // Only release read locks before end of transaction in level 2.
        lf.unlockGroup(
            t.getCompatibilitySpace(), container_handle.getUniqueId());
	}
}
