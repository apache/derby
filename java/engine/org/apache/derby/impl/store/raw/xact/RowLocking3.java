/*

   Derby - Class org.apache.derby.impl.store.raw.xact.RowLocking3

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
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.RowLock;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.LockingPolicy;

import org.apache.derby.iapi.error.StandardException;


/**
	A locking policy that implements row level locking with isolation degree 3.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLocking3 extends NoLocking 
{
	// no locking has no state, so it's safe to hold
	// it as a static
	private static final LockingPolicy NO_LOCK = new NoLocking();

	protected final LockFactory	lf;

	protected RowLocking3(LockFactory lf) 
    {
		this.lf = lf;
	}

    /**
     * Get type of lock to get while reading data.
     * <p>
     * This routine is provided so that class's like RowLockingRR can
     * override just this routine to get RS2 locks vs RS3 locks, and still
     * reuse all the other code in this class.
     * <p>
     *
	 * @return The lock type of a shared lock for this locking policy.
     **/
	protected RowLock getReadLockType() 
    {
		return(RowLock.RS3);
    }

    /**
     * Get type of lock to get while requesting "update" lock.
     * <p>
     * This routine is provided so that class's like RowLockingRR can
     * override just this routine to get RU2 locks vs RU3 locks, and still
     * reuse all the other code in this class.
     * <p>
     *
	 * @return The lock type of a shared lock for this locking policy.
     **/
	protected RowLock getUpdateLockType() 
    {
		return(RowLock.RU3);
    }

    /**
     * Get type of lock to get while writing data.
     * <p>
     * This routine is provided so that class's like RowLockingRR can
     * override just this routine to get RX2 locks vs RX3 locks, and still
     * reuse all the other code in this class.
     * <p>
     *
	 * @return The lock type of a shared lock for this locking policy.
     **/
	protected RowLock getWriteLockType() 
    {
		return(RowLock.RX3);
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

		boolean gotLock = 
            lf.lockObject(
                t.getCompatibilitySpace(), t, container.getId(), qualifier,
                waitForLock ? C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT);

		if (gotLock) {
			// look for covering table locks
			// CIS is covered by CX or CS
			// CIX is covered by CX

			if (lf.isLockHeld(t.getCompatibilitySpace(), t, container.getId(), ContainerLock.CX) ||
				((!forUpdate) && lf.isLockHeld(t.getCompatibilitySpace(), t, container.getId(), ContainerLock.CS))) {


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
     *
     * @param t             The transaction to associate the lock with.
     * @param record        The record to be locked.
     * @param waitForLock   Should lock request wait until granted?
     * @param forUpdate     Whether to open for read or write access.
     *
     * @return true if the lock was granted, false if waitForLock was false 
     *              and the lock could not be granted.
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
        // RESOLVE - Did I do the right thing with the "forUpdate" variable.

        // For now just lock the row in Shared mode.
		Object qualifier = forUpdate ? getUpdateLockType() : getReadLockType();

        return(
            lf.lockObject(
                t.getCompatibilitySpace(), t, record, qualifier,
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

        // For now just lock the row in Shared mode.
		Object qualifier = forUpdate ? getUpdateLockType() : getReadLockType();

        lf.lockObject(
            latch.getCompatabilitySpace(), record, qualifier, 
            C_LockFactory.TIMED_WAIT, latch);
	}

    /**
     * Obtain lock on record being written.
     * <p>
     * Assumes that a table level IX has been acquired.  Will acquire an
     * Exclusive (X) lock on the row.
     * <p>
     *
     * @param t                        transaction to associate the lock with.
     * @param record                   The record to be locked.
     * @param lockForInsertPreviouskey Lock is for a previous key of a insert.
     * @param waitForLock              Should lock request wait until granted?
     *
     * @return true if the lock was granted, false if waitForLock was false 
     *              and the lock could not be granted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean zeroDurationLockRecordForWrite(
    Transaction     t, 
    RecordHandle    record,
    boolean         lockForPreviousKey,
    boolean         waitForLock)
		throws StandardException
	{
        return(lf.zeroDurationlockObject(
                t.getCompatibilitySpace(), record, 
                (lockForPreviousKey ? RowLock.RIP : getWriteLockType()),
                waitForLock ? C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT));
	}

    /**
     * Obtain lock on record being written.
     * <p>
     * Assumes that a table level IX has been acquired.  Will acquire an
     * Exclusive (X) lock on the row.
     * <p>
     *
     * @param t             The transaction to associate the lock with.
     * @param record        The record to be locked.
     * @param lockForInsert Lock is for an insert.
     * @param waitForLock   Should lock request wait until granted?
     *
     * @return true if the lock was granted, false if waitForLock was false 
     *              and the lock could not be granted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean lockRecordForWrite(
    Transaction     t, 
    RecordHandle    record,
    boolean         lockForInsert,
    boolean         waitForLock)
		throws StandardException
	{
        return(lf.lockObject(
            t.getCompatibilitySpace(), t, record, 
            lockForInsert ? RowLock.RI : getWriteLockType(),
            waitForLock   ? C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT));
	}

    /**
     * Obtain lock on record being written while holding a latch.
     * <p>
     * Assumes that a table level IX has been acquired.  Will acquire an
     * Exclusive (X) lock on the row.
     * <p>
     *
     * @param latch         The latch being held
     * @param record        The record to be locked.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void lockRecordForWrite(
    Latch		    latch, 
    RecordHandle    record)
		throws StandardException
	{
        lf.lockObject(
            latch.getCompatabilitySpace(), 
            record, 
            getWriteLockType(),
            C_LockFactory.TIMED_WAIT, 
            latch);
	}

	public int getMode() {
		return MODE_RECORD;
	}


	/*
	** We can inherit all the others methods of NoLocking since we hold the 
    ** container lock and row locks until the end of transaction.
	*/
}
