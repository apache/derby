/*

   Derby - Class org.apache.derby.impl.store.raw.xact.RowLocking2nohold

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
	A locking policy that implements row level locking with isolation degree 2,
    never holding read locks after they are granted.

    Exactly the same as RowLocking2, except that read locks are acquired using
    zeroDuration locks, which are immediately released by the lock manager
    after they are granted.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLocking2nohold extends RowLocking2
{
	protected RowLocking2nohold(LockFactory lf) 
    {
		super(lf);
	}

    /**
     * Obtain lock on record being read.
     * <p>
     * Assumes that a table level IS has been acquired.  Will acquire a Shared
     * or Update lock on the row, depending on the "forUpdate" parameter.
     * <p>
     * Read lock will be acquired using zeroDuration lock.
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
        // RESOLVE - figure out what is right for update locks, for now throw
        // if they are used.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(!forUpdate);
        }

        return(lf.zeroDurationlockObject(
                t.getCompatibilitySpace(),
                record, 
                (forUpdate ? RowLock.RU2 : RowLock.RS2),
                waitForLock ? 
                    C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT));
	}

	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container_handle,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
	{
        return;
	}
}
