/*

   Derby - Class org.apache.derby.impl.store.raw.xact.RowLocking1

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
	A locking policy that implements row level locking with isolation degree 1.

    This is an implementation of Gray's degree 1 isolation, read uncommitted,
    or often referred to as dirty reads.  Basically read operations are 
    done with no locking.

    This locking policy is only to be used for read operations.

    The approach is to place all "write" container and row locks on the 
    transaction group lock list.  Locks on this group will last until end
    of transaction.  

    This implementation will still get table level intent locks.  This is to
    prevent hard cases where the container otherwise could be deleted while
    read uncommitted reader is still accessing it.  In order to not get table
    level intent locks some sort of other ddl level lock would have to be
    implemented.

    All "read" row locks will be not be requested.

    Note that write operations extend from the RowLocking3 implementations.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLocking1 extends RowLocking2
{

	protected RowLocking1(LockFactory lf) 
    {
		super(lf);
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

        return(
            !forUpdate ?
                true : 
                super.lockRecordForRead(
                    t, container_handle, record, waitForLock, forUpdate));
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
        if (forUpdate)
            super.lockRecordForRead(latch, record, forUpdate);
	}

	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container_handle,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
	{
        if (forUpdate)
        {
            super.unlockRecordAfterRead(
                t, container_handle, record, forUpdate, row_qualified);
        }
        return;
	}
}
