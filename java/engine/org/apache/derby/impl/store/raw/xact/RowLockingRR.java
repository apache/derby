/*

   Derby - Class org.apache.derby.impl.store.raw.xact.RowLockingRR

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
	A locking policy that implements row level locking with repeatable read
    isolation.  Since phantom protection with previous key locking is actually 
    handled by the upper level access methods, the only difference in repeatable
    read is that read locks are of type RowLock.RS2.  This type will not 
    conflict with a previous key insert lock.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLockingRR extends RowLocking3 
{

    protected RowLockingRR(LockFactory lf)
    {
            super(lf);
    }

	protected RowLock getReadLockType() 
    {
		return(RowLock.RS2);
    }

	protected RowLock getUpdateLockType() 
    {
		return(RowLock.RU2);
    }

	protected RowLock getWriteLockType() 
    {
		return(RowLock.RX2);
    }

    /**
     * Unlock a record after it has been locked for read.
     * <p>
     * In repeatable read only unlock records which "did not qualify".  For
     * example in a query like "select * from foo where a = 1" on a table
     * with no index it is only necessary to hold locks on rows where a=1, but
     * in the process of finding those rows the system will get locks on other
     * rows to verify they are committed before applying the qualifier.  Those
     * locks can be released under repeatable read isolation.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container_handle,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
	{
        if (!row_qualified)
        {
            Object qualifier = forUpdate ? RowLock.RU2 : RowLock.RS2;

            int count = 
                lf.unlock(t.getCompatibilitySpace(), t, record, qualifier);

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
	}

}
