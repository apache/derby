/*

   Derby - Class org.apache.derby.impl.store.raw.xact.RowLocking3Escalate

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
import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerLock;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException;


/**
	A locking policy that implements row level locking with isolation degree 3.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLocking3Escalate extends ContainerLocking3 
{
	protected RowLocking3Escalate(LockFactory lf) 
    {
		super(lf);
	}

    /**
     * Escalates Row Locking 3 to Container Locking 3.
     * <p>
     * This call is made by code which tracks the number of locks on a 
     * container. When the number of locks exceeds the escalate threshold
     * the caller creates this new locking policy, calls lockContainer(), 
     * and substitues it for the old locking policy.  The lockContainer call
     * determines which table lock to get (S or X), gets that table lock, and
     * then releases the row locks on the table.
     *
     * It is assumed that this is called on a open container for lock only.
     * <p>
     *
     * @param t            Transaction to associate lock with.
     * @param container    Container to lock.
     * @param waitForLock  Ignored - will never wait for a lock.
     * @param forUpdate    Ignored, mode determined from current lock state.
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
		forUpdate = false;

        // If an IX lock exists then escalate to X table lock, else escalate
        // to S table lock.
		if (lf.isLockHeld(
                t.getCompatibilitySpace(), t, 
                container.getId(), ContainerLock.CIX))
        {
			forUpdate = true;
        }

        // Get the new X or S table lock.
		boolean gotLock = 
            super.lockContainer(t, container, waitForLock, forUpdate);

		if (!gotLock)
			return false;

        // now remove all matching ROW locks, this is done using the special
        // EscalateContainerKey() class which through the Matchable interface
        // only matches row locks of this container.
		lf.unlockGroup(
            t.getCompatibilitySpace(), t, 
            new EscalateContainerKey(container.getId()));

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                lf.isLockHeld(
                    t.getCompatibilitySpace(), t, 
                    container.getId(), 
                    (forUpdate ? ContainerLock.CX : ContainerLock.CS)),
                "Covering table lock (" +
                (forUpdate ? ContainerLock.CX : ContainerLock.CS) +
                " is not held after lock escalation.");
        }

		return true;
	}
}
