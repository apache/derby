/*

   Derby - Class org.apache.derby.impl.store.raw.xact.ContainerLocking2

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

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerLock;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException;


/**
	A locking policy that implements container level locking with
	isolation degree 2.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class ContainerLocking2 extends NoLocking {

	private final LockFactory	lf;

	protected ContainerLocking2()
    {
		this.lf = null;
	}

	protected ContainerLocking2(LockFactory lf)
    {
		this.lf = lf;
	}

	/**
		Obtain a Container shared or exclusive lock	until
		the end of the nested transaction.

		@exception StandardException Standard Cloudscape error policy
	*/
	public boolean lockContainer(
    Transaction     t, 
    ContainerHandle container, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException
    {
		Object qualifier = forUpdate ? ContainerLock.CX : ContainerLock.CS;

        // for cursor stability put read locks on a separate lock chain, which
        // will be released when the container is unlocked.
        Object group = 
            forUpdate ? t : container.getUniqueId();

		return(
            lf.lockObject(
                t.getCompatibilitySpace(), group, container.getId(), qualifier,
                waitForLock ? 
                    C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT));
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
     *
     **/
	public void unlockContainer(
    Transaction     t, 
    ContainerHandle container)
    {
        // Only release read locks before end of transaction in level 2.
        if (container.isReadOnly())
        {
            lf.unlockGroup(t.getCompatibilitySpace(), container.getUniqueId());
        }
	}

	public int getMode() {
		return MODE_CONTAINER;
	}


	/*
	** We can inherit all the others methods of NoLocking since we hold the 
    ** container lock until the end of transaction, and we don't obtain row
    ** locks.
	*/
}
