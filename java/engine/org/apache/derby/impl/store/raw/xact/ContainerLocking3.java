/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.xact
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	isolation degree 3.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class ContainerLocking3 extends NoLocking {

	protected final LockFactory	lf;

	protected ContainerLocking3(LockFactory lf)
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

		return(
            lf.lockObject(
                t.getCompatibilitySpace(), t, container.getId(), qualifier,
                waitForLock ? 
                    C_LockFactory.TIMED_WAIT : C_LockFactory.NO_WAIT));
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
