/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.xact
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException;


/**
	A locking policy that implements no locking.

	@see LockingPolicy
*/
class NoLocking implements LockingPolicy {

	protected NoLocking() {
	}

	public boolean lockContainer(
    Transaction     t, 
    ContainerHandle container, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException {
		return true;
	}

	public void unlockContainer(
    Transaction     t, 
    ContainerHandle container)
    {
	}

	public boolean lockRecordForRead(
    Transaction     t, 
    ContainerHandle container,
    RecordHandle    record, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException
	{
		return true;
	}

	public void lockRecordForRead(
    Latch			latch, 
    RecordHandle    record, 
    boolean         forUpdate)
		throws StandardException
	{
	}

	public boolean zeroDurationLockRecordForWrite(
    Transaction     t, 
    RecordHandle    record,
    boolean         lockForPreviousKey,
    boolean         waitForLock)
		throws StandardException
	{
		return true;
	}

	public boolean lockRecordForWrite(
    Transaction     t, 
    RecordHandle    record, 
    boolean         lockForInsert,
    boolean         waitForLock)
		throws StandardException
	{
		return true;
	}

	public void lockRecordForWrite(
    Latch			latch, 
    RecordHandle    record)
		throws StandardException
	{
	}

	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
	{
	}

	public int getMode() {
		return LockingPolicy.MODE_NONE;
	}

}
