/*

   Derby - Class org.apache.derby.iapi.store.raw.LockingPolicy

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.error.StandardException;

/**
	Any object that implements this interface can be used as a locking
	policy for accessing a container. 
	<P>
	The locking policy must use the defined lock qualifiers 
    (ContainerLock.CIS, RowLock.RS, etc.) and the standard lock manager.
    (A locking policy that just performs no locking wouldn't need to use 
    these :-)
	<P>
	A locking policy must use the object that is an instance of Transaction
    (originally obtained via startTransaction() in RawStoreFactory) as the 
    compatibilitySpace for the LockFactory calls.
	<BR>
	A locking policy must use the passed in transaction as the 
    compatability space and the lock group.
	This chain (group) of locks has the following defined behaviour
		<UL>
		<LI>Locks are released at transaction.commit()
		<LI>Locks are released at transaction.abort()
		</UL>


    <BR>
	MT - Thread Safe

	@see ContainerHandle
	@see RecordHandle
	@see org.apache.derby.iapi.services.locks.LockFactory
	@see org.apache.derby.iapi.services.locks.Lockable

*/

public interface LockingPolicy {

	/**
		No locking what so ever, isolation parameter will be ignored by
		getLockingPolicy().

		@see  	RawStoreFactory
	*/
	static final int MODE_NONE = 0;

	/**
		Record level locking.
	*/
	static final int MODE_RECORD = 1;

	/**
		ContainerHandle level locking.
	*/
	static final int MODE_CONTAINER = 2;

	/**
		Called when a container is opened.

        @param t            Transaction to associate lock with.
        @param container    Container to lock.
        @param waitForLock  Should lock request wait until granted?
        @param forUpdate    Should container be locked for update, or read?

		@return true if the lock was obtained, false if it wasn't. 
        False should only be returned if the waitForLock policy was set to
        "false," and the lock was unavailable.

		@exception StandardException	Standard Cloudscape error policy

		@see ContainerHandle

	*/
	public boolean lockContainer(
    Transaction         t, 
    ContainerHandle     container, 
    boolean             waitForLock,
    boolean             forUpdate)
		throws StandardException;

	/**
		Called when a container is closed.

		@see ContainerHandle
		@see ContainerHandle#close
	*/
	public void unlockContainer(
    Transaction t, 
    ContainerHandle container);

	/**
		Called before a record is fetched.

        @param t            Transaction to associate lock with.
        @param container    Open Container used to get record.  Will be used
                            to row locks by the container they belong to.
        @param record       Record to lock.
        @param waitForLock  Should lock request wait until granted?
        @param forUpdate    Should container be locked for update, or read?


		@exception StandardException	Standard Cloudscape error policy

		@see Page

	*/
	public boolean lockRecordForRead(
    Transaction     t, 
    ContainerHandle container, 
    RecordHandle    record, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException;


	/**
		Lock a record while holding a page latch.

        @param latch        Latch held.
        @param record       Record to lock.
        @param forUpdate    Should container be locked for update, or read?


		@exception StandardException	Standard Cloudscape error policy

		@see Page

	*/
	public void lockRecordForRead(
		Latch			latch, 
		RecordHandle    record, 
		boolean         forUpdate)
			throws StandardException;

	/**
        Request a write lock which will be released immediately upon grant.

        @param t                        Transaction to associate lock with.
        @param record                   Record to lock.
        @param lockForInsertPreviouskey Lock is for a previous key of a insert.
        @param waitForLock              Should lock request wait until granted?

		@return true if the lock was obtained, false if it wasn't. 
        False should only be returned if the waitForLock argument was set to
        "false," and the lock was unavailable.

		@exception StandardException	Standard Cloudscape error policy

		@see Page
	*/
	public boolean zeroDurationLockRecordForWrite(
    Transaction     t, 
    RecordHandle    record,
    boolean         lockForPreviousKey,
    boolean         waitForLock)
		throws StandardException;

	/**
	    Called before a record is inserted, updated or deleted.

        If zeroDuration is true then lock is released immediately after it
        has been granted.

        @param t             Transaction to associate lock with.
        @param record        Record to lock.
        @param lockForInsert Lock is for an insert.
        @param waitForLock   Should lock request wait until granted?

		@return true if the lock was obtained, false if it wasn't. 
        False should only be returned if the waitForLock argument was set to
        "false," and the lock was unavailable.

		@exception StandardException	Standard Cloudscape error policy

		@see Page
	*/
	public boolean lockRecordForWrite(
    Transaction     t, 
    RecordHandle    record,
    boolean         lockForInsert,
    boolean         waitForLock)
		throws StandardException;

	/**
	    Lock a record for write while holding a page latch.


        @param latch        Page latch held.
        @param record       Record to lock.

		@exception StandardException	Standard Cloudscape error policy

		@see Page
	*/
	public void lockRecordForWrite(
    Latch			latch, 
    RecordHandle    record)
		throws StandardException;
	/**
		Called after a record has been fetched.

		@exception StandardException	Standard Cloudscape error policy

  		@see Page

	*/
	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container, 
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException;


	/**
		Get the mode of this policy
	*/
	public int getMode();
}
