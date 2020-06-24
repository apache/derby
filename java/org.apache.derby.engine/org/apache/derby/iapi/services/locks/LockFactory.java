/*

   Derby - Class org.apache.derby.iapi.services.locks.LockFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.locks;

import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.property.PropertySetCallback;
import java.util.Enumeration;


/**
    Generic locking of objects. Enables deadlock detection.

  <BR>
    MT - Mutable - Container Object - Thread Safe


*/
public interface LockFactory extends PropertySetCallback {

	/**
	 * Create an object which can be used as a compatibility space. A
	 * compatibility space object can only be used in the
	 * <code>LockFactory</code> that created it.
	 *
	 * @param owner the owner of the compatibility space (typically a
	 * transaction object). Might be <code>null</code>.
	 * @return an object which represents a compatibility space
	 */
	public CompatibilitySpace createCompatibilitySpace(LockOwner owner);

	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
		Lock an object within a compatibility space
		and associate the lock with a group object,
		waits up to timeout milli-seconds for the object to become unlocked. A 
        timeout of 0 means do not wait for the lock to be unlocked.
		Note the actual time waited is approximate.
		<P>
		A compatibility space in an space where lock requests are assumed to be
        compatible and granted by the lock manager if the trio
        {compatibilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier and compatibilitySpace, equals() method for ref).
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatibility space is not assumed to be owned by a single thread.
	


		@param compatibilitySpace object defining compatibility space
		@param group handle of group, must be private to a thread.
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout the maximum time to wait in milliseconds, LockFactory.NO_WAIT means don't wait.

		@return true if the lock was obtained, false if timeout is equal to LockFactory.NO_WAIT and the lock
		could not be granted.

        @exception StandardException if a deadlock has occurred (message id
//IC see: https://issues.apache.org/jira/browse/DERBY-6323
            will be LockFactory.Deadlock), or if the wait for the lock timed
            out (message id will be LockFactory.TimeOut), or if another thread
            interrupted this thread while it was waiting for the lock (this will
            be a StandardException with a nested InterruptedException, and the
            message id will be LockFactory.InterruptedExceptionId), or if
            any other error occurs when locking the object
	*/
	public boolean lockObject(CompatibilitySpace compatibilitySpace,
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
							  Object group, Lockable ref, Object qualifier,
							  int timeout)
		throws StandardException;

	/**
		Unlock a single lock on a single object held within this compatibility
		space and locked with the supplied qualifier.

		@param compatibilitySpace object defining compatibility space
		@param group handle of group.
		@param ref Reference to object to be unlocked.
		@param qualifier qualifier of lock to be unlocked

		@return number of locks released (one or zero).
	*/
	public int unlock(CompatibilitySpace compatibilitySpace, Object group,
					  Lockable ref, Object qualifier);

	/**
		Unlock all locks in a group. 

		@param compatibilitySpace object defining compatibility space
		@param group handle of group that objects were locked with.
	*/
	public void unlockGroup(CompatibilitySpace compatibilitySpace,
							Object group);

	/**
		Unlock all locks on a group that match the passed in value.

        @param compatibilitySpace A lock space
        @param group A group
        @param key The key
	*/
	public void unlockGroup(CompatibilitySpace compatibilitySpace,
							Object group, Matchable key);

	/**
		Transfer a set of locks from one group to another.

        @param compatibilitySpace A lock space
        @param oldGroup The source group
        @param newGroup The target group
	*/
	public void transfer(CompatibilitySpace compatibilitySpace,
						 Object oldGroup, Object newGroup);

	/**
		Returns true if locks held by anyone are blocking anyone else

        @return true if someone is blocked
	*/
	public boolean anyoneBlocked();

	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
		Return true if locks are held in this compatibility space and
		 this group.

        @param compatibilitySpace A lock space
		@param group handle of group that objects were locked with.
        @return true if locks matching the arguments are held

	*/
	public boolean areLocksHeld(CompatibilitySpace compatibilitySpace,
								Object group);

	/**
		Return true if locks are held in this compatibility space.

        @param compatibilitySpace A lock space
        @return true if locks matching the arguments are held

	*/
	public boolean areLocksHeld(CompatibilitySpace compatibilitySpace);

	/**
		Lock an object with zero duration within a compatibility space,
		waits up to timeout milli-seconds for the object to become unlocked. A 
        timeout of 0 means do not wait for the lock to be unlocked.
		Note the actual time waited is approximate.
		<P>
		Zero duration means the lock is released as soon as it is obtained.
		<P>
		A compatibility space in an space where lock requests are assumed to be
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
        compatible and granted by the lock manager if the trio
        {compatibilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier and compatibilitySpace, equals() method for ref).
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatibility space is not assumed to be owned by a single thread.
	


		@param compatibilitySpace object defining compatibility space
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout the maximum time to wait in milliseconds, LockFactory.NO_WAIT means don't wait.

		@return true if the lock was obtained, false if timeout is equal to LockFactory.NO_WAIT and the lock
		could not be granted.

        @exception StandardException if a deadlock has occurred (message id
//IC see: https://issues.apache.org/jira/browse/DERBY-6323
            will be LockFactory.Deadlock), or if the wait for the lock timed
            out (message id will be LockFactory.TimeOut), or if another thread
            interrupted this thread while it was waiting for the lock (this will
            be a StandardException with a nested InterruptedException, and the
            message id will be LockFactory.InterruptedExceptionId), or if
            any other error occurs when locking the object

	*/
	public boolean zeroDurationlockObject(CompatibilitySpace compatibilitySpace,
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
										  Lockable ref, Object qualifier,
										  int timeout)
		throws StandardException;

	/**
		Check to see if a specific lock is held.

        @param compatibilitySpace A lock space
        @param group A group
        @param ref Something which can be locked
        @param qualifier The kind of lock

        @return true if the indicated lock is held
	*/
	public boolean isLockHeld(CompatibilitySpace compatibilitySpace,
							  Object group, Lockable ref, Object qualifier);

	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-4565
		Get the lock timeout in milliseconds. A negative number means that
        there is no timeout.

        @return the lock timeout in milliseconds
	*/
	public int getWaitTimeout();

	/**
		Install a limit that is called when the size of the group exceeds
		the required limit.
		<BR>
		It is not guaranteed that the callback method (Limit.reached) is
		called as soon as the group size exceeds the given limit.
		If the callback method does not result in a decrease in the
		number of locks held then the lock factory implementation
		may delay calling the method again. E.g. with a limit
		of 500 and a reached() method that does nothing, may result
		in the call back method only being called when the group size reaches
		550.
		<BR>
		Only one limit may be in place for a group at any time.
		@see Limit

        @param compatibilitySpace A lock space
        @param group A group
        @param limit A limit for the group
        @param callback Code to call when the limit is reached
	*/
	public void setLimit(CompatibilitySpace compatibilitySpace, Object group,
						 int limit, Limit callback);

	/**
		Clear a limit set by setLimit.

        @param compatibilitySpace A lock space
        @param group A group
	*/
	public void clearLimit(CompatibilitySpace compatibilitySpace, Object group);

	/**
		Make a virtual lock table for diagnostics.

        @return a virtual lock table
	 */
	public Enumeration makeVirtualLockTable();

}


