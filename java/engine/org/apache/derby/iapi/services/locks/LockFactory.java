/*

   Derby - Class org.apache.derby.iapi.services.locks.LockFactory

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

package org.apache.derby.iapi.services.locks;

import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.property.PropertySetCallback;
import java.util.Enumeration;


/**
    Generic locking of objects. Enables deadlock detection.

  <BR>
    MT - Mutable - Container Object - Thread Safe


*/
public interface LockFactory extends PropertySetCallback {

	/**
		Lock an object within a compatability space
		and associate the lock with a group object,
		waits up to timeout milli-seconds for the object to become unlocked. A 
        timeout of 0 means do not wait for the lock to be unlocked.
		Note the actual time waited is approximate.
		<P>
		A compatibility space in an space where lock requests are assumed to be
        compatabile and granted by the lock manager if the trio
        {compatabilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier, equals() method
		for compatabilitySpace and ref ). A typical reference to use for the compatability
		space is a reference to an object representing a transaction.
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatability space is not assumed to be owned by a single thread.
	


		@param compatabilitySpace object defining compatability space (by value)
		@param group handle of group, must be private to a thread.
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout the maximum time to wait in milliseconds, LockFactory.NO_WAIT means don't wait.

		@return true if the lock was obtained, false if timeout is equal to LockFactory.NO_WAIT and the lock
		could not be granted.

		@exception org.apache.derby.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception org.apache.derby.iapi.error.StandardException The wait for the lock timed out (message id will be LockFactory.TimeOut).
		@exception org.apache.derby.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the lock. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Cloudscape error policy.

	*/
	public boolean lockObject(Object compatabilitySpace, Object group, Lockable ref, Object qualifier, int timeout)
		throws StandardException;

	/**
		Lock an object within a compatability space
		and associate the lock with a group object,
		waits forever the object to become unlocked.
		<P>
		A compatibility space in an space where lock requests are assumed to be
        compatabile and granted by the lock manager if the trio
        {compatabilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier, equals() method
		for compatabilitySpace and ref ). A typical reference to use for the compatability
		space is a reference to an object representing a transaction.
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatability space is not assumed to be owned by a single thread.
	


		@param compatabilitySpace object defining compatability space (by value)
		@param group handle of group, must be private to a thread.
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.

		@exception org.apache.derby.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception org.apache.derby.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the lock. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Cloudscape error policy.

	*/
	//public void lockObject(Object compatabilitySpace, Object group, Lockable ref, Object qualifier)
	//	throws StandardException;

	/**
		Lock an object within a compatability space
		and associate the lock with a group object,
		In addition a held latch is passed in. If the lock
		cannot be granted immediately, the latch will be released
		and relatched after the lock is obtained. If the lock can be granted
		immediately the latch is not released.
		<BR>
		The compatability space of the request is defined by the compatability
		space of the latch.
		<P>
		@param group handle of group, must be private to a compatability space.
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout amount of time to wait, <B>NO_WAIT is not supported</B>
		@param latch latch to be atomically released/re-latched in a wait.

		@return true if the latch was released, false otherwise.

		@exception org.apache.derby.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception org.apache.derby.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the lock. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Cloudscape error policy.

	*/
	public boolean lockObject(Object group, Lockable ref, Object qualifier, int timeout, Latch latch)
		throws StandardException;

	/**
		Unlock a single lock on a single object held within this compatability space
		that was locked with the supplied qualifier.

		@param compatabilitySpace object defining compatability space (by value)
		@param group handle of group.
		@param ref Reference to object to be unlocked.
		@param qualifier qualifier of lock to be unlocked

		@return number of locks released (one or zero).
	*/
	public int unlock(Object compatabilitySpace, Object group, Lockable ref, Object qualifier);

	/**
		Unlock all locks in a group. 

		@param group handle of group that objects were locked with.
	*/
	public void unlockGroup(Object compatabilitySpace, Object group);

	/**
		Unlock all locks on a group that match the passed in value.
	*/
	public void unlockGroup(Object compatabilitySpace, Object group, Matchable key);

	/**
		Transfer a set of locks from one group to another.
	*/
	public void transfer(Object compatabilitySpace, Object oldGroup, Object newGroup);

	/**
		Returns true if locks held by anyone are blocking anyone else
	*/
	public boolean anyoneBlocked();

	/**
		Return true if locks are held in this compatability space and
		 this group.

		@param group handle of group that objects were locked with.

	*/
	public boolean areLocksHeld(Object compatabilitySpace, Object group);

	/**
		Return true if locks are held in this compatability space.
	*/
	public boolean areLocksHeld(Object compatabilitySpace);


	/**
		Latch an object. A latch is a lock without a group.
		This means that it must be released explicitly by the owner.
		A latch is not released by any unlock methods, it must be
		released by the unlatch method. A latch is assumed to only
		be held by one locker at a time.
<BR>
		The first argument passed to lockEvent() is the Latch that
		is to be used in the unlatch() call.
		The firstArgument passed to unlockEvent() should be ignored.

  		@return true if the latch was obtained,
		false if timeout is equal to LockFactory.NO_WAIT and the lock could not be granted.


		@exception org.apache.derby.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception org.apache.derby.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the latch. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Cloudscape error policy.
	*/
	public boolean latchObject(Object compatabilitySpace, Lockable ref, Object qualifier, int timeout)
		throws StandardException;

	/**
		Unlatch an object.
	*/
	public void unlatch(Latch heldLatch);

	
	/**
		Lock an object with zero duration within a compatability space,
		waits up to timeout milli-seconds for the object to become unlocked. A 
        timeout of 0 means do not wait for the lock to be unlocked.
		Note the actual time waited is approximate.
		<P>
		Zero duration means the lock is released as soon as it is obtained.
		<P>
		A compatibility space in an space where lock requests are assumed to be
        compatabile and granted by the lock manager if the trio
        {compatabilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier, equals() method
		for compatabilitySpace and ref ). A typical reference to use for the compatability
		space is a reference to an object representing a transaction.
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatability space is not assumed to be owned by a single thread.
	


		@param compatabilitySpace object defining compatability space (by value)
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout the maximum time to wait in milliseconds, LockFactory.NO_WAIT means don't wait.

		@return true if the lock was obtained, false if timeout is equal to LockFactory.NO_WAIT and the lock
		could not be granted.

		@exception org.apache.derby.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception org.apache.derby.iapi.error.StandardException The wait for the lock timed out (message id will be LockFactory.TimeOut).
		@exception org.apache.derby.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the lock. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Cloudscape error policy.

	*/
	public boolean zeroDurationlockObject(Object compatabilitySpace, Lockable ref, Object qualifier, int timeout)
		throws StandardException;

	/**
		Check to see if a specific lock is held.
	*/
	public boolean isLockHeld(Object compatabilitySpace, Object group, Lockable ref, Object qualifier);

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
	*/
	public void setLimit(Object compatabilitySpace, Object group, int limit, Limit callback);

	/**
		Clear a limit set by setLimit.
	*/
	public void clearLimit(Object compatabilitySpace, Object group); 

	/**
		Make a virtual lock table for diagnostics.
	 */
	public Enumeration makeVirtualLockTable();

}


