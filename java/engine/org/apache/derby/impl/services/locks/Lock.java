/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.locks
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.List;

/**
	A Lock represents a granted or waiting lock request.

	<BR>
	MT - Mutable - Immutable identity : Thread Aware
*/

public class Lock implements Latch, Control {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
		Compatibility space the object is locked in.
		MT - immutable - reference only
	*/
	private final Object	space;

	/**
		Object being locked.
		MT - immutable - reference only
	*/
	private final Lockable	ref;
	/**
		Qualifier used in the lock request..
		MT - immutable - reference only
	*/
	private final Object	qualifier;

	int count;

	protected Lock(Object space, Lockable ref, Object qualifier) {
		super();
		this.space = space;
		this.ref = ref;
		this.qualifier = qualifier;
	}

	/**
		Return the object this lock is held on

		MT - Thread safe
	*/
	public final Lockable getLockable() {
		return ref;
	}

	/**
		Return the compatability space this lock is held in

		MT - Thread safe
	*/
	public final Object getCompatabilitySpace() {
		return space;
	}

	/**
		Return the qualifier lock was obtained with.

		MT - Thread safe
	*/

	public final Object getQualifier() {
		return qualifier;
	}

	/**
		Return the count of locks.

		MT - Thread safe
	*/

	public final int getCount() {
		return count;
	}

	// make a copy of this lock with the count set to zero, copies are only
	// to be used in the LockSpace code.
	final Lock copy() {

		return new Lock(space, ref, qualifier);
	}

	void grant() {

		count++;

		// Tell the object it has been locked by this type of qualifier.
		ref.lockEvent(this);
	}

	int unlock(int unlockCount) {

		if (unlockCount > count)
			unlockCount = count;

		count -= unlockCount;
		if (count == 0) {

			// Inform the object an unlock event occured with this qualifier

			ref.unlockEvent(this);
		}

		return unlockCount;
	}

	/*
	** Methods of object
	*/

	public final int hashCode() {

		// qualifier can be null so don't use it in hashCode

		return ref.hashCode() ^ space.hashCode();
	}

	public final boolean equals(Object other) {

		if (other instanceof Lock) {
			Lock ol = (Lock) other;

			return (space.equals(ol.space)) && ref.equals(ol.ref) && (qualifier == ol.qualifier);
		}

		return false;
	}

	/*
	** Methods of Control
	*/

	public LockControl getLockControl() {
		return new LockControl(this, ref);
	}

	public Lock getLock(Object compatabilitySpace, Object qualifier) {
		if (space.equals(compatabilitySpace) && (this.qualifier == qualifier))
			return this;
		return null;
	}

//EXCLUDE-START-lockdiag- 
	/**
		We can return ourselves here because our identity
		is immutable and what we returned will not be accessed
		as a Lock, so the count cannot be changed.
	*/
	public Control shallowClone() {
		return this;
	}
//EXCLUDE-END-lockdiag- 

	public ActiveLock firstWaiter() {
		return null;
	}

	public boolean isEmpty() {
		return count == 0;
	}

	public boolean unlock(Latch lockInGroup, int unlockCount) {

		if (unlockCount == 0)
			unlockCount = lockInGroup.getCount();
		
		if (SanityManager.DEBUG) {
			if (unlockCount > getCount())
				SanityManager.THROWASSERT(this + " unlockCount " + unlockCount + " is greater than lockCount " + getCount());
			if (!equals(lockInGroup))
				SanityManager.THROWASSERT(this + " mismatched locks " + lockInGroup);

		}

		unlock(unlockCount);

		return false;
	}
	public void addWaiters(java.util.Dictionary waiters) {
	}
	public Lock getFirstGrant() {
		return this;
	}
	public List getGranted() {
		return null;
	}
	public List getWaiting() {
		return null;
	}

    public boolean isGrantable(boolean noWaitersBeforeMe, Object compatabilitySpace, Object  requestQualifier)
    {
        boolean sameSpace = space.equals(compatabilitySpace);
		if (sameSpace && ref.lockerAlwaysCompatible())
			return true;

		return ref.requestCompatible(requestQualifier, this.qualifier);
	}
}

