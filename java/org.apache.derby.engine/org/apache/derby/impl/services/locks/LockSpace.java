/*

   Derby - Class org.apache.derby.impl.services.locks.LockSpace

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

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Limit;

import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.StandardException;

import java.util.ArrayDeque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.derby.iapi.services.locks.LockOwner;

/**

	A LockSpace represents the complete set of locks held within
	a single compatibility space, broken into groups of locks.
//IC see: https://issues.apache.org/jira/browse/DERBY-2328

    A LockSpace contains a HashMap keyed by the group reference,
    the data for each key is a HashMap of Lock's.

    <p> A <code>LockSpace</code> can have an owner (for instance a
    transaction). Currently, the owner is used by the virtual lock table to
    find out which transaction a lock belongs to. Some parts of the code also
    use the owner as a group object which guarantees that the lock is released
    on a commit or an abort. The owner has no special meaning to the lock
    manager and can be any object, including <code>null</code>. </p>
*/
final class LockSpace implements CompatibilitySpace {

	/** Map from group references to groups of locks. */
    private final HashMap<Object, HashMap<Lock, Lock>> groups;
	/** Reference to the owner of this compatibility space. */
	private final LockOwner owner;

    /** The maximum number of elements to cache in {@link #spareGroups}. */
    private static final int MAX_CACHED_GROUPS = 3;

    /** Cached HashMaps for storing lock groups. */
    private final ArrayDeque<HashMap<Lock, Lock>> spareGroups =
            new ArrayDeque<HashMap<Lock, Lock>>(MAX_CACHED_GROUPS);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

	// the Limit info.
	private Object callbackGroup;
	private int    limit;
	private int    nextLimitCall;
	private Limit  callback;

	/**
	 * Creates a new <code>LockSpace</code> instance.
	 *
	 * @param owner an object representing the owner of the compatibility space
	 */
	LockSpace(LockOwner owner) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        groups = new HashMap<Object, HashMap<Lock, Lock>>();
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
		this.owner = owner;
	}

	/**
	 * Get the object representing the owner of the compatibility space.
	 *
	 * @return the owner of the compatibility space
	 */
	public LockOwner getOwner() {
		return owner;
	}

	/**
		Add a lock to a group.
	*/
	protected synchronized void addLock(Object group, Lock lock)
		throws StandardException {

		Lock lockInGroup = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<Lock, Lock> dl = groups.get(group);
		if (dl == null)	{
			dl = getGroupMap(group);
		} else if (lock.getCount() != 1) {
            lockInGroup = dl.get(lock);
		}

		if (lockInGroup == null) {
			lockInGroup = lock.copy();
			dl.put(lockInGroup, lockInGroup);
		}
		lockInGroup.count++;

		if (inLimit)
			return;

		if (!group.equals(callbackGroup))
			return;

		int groupSize = dl.size();
		
		if (groupSize > nextLimitCall) {

			inLimit = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
			callback.reached(this, group, limit,
				new LockList(java.util.Collections.enumeration(dl.keySet())), groupSize);
			inLimit = false;

			// see when the next callback should occur, if the callback
			// failed to release a sufficent amount of locks then
			// delay until another "limit" locks are obtained.
			int newGroupSize = dl.size();
			if (newGroupSize < (limit / 2))
				nextLimitCall = limit;
			else if (newGroupSize < (nextLimitCall / 2))
				nextLimitCall -= limit;
			else
				nextLimitCall += limit;

		}
	}
	
	private boolean inLimit;
	/**
		Unlock all the locks in a group and then remove the group.
	*/

//IC see: https://issues.apache.org/jira/browse/DERBY-2327
	synchronized void unlockGroup(LockTable lset, Object group) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<Lock, Lock> dl = groups.remove(group);
		if (dl == null)
			return;

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (Lock lock : dl.keySet()) {
            lset.unlock(lock, 0);
		}

		if ((callbackGroup != null) && group.equals(callbackGroup)) {
			nextLimitCall = limit;
		}

		saveGroup(dl);
	}

    private HashMap<Lock, Lock> getGroupMap(Object group) {
        HashMap<Lock, Lock> dl = spareGroups.poll();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

		if (dl == null)
            dl = new HashMap<Lock, Lock>(5, 0.8f);

//IC see: https://issues.apache.org/jira/browse/DERBY-1704
		groups.put(group, dl);
		return dl;
	}

    private void saveGroup(HashMap<Lock, Lock> dl) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        if (spareGroups.size() < MAX_CACHED_GROUPS) {
            spareGroups.offer(dl);
            dl.clear();
        }
	}

	/**
		Unlock all locks in the group that match the key
	*/
//IC see: https://issues.apache.org/jira/browse/DERBY-2327
	synchronized void unlockGroup(LockTable lset, Object group, Matchable key) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<Lock, Lock> dl = groups.get(group);
		if (dl == null)
			return; //  no group at all

		boolean allUnlocked = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (Iterator<Lock> e = dl.keySet().iterator(); e.hasNext(); ) {
            Lock lock = e.next();
			if (!key.match(lock.getLockable())) {
				allUnlocked = false;
				continue;
			}
			lset.unlock(lock, 0);
			e.remove();
		}

		if (allUnlocked) {
			groups.remove(group);
			saveGroup(dl);
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
			if ((callbackGroup != null) && group.equals(callbackGroup)) {
				nextLimitCall = limit;
			}
		}
	}

	synchronized void transfer(Object oldGroup, Object newGroup) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<Lock, Lock> from = groups.get(oldGroup);
		if (from == null)
			return;

        HashMap<Lock, Lock> to = groups.get(newGroup);
		if (to == null) {
			// simple case 
			groups.put(newGroup, from);
			clearLimit(oldGroup);
			groups.remove(oldGroup);
			return;
		}

		if (to.size() < from.size()) {

			// place the contents of to into from
			mergeGroups(to, from);

//IC see: https://issues.apache.org/jira/browse/DERBY-1704
			Object oldTo = groups.put(newGroup, from);
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(oldTo == to, "inconsistent state in LockSpace");
			}

		} else {
			mergeGroups(from, to);
		}
		
		clearLimit(oldGroup);
//IC see: https://issues.apache.org/jira/browse/DERBY-1704
		groups.remove(oldGroup);
	}

    private void mergeGroups(HashMap<Lock, Lock> from, HashMap<Lock, Lock> into) {

        for (Lock lock : from.keySet()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

            Lock lockI = into.get(lock);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

			if (lockI == null) {
				// lock is only in from list
				into.put(lock, lock);
			} else {
				// merge the locks
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				Lock fromL = lock;
                Lock intoL = lockI;
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

				intoL.count += fromL.getCount();
			}
		}

	}

//IC see: https://issues.apache.org/jira/browse/DERBY-2327
	synchronized int unlockReference(LockTable lset, Lockable ref,
									 Object qualifier, Object group) {

		// look for locks matching our reference and qualifier.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<Lock, Lock> dl = groups.get(group);
		if (dl == null)
			return 0;

		Lock lockInGroup = lset.unlockReference(this, ref, qualifier, dl);
		if (lockInGroup == null) {
			return 0;
		}

		if (lockInGroup.getCount() == 1) {

			if (dl.isEmpty()) {
				groups.remove(group);
				saveGroup(dl);
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
				if ((callbackGroup != null) && group.equals(callbackGroup)) {
					nextLimitCall = limit;
				}
			}

			return 1;
		}
			
		// the lock item will be left in the group
		lockInGroup.count--;
		dl.put(lockInGroup, lockInGroup);
		return 1;
	}

	/**
		Return true if locks are held in a group
	*/
	synchronized boolean areLocksHeld(Object group) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1704
		return groups.containsKey(group);
	}

	/**
	 * Return true if locks are held in this compatibility space.
	 * @return true if locks are held, false otherwise
	 */
	synchronized boolean areLocksHeld() {
		return !groups.isEmpty();
	}
	
	synchronized boolean isLockHeld(Object group, Lockable ref, Object qualifier) {

		// look for locks matching our reference and qualifier.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        HashMap<Lock, Lock> dl = groups.get(group);
		if (dl == null)
			return false;

        return dl.containsKey(new Lock(this, ref, qualifier));
	}

	synchronized void setLimit(Object group, int limit, Limit callback) {
		callbackGroup = group;
		this.nextLimitCall = this.limit = limit;
		this.callback = callback;
	}

	/**
		Clear a limit set by setLimit.
	*/
	synchronized void clearLimit(Object group) {
		if (group.equals(callbackGroup)) {
			callbackGroup = null;
			nextLimitCall = limit = Integer.MAX_VALUE;
			callback = null;
		}
	}

	/**
		Return a count of the number of locks
		held by this space. The argument bail
		indicates at which point the counting
		should bail out and return the current
		count. This routine will bail if the
		count is greater than bail. Thus this
		routine is intended to for deadlock
		code to find the space with the
		fewest number of locks.
	*/
	synchronized int deadlockCount(int bail) {

		int count = 0;

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (HashMap<Lock, Lock> group: groups.values()) {
            for (Lock lock: group.keySet()) {
					count += lock.getCount();
					if (count > bail)
						return count;
			}
		}
		return count;

	}
}

/**
	An Enumeration that returns the the Lockables
	in a group.
*/

class LockList implements Enumeration<Lockable> {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

	private Enumeration<Lock> lockGroup;

	LockList(Enumeration<Lock> lockGroup) {
		this.lockGroup = lockGroup;
	}

	public boolean hasMoreElements() {
		return lockGroup.hasMoreElements();
	}

	public Lockable nextElement() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		return (lockGroup.nextElement()).getLockable();
	}
}
