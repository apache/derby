/*

   Derby - Class org.apache.derby.impl.services.locks.LockSpace

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

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Limit;

import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;

/**

	A LockSpace represents the complete set of locks held within
	a single compatability space, broken into groups of locks.

    A LockSpace is a hashtable keyed by the group reference,
	the data for each key is a Hashtable of Lock's.

*/
class LockSpace extends Hashtable {

	private final Object compatSpace;
	// the object I live in
	private final Dictionary holder;

	private HashMap spareGroups[] = new HashMap[3];

	// the Limit info.
	private Object callbackGroup;
	private int    limit;
	private int    nextLimitCall;
	private Limit  callback;

	LockSpace(Dictionary holder, Object compatSpace) {
		super();
		this.compatSpace = compatSpace;
		this.holder = holder;
	}

	/**
		Add a lock to a group.
	*/
	protected synchronized void addLock(Object group, Lock lock)
		throws StandardException {

		Lock lockInGroup = null;

		HashMap dl = (HashMap) get(group);
		if (dl == null)	{
			dl = getGroupMap(group);
		} else if (lock.getCount() != 1) {
			lockInGroup = (Lock) dl.get(lock);
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
			callback.reached(compatSpace, group, limit,
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

	synchronized void unlockGroup(LockSet lset, Object group) {
		HashMap dl = (HashMap) remove(group);
		if (dl == null)
			return;

		for (Iterator list = dl.keySet().iterator(); list.hasNext(); ) {
			lset.unlock((Lock) list.next(), 0);
		}

		if ((callbackGroup == null) && isEmpty())
			holder.remove(compatSpace);
		else if (group.equals(callbackGroup))
			nextLimitCall = limit;

		saveGroup(dl);
	}

	private HashMap getGroupMap(Object group) {
		HashMap[] sg = spareGroups;
		HashMap dl = null;
		for (int i = 0; i < 3; i++) {
			dl = sg[i];
			if (dl != null) {
				sg[i] = null;
				break;
			}
		}

		if (dl == null)
			dl = new HashMap(5, 0.8f);

		put(group, dl);
		return dl;
	}
	private void saveGroup(HashMap dl) {
		HashMap[] sg = spareGroups;
		for (int i = 0; i < 3; i++) {
			if (sg[i] == null) {
				sg[i] = dl;
				dl.clear();
				break;
			}
		}
	}

	/**
		Unlock all locks in the group that match the key
	*/
	synchronized void unlockGroup(LockSet lset, Object group, Matchable key) {
		HashMap dl = (HashMap) get(group);
		if (dl == null)
			return; //  no group at all

		boolean allUnlocked = true;
		for (Iterator e = dl.keySet().iterator(); e.hasNext(); ) {

			Lock lock = (Lock) e.next();
			if (!key.match(lock.getLockable())) {
				allUnlocked = false;
				continue;
			}
			lset.unlock(lock, 0);
			e.remove();
		}

		if (allUnlocked) {
			remove(group);
			saveGroup(dl);
			if ((callbackGroup == null) && isEmpty())
				holder.remove(compatSpace);
			else if (group.equals(callbackGroup))
				nextLimitCall = limit;

		}
	}

	synchronized void transfer(Object oldGroup, Object newGroup) {
		HashMap from = (HashMap) get(oldGroup);
		if (from == null)
			return;

		HashMap to = (HashMap) get(newGroup);
		if (to == null) {
			// simple case 
			put(newGroup, from);
			clearLimit(oldGroup);
			remove(oldGroup);
			return;
		}

		if (to.size() < from.size()) {

			// place the contents of to into from
			mergeGroups(to, from);

			Object oldTo = put(newGroup, from);
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(oldTo == to, "inconsistent state in LockSpace");
			}

		} else {
			mergeGroups(from, to);
		}
		
		clearLimit(oldGroup);
		remove(oldGroup);
	}

	private void mergeGroups(HashMap from, HashMap into) {

		for (Iterator e = from.keySet().iterator(); e.hasNext(); ) {

			Object lock = e.next();

			Object lockI = into.get(lock);

			if (lockI == null) {
				// lock is only in from list
				into.put(lock, lock);
			} else {
				// merge the locks
				Lock fromL = (Lock) lock;
				Lock intoL = (Lock) lockI;

				intoL.count += fromL.getCount();
			}
		}

	}

	synchronized int unlockReference(LockSet lset, Lockable ref, Object qualifier, Object group) {

		// look for locks matching our reference and qualifier.
		HashMap dl = (HashMap) get(group);
		if (dl == null)
			return 0;

		Lock lockInGroup;
		synchronized (lset) {
			Control control = lset.getControl(ref);
			if (control == null)
				return 0;

			Lock setLock = control.getLock(compatSpace, qualifier);
			if (setLock == null)
				return 0;

			lockInGroup = (Lock) dl.remove(setLock);
			if (lockInGroup == null)
				return 0;
			setLock = null;

			lset.unlock(lockInGroup, 1);
		}

		if (lockInGroup.getCount() == 1) {

			if (dl.isEmpty()) {
				remove(group);
				saveGroup(dl);
				if ((callbackGroup == null) && isEmpty())
					holder.remove(compatSpace);
				else if (group.equals(callbackGroup))
					nextLimitCall = limit;

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
		return  (get(group) != null);
	}
	
	synchronized boolean isLockHeld(Object group, Lockable ref, Object qualifier) {

		// look for locks matching our reference and qualifier.
		HashMap dl = (HashMap) get(group);
		if (dl == null)
			return false;

		Object heldLock = dl.get(new Lock(compatSpace, ref, qualifier));
		return (heldLock != null);
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

			if (isEmpty())
				holder.remove(compatSpace);
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

		for (Enumeration groups = elements(); groups.hasMoreElements(); ) {
			HashMap group = (HashMap) groups.nextElement();
			for (Iterator locks = group.keySet().iterator(); locks.hasNext(); ) {
					Lock lock = (Lock) locks.next();
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

class LockList implements Enumeration {

	private Enumeration lockGroup;

	LockList(Enumeration lockGroup) {
		this.lockGroup = lockGroup;
	}

	public boolean hasMoreElements() {
		return lockGroup.hasMoreElements();
	}

	public Object nextElement() {
		return ((Lock) lockGroup.nextElement()).getLockable();
	}
}
