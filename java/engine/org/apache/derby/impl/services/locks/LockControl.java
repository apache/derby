/*

   Derby - Class org.apache.derby.impl.services.locks.LockControl

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
import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Dictionary;
import java.util.Stack;
import java.util.List;
import java.util.ListIterator;

/**
	A LockControl contains a reference to the item being locked
	and doubly linked lists for the granted locks and the waiting
	locks.

	<P>
	MT - Mutable - Container object : single thread required

*/

public class LockControl implements Control {

	private final Lockable		ref;

	/**
		This lock control uses an optimistic locking scheme.
		When the first lock on an object is granted it
		simply sets firstGrant to be that object, removing the
		need to allocate a list if no other locks are granted
		before the first lock is release. If a second lock
		is granted then a list is allocated and the
		firstGrant lock is moved into the list. Once a list
		has been created it is always used.
	*/
	private Lock				firstGrant;
	private List				granted;
	private List				waiting;
	private Lock				lastPossibleSkip;

	protected LockControl(Lock firstLock, Lockable ref) {
		super();
		this.ref = ref;

		// System.out.println("new lockcontrol");

		firstGrant = firstLock;
	}

	// make a copy by cloning the granted and waiting lists
	private LockControl(LockControl copyFrom)
	{
		super();

		this.ref = copyFrom.ref;
		this.firstGrant = copyFrom.firstGrant;

		if (copyFrom.granted != null)
			this.granted = new java.util.LinkedList(copyFrom.granted);

		if (copyFrom.waiting != null)
			this.waiting = new java.util.LinkedList(copyFrom.waiting);

		this.lastPossibleSkip = copyFrom.lastPossibleSkip;
	}
	
	public LockControl getLockControl() {
		return this;
	}

	/**
	*/
	public boolean isEmpty() {

		// if we are locked then we are not empty
		if (!isUnlocked())
			return false;

		return (waiting == null) || waiting.isEmpty();
	}

	/**
		Grant this lock.
	*/
	void grant(Lock lockItem) {

		lockItem.grant();

		List lgranted = granted;
		
		if (lgranted == null) {
			if (firstGrant == null) {
				// first ever lock on this item
				firstGrant = lockItem;
			} else {
				// second ever lock on this item
				lgranted = granted = new java.util.LinkedList();
				lgranted.add(firstGrant);
				lgranted.add(lockItem);
				firstGrant = null;
			}
		} else {
			// this grants the lock
			lgranted.add(lockItem);
		}
	}

	/**
	*/
	public boolean unlock(Latch lockInGroup, int unlockCount) {

		// note that lockInGroup is not the actual Lock object held in the lock set.

		if (unlockCount == 0)
			unlockCount = lockInGroup.getCount();

		List lgranted = granted;
			
		// start at the begining of the list when there is one
		for (int index = 0; unlockCount > 0; ) {

			Lock lockInSet;

			if (firstGrant != null) {
				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(lockInGroup.equals(firstGrant));
				}
				lockInSet = firstGrant;
			} else {
				// index = lgranted.indexOf(index, lgranted.size() - 1, lockInGroup);
				/*List*/ index = lgranted.indexOf(lockInGroup);
			
				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(index != -1);
				}
				lockInSet = (Lock) lgranted.get(index);
			}

			unlockCount -= lockInSet.unlock(unlockCount);

			if (lockInSet.getCount() != 0) {
				if (SanityManager.DEBUG) {
					if (unlockCount != 0)
						SanityManager.THROWASSERT("locked item didn't reduce unlock count to zero " + unlockCount);
				}
				continue;
			}

			if (firstGrant == lockInSet) {
				if (SanityManager.DEBUG) {
					if (unlockCount != 0)
						SanityManager.THROWASSERT("item is still locked! " + unlockCount);
				}
				firstGrant = null;
			}
			else {
				lgranted.remove(index);
			}
		}

		return true;
	}

	/**
        This routine can be called to see if a lock currently on the wait
        list could be granted. If this lock has waiters ahead of it
		then we do not jump over the waiter(s) even if we can be granted.
		 This avoids the first waiter being starved out.
	*/

    public boolean isGrantable(
    boolean noWaitersBeforeMe,
    Object  compatabilitySpace, 
    Object  qualifier)
    {
		if (isUnlocked())
			return true;

        boolean grantLock    = false;

		Lockable lref = ref;
		List lgranted = granted;

        {
            // Check to see if the only locks on the granted queue that
            // we are incompatible with are locks we own.
            boolean selfCompatible = lref.lockerAlwaysCompatible();

			int index = 0;
			int endIndex = firstGrant == null ? lgranted.size() : 0;
			do {

				Lock gl = firstGrant == null ? (Lock) lgranted.get(index) : firstGrant;

                boolean sameSpace = 
                    (gl.getCompatabilitySpace().equals(compatabilitySpace));

                if (sameSpace && selfCompatible) 
                {
                    // if it's one of our locks and we are always compatible 
                    // with our own locks then yes, we can be granted.
                    
                    grantLock = true;
                    continue;
                }
                else if (!lref.requestCompatible(qualifier, gl.getQualifier())) 
                {
                    // If we are not compatible with some already granted lock
                    // then we can't be granted, give up right away.
                    
                    grantLock = false;
                    break;
                }
                else
                {
                    // We are compatible with this lock, if it's our own or 
                    // there are no other waiters then we can be granted.
                
                    if (sameSpace || noWaitersBeforeMe) 
                    {
                        grantLock = true;
                    }
                }
            } while (++index < endIndex);
		}

        return(grantLock);
    }

	/**
		Add a lock into this control, granted it if possible.

		This can be entered in several states.

		</OL>
		<LI>The Lockable is locked (granted queue not empty), and there are no waiters (waiting queue is empty)
		<LI>The Lockable is locked and there are waiters
		<LI>The Lockable is locked and there are waiters and the first is potentially granted
		<LI>The Lockable is unlocked and there are waiters and the first is potentially granted. Logically the item is
		    still locked, it's just that the lock has just been released and the first waker has not woken up yet.
		</OL>
		This call is never entered when the object is unlocked and there are no waiters.

	
		1) The Lockable has just been unlocked, 
	*/

	public Lock addLock(LockSet ls, Object compatabilitySpace, Object qualifier) {

		if (SanityManager.DEBUG) {

			if (!(!isUnlocked() || (firstWaiter() != null)))
				SanityManager.THROWASSERT("entered in totally unlocked mode " + isUnlocked() + " " + (firstWaiter() != null));
		}

		// If there are other waiters for this lock then we
		// will only grant this lock if we already hold a lock.
		// This stops a lock being frozen out while compatible locks
		// jump past it.
		boolean grantLock = false;		
		boolean otherWaiters = (firstWaiter() != null);

		Lock lockItem = null;
		Lockable lref = ref;

		// If we haven't been able to grant the lock yet then see if we hold a 
        // lock already that we are compatible with and there are no granted 
        // incompatible locks. If the object appears unlocked (due to a just 
        // released lock, but the first waiter hasn't woken yet)
		// then we obviously don't hold a lock, so just join the wait queue.
		boolean spaceHasALock = false;
		boolean noGrantAtAll = false;
		if (!grantLock && !isUnlocked()) {

			boolean selfCompatible = lref.lockerAlwaysCompatible();
			
			int index = 0;
			int endIndex = firstGrant == null ? granted.size() : 0;
			do {

				Lock gl = firstGrant == null ? (Lock) granted.get(index) : firstGrant;


				boolean sameSpace = (gl.getCompatabilitySpace().equals(compatabilitySpace));

				// if it's one of our locks and we are always compatible with 
                // our own locks then yes, we can be granted.
				if (sameSpace && selfCompatible) {

					spaceHasALock = true;

					if (noGrantAtAll)
						break;

					if (qualifier == gl.getQualifier())
						lockItem = gl;

					grantLock = true;
					continue;
				}
				
				// If we are not compatible with some already granted lock
                // then we can't be granted, give up right away.
				if (!lref.requestCompatible(qualifier, gl.getQualifier())) {
					grantLock = false;
					lockItem = null;

					// we can't give up rightaway if spaceHasALock is false
					// because we need to ensure that canSkip is set correctly
					if (spaceHasALock)
						break;

					noGrantAtAll = true;
				}

				// We are compatible with this lock, if it's our own or there
                // are no other waiters then yes we can still be granted ...
				
				if (!noGrantAtAll && (sameSpace || !otherWaiters)) {
					grantLock = true;
				}
			} while (++index < endIndex);
		}

		if (lockItem != null) {
			if (SanityManager.DEBUG) {
				if (!grantLock) {
					SanityManager.THROWASSERT("lock is not granted !" + lockItem);
				}
			}

			// we already held a lock of this type, just bump the lock count
			lockItem.count++;
			return lockItem;
		}

		if (grantLock) {
			lockItem = new Lock(compatabilitySpace, lref, qualifier);
			grant(lockItem);
			return lockItem;
		}
		
		ActiveLock waitingLock = new ActiveLock(compatabilitySpace, lref, qualifier);

		// If the object is already locked by this compatability space
		// then this lock can be granted by skipping other waiters.
		if (spaceHasALock) {
			waitingLock.canSkip = true;
		}

		if (waiting == null)
			waiting = new java.util.LinkedList();

		// Add lock to the waiting list
		addWaiter(waiting, waitingLock, ls);

		if (waitingLock.canSkip) {
			lastPossibleSkip = waitingLock;
		}

		return waitingLock;
	}

	protected boolean isUnlocked() {

		// If firstGrant is set then this object is locked
		if (firstGrant != null)
			return false;

		List lgranted = granted;

		return (lgranted == null) || lgranted.isEmpty();
	}

	/**
		Return the first lock in the wait line, null if the
		line is empty.
	*/
	public ActiveLock firstWaiter() {
		if ((waiting == null) || waiting.isEmpty())
			return null;
		return (ActiveLock) waiting.get(0);
	}


	/**
		Get the next waiting lock (if any).
	*/
	ActiveLock getNextWaiter(ActiveLock item, boolean remove, LockSet ls) {

		ActiveLock nextWaitingLock = null;

		if (remove && (waiting.get(0) == item))
		{
			// item is at the head of the list and being removed,
			// always return the next waiter
			popFrontWaiter(waiting, ls);

			nextWaitingLock = firstWaiter();
		}
		else if ((lastPossibleSkip != null) && (lastPossibleSkip != item))
		{
			// there are potential locks that could be granted
			// and the last one is not the lock we just looked at.

			// need to find the first lock after the one passed
			// in that has the canSkip flag set.

			int itemIndex = waiting.indexOf(item);
			int removeIndex = remove ? itemIndex : -1;



			// skip the entry we just looked at.
			/*List*/
			// dli.advance();
			// for (; !dli.atEnd(); dli.advance()) {

			if (itemIndex != waiting.size() - 1) {

			for (ListIterator li = waiting.listIterator(itemIndex + 1); li.hasNext();) {
				//ActiveLock al = (ActiveLock) dli.get();
				ActiveLock al = (ActiveLock) li.next();

				if (al.canSkip) {
					nextWaitingLock = al;
					break;
				}
			}
			}

			if (remove) {
				removeWaiter(waiting, removeIndex, ls);
			}

		} else {
			if (remove) {
				int count = removeWaiter(waiting, item, ls);

				if (SanityManager.DEBUG) {
					if (count != 1)
					{
						SanityManager.THROWASSERT(
							"count = " + count + "item = " + item + 
							"waiting = " + waiting);
					}
				}
			}
		}

		if (remove && (item == lastPossibleSkip))
			lastPossibleSkip = null;

		if (nextWaitingLock != null) {
			if (!nextWaitingLock.setPotentiallyGranted())
				nextWaitingLock = null;
		}

		return nextWaitingLock;
	}

	/**
		Return the lockable object controlled by me.
	*/
	public Lockable getLockable() {
		return ref;
	}
	public Lock getFirstGrant() {
		return firstGrant;
	}
	public List getGranted() {
		return granted;
	}
	public List getWaiting() {
		return waiting;
	}

	/**
		Give up waiting up on a lock
	*/

	protected void giveUpWait(Object item, LockSet ls) {

		int count = removeWaiter(waiting, item, ls);
		if (item == lastPossibleSkip)
			lastPossibleSkip = null;	

		if (SanityManager.DEBUG) {
            if (count != 1)
            {
                SanityManager.THROWASSERT(
                    "count = " + count + "item = " + item + 
                    "waiting = " + waiting);
            }
        }
	}

	/*
	** Deadlock support.
	*/

	/**
		Add the waiters of this lock into this Dictionary object.
		<BR>
		Each waiting thread gets two entries in the hashtable
		<OL>
		<LI>key=compatibility space - value=ActiveLock
		<LI>key=ActiveLock - value={LockControl for first waiter|ActiveLock of previosue waiter}
		</OL>
	*/
	public void addWaiters(Dictionary waiters) {
		
		if ((waiting == null) || waiting.isEmpty())
			return;

		Object previous = this;
		for (ListIterator li = waiting.listIterator(); li.hasNext(); ) {

			ActiveLock waitingLock = ((ActiveLock) li.next());

			Object waiter = waitingLock.getCompatabilitySpace();

			waiters.put(waiter, waitingLock);
			waiters.put(waitingLock, previous);
			previous = waitingLock;
		}
	}

	/**
		Return a Stack of the
		held locks (Lock objects) on this Lockable.
	*/
	List getGrants() {

		List ret;

		if (firstGrant != null) {
			ret = new java.util.LinkedList();
			ret.add(firstGrant);
		}
		else
		{
			ret = new java.util.LinkedList(granted);
		}

		return ret;
	}

	/**
		Find a granted lock matching this space and qualifier
	*/
	public final Lock getLock(Object compatabilitySpace, Object qualifier) {

		if (isUnlocked())
			return null;

		List lgranted = granted;


		int index = 0;
		int endIndex = firstGrant == null ? lgranted.size() : 0;
		do {

			Lock gl = firstGrant == null ? (Lock) lgranted.get(index) : firstGrant;

            if (!gl.getCompatabilitySpace().equals(compatabilitySpace))
				continue;

			if (gl.getQualifier() == qualifier)
				return gl;

        } while (++index < endIndex);
		return null;
	}

//EXCLUDE-START-lockdiag- 
	/**
	 * make a shallow clone of myself
	 */
	/* package */
	public Control shallowClone()
	{
		return new LockControl(this);
	}
//EXCLUDE-END-lockdiag- 

	/**
	 * Add a lock request to a list of waiters.
	 *
	 * @param waiting	The list of waiters to add to
	 * @param lockItem	The lock request
	 * @param ls		The LockSet
	 */
	private void addWaiter(List waiting,
						Lock lockItem,
						LockSet ls) {

		// Add lock to the waiting list
		waiting.add(lockItem);

		// Maintain count of waiters
		ls.oneMoreWaiter();
	}

	/**
	 * Remove and return the first lock request from a list of waiters.
	 *
	 * @param waiting	The list of waiters to pop from
	 * @param ls		The LockSet
	 *
	 * @return	The removed lock request
	 */
	private Object popFrontWaiter(List waiting, LockSet ls) {
		// Maintain count of waiters
		ls.oneLessWaiter();

		// Remove and return the first lock request
		return waiting.remove(0);
	}


	/**
	 * Remove and return the lock request at the given index
	 * from a list of waiters.
	 *
	 * @param waiting	The list of waiters to pop from
	 * @param index		The index at which to remove the lock request
	 * @param ls		The LockSet
	 *
	 * @return	The removed lock request
	 */
	private Object removeWaiter(List waiting,
								int index,
								LockSet ls) {
		// Maintain count of waiters
		ls.oneLessWaiter();

		// Remove and return the first lock request
		return waiting.remove(index);
	}

	/**
	 * Remove and return the given lock request from a list of waiters.
	 *
	 * @param waiting	The list of waiters to pop from
	 * @param item		The item to remove
	 * @param ls		The LockSet
	 *
	 * @return	The number of items removed
	 */
	private int removeWaiter(List waiting,
								Object item,
								LockSet ls) {
		// Maintain count of waiters
		ls.oneLessWaiter();

		// Remove item and return number of items removed
		return waiting.remove(item) ? 1 : 0;
	}
}


