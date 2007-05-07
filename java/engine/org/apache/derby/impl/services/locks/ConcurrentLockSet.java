/*

   Derby - Class org.apache.derby.impl.services.locks.ConcurrentLockSet

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
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.C_LockFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.Map;


/**
    A ConcurrentLockSet is a complete lock table which maps
    <code>Lockable</code>s to <code>LockControl</code> objects.

	<P>
	A LockControl contains information about the locks held on a Lockable.

	<BR>
    MT - Mutable : All public methods of this class, except addWaiters, are
    thread safe. addWaiters can only be called from the thread which performs
    deadlock detection. Only one thread can perform deadlock detection at a
    time.

	<BR>
	The class creates ActiveLock and LockControl objects.
	
	LockControl objects are never passed out of this class, All the methods of 
    LockControl are called while holding a ReentrantLock associated with the
    Lockable controlled by the LockControl, thus providing the
    single threading that LockControl required.

	Methods of Lockables are only called by this class or LockControl, and 
    always while holding the corresponding ReentrantLock, thus providing the
    single threading that Lockable requires.
	
	@see LockControl
*/

final class ConcurrentLockSet implements LockTable {
	/*
	** Fields
	*/
	private final AbstractPool factory;

    /** Hash table which maps <code>Lockable</code> objects to
     * <code>Lock</code>s. */
    private final ConcurrentHashMap<Lockable, Entry> locks;

    /**
     * List containing all entries seen by the last call to
     * <code>addWaiters()</code>. Makes it possible for the deadlock detection
     * thread to lock all the entries it has visited until it has
     * finished. This prevents false deadlocks from being reported (because all
     * observed waiters must still be waiting when the deadlock detection has
     * completed).
     */
    private ArrayList<Entry> seenByDeadlockDetection;

	/**
		Timeout for deadlocks, in ms.
		<BR>
		MT - immutable
	*/
	private int deadlockTimeout = Property.DEADLOCK_TIMEOUT_DEFAULT * 1000;
	private int waitTimeout = Property.WAIT_TIMEOUT_DEFAULT * 1000;

//EXCLUDE-START-lockdiag- 

	// this varible is set and get without synchronization.  
	// Only one thread should be setting it at one time.
	private boolean deadlockTrace;

//EXCLUDE-END-lockdiag- 

	// The number of waiters for locks
	private final AtomicInteger blockCount;

	/*
	** Constructor
	*/

	ConcurrentLockSet(AbstractPool factory) {
		this.factory = factory;
        blockCount = new AtomicInteger();
		locks = new ConcurrentHashMap<Lockable, Entry>();
	}

    /**
     * Class representing an entry in the lock table.
     */
    private static final class Entry {
        /** The lock control. */
        Control control;
        /**
         * Mutex used to ensure single-threaded access to the LockControls. To
         * avoid Java deadlocks, no thread should ever hold the mutex of more
         * than one entry. Excepted from this requirement is a thread which
         * performs deadlock detection. During deadlock detection, a thread
         * might hold several mutexes, but it is not allowed to hold any mutex
         * when entering the deadlock detection. Only one thread is allowed to
         * perform deadlock detection at a time.
         */
        private final ReentrantLock mutex = new ReentrantLock();
        /**
         * Condition variable which prevents calls to <code>lock()</code> from
         * locking the entry. If it is not <code>null</code>, only the thread
         * performing deadlock detection may lock the entry (by calling
         * <code>lockForDeadlockDetection()</code>).
         */
        private Condition deadlockDetection;

        /**
         * Lock the entry, ensuring exclusive access to the contained
         * <code>Control</code> object. The call will block until the entry can
         * be locked. If the entry is unlocked and
         * <code>deadlockDetection</code> is not <code>null</code>, the entry
         * belongs to a thread which waits for deadlock detection to be
         * initiated, and the call will block until that thread has finished
         * its deadlock detection.
         */
        void lock() {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!mutex.isHeldByCurrentThread());
            }
            mutex.lock();
            while (deadlockDetection != null) {
                deadlockDetection.awaitUninterruptibly();
            }
        }

        /**
         * Unlock the entry, allowing other threads to lock and access the
         * contained <code>Control</code> object.
         */
        void unlock() {
            mutex.unlock();
        }

        /**
         * Lock the entry while performing deadlock detection. This method will
         * lock the entry even when <code>deadlockDetection</code> is not
         * <code>null</code>. If <code>deadlockDetection</code> is not
         * <code>null</code>, we know the entry and its <code>Control</code>
         * will not be accessed by others until we have finished the deadlock
         * detection, so it's OK for us to access it.
         *
         */
        void lockForDeadlockDetection() {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!mutex.isHeldByCurrentThread());
            }
            mutex.lock();
        }

        /**
         * Notify that the lock request that is currently accessing the entry
         * will be entering deadlock detection. Unlock the entry to allow the
         * current thread or other threads to lock the entry for deadlock
         * detection, but set the condition variable to prevent regular locking
         * of the entry.
         */
        void enterDeadlockDetection() {
            deadlockDetection = mutex.newCondition();
            mutex.unlock();
        }

        /**
         * Notify that the deadlock detection triggered by the current thread
         * has finished. Re-lock the entry and notify any waiters that the
         * deadlock detection has completed.
         */
        void exitDeadlockDetection() {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!mutex.isHeldByCurrentThread());
            }
            mutex.lock();
            deadlockDetection.signalAll();
            deadlockDetection = null;
        }
    }

    /**
     * Get an entry from the lock table. If no entry exists for the
     * <code>Lockable</code>, insert an entry. The returned entry will be
     * locked and is guaranteed to still be present in the table.
     *
     * @param ref the <code>Lockable</code> whose entry to return
     * @return the entry for the <code>Lockable</code>, locked for exclusive
     * access
     */
    private Entry getEntry(Lockable ref) {
        Entry e = locks.get(ref);
        while (true) {
            if (e != null) {
                e.lock();
                if (e.control != null) {
                    // entry is found and in use, return it
                    return e;
                }
                // entry is empty, hence it was removed from the table after we
                // retrieved it. Try to reuse it later.
            } else {
                // no entry found, create a new one
                e = new Entry();
                e.lock();
            }
            // reinsert empty entry, or insert the new entry
            Entry current = locks.putIfAbsent(ref, e);
            if (current == null) {
                // successfully (re-)inserted entry, return it
                return e;
            }
            // someone beat us, unlock the old entry and retry with the entry
            // they inserted
            e.unlock();
            e = current;
        }
    }

    /**
     * Check whether there is a deadlock. Make sure that only one thread enters
     * deadlock detection at a time.
     *
     * @param entry the entry in the lock table for the lock request that
     * triggered deadlock detection
     * @param waitingLock the waiting lock
     * @param wakeupReason the reason for waking up the waiter
     * @return an object describing the deadlock
     */
    private Object[] checkDeadlock(Entry entry, ActiveLock waitingLock,
                                   byte wakeupReason) {
        LockControl control = (LockControl) entry.control;
        // make sure that the entry is not blocking other threads performing
        // deadlock detection since we have to wait for them to finish
        entry.enterDeadlockDetection();
        synchronized (Deadlock.class) {
            try {
                return Deadlock.look(factory, this, control, waitingLock,
                                     wakeupReason);
            } finally {
                // unlock all entries we visited
                for (Entry e : seenByDeadlockDetection) {
                    e.unlock();
                }
                seenByDeadlockDetection = null;
                // re-lock the entry
                entry.exitDeadlockDetection();
            }
        }
    }

	/*
	** Public Methods
	*/

	/**
	 *	Lock an object within a specific compatibility space.
	 *
	 *	@param	compatibilitySpace Compatibility space.
	 *	@param	ref Lockable reference.
	 *	@param	qualifier Qualifier.
	 *	@param	timeout Timeout in milli-seconds
	 *
	 *	@return	Object that represents the lock.
	 *
	 *	@exception	StandardException Standard Derby policy.

	*/
	public Lock lockObject(CompatibilitySpace compatibilitySpace, Lockable ref,
						   Object qualifier, int timeout)
		throws StandardException
	{		
		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (locks.size() > 1000)
					System.out.println("memoryLeakTrace:LockSet: " +
                                           locks.size());
			}
		}

		LockControl control;
		Lock lockItem;
        String  lockDebug = null;

        Entry entry = getEntry(ref);
        try {

            Control gc = entry.control;

			if (gc == null) {

				// object is not locked, can be granted
				Lock gl = new Lock(compatibilitySpace, ref, qualifier);

				gl.grant();

				entry.control = gl;

				return gl;
			}

			control = gc.getLockControl();
			if (control != gc) {
				entry.control = control;
			}

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(ref.equals(control.getLockable()));
				// ASSERT item is in the list
                SanityManager.ASSERT(
                    locks.get(control.getLockable()).control == control);
			}

			lockItem = control.addLock(this, compatibilitySpace, qualifier);

			if (lockItem.getCount() != 0) {
				return lockItem;
			}

			if (timeout == C_LockFactory.NO_WAIT) {

    			// remove all trace of lock
    			control.giveUpWait(lockItem, this);

                if (SanityManager.DEBUG) 
                {
                    if (SanityManager.DEBUG_ON("DeadlockTrace"))
                    {

                        SanityManager.showTrace(new Throwable());

                        // The following dumps the lock table as it 
                        // exists at the time a timeout is about to 
                        // cause a deadlock exception to be thrown.

                        lockDebug = 
                            DiagnosticUtil.toDiagString(lockItem)   +
                            "\nCould not grant lock with zero timeout, " +
                            "here's the table";

                        // We cannot hold a lock on an entry while calling
                        // toDebugString() since it will lock other entries in
                        // the lock table. Holding the lock could cause a
                        // deadlock.
                        entry.unlock();
                        try {
                            lockDebug += toDebugString();
                        } finally {
                            // Re-lock the entry so that the outer finally
                            // clause doesn't fail.
                            entry.lock();
                        }
                    }
                }

				return null;
			}

        } finally {
            entry.unlock();
        }

		boolean deadlockWait = false;
		int actualTimeout;

		if (timeout == C_LockFactory.WAIT_FOREVER)
		{
			// always check for deadlocks as there should not be any
			deadlockWait = true;
			if ((actualTimeout = deadlockTimeout) == C_LockFactory.WAIT_FOREVER)
				actualTimeout = Property.DEADLOCK_TIMEOUT_DEFAULT * 1000;
		}
		else
		{

			if (timeout == C_LockFactory.TIMED_WAIT)
				timeout = actualTimeout = waitTimeout;
			else
				actualTimeout = timeout;


			// five posible cases
			// i)   timeout -1, deadlock -1         -> 
            //          just wait forever, no deadlock check
			// ii)  timeout >= 0, deadlock -1       -> 
            //          just wait for timeout, no deadlock check
			// iii) timeout -1, deadlock >= 0       -> 
            //          wait for deadlock, then deadlock check, 
            //          then infinite timeout
			// iv)  timeout >=0, deadlock < timeout -> 
            //          wait for deadlock, then deadlock check, 
            //          then wait for (timeout - deadlock)
			// v)   timeout >=0, deadlock >= timeout -> 
            //          just wait for timeout, no deadlock check


			if (deadlockTimeout >= 0) {

				if (actualTimeout < 0) {
					// infinite wait but perform a deadlock check first
					deadlockWait = true;
					actualTimeout = deadlockTimeout;
				} else if (deadlockTimeout < actualTimeout) {

					// deadlock wait followed by a timeout wait

					deadlockWait = true;
					actualTimeout = deadlockTimeout;

					// leave timeout as the remaining time
					timeout -= deadlockTimeout;
				}
			}
		}


        ActiveLock waitingLock = (ActiveLock) lockItem;
        lockItem = null;

        int earlyWakeupCount = 0;
        long startWaitTime = 0;

forever:	for (;;) {

                byte wakeupReason = waitingLock.waitForGrant(actualTimeout);
                
                ActiveLock nextWaitingLock = null;
                Object[] deadlockData = null;

                try {
                    boolean willQuitWait;
                    Enumeration timeoutLockTable = null;
                    long currentTime = 0;
        
                    entry.lock();
                    try {

                        if (control.isGrantable(
                                control.firstWaiter() == waitingLock,
                                compatibilitySpace,
                                qualifier)) {

                            // Yes, we are granted, put us on the granted queue.
                            control.grant(waitingLock);

                            // Remove from the waiting queue & get next waiter
                            nextWaitingLock = 
                                control.getNextWaiter(waitingLock, true, this);

                            return waitingLock;
                        }

                        // try again later
                        waitingLock.clearPotentiallyGranted(); 

                        willQuitWait = 
                            (wakeupReason != Constants.WAITING_LOCK_GRANT);

                        if (((wakeupReason == Constants.WAITING_LOCK_IN_WAIT) &&
                                    deadlockWait) ||
                            (wakeupReason == Constants.WAITING_LOCK_DEADLOCK))
                        {

                            // check for a deadlock, even if we were woken up 
                            // because we were selected as a victim we still 
                            // check because the situation may have changed.
                            deadlockData = 
                                checkDeadlock(entry, waitingLock, wakeupReason);

                            if (deadlockData == null) {
                                // we don't have a deadlock
                                deadlockWait = false;

                                actualTimeout = timeout;
                                startWaitTime = 0;
                                willQuitWait = false;
                            } else {
                                willQuitWait = true;
                            }
                        }

                        nextWaitingLock = 
                            control.getNextWaiter(
                                waitingLock, willQuitWait, this);


                        // If we were not woken by another then we have
                        // timed out. Either deadlock out or timeout
                        if (SanityManager.DEBUG &&
                                SanityManager.DEBUG_ON("DeadlockTrace") &&
                                willQuitWait) {
                            // Generate the first part of the debug message
                            // while holding the lock on entry, so that we have
                            // exclusive access to waitingLock. Wait until the
                            // entry has been unlocked before appending the
                            // contents of the lock table (to avoid deadlocks).
                            lockDebug =
                                DiagnosticUtil.toDiagString(waitingLock) +
                                "\nGot deadlock/timeout, here's the table";
                        }

                    } finally {
                        entry.unlock();
                    }

                    // need to do this outside of the synchronized block as the
                    // message text building (timeouts and deadlocks) may 
                    // involve getting locks to look up table names from 
                    // identifiers.

                    if (willQuitWait)
                    {
                        if (deadlockTrace && (deadlockData == null)) {
                            // if ending lock request due to lock timeout
                            // want a copy of the LockTable and the time,
                            // in case of deadlock deadlockData has the
                            // info we need.
                            currentTime = System.currentTimeMillis();
                            timeoutLockTable =
                                factory.makeVirtualLockTable();
                        }

                        if (SanityManager.DEBUG)
                        {
                            if (SanityManager.DEBUG_ON("DeadlockTrace")) {
                                SanityManager.showTrace(new Throwable());

                                // The following dumps the lock table as it
                                // exists at the time a timeout is about to
                                // cause a deadlock exception to be thrown.

                                lockDebug += toDebugString();
                            }

                            if (lockDebug != null)
                            {
                                String type = 
                                    ((deadlockData != null) ? 
                                         "deadlock:" : "timeout:"); 

                                SanityManager.DEBUG_PRINT(
                                    type,
                                    "wait on lockitem caused " + type + 
                                    lockDebug);
                            }

                        }

                        if (deadlockData == null)
                        {
                            // ending wait because of lock timeout.

                            if (deadlockTrace)
                            {   
                                // Turn ON derby.locks.deadlockTrace to build 
                                // the lockTable.
                                    
                                
                                throw Timeout.buildException(
                                    waitingLock, timeoutLockTable, currentTime);
                            }
                            else
                            {
                                StandardException se = 
                                    StandardException.newException(
                                        SQLState.LOCK_TIMEOUT);

                                throw se;
                            }
                        }
                        else 
                        {
                            // ending wait because of lock deadlock.

                            throw Deadlock.buildException(
                                    factory, deadlockData);
                        }
                    }
                } finally {
                    if (nextWaitingLock != null) {
                        nextWaitingLock.wakeUp(Constants.WAITING_LOCK_GRANT);
                        nextWaitingLock = null;
                    }
                }

                if (actualTimeout != C_LockFactory.WAIT_FOREVER) {

                    if (wakeupReason != Constants.WAITING_LOCK_IN_WAIT)
                        earlyWakeupCount++;

                    if (earlyWakeupCount > 5) {

                        long now = System.currentTimeMillis();

                        if (startWaitTime != 0) {

                            long sleepTime = now - startWaitTime;

                            actualTimeout -= sleepTime;
                        }

                        startWaitTime = now;
                    }
                }


            } // for(;;)
	}

	/**
		Unlock an object, previously locked by lockObject(). 

		If unlockCOunt is not zero then the lock will be unlocked
		that many times, otherwise the unlock count is taken from
		item.

	*/
	public void unlock(Latch item, int unlockCount) {
        // assume LockEntry is there
        Entry entry = locks.get(item.getLockable());
        entry.lock();
        try {
            unlock(entry, item, unlockCount);
        } finally {
            entry.unlock();
        }
    }

    /**
     * Unlock an object, previously locked by lockObject().
     *
     * @param entry the entry in which the lock is contained (the current
     * thread must have locked the entry)
     * @param item the item to unlock
     * @param unlockCount the number of times to unlock the item (if zero, take
     * the unlock count from item)
     */
    private void unlock(Entry entry, Latch item, int unlockCount) {
		if (SanityManager.DEBUG) {
            SanityManager.ASSERT(entry.mutex.isHeldByCurrentThread());
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				/*
				** I don't like checking the trace flag twice, but SanityManager
				** doesn't provide a way to get to the debug trace stream
				** directly.
				*/
				SanityManager.DEBUG(
                    Constants.LOCK_TRACE, 
                    "Release lock: " + DiagnosticUtil.toDiagString(item));
			}
		}

		boolean tryGrant = false;
		ActiveLock nextGrant = null;

        Control control = entry.control;
			
			if (SanityManager.DEBUG) {

                // only valid Lock's expected
                if (item.getLockable() == null)
                {
                    SanityManager.THROWASSERT(
                        "item.getLockable() = null." +
                        "unlockCount " + unlockCount + 
                        "item = " + DiagnosticUtil.toDiagString(item));
                }

                // only valid Lock's expected
                if (control == null)
                {
                    SanityManager.THROWASSERT(
                        "control = null." +
                        "unlockCount " + unlockCount + 
                        "item = " + DiagnosticUtil.toDiagString(item));
                }

                SanityManager.ASSERT(
                    locks.get(control.getLockable()).control == control);

				if ((unlockCount != 0) && (unlockCount > item.getCount()))
					SanityManager.THROWASSERT("unlockCount " + unlockCount +
						" larger than actual lock count " + item.getCount() + " item " + item);
			}

			tryGrant = control.unlock(item, unlockCount);
			item = null;

			boolean mayBeEmpty = true;
			if (tryGrant) {
				nextGrant = control.firstWaiter();
				if (nextGrant != null) {
					mayBeEmpty = false;
					if (!nextGrant.setPotentiallyGranted())
						nextGrant = null;
				}
			}

			if (mayBeEmpty) {
				if (control.isEmpty()) {
					// no-one granted, no-one waiting, remove lock control
					locks.remove(control.getLockable());
                    entry.control = null;
				}
				return;
			}

		if (tryGrant && (nextGrant != null)) {
			nextGrant.wakeUp(Constants.WAITING_LOCK_GRANT);
		}
	}

    /**
     * Unlock an object once if it is present in the specified group. Also
     * remove the object from the group.
     *
     * @param space the compatibility space
     * @param ref a reference to the locked object
     * @param qualifier qualifier of the lock
     * @param group a map representing the locks in a group
     * @return the corresponding lock in the group map, or <code>null</code> if
     * the object was not unlocked
     */
    public Lock unlockReference(CompatibilitySpace space, Lockable ref,
                                Object qualifier, Map group) {

        Entry entry = locks.get(ref);
        if (entry == null) {
            return null;
        }

        entry.lock();
        try {
            Control control = entry.control;
            if (control == null) {
                return null;
            }

            Lock setLock = control.getLock(space, qualifier);
            if (setLock == null) {
                return null;
            }

            Lock lockInGroup = (Lock) group.remove(setLock);
            if (lockInGroup != null) {
                unlock(entry, lockInGroup, 1);
            }

            return lockInGroup;

        } finally {
            entry.unlock();
        }
    }

    /**
     * Lock an object and release the lock immediately. Equivalent to
     * <pre>
     * Lock lock = lockTable.lockObject(space, ref, qualifier, timeout);
     * lockTable.unlock(lock, 1);
     * </pre>
     * except that the implementation is more efficient.
     *
     * @param space the compatibility space
     * @param ref a reference to the locked object
     * @param qualifier qualifier of the lock
     * @param timeout maximum time to wait in milliseconds
     * (<code>LockFactory.NO_WAIT</code> means don't wait)
     * @return <code>true</code> if the object was locked, or
     * <code>false</code>if the timeout was <code>NO_WAIT</code> and the lock
     * couldn't be obtained immediately
     * @exception StandardException if the lock could not be obtained
     */
    public boolean zeroDurationLockObject(
        CompatibilitySpace space, Lockable ref, Object qualifier, int timeout)
            throws StandardException {

        if (SanityManager.DEBUG) {
            if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
                D_LockControl.debugLock(
                    "Zero Duration Lock Request before Grant: ",
                    space, null, ref, qualifier, timeout);
                if (SanityManager.DEBUG_ON(Constants.LOCK_STACK_TRACE)) {
                    // The following will print the stack trace of the lock
                    // request to the log.
                    Throwable t = new Throwable();
                    java.io.PrintWriter istream =
                        SanityManager.GET_DEBUG_STREAM();
                    istream.println("Stack trace of lock request:");
                    t.printStackTrace(istream);
                }
            }
        }

        // Very fast zeroDurationLockObject() for unlocked objects.
        // If no entry exists in the lock manager for this reference
        // then it must be unlocked.
        // If the object is locked then we perform a grantable
        // check, skipping over any waiters.
        // If the caller wants to wait and the lock cannot
        // be granted then we do the slow join the queue and
        // release the lock method.

        Entry entry = locks.get(ref);
        if (entry == null) {
            return true;
        }

        entry.lock();
        try {
            Control control = entry.control;
            if (control == null) {
                return true;
            }

            // If we are grantable, ignoring waiting locks then
            // we can also grant this request now, as skipping
            // over the waiters won't block them as we release
            // the lock rightway.
            if (control.isGrantable(true, space, qualifier)) {
                return true;
            }

            // can't be granted and are not willing to wait.
            if (timeout == C_LockFactory.NO_WAIT) {
                return false;
            }
        } finally {
            entry.unlock();
        }

        Lock lock = lockObject(space, ref, qualifier, timeout);

        if (SanityManager.DEBUG) {
            if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
                D_LockControl.debugLock(
                    "Zero Lock Request Granted: ",
                    space, null, ref, qualifier, timeout);
            }
        }

        // and simply unlock it once
        unlock(lock, 1);

        return true;
    }

    /**
     * Set the deadlock timeout.
     *
     * @param timeout deadlock timeout in milliseconds
     */
    public void setDeadlockTimeout(int timeout) {
        deadlockTimeout = timeout;
    }

    /**
     * Set the wait timeout.
     *
     * @param timeout wait timeout in milliseconds
     */
    public void setWaitTimeout(int timeout) {
        waitTimeout = timeout;
    }
	
	/*
	** Non public methods
	*/
//EXCLUDE-START-lockdiag- 

	public void setDeadlockTrace(boolean val)
	{
		// set this without synchronization
		deadlockTrace = val;
	}			
//EXCLUDE-END-lockdiag- 

    private String toDebugString()
    {
        if (SanityManager.DEBUG)
        {
            String str = new String();

            int i = 0;
            for (Entry entry : locks.values())
            {
                entry.lock();
                try {
                    str += "\n  lock[" + i + "]: " +
                        DiagnosticUtil.toDiagString(entry.control);
                } finally {
                    entry.unlock();
                }
            }

            return(str);
        }
        else
        {
            return(null);
        }
    }

    /**
     * Add all waiters in this lock table to a <code>Map</code> object.
     * This method can only be called by the thread that is currently
     * performing deadlock detection. All entries that are visited in the lock
     * table will be locked when this method returns. The entries that have
     * been seen and locked will be unlocked after the deadlock detection has
     * finished.
     */
    public void addWaiters(Map waiters) {
        seenByDeadlockDetection = new ArrayList<Entry>(locks.size());
        for (Entry entry : locks.values()) {
            seenByDeadlockDetection.add(entry);
            entry.lockForDeadlockDetection();
            if (entry.control != null) {
                entry.control.addWaiters(waiters);
            }
        }
    }

//EXCLUDE-START-lockdiag- 
	/**
	 * make a shallow clone of myself and my lock controls
	 */
    public Map<Lockable, Control> shallowClone() {
        HashMap<Lockable, Control> clone = new HashMap<Lockable, Control>();

        for (Entry entry : locks.values()) {
            entry.lock();
            try {
                Control control = entry.control;
                if (control != null) {
                    clone.put(control.getLockable(), control.shallowClone());
                }
            } finally {
                entry.unlock();
            }
		}

		return clone;
	}
//EXCLUDE-END-lockdiag- 

	/**
	 * Increase blockCount by one.
	 */
	public void oneMoreWaiter() {
        blockCount.incrementAndGet();
	}

	/**
	 * Decrease blockCount by one.
	 */
	public void oneLessWaiter() {
		blockCount.decrementAndGet();
	}

    /**
     * Check whether anyone is blocked.
     * @return <code>true</code> if someone is blocked, <code>false</code>
     * otherwise
     */
	public boolean anyoneBlocked() {
        int blocked = blockCount.get();
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(
				blocked >= 0, "blockCount should not be negative");
		}
		return blocked != 0;
	}
}
