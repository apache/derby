/*

   Derby - Class org.apache.derby.impl.services.locks.LockSet

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

import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import java.util.Hashtable;
import java.util.Enumeration;


/**
	A LockSet is a complete lock table.	A lock table is a hash table
	keyed by a Lockable and with a LockControl as the data element.

	<P>
	A LockControl contains information about the locks held on a Lockable.

	<BR>
	MT - Mutable - Container Object : Thread Safe

	<BR>
	The Hashtable we extend is synchronized on this, all addition, searching of
    the hashtable is performed using java synchroization(this).
	<BR>
	The class creates ActiveLock and LockControl objects.
	
	LockControl objects are never passed out of this class, All the methods of 
    LockControl are called while being synchronized on this, thus providing the
    single threading that LockControl required.

	Methods of Lockables are only called by this class or LockControl, and 
    always while being synchronized on this, thus providing the single 
    threading that Lockable requires.
	
	@see LockControl
*/

public final class LockSet extends Hashtable
{
	/*
	** Fields
	*/
	private final SinglePool factory;

	/**
		Timeout for deadlocks, in ms.
		<BR>
		MT - immutable
	*/
	protected int deadlockTimeout = Property.DEADLOCK_TIMEOUT_DEFAULT * 1000;
	protected int waitTimeout = Property.WAIT_TIMEOUT_DEFAULT * 1000;

//EXCLUDE-START-lockdiag- 

	// this varible is set and get without synchronization.  
	// Only one thread should be setting it at one time.
	private boolean deadlockTrace;

	private Hashtable lockTraces; // rather than burden each lock with
								  // its stack trace, keep a look aside table
								  // that maps a lock to a stack trace
//EXCLUDE-END-lockdiag- 

	// The number of waiters for locks
	protected int	blockCount;

	/*
	** Constructor
	*/

	protected LockSet(SinglePool factory) {
		super();
		this.factory = factory;
	}


	/*
	** Public Methods
	*/

	/**
	 *	Lock an object within a specific compatibility space.
	 *
	 *	@param	compatabilitySpace Compatibility space.
	 *	@param	ref Lockable reference.
	 *	@param	qualifier Qualifier.
	 *	@param	timeout Timeout in milli-seconds
	 *
	 *	@return	Object that represents the lock.
	 *
	 *	@exception	StandardException Standard Cloudscape policy.

	*/
	public Lock lockObject(Object compatabilitySpace, Lockable ref, Object qualifier, int timeout, Latch latch)
		throws StandardException
	{		
		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (size() > 1000)
					System.out.println("memoryLeakTrace:LockSet: " + size());
			}
		}

		Control gc;
		LockControl control;
		Lock lockItem;
        String  lockDebug = null;

		synchronized (this) {

			gc = getControl(ref);

			if (gc == null) {

				// object is not locked, can be granted
				Lock gl = new Lock(compatabilitySpace, ref, qualifier);

				gl.grant();

				put(ref, gl);

				return gl;
			}

			control = gc.getLockControl();
			if (control != gc) {
				put(ref, control);
			}
			

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(ref.equals(control.getLockable()));

				// ASSERT item is in the list
                if (getControl(control.getLockable()) != control)
                {
					SanityManager.THROWASSERT(
                        "lockObject mismatched lock items " + 
                        getControl(control.getLockable()) + " " + control);
                }
			}

			lockItem = control.addLock(this, compatabilitySpace, qualifier);

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
                            "\nCould not grant lock with zero timeout, here's the table" +
                            this.toDebugString();
                    }
                }

				return null;
			}

			// this is where we need to release the latch
			if (latch != null)
				unlock(latch, 1);


		} // synchronized block

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
			// i)   timeout -1, deadlock -1 -> just wait forever, no deadlock check
			// ii)  timeout >= 0, deadlock -1 -> just wait for timeout, no deadlock check
			// iii) timeout -1, deadlock >= 0 -> wait for deadlock, then deadlock check, then infinite timeout
			// iv)  timeout >=0, deadlock < timeout -> wait for deadlock, then deadlock check, then wait for (timeout - deadlock)
			// v)   timeout >=0, deadlock >= timeout -> just wait for timeout, no deadlock check


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

			if (deadlockTrace)
			{
				// we want to keep a stack trace of this thread just before it goes
				// into wait state, no need to synchronized because Hashtable.put
				// is synchronized and the new throwable is local to this thread.
				lockTraces.put(waitingLock, new Throwable());
			}

			int earlyWakeupCount = 0;
			long startWaitTime = 0;

		try {
forever:	for (;;) {


			byte wakeupReason = waitingLock.waitForGrant(actualTimeout);
			
			ActiveLock nextWaitingLock = null;
			Object[] deadlockData = null;

			try {
				boolean willQuitWait;
                Enumeration timeoutLockTable = null;
                long currentTime = 0;
	
				synchronized (this) {

					if (control.isGrantable(control.firstWaiter() == waitingLock,
							compatabilitySpace, qualifier)) {

						// Yes, we are granted, put us on the granted queue.
						control.grant(waitingLock);

						// Remove from the waiting queue & get next waiter
						nextWaitingLock = control.getNextWaiter(waitingLock, true, this);

						// this is where we need to re-obtain the latch, it's 
						// safe to call this lockObject() which will get the 
						// synchronization we already hold, because java allows
						// nested synchronization and it will be released 
						// automatically if we have to wait
						if (latch != null) {
							lockObject(
								compatabilitySpace, latch.getLockable(), 
								latch.getQualifier(), C_LockFactory.WAIT_FOREVER,
								(Latch) null);
						}
						return waitingLock;
					}

					waitingLock.clearPotentiallyGranted(); // try again later

					willQuitWait = (wakeupReason != Constants.WAITING_LOCK_GRANT);

					StandardException deadlockException = null;

					if (((wakeupReason == Constants.WAITING_LOCK_IN_WAIT) && deadlockWait) ||
						(wakeupReason == Constants.WAITING_LOCK_DEADLOCK))
					{

						// check for a deadlock, even if we were woken up to because
						// we were selected as a victim we still check because the situation
						// may have changed.
						deadlockData = Deadlock.look(factory, this, control, waitingLock, wakeupReason);
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

					nextWaitingLock = control.getNextWaiter(waitingLock, willQuitWait, this);


					// If we were not woken by another then we have
					// timed out. Either deadlock out or timeout
					if (willQuitWait) {

						// Even if we deadlocked trying to get the lock, still 
                        // reget the latch so that client's need not know 
                        // latch was released.

						if (latch != null) {
							lockObject(
								compatabilitySpace, latch.getLockable(), 
								latch.getQualifier(), C_LockFactory.WAIT_FOREVER,
								(Latch) null);
						}

                        if (SanityManager.DEBUG) 
                        {
                            if (SanityManager.DEBUG_ON("DeadlockTrace"))
                            {

                                SanityManager.showTrace(new Throwable());

                                // The following dumps the lock table as it 
                                // exists at the time a timeout is about to 
                                // cause a deadlock exception to be thrown.

                                lockDebug = 
                                    DiagnosticUtil.toDiagString(waitingLock)   +
                                    "\nGot deadlock/timeout, here's the table" +
                                    this.toDebugString();
                            }
                        }
                        
                        if(!deadlockWait)
                        {
                            if( deadlockTrace )
                            {   // want a copy of the LockTable and the time
                                currentTime = System.currentTimeMillis(); 
                                timeoutLockTable = factory.makeVirtualLockTable();
                            }
                        }
					}

				} // synchronized block

				// need to do this outside of the synchronized block as the
                // message text building (timeouts and deadlocks) may involve
                // getting locks to look up table names from identifiers.
                if (willQuitWait)
                {
                    if (SanityManager.DEBUG)
                    {
                        if (lockDebug != null)
                        {
                            String type = 
                                (deadlockWait ? "deadlock:" : "timeout:"); 

                            SanityManager.DEBUG_PRINT(
                                type,
                                "wait on lockitem caused " + type + lockDebug);
                        }

                    }

                    if(!deadlockWait)
                    {
                        if( deadlockTrace )
                        {   //Turn ON derby.locks.deadlockTrace to build the lockTable.
                            throw Timeout.buildException(waitingLock, timeoutLockTable, currentTime);
                        }
                        else
                        {
			                StandardException se = 
                                StandardException.newException(
                                    SQLState.LOCK_TIMEOUT);

			                throw se;
                        }
                    }
					if (deadlockData != null) {
						throw Deadlock.buildException(factory, deadlockData);
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
	} finally {
		if (deadlockTrace)
		{
				// I am out of the wait state, either I got my lock or I am the
				// one who is going to detect the deadlock, don't need the
				// stack trace anymore.
				lockTraces.remove(waitingLock);
		}
	}
	}

	/**
		Unlock an object, previously locked by lockObject(). 

		If unlockCOunt is not zero then the lock will be unlocked
		that many times, otherwise the unlock count is taken from
		item.

	*/
	void unlock(Latch item, int unlockCount) {

		if (SanityManager.DEBUG) {
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

		synchronized (this) {

			Control control = getControl(item.getLockable());
			
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

				if (getControl(control.getLockable()) != control)
                {
                    SanityManager.THROWASSERT(
                        "unlock mismatched lock items " + 
                        getControl(control.getLockable()) + " " + control);
                }

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
					remove(control.getLockable());
				}
				return;
			}
		} // synchronized (this)

		if (tryGrant && (nextGrant != null)) {
			nextGrant.wakeUp(Constants.WAITING_LOCK_GRANT);
		}
	}
	
	/*
	** Non public methods
	*/
//EXCLUDE-START-lockdiag- 

	void setDeadlockTrace(boolean val)
	{
		// set this without synchronization
		deadlockTrace = val;

		if (val && lockTraces == null)
			lockTraces = new Hashtable();
		else if (!val && lockTraces != null)
		{
			lockTraces = null;
		}
	}			
//EXCLUDE-END-lockdiag- 

    public String toDebugString()
    {
        if (SanityManager.DEBUG)
        {
            String str = new String();

            int i = 0;
            for (Enumeration e = this.elements(); 
                 e.hasMoreElements();
                 i++)
            {
                str += "\n  lock[" + i + "]: " + 
                    DiagnosticUtil.toDiagString(e.nextElement());
            }

            return(str);
        }
        else
        {
            return(null);
        }
    }

//EXCLUDE-START-lockdiag- 
	/*
	 * make a shallow clone of myself and my lock controls
	 */
	/* package */
	synchronized LockSet shallowClone()
	{
		LockSet clone = new LockSet(factory);

		for (Enumeration e = keys(); e.hasMoreElements(); )
		{
			Lockable lockable = (Lockable)e.nextElement();
			Control control = getControl(lockable);

			clone.put(lockable, control.shallowClone());
		}

		return clone;
	}
//EXCLUDE-END-lockdiag- 

	/*
	** Support for anyoneBlocked(). These methods assume that caller
	** is synchronized on this LockSet object.
	*/
	void oneMoreWaiter() {
		blockCount++;
	}

	void oneLessWaiter() {
		blockCount--;
	}

	boolean anyoneBlocked() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(
				blockCount >= 0, "blockCount should not be negative");
		}

		// no synchronization needed because reads of ints are atomic
		return blockCount != 0;
	}

	public final Control getControl(Lockable ref) {
		return (Control) get(ref);
	}
}
