/*

   Derby - Class org.apache.derby.impl.services.locks.SinglePool

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.Limit;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.daemon.Serviceable;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.iapi.reference.Property;

import java.util.Hashtable;
import java.util.Properties;
import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;

// debugging
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

/**
	An implementation of LockFactory that uses a single pool
	for the locks, i.e. all lock requests go through a single
	point of synchronisation.
    <p>
    The default concrete class "SinglePool.java", prints nothing and thus 
    incurs no overhead associated with the code to dump lock information.  An
    alternate concrete class "LockDebug/TracingSinglePool.java", attempts to 
    output only lock information that "makes sense" to a user - for instance it
    doesn't print latch locks.

    <BR>
	MT - Mutable - Container Object : Thread Aware
*/

public class SinglePool extends Hashtable
	implements  LockFactory
{
	/**
		The complete set of locks in the system

		<BR>
		MT - immutable - content dynamic : LockSet is ThreadSafe
	*/
	protected final LockSet			lockTable;

	/**
	    This is now in this object, it now extends Hashtable.
		A hash table of all compatability spaces. Key is the object 
        representing the compatability space, value is a LockSpace object. 
        Addition and removal from the Hashtable is performed under the 
        Hashtable's monitor. This requires holding this monitor while making 
        calls to the thread safe methods of LockSpace. This is to ensure
        that it is guaranteed that a LockSpace is only removed when it is 
        empty and no-one is in the process of adding to it. No deadlocks are 
        possible because the spaces reference is not visible outside this 
        class and the LockSpace class does not call back into this class.

		<BR>
		MT - immutable - content dynamic : Java synchronized(spaces)

		This class creates a LockSet and LockSpaces, both classes are thread 
        safe.
		
	*/

	/**
		True if all deadlocks errors should be logged.
	*/
	int deadlockMonitor;

	public SinglePool() {
		lockTable = new LockSet(this);
	}

	/*
	** Methods of LockFactory
	*/

	/**
		Latch a specific object with a timeout.

		<BR>
		MT - thread safe

		@exception StandardException Standard Cloudscape error policy

		@see LockFactory#latchObject
	*/
	public boolean latchObject(Object compatabilitySpace, Lockable ref, Object qualifier, int timeout) 
		throws StandardException {

		Lock latch = lockTable.lockObject(compatabilitySpace, ref, qualifier, timeout, (Latch) null);

		if (SanityManager.DEBUG) {
			if (latch == null)
				SanityManager.ASSERT(timeout == C_LockFactory.NO_WAIT, "timeout not NO_WAIT");
		}
		return latch != null;
	}

	/**
		Unlatch an  object.

		<BR>
		MT - thread safe

		@see LockFactory#unlatch
	*/
	public void unlatch(Latch heldLatch) {
		lockTable.unlock(heldLatch, 1);
	}

	/**
		Lock a specific object with a timeout.

		<BR>
		MT - thread safe

		@exception StandardException Standard Cloudscape error policy

		@see LockFactory#lockObject
	*/
	protected Lock lockAnObject(Object compatabilitySpace, Object group, Lockable ref, Object qualifier, int timeout, Latch heldLatch)
			throws StandardException
	{
		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {

				D_LockControl.debugLock("Lock Request before Grant: ", 
                    compatabilitySpace, group, ref, qualifier, timeout);

                if (SanityManager.DEBUG_ON(Constants.LOCK_STACK_TRACE))
                {
                    // The following will print the stack trace of the lock
                    // request to the log.  
                    Throwable t = new Throwable();
                   java.io.PrintWriter istream = SanityManager.GET_DEBUG_STREAM();

                    istream.println("Stack trace of lock request:");
                    t.printStackTrace(istream);
                }
			}
		}

		Lock lock = 
            lockTable.lockObject(compatabilitySpace, ref, qualifier, timeout, heldLatch);

		// See if NO_WAIT was passed in and the lock could not be granted.
		if (lock == null) {
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(timeout == C_LockFactory.NO_WAIT, "timeout not NO_WAIT");
			}
			return null;
		}

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock(
                    "Lock Request Granted: ", 
                    compatabilitySpace, group, ref, qualifier, timeout);
			}
		}

		// find the space and atomically add lock to required group
		synchronized (this) {

			LockSpace ls = (LockSpace) get(compatabilitySpace);
			if (ls == null)	{
				 ls = new LockSpace(this, compatabilitySpace);
				 put(compatabilitySpace, ls);
			}

			// we hold the spaces monitor while adding the lock to close 
            // the window between finding the LockSpace and adding a lock 
            // to it, thus ensuring the LockSpace is not removed from the 
            // spaces Hashtable underneath us.

			ls.addLock(group, lock);
		}

		return lock;
	}

	/**
		Lock a specific object

		<BR>
		MT - thread safe

		@exception StandardException Standard Cloudscape error policy

		@see LockFactory#lockObject
	*/
	public boolean lockObject(Object compatabilitySpace, Object group, Lockable ref, Object qualifier, int timeout)
		throws StandardException {

		return lockAnObject(compatabilitySpace, group, ref, qualifier, timeout, (Latch) null) != null;
	}

	/**
		Lock a specific object while holding a latch

		<BR>
		MT - thread safe

		@exception StandardException Standard Cloudscape error policy

		@see LockFactory#lockObject
	*/
	public boolean lockObject(Object group, Lockable ref, Object qualifier, int timeout, Latch latch)
		throws StandardException {

		if (SanityManager.DEBUG) {
			if (timeout == C_LockFactory.NO_WAIT)
				SanityManager.THROWASSERT("no wait lock requested in lockObject() with latch");
		}

		Lock lock = lockAnObject(latch.getCompatabilitySpace(), group, ref, qualifier, timeout, latch);
		return lock instanceof ActiveLock;
	}

	/**
		Unlock a specific object

		<BR>
		MT - thread safe

		@see LockFactory#unlock
	*/

	public int unlock(Object compatabilitySpace, Object group, Lockable ref, Object qualifier)
	{
		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock("Lock Unlock: ", 
                    compatabilitySpace, group, ref, qualifier, -1);
			}
		}

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return 0;

		int count = ls.unlockReference(lockTable, ref, qualifier, group);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(
                (count == 0) || (count == 1), "count = " + count);
		}

		return count;
	}

	/**
		Unlock a group of objects. 

		<BR>
		MT - thread safe

		@param group handle of group that objects were locked with.
		If group is	null then this call is equivilent to unlockAll().

		@see LockFactory#unlockGroup
	*/
	public void unlockGroup(Object compatabilitySpace, Object group) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock("Lock Unlock Group: ", compatabilitySpace, group);
			}
		}

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return;

		ls.unlockGroup(lockTable, group);
	}

	public void unlockGroup(Object compatabilitySpace, Object group, Matchable key) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock("Lock Unlock Group: ", compatabilitySpace, group);
			}
		}

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return;

		ls.unlockGroup(lockTable, group, key);


	}

	/**
		Transfer a set of locks from one group to another.

  		<BR>
		MT - thread safe

		@see LockFactory#transfer
	*/
	public void transfer(Object compatabilitySpace, Object oldGroup, Object newGroup) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				StringBuffer sb = new StringBuffer("Lock Transfer:");

				D_LockControl.debugAppendObject(
                    sb, " CompatabilitySpace=", compatabilitySpace);
				D_LockControl.debugAppendObject(sb, " Old Group=", oldGroup);
				D_LockControl.debugAppendObject(sb, " New Group=", newGroup);

				D_LockControl.debugAddThreadInfo(sb);

				SanityManager.DEBUG(Constants.LOCK_TRACE, sb.toString());
			}
		}

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return;

		// there is a window where someone could remove the LockSpace from the
        // spaces Hashtable, since we do not hold the spaces' monitor. This is
        // Ok as the LockSpace will have no locks and this method 
        // will correctly do nothing.

		ls.transfer(oldGroup, newGroup);
	}

	/**
		Returns true if locks by anyone are blocking anyone else
	*/
	public boolean anyoneBlocked() {
		return lockTable.anyoneBlocked();
	}

	/**
		Return true if locks are held in this group and this space.

		<BR>
		MT - thread safe

		@param group handle of group that objects were locked with.

		@see LockFactory#areLocksHeld
	*/
	public boolean areLocksHeld(Object compatabilitySpace, Object group) {

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return false;

		// there is a window where someone could remove the LockSpace from the 
        // spaces Hashtable, since we do not hold the spaces' monitor. This is 
        // Ok as the LockSpace will have no locks and this method will 
        // correctly return false.

		return ls.areLocksHeld(group);
	}

	/**
		Return true if locks are held in this space
		
		<BR>
		MT - thread safe

		@see LockFactory#areLocksHeld
	*/
	public boolean areLocksHeld(Object compatabilitySpace) {
		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return false;
		return !ls.isEmpty();
	}

	public boolean zeroDurationlockObject(Object compatabilitySpace, Lockable ref, Object qualifier, int timeout)
		throws StandardException {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {

				D_LockControl.debugLock("Zero Duration Lock Request before Grant: ", 
                    compatabilitySpace, (Object) null, ref, qualifier, timeout);

                if (SanityManager.DEBUG_ON(Constants.LOCK_STACK_TRACE))
                {
                    // The following will print the stack trace of the lock
                    // request to the log.  
                    Throwable t = new Throwable();
                   java.io.PrintWriter istream = SanityManager.GET_DEBUG_STREAM();

                    istream.println("Stack trace of lock request:");
                    t.printStackTrace(istream);
                }
			}
		}

		// Very fast zeroDurationlockObject() for unlocked objects.
		// If no entry exists in the lock manager for this reference
		// then it must be unlocked.
		// If the object is locked then we perform a grantable
		// check, skipping over any waiters.
		// If the caller wants to wait and the lock cannot
		// be granted then we do the slow join the queue and
		// release the lock method.
		synchronized (lockTable) {

			Control control = (Control) lockTable.get(ref);
			if (control == null) {
				return true;
			}

			// If we are grantable, ignoring waiting locks then
			// we can also grant this request now, as skipping
			// over the waiters won't block them as we release
			// the lock rightway.
			if (control.isGrantable(true, compatabilitySpace, qualifier))
				return true;

			// can't be granted and are not willing to wait.
			if (timeout == C_LockFactory.NO_WAIT)
				return false;
		}

		Lock lock = 
            lockTable.lockObject(compatabilitySpace, ref, qualifier, timeout, (Latch) null);

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock(
                    "Zero Lock Request Granted: ", 
                    compatabilitySpace, (Object) null, ref, qualifier, timeout);
			}
		}

		// and simply unlock it once
		lockTable.unlock(lock, 1);

		return true;
	}

	public boolean isLockHeld(Object compatabilitySpace, Object group, Lockable ref, Object qualifier) {

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return false;

		return ls.isLockHeld(group, ref, qualifier);
	}

	public synchronized void setLimit(Object compatabilitySpace, Object group, int limit, Limit callback) {

		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)	{
			 ls = new LockSpace(this, compatabilitySpace);
			 put(compatabilitySpace, ls);
		}

		ls.setLimit(group, limit, callback);
		
	}

	/**
		Clear a limit set by setLimit.
	*/
	public void clearLimit(Object compatabilitySpace, Object group) {
		LockSpace ls = (LockSpace) get(compatabilitySpace);
		if (ls == null)
			return;

		ls.clearLimit(group);
	}

//EXCLUDE-START-lockdiag- 

	/**
		Routines to support lock diagnostics VTIs for the benefit of VirtualLockTable
	 */
	/* package */
	public Enumeration makeVirtualLockTable()
	{
		// make a shallow copy of the locktable.
		LockTableVTI myclone = new LockTableVTI(lockTable.shallowClone());

		return myclone;
	}
//EXCLUDE-END-lockdiag- 


	/*
	** Non-public methods
	*/

//EXCLUDE-START-debug- 

    public String toDebugString()
    {
        return(lockTable.toDebugString());
    }

//EXCLUDE-END-debug- 
	
	/*
	** Methods of PropertySetCallback
	*/

	public void init(boolean dbOnly, Dictionary p) {

		getAndApply(dbOnly, p, Property.DEADLOCK_TIMEOUT);
		getAndApply(dbOnly, p, Property.LOCKWAIT_TIMEOUT);
		getAndApply(dbOnly, p, Property.DEADLOCK_MONITOR);
//EXCLUDE-START-lockdiag- 
        getAndApply(dbOnly, p, Property.DEADLOCK_TRACE);
//EXCLUDE-END-lockdiag- 
	}

	private void getAndApply(boolean dbOnly, Dictionary p, String key) {

		try {

			Serializable value = (String) PropertyUtil.getPropertyFromSet(dbOnly, p, key);
			if (value != null) {
				validate(key, value, p);
				apply(key, value, p);
			}
		} catch (StandardException se) {
			// just ignore value at bootup.
		}
	}

	
	public boolean validate(String key, Serializable value, Dictionary p)
		throws StandardException {

		if (!key.startsWith(Property.LOCKS_INTRO))
			return false;

		if (value != null) {

			if (key.equals(Property.DEADLOCK_TIMEOUT))
				getWaitValue((String) value,  Property.DEADLOCK_TIMEOUT_DEFAULT);
			else if (key.equals(Property.LOCKWAIT_TIMEOUT))
				getWaitValue((String) value,  Property.WAIT_TIMEOUT_DEFAULT);
			else if (key.equals(Property.DEADLOCK_MONITOR))
				PropertyUtil.booleanProperty(Property.DEADLOCK_MONITOR, value, false);
            else if (key.equals(Property.DEADLOCK_TRACE))
                PropertyUtil.booleanProperty(Property.DEADLOCK_TRACE, value, false);
		}

		return true;
	}

    public Serviceable apply(String key, Serializable value, Dictionary p)
		throws StandardException {

		if (value == null) {
			// a delete, fill in the new value
			value = PropertyUtil.getPropertyFromSet(p, key);
		}

		String svalue = (String) value;

		if (key.equals(Property.DEADLOCK_TIMEOUT))
			lockTable.deadlockTimeout = getWaitValue(svalue,  Property.DEADLOCK_TIMEOUT_DEFAULT);
		else if (key.equals(Property.LOCKWAIT_TIMEOUT))
			lockTable.waitTimeout = getWaitValue(svalue,  Property.WAIT_TIMEOUT_DEFAULT);
		else if (key.equals(Property.DEADLOCK_MONITOR)) {
			deadlockMonitor = PropertyUtil.booleanProperty(Property.DEADLOCK_MONITOR, svalue, false) ?
				StandardException.REPORT_ALWAYS : StandardException.REPORT_DEFAULT;
		}
//EXCLUDE-START-lockdiag- 
        else if (key.equals(Property.DEADLOCK_TRACE))
            lockTable.setDeadlockTrace(PropertyUtil.booleanProperty(Property.DEADLOCK_TRACE, svalue, false));
//EXCLUDE-END-lockdiag- 

		return null;
	}
	
    public Serializable map(String key, Serializable value, Dictionary p) {
		return null;
	}

	/*
	** Property related methods
	*/
	
	private static int getWaitValue(String value, int defaultValue ) {

		// properties are defined in seconds
		int wait = PropertyUtil.handleInt(value, Integer.MIN_VALUE, Integer.MAX_VALUE / 1000, defaultValue);

		if (wait < 0)
			wait = C_LockFactory.WAIT_FOREVER;
		else
			// convert to milliseconds
			wait *= 1000;

		return wait;
	}
}
