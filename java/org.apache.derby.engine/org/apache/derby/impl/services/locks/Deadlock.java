/*

   Derby - Class org.apache.derby.impl.services.locks.Deadlock

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

import org.apache.derby.iapi.services.locks.VirtualLockTable;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.TransactionInfo;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Hashtable;
import java.util.Dictionary;
import java.util.Stack;

import java.util.List;

/**
 * <p>
 * Code to support deadlock detection.
 * </p>
 *
 * <p>
 * This class implements deadlock detection by searching for cycles in the
 * wait graph. If a cycle is found, it means that (at least) two transactions
 * are blocked by each other, and one of them must be aborted to allow the
 * other one to continue.
 * </p>
 *
 * <p>
 * The wait graph is obtained by asking the {@code LockSet} instance to
 * provide a map representing all wait relations, see {@link #getWaiters}.
 * The map consists of two distinct sets of (key, value) pairs:
 * </p>
 *
 * <ol>
 * <li>(space, lock) pairs, where {@code space} is the compatibility space
 * of a waiting transaction and {@code lock} is the {@code ActiveLock}
 * instance on which the transaction is waiting</li>
 * <li>(lock, prevLock) pairs, where {@code lock} is an {@code ActiveLock} and
 * {@code prevLock} is the {@code ActiveLock} or {@code LockControl} for the
 * first waiter in the queue behind {@code lock}</li>
 * </ol>
 *
 * <p>
 * The search is performed as a depth-first search starting from the lock
 * request of a waiter that has been awoken for deadlock detection (either
 * because {@code derby.locks.deadlockTimeout} has expired or because some
 * other waiter had picked it as a victim in order to break a deadlock).
 * From this lock request, the wait graph is traversed by checking which
 * transactions have already been granted a lock on the object, and who they
 * are waiting for.
 * </p>
 *
 * <p>
 * The state of the search is maintained by pushing compatibility spaces
 * (representing waiting transactions) and granted locks onto a stack. When a
 * dead end is found (that is, a transaction that holds locks without waiting
 * for any other transaction), the stack is popped and the search continues
 * down a different path. This continues until a cycle is found or the stack is
 * empty. Detection of cycles happens when pushing a new compatibility space
 * onto the stack. If the same space already exists on the stack, it means the
 * graph has a cycle and we have a deadlock.
 * </p>
 *
 * <p>
 * When a deadlock is found, one of the waiters in the deadlock cycle is awoken
 * and it will terminate itself, unless it finds that the deadlock has been
 * broken in the meantime, for example because one of the involved waiters
 * has timed out.
 * </p>
 */

class Deadlock  {

	private Deadlock() {}

	/**
     * <p>
	 * Look for a deadlock.
     * </p>
	 *
     * <p>
	 * Walk through the graph of all locks and search for cycles among
	 * the waiting lock requests which would indicate a deadlock. A simple
	 * deadlock cycle is where the granted locks of waiting compatibility
	 * space A is blocking compatibility space B and space B holds locks causing
	 * space A to wait.
     * </p>
     *
	 * <p> 
	 * MT - if the <code>LockTable</code> is a <code>LockSet</code> object, the
	 * callers must be synchronized on the <code>LockSet</code> object in order
	 * to satisfy the synchronization requirements of
	 * <code>LockSet.addWaiters()</code>. If it is a
	 * <code>ConcurrentLockSet</code> object, the callers must not hold any of
	 * the <code>ReentrantLock</code>s guarding the entries in the lock table,
	 * and the callers must make sure that only a single thread calls
	 * <code>look()</code> at a time.
     * </p>
	 *
	 *
	 * @param factory The locking system factory
	 * @param set The complete lock table. A lock table is a hash
	 * table keyed by a Lockable and with a LockControl as
	 * the data element.
	 * @param control A LockControl contains a reference to the item being
	 * locked and doubly linked lists for the granted locks
	 * and the waiting locks. The passed in value is the
	 * lock that the caller was waiting on when woken up
	 * to do the deadlock check.
	 * @param startingLock represents the specific waiting lock request that
	 * the caller has been waiting on, before just being
	 * woken up to do this search.
	 * @param deadlockWake Either Constants.WAITING_LOCK_IN_WAIT, or
	 * Constants.WAITING_LOCK_DEADLOCK. 
	 *
	 * @return The identifier to be used to open the conglomerate later.
	 *
	 * @exception StandardException Standard exception policy.
	 */
	static Object[] look(AbstractPool factory, LockTable set,
						 LockControl control, ActiveLock startingLock,
						 byte deadlockWake) {

		// step one, get a list of all waiters
		Dictionary waiters = Deadlock.getWaiters(set);

		// This stack will track the potential deadlock chain
		// The Stack consists of

		// start (Vector element 0)
		// - Compatibility space of waiter A
		// - Stack of compatibility spaces with granted lock for waiter A

		Stack<Object> chain = new Stack<Object>();

		chain.push(startingLock.getCompatabilitySpace());
		chain.push(control.getGrants());
outer:	for (;;) {

			if (chain.isEmpty()) {
                // All paths from the initial waiting lock request have been
                // examined without finding a deadlock. We're done.
				break outer;
			}

			List grants = (List) chain.peek();
			if (grants.isEmpty()) {
                // All granted locks in this lock control have been examined.
				// pop this list of granted locks and back to the previous one
				rollback(chain);
				continue outer;
			}

            // Pick one of the granted lock for examination. rollback()
            // expects us to have examined the last one in the list, so
            // always pick that one.
			int endStack = grants.size() - 1;
			Object space = ((Lock) grants.get(endStack)).getCompatabilitySpace();

			// this stack of granted locks can contain multiple entries
			// for a single space. We don't want to do deadlock detection
			// twice so check to see if we have seen this space already.
			for (int gs = 0; gs < endStack; gs++) {
				if (space.equals(((Lock) grants.get(gs)).getCompatabilitySpace())) {
					chain.push(space); // set up as rollback() expects.
					rollback(chain);
					continue outer;
				}
			}

			// find if this space is waiting on anyone
inner:		for (;;) {
				int index = chain.indexOf(space);
				if (index != -1) {
                    // Oops... The space has been examined once before, so
                    // we have what appears to be a cycle in the wait graph.
                    // In most cases this means we have a deadlock.
                    //
                    // However, in some cases, the cycle in the graph may be
                    // an illusion. For example, we could have a situation
                    // here like this:
                    //
					// Granted T1{S}, T2{S}
					// Waiting T1{X} - deadlock checking on this
					//
                    // In this case it's not necessarily a deadlock. If the
                    // Lockable returns true from its lockerAlwaysCompatible()
                    // method, which means that lock requests within the same
                    // compatibility space never conflict with each other,
                    // T1 is only waiting for T2 to release its shared lock.
                    // T2 isn't waiting for anyone, so there is no deadlock.
                    //
                    // This is only true if T1 is the first one waiting for
                    // a lock on the object. If there are other waiters in
                    // between, we have a deadlock regardless of what
                    // lockerAlwaysCompatible() returns. Take for example this
                    // similar scenario, where T3 is also waiting:
                    //
                    // Granted T1{S}, T2{S}
                    // Waiting T3{X}
                    // Waiting T1{X} - deadlock checking on this
                    //
                    // Here, T1 is stuck behind T3, and T3 is waiting for T1,
                    // so we have a deadlock.

					if ((index == (chain.size() - 1)) ||
						((index == (chain.size() - 2))
						&& (index == (chain.indexOf(grants) - 1)))) {

                        // The two identical compatibility spaces were right
                        // next to each other on the stack. This means we have
                        // the first scenario described above, with the first
                        // waiter already having a lock on the object. It is a
						// potential self deadlock, but probably not!
						ActiveLock lock = (ActiveLock) waiters.get(space);

						if (lock.canSkip) {
							// not a deadlock ...
							chain.push(space); // set up as rollback() expects.

							rollback(chain);
							continue outer;
						}
					}

                    // So it wasn't an illusion after all. Pick a victim.
					return Deadlock.handle(factory, chain, index, waiters, deadlockWake);
				}

                // Otherwise... The space hasn't been examined yet, so put it
                // on the stack and start examining it.
				chain.push(space);

                skip_space: while (true) {

                    // Who is this space waiting for?
                    Lock waitingLock = (Lock) waiters.get(space);
                    if (waitingLock == null) {
                        // The space isn't waiting for anyone, so we're at the
                        // end of the road, no deadlock in this path
                        // pop items until the previous Stack
                        rollback(chain);
                        continue outer;
                    }

                    // Is a LockControl or another ActiveLock
                    Object waitOn = waiters.get(waitingLock);
                    if (waitOn instanceof LockControl) {

                        LockControl waitOnControl = (LockControl) waitOn;

                        // This lock control may have waiters but no
                        // one holding the lock. This is true if lock
                        // has just been released but the waiters haven't
                        // woken up, or they are trying to get the
                        // synchronization we hold.

                        if (waitOnControl.isUnlocked()) {
                            // end of the road, no deadlock in this path
                            // pop items until the previous Stack
                            rollback(chain);
                            continue outer;
                        }

                        // Push all the granted locks on this object onto the
                        // stack, and go ahead examining them one by one.
                        chain.push(waitOnControl.getGrants());
                        continue outer;
                    } else {
                        // simply waiting on another waiter
                        ActiveLock waitOnLock = (ActiveLock) waitOn;

                        // Set up the next space for examination.
                        space = waitOnLock.getCompatabilitySpace();

                        // Now, there is a possibility that we're not actually
                        // waiting behind the other other waiter. Take for
                        // example this scenario:
                        //
                        // Granted T1{X}
                        // Waiting T2{S}
                        // Waiting T3{S} - deadlock checking on this
                        //
                        // Here, T3 isn't blocked by T2. As soon as T1 releases
                        // its X lock on the object, both T2 and T3 will be
                        // granted an S lock. And if T1 also turns out to be
                        // blocked by T3 and we have a deadlock, aborting T2
                        // won't resolve the deadlock, so it's not actually
                        // part of the deadlock. If we have this scenario, we
                        // just skip past T2's space and consider T3 to be
                        // waiting on T1 directly.

                        if (waitingLock.getLockable().requestCompatible(
                                waitingLock.getQualifier(),
                                waitOnLock.getQualifier())) {
                            // We're behind another waiter with a compatible
                            // lock request. Skip it since we're not really
                            // blocked by it.
                            continue skip_space;
                        } else {
                            // We are really blocked by the other waiter. Go
                            // ahead and investigate its compatibility space.
                            continue inner;
                        }
                    }
                }
            }
		}

		return null;
	}

    /**
     * Backtrack in the depth-first search through the wait graph. Expect
     * the top of the stack to hold the compatibility space we've just
     * investigated. Pop the stack until the most recently examined granted
     * lock has been removed.
     *
     * @param chain the stack representing the state of the search
     */
	private static void rollback(Stack chain) {
		do {
			chain.pop();
			if (chain.isEmpty())
				return;
		} while (!(chain.peek() instanceof List));

		// remove the last element, the one we were looking at
		List grants = (List) chain.peek();
		grants.remove(grants.size() - 1);
	}

    /**
     * Get all the waiters in a {@code LockTable}. The waiters are returned
     * as pairs (space, lock) mapping waiting compatibility spaces to the
     * lock request in which they are blocked, and (lock, prevLock) linking
     * a lock request with the lock request that's behind it in the queue of
     * waiters.
     *
     * @param set the lock table
     * @return all waiters in the lock table
     * @see LockControl#addWaiters(java.util.Map)
     */
	private static Hashtable getWaiters(LockTable set) {
		Hashtable<Object,Object> waiters = new Hashtable<Object,Object>();
		set.addWaiters(waiters);
		return waiters;
	}

    /**
     * Handle a deadlock when it has been detected. Find out if the waiter
     * that started looking for the deadlock is involved in it. If it isn't,
     * pick a victim among the waiters that are involved.
     *
     * @return {@code null} if the waiter that started looking for the deadlock
     * isn't involved in the deadlock (in which case another victim will have
     * been picked and awoken), or an array describing the deadlock otherwise
     */
	private static Object[] handle(AbstractPool factory, Stack chain, int start,
								   Dictionary waiters, byte deadlockWake) {

		// If start is zero then the space that started looking for the
		// deadlock is activly involved in the deadlock.

		Object checker = chain.elementAt(0);

		int minLockCount = Integer.MAX_VALUE;
		Object victim = null;
		for (int i = start; i < chain.size(); i++) {
			Object space = chain.elementAt(i);
			if (space instanceof List) {
				continue;
			}

			// See if the checker is in the deadlock and we
			// already picked as a victim
			if ((checker.equals(space)) && (deadlockWake == Constants.WAITING_LOCK_DEADLOCK)) {
				victim = checker;
				break;
			}

			LockSpace ls = (LockSpace) space;
			int spaceCount = ls.deadlockCount(minLockCount);

			if (spaceCount <= minLockCount) {
				victim = space;
				minLockCount = spaceCount;
			}
		}

		// See if the vitim is the one doing the checking
		if (checker.equals(victim)) {
			Object[] data = new Object[2];
			data[0] = chain;
			data[1] = waiters;
			return data;
		}

		ActiveLock victimLock = (ActiveLock) waiters.get(victim);

		victimLock.wakeUp(Constants.WAITING_LOCK_DEADLOCK);

		return null;

	}

    /**
     * Build an exception that describes a deadlock.
     *
     * @param factory the lock factory requesting the exception
     * @param data an array with information about who's involved in
     * a deadlock (as returned by {@link #handle})
     * @return a deadlock exception
     */
	static StandardException buildException(AbstractPool factory,
											Object[] data) {

		Stack chain = (Stack) data[0];
		Dictionary waiters = (Dictionary) data[1];


		LanguageConnectionContext lcc = (LanguageConnectionContext)
			getContext(LanguageConnectionContext.CONTEXT_ID);

		TableNameInfo tabInfo = null;
		TransactionInfo[] tt = null;
		TransactionController tc = null;

		if (lcc != null) {

			try {
				tc = lcc.getTransactionExecute();
				tabInfo = new TableNameInfo(lcc, false);

				tt = tc.getAccessManager().getTransactionInfo();

			} catch (StandardException se) {
				// just don't get any table info.
			}
		}


		StringBuffer sb = new StringBuffer(200);

		Hashtable<String,Object> attributes = new Hashtable<String,Object>(17);

		String victimXID = null;

		for (int i = 0; i < chain.size(); i++) {
			Object space = chain.elementAt(i);
			if (space instanceof List) {
				List grants = (List) space;

				if (grants.size() != 0) {

					sb.append("  Granted XID : ");

					for (int j = 0; j < grants.size(); j ++) {

						if (j != 0)
							sb.append(", ");

						Lock gl = (Lock) grants.get(j);

						sb.append("{");
						sb.append(gl.getCompatabilitySpace().getOwner());
						sb.append(", ");
						sb.append(gl.getQualifier());
						sb.append("} ");
					}
					sb.append('\n');
				}
				continue;
			}
			// Information about the lock we are waiting on
			// TYPE |TABLENAME                     |LOCKNAME 
			Lock lock = ((Lock) waiters.get(space));
			
			// see if this lockable object wants to participate
			lock.getLockable().lockAttributes(VirtualLockTable.ALL, attributes);

			addInfo(sb, "Lock : ", attributes.get(VirtualLockTable.LOCKTYPE));
			if (tabInfo != null) {
				Long conglomId = (Long) attributes.get(VirtualLockTable.CONGLOMID);
				if (conglomId == null) {
					Long containerId = (Long) attributes.get(VirtualLockTable.CONTAINERID);
					try {
						conglomId = tc.findConglomid(containerId.longValue());
					} catch (StandardException se) {
					}
				}
				addInfo(sb, ", ", tabInfo.getTableName(conglomId));
			}
			addInfo(sb, ", ", attributes.get(VirtualLockTable.LOCKNAME));
			sb.append('\n');

			String xid =
				String.valueOf(lock.getCompatabilitySpace().getOwner());
			if (i == 0)
				victimXID = xid;


			addInfo(sb, "  Waiting XID : {", xid);
			addInfo(sb, ", ", lock.getQualifier());
			sb.append("} ");
			if (tt != null) {
				for (int tti = tt.length - 1; tti >= 0; tti--) {
					TransactionInfo ti = tt[tti];

                    // RESOLVE (track 2771) - not sure why 
                    // ti.getTransactionIdString() or ti can return null.
                    if (ti != null)
                    {
                        String idString = ti.getTransactionIdString();

                        if (idString != null && idString.equals(xid)) {

                            addInfo(sb, ", ", ti.getUsernameString());
                            addInfo(sb, ", ", ti.getStatementTextString());
                            break;
                        }
                    }
				}
			}
			sb.append('\n');

			attributes.clear();
		}

		StandardException se = StandardException.newException(SQLState.DEADLOCK, sb.toString(), victimXID);
		se.setReport(factory.deadlockMonitor);
		return se;
	}

	private static void addInfo(StringBuffer sb, String desc, Object data) {
		sb.append(desc);
		if (data == null)
			data = "?";
		sb.append(data);
	}

    /**
     * Privileged lookup of a Context. Must be package protected so that user code
     * can't call this entry point.
     */
    static  Context    getContext( final String contextID )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContext( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContext( contextID );
                     }
                 }
                 );
        }
    }

} 
