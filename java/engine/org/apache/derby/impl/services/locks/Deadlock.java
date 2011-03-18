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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.TransactionInfo;

import java.util.Hashtable;
import java.util.Dictionary;
import java.util.Stack;

import java.util.List;

/**
	Code to support deadlock detection.
*/

class Deadlock  {

	private Deadlock() {}

	/**
	 * Look for a deadlock.
	 * <BR>
	 * Walk through the graph of all locks and search for cycles among
	 * the waiting lock requests which would indicate a deadlock. A simple
	 * deadlock cycle is where the granted locks of waiting compatibility
	 * space A is blocking compatibility space B and space B holds locks causing
	 * space A to wait.
	 * <p>
	 * Would be nice to get a better high level description of deadlock
	 * search.
	 * <p> 
	 * MT - if the <code>LockTable</code> is a <code>LockSet</code> object, the
	 * callers must be synchronized on the <code>LockSet</code> object in order
	 * to satisfy the syncronization requirements of
	 * <code>LockSet.addWaiters()</code>. If it is a
	 * <code>ConcurrentLockSet</code> object, the callers must not hold any of
	 * the <code>ReentrantLock</code>s guarding the entries in the lock table,
	 * and the callers must make sure that only a single thread calls
	 * <code>look()</code> at a time.
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

		Stack chain = new Stack();

		chain.push(startingLock.getCompatabilitySpace());
		chain.push(control.getGrants());
outer:	for (;;) {

			if (chain.isEmpty()) {
				// all done
				break outer;
			}

			List grants = (List) chain.peek();
			if (grants.isEmpty()) {
				// pop this list of granted locks and back to the previous one
				rollback(chain);
				continue outer;
			}
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

					// We could be seeing a situation here like
					// Granted T1{S}, T2{S}
					// Waiting T1{X} - deadlock checking on this
					//
					// In this case it's not a deadlock, although it
					// depends on the locking policy of the Lockable. E.g.
					// Granted T1(latch)
					// Waiting T1(latch)
					//  is a deadlock.
					//

					if ((index == (chain.size() - 1)) ||
						((index == (chain.size() - 2))
						&& (index == (chain.indexOf(grants) - 1)))) {

						// potential self deadlock, but probably not!
						ActiveLock lock = (ActiveLock) waiters.get(space);

						if (lock.canSkip) {
							// not a deadlock ...
							chain.push(space); // set up as rollback() expects.

							rollback(chain);
							continue outer;
						}
					}

					return Deadlock.handle(factory, chain, index, waiters, deadlockWake);
				}
				chain.push(space);

                skip_space: while (true) {

                    Lock waitingLock = (Lock) waiters.get(space);
                    if (waitingLock == null) {
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

                        chain.push(waitOnControl.getGrants());

                        continue outer;
                    } else {
                        // simply waiting on another waiter
                        ActiveLock waitOnLock = (ActiveLock) waitOn;

                        space = waitOnLock.getCompatabilitySpace();

                        if (waitingLock.getLockable().requestCompatible(
                                waitingLock.getQualifier(),
                                waitOnLock.getQualifier())) {
                            // We're behind another waiter in the queue, but we
                            // request compatible locks, so we'll get the lock
                            // too once it gets it. Since we're not actually
                            // blocked by the waiter, skip it and see what's
                            // blocking it instead.
                            continue skip_space;
                        } else {
                            continue inner;
                        }
                    }
                }
            }
		}

		return null;
	}

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

	private static Hashtable getWaiters(LockTable set) {
		Hashtable waiters = new Hashtable();
		set.addWaiters(waiters);
		return waiters;
	}

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

	static StandardException buildException(AbstractPool factory,
											Object[] data) {

		Stack chain = (Stack) data[0];
		Dictionary waiters = (Dictionary) data[1];


		LanguageConnectionContext lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

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

		Hashtable attributes = new Hashtable(17);

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
						conglomId = new Long(tc.findConglomid(containerId.longValue()));
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

} 
