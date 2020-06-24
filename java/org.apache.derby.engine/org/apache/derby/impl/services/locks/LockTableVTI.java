/*

   Derby - Class org.apache.derby.impl.services.locks.LockTableVTI

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

import org.apache.derby.iapi.services.locks.Latch;

import java.util.Enumeration;
import java.util.NoSuchElementException;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.List;
import java.util.Map;

/**
	This provides an Enumeration of Latch's
	from a clone of the lock table. A Latch is badly named,
	it represents lock information.
 */
class LockTableVTI implements Enumeration
{
	// the clonedLockTable temporarily holds a copy of the lock table.
	//
	// The copy is necessary because the real lock manager needs to be single
	// threaded while a snap shot is made.  After the copy is made, it can take
	// its time digesting the information without blocking the real lock
	// manager.

	private final Iterator outerControl;
	private Control control;
	private ListIterator grantedList;
	private ListIterator waitingList;
	private Latch nextLock;

//IC see: https://issues.apache.org/jira/browse/DERBY-1704
	LockTableVTI(Map clonedLockTable)
	{
		outerControl = clonedLockTable.values().iterator();
	}


	public boolean hasMoreElements() {

		if (nextLock != null)
			return true;

		for (;;) {

			if (control == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1704
				if (!outerControl.hasNext())
					return false;
//System.out.println("new control lock ");

				control = (Control) outerControl.next();

				List granted = control.getGranted();
				if (granted != null)
					grantedList = granted.listIterator();


				List waiting = control.getWaiting();
				if (waiting != null)
					waitingList = waiting.listIterator();

				nextLock = control.getFirstGrant();
				if (nextLock == null) {

					nextLock = getNextLock(control);
				}
				
			} else {
				nextLock = getNextLock(control);
			}


			if (nextLock != null)
				return true;

			control = null;
		}
	}

	private Latch getNextLock(Control control) {
		Latch lock = null;
		if (grantedList != null) {
			if (grantedList.hasNext()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5224
				lock = (Latch) grantedList.next();
			}
			else
				grantedList = null;
		}

		if (lock == null) {
			if (waitingList != null) {
				if (waitingList.hasNext()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5224
					lock = (Latch) waitingList.next();
				}
				else
					waitingList = null;
			}
		}

		return lock;
	}

	public Object nextElement() {

		if (!hasMoreElements())
			throw new NoSuchElementException();

		Latch ret = nextLock;

		nextLock = null;
		return ret;
	}
}



