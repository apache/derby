/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_L2

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.iapi.services.sanity.SanityManager;
import java.util.Hashtable;
import org.apache.derby.iapi.services.locks.*;

/**
	A semaphore that implements Lockable for unit testing.
*/
class T_L2 implements Lockable {

	private int allowed;
	private Object[]	lockers;
	private int[]		counts;

	T_L2(int allowed) {
		this.allowed = allowed;
		lockers = new Object[allowed];
		counts = new int[allowed];
	}

	/*
	** Lockable methods (Simple, qualifier assumed to be null), allows
	** up to 'allowed' lockers in at the same time.
	*/

	public void lockEvent(Latch lockInfo) {

		int empty = -1;
		for (int i = 0; i < allowed; i++) {
			if (lockers[i] == lockInfo.getCompatabilitySpace()) {
				counts[i]++;
				return;
			}

			if (lockers[i] == null)
				empty = i;
		}

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(empty != -1);
		lockers[empty] = lockInfo.getCompatabilitySpace();
		counts[empty] = 1;

	}

	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		return false;
	}

	public boolean lockerAlwaysCompatible() {
		return true;
	}

	public void unlockEvent(Latch lockInfo) {

		for (int i = 0; i < allowed; i++) {

			if (lockers[i] == lockInfo.getCompatabilitySpace()) {
				counts[i]--;
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(counts[i] >= 0);
				if (counts[i] == 0) {
					lockers[i] = null;
					return;
				}

				return;
			}
		}

        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("unlocked by a compatability space that does not exist");
	}

	public boolean lockAttributes(int flag, Hashtable t)
	{
		return false;
	}
	
}
