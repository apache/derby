/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_User

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

import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.locks.*;

class T_User implements Runnable {

	private LockFactory lf;
	private Lockable[] refs;
	private long iterations;
	private long offset;
	private int test;

	Throwable error = null;



	T_User(int test, LockFactory lf, Lockable[] refs, long iterations, long offset) {

		this.lf = lf;
		this.refs = refs;
		this.iterations = iterations;
		this.test = test;
		this.offset = offset;
	}

	public void run() {

		try {
			switch (test) {
			case 1:
				T001();
				break;
			case 2:
				T002();
				break;
			case 3:
				T003();
				break;
			}
		} catch (Throwable t) {
			error = t;
		}
	}

	private void T001() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Integer g0 = new Integer(1);	// create an object for a lock group

		// check we have no locks held
		checkLockCount(cs, 0);

		T_L1 ref;

		while (--iterations > 0) {
			long value = offset + iterations;

			lf.lockObject(cs, g0, refs[0], null, C_LockFactory.WAIT_FOREVER);
			ref = (T_L1) refs[0];
			ref.value = value;
			checkLockCount(cs, 1);
			Thread.yield();
			checkValue(ref, value);

			lf.lockObject(cs, g0, refs[1], null, C_LockFactory.WAIT_FOREVER);
			ref = (T_L1) refs[1];
			ref.value = value;
			Thread.yield();

			checkValue((T_L1) refs[0], value);
			checkValue((T_L1) refs[1], value);

			lf.unlock(cs, g0, refs[0], null);
			checkValue((T_L1) refs[1], value);

			Thread.yield();

			lf.unlock(cs, g0, refs[1], null);

			// check we have no locks held
			checkLockCount(cs, 0);

			Thread.yield();

		}
	}

	private void T002() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Integer g0 = new Integer(1);	// create an object for a lock group

		// check we have no locks held
		checkLockCount(cs, 0);

		while (--iterations > 0) {
			long value = offset + iterations;
			T_L1 ref = (T_L1) refs[0];

			lf.lockObject(cs, g0, refs[0], null, C_LockFactory.WAIT_FOREVER);
			ref.value = value;
			checkLockCount(cs, 1);
			Thread.yield();
			checkValue(ref, value);

			lf.unlock(cs, g0, refs[0], null);

			// check we have no locks held
			checkLockCount(cs, 0);
		}
	}

	private void T003() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Integer g0 = new Integer(1);	// create an object for a lock group

		// check we have no locks held
		checkLockCount(cs, 0);

		while (--iterations > 0) {

			lf.lockObject(cs, g0, refs[0], null, C_LockFactory.WAIT_FOREVER);
			checkLockCount(cs, 1);
			Thread.yield();
			lf.unlock(cs, g0, refs[0], null);

			// check we have no locks held
			checkLockCount(cs, 0);
		}
	}
	private void T004() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Integer g0 = new Integer(1);	// create an object for a lock group

		// check we have no locks held
		checkLockCount(cs, 0);

		while (--iterations > 0) {

			lf.lockObject(cs, g0, refs[0], null, C_LockFactory.WAIT_FOREVER);
			checkLockCount(cs, 1);
			Thread.yield();


			lf.lockObject(cs, g0, refs[0], null, C_LockFactory.WAIT_FOREVER);
			checkLockCount(cs, 2);
			Thread.yield();

			lf.unlockGroup(cs, g0);

			// check we have no locks held
			checkLockCount(cs, 0);
		}
	}

	private void checkValue(T_L1 item, long value) throws T_Fail {
		if (item.value  != value)
			throw T_Fail.testFailMsg("value corrupted in multi-user test, exapected " + value + ", got " + item.value);
	}

	void checkLockCount(CompatibilitySpace cs, int expected) throws T_Fail {
		boolean expect = expected != 0;
		boolean got = lf.areLocksHeld(cs);
		if (got != expect)
			throw T_Fail.testFailMsg("Expected lock count (" + expect + "), got (" + got + ")");
	}


}
