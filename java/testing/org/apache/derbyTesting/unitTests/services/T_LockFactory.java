/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_LockFactory

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

import org.apache.derbyTesting.unitTests.harness.T_MultiIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.locks.*;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

/**
	Protocol unit test for the LockManager.

	@see LockFactory
	@see org.apache.derbyTesting.unitTests.harness.UnitTest
*/

public class T_LockFactory extends T_MultiIterations
{
	protected final static int ITERATIONS = 100;	// iterations of multi-user tests

	protected LockFactory	lf;

	public T_LockFactory() {
		super();
	}

	/*
	** The tests
	*/

	protected String getModuleToTestProtocolName() {

		return org.apache.derby.iapi.reference.Module.LockFactory;
	}

	/**
		Run all the tests, each test that starts with 'S' is a single user
		test, each test that starts with 'M' is a multi-user test.

		@exception T_Fail The test failed in some way.
	*/
	protected  void setupTest() throws T_Fail {

		try {
			lf = (LockFactory) Monitor.startSystemModule(getModuleToTestProtocolName());
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}
		if (lf == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " module not started.");
		}
	}

	/**
		Run once per-iteration to run the actual test.
		@exception T_Fail the test failed in some way.
	*/
	protected void runTestSet() throws T_Fail {

		// Set up the expected error handling
		try {
			
			S001();
			S002();
			S003();
			S004();
			S005();
			S007();

			M001();
			M002();
			M003();
			M004();
			

		} catch (StandardException se) {

			throw T_Fail.exceptionFail(se);

		}
	}

	/*
	** Test functions
	*/

	/**
		Single user API test 001.

		Lock an single object in a single group with all lock methods and
		then unlock the object with all unlock methods.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/
	void S001() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Object g0 = new Object();	// create an object for a lock group
		Lockable l0 = new T_L1();		// simple lockable

		int count;
		
		// check we have no locks held
		checkLockCount(cs, 0);

		// lock and unlock specifically (no timeout)
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs, 1);
		count = lf.unlock(cs, g0, l0, null);
		if (count != 1)
			throw T_Fail.testFailMsg("invalid unlock count, expected 1, got " + count);

		// check we have no locks held
		checkLockCount(cs, 0);

		// lock twice and unlock all ...
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs, 2);
		lf.unlock(cs, g0, l0, null);
		lf.unlock(cs, g0, l0, null);

		// check we have no locks held
		checkLockCount(cs, 0);

		// lock three times and unlock by group
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs, 3);
		lf.unlockGroup(cs, g0);

		// check we have no locks held
		checkLockCount(cs, 0);


		// lock three times and unlock explicitly
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs, 3);

		lf.unlock(cs, g0, l0, null);
		checkLockCount(cs, 2);

		lf.unlock(cs, g0, l0, null);
		checkLockCount(cs, 1);

		lf.unlock(cs, g0, l0, null);
		checkLockCount(cs, 0);

		// lock and unlock specifically with timeout
		lf.lockObject(cs, g0, l0, null, 1000 /*ms*/);
		checkLockCount(cs, 1);
		count = lf.unlock(cs, g0, l0, null);
		if (count != 1)
			throw T_Fail.testFailMsg("invalid unlock count, expected 1, got " + count);

		PASS("S001");
	}

	/**
		Single user API test 002.

		Lock an object in different groups and check unlocks
		apply to a single group.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/

	void S002() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Object g0 = new Object();	// create an object for a lock group
		Object g1 = new Object();
		Lockable l0 = new T_L1();		// simple lockable

		int count;
		
		// check we have no locks held
		checkLockCount(cs, 0);

		// lock object in two groups and unlock specifically
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockGroupCount(cs, g0, 1);
		checkLockGroupCount(cs, g1, 1);
		checkLockCount(cs, 2);

		count = lf.unlock(cs, g0, l0, null);
		if (count != 1)
			throw T_Fail.testFailMsg("invalid unlock count, expected 1, got " + count);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 1);
		checkLockCount(cs, 1);

		count = lf.unlock(cs, g1, l0, null);
		if (count != 1)
			throw T_Fail.testFailMsg("invalid unlock count, expected 1, got " + count);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 0);


		// check we have no locks held
		checkLockCount(cs, 0);

		// lock object in two groups and unlock by group
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs, 2);

		lf.unlockGroup(cs, g1);
		checkLockGroupCount(cs, g0, 1);
		checkLockGroupCount(cs, g1, 0);
		checkLockCount(cs, 1);

		lf.unlockGroup(cs, g0);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 0);

		// check we have no locks held
		checkLockCount(cs, 0);

		PASS("S002");
	}

	/**
		Single user API test 003.

		Lock multiple objects in different groups and check unlocks
		apply to a single group.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect

	*/

	void S003() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Object g0 = new Object();	// create an object for a lock group
		Object g1 = new Object();
		Lockable l0 = new T_L1();		// simple lockable
		Lockable l1 = new T_L1();
		Lockable l2 = new T_L1();

		int count;
		
		// check we have no locks held
		checkLockCount(cs, 0);

		// lock l0 object in two groups and l1,l2 in group l1
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l1, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l2, null, C_LockFactory.WAIT_FOREVER);

		checkLockGroupCount(cs, g0, 1);
		checkLockGroupCount(cs, g1, 3);
		checkLockCount(cs, 4);

		// quick check to see that no one is blocked
		if (lf.anyoneBlocked())
			throw T_Fail.testFailMsg("anyoneBlocked() returned true on a set of private locks");

		lf.unlock(cs, g1, l1, null);
		checkLockGroupCount(cs, g0, 1);
		checkLockGroupCount(cs, g1, 2);
		checkLockCount(cs, 3);

		lf.unlockGroup(cs, g1);
		checkLockGroupCount(cs, g0, 1);
		checkLockGroupCount(cs, g1, 0);
		checkLockCount(cs, 1);

		lf.unlockGroup(cs, g0);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 0);

		// check we have no locks held
		checkLockCount(cs, 0);

		PASS("S003");
	}

	/**
		Single user API test 004.

		Lock multiple objects in different groups and transfer
		locks between groups.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/

	void S004() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Object g0 = new Object();	// create an object for a lock group
		Object g1 = new Object();
		Object g2 = new Object();
		Lockable l0 = new T_L1();		// simple lockable
		Lockable l1 = new T_L1();
		Lockable l2 = new T_L1();

		int count = 0;

		// check we have no locks held
		checkLockCount(cs, 0);

		// lock l0 object in two groups and l1,l2 in group l1
		lf.lockObject(cs, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l1, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs, g1, l2, null, C_LockFactory.WAIT_FOREVER);

		checkLockGroupCount(cs, g0, 1);
		checkLockGroupCount(cs, g1, 3);
		checkLockCount(cs, 4);

		lf.transfer(cs, g0, g1);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 4);
		checkLockCount(cs, 4);

		// transfer an empty to a non-existent one
		lf.transfer(cs, g0, g2);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 4);
		checkLockGroupCount(cs, g2, 0);
		checkLockCount(cs, 4);
		
		lf.lockObject(cs, g2, l0, null, C_LockFactory.WAIT_FOREVER);
		checkLockGroupCount(cs, g2, 1);
		checkLockCount(cs, 5);

		lf.transfer(cs, g1, g2);
		checkLockGroupCount(cs, g1, 0);
		checkLockGroupCount(cs, g2, 5);
		checkLockCount(cs, 5);

		lf.transfer(cs, g2, g1);
		checkLockGroupCount(cs, g1, 5);
		checkLockGroupCount(cs, g2, 0);
		checkLockCount(cs, 5);


		lf.unlockGroup(cs, g2);
		checkLockGroupCount(cs, g1, 5);
		checkLockGroupCount(cs, g2, 0);
		checkLockCount(cs, 5);

		lf.unlockGroup(cs, g1);
		checkLockGroupCount(cs, g1, 0);
		checkLockGroupCount(cs, g2, 0);

		// check we have no locks held
		checkLockCount(cs, 0);

		PASS("S004");
	}

	/**
		Single user API test 005.

		Create two compatability spaces and ensure that locks
		block each other out.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/
	void S005() throws StandardException, T_Fail {

		CompatibilitySpace cs0 = lf.createCompatibilitySpace(null);
		CompatibilitySpace cs1 = lf.createCompatibilitySpace(null);

		Object g0 = new Object();	// create an object for a lock group
		Object g1 = new Object();	// create an object for a lock group
		Lockable l0 = new T_L1();
		Lockable l1 = new T_L1();
		Lockable l2 = new T_L1();

		int count;

		// check we have no locks held
		checkLockCount(cs0, 0);
		checkLockCount(cs1, 0);

		lf.lockObject(cs0, g0, l0, null, C_LockFactory.WAIT_FOREVER);
		lf.lockObject(cs1, g1, l1, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs0, 1);
		checkLockCount(cs1, 1);

		lf.lockObject(cs0, g0, l2, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs0, 2);
		checkLockCount(cs1, 1);

		// now attempt to lock l2 in cs1 with a timeout, should fail
		try {
			lf.lockObject(cs1, g1, l2, null, 200 /* ms */);
			throw T_Fail.testFailMsg("lock succeeded on already locked object");
		}
		catch (StandardException lfe) {
			// we are expecting the timout exception, anything else is an error
			if (!lfe.getMessageId().equals(SQLState.LOCK_TIMEOUT)) {
				throw lfe;
			}
			checkLockCount(cs0, 2);
			checkLockCount(cs1, 1);

		}

		// now unlock the object, and re-attempt the lock
		lf.unlock(cs0, g0, l2, null);
		checkLockCount(cs0, 1);
		checkLockCount(cs1, 1);
		lf.lockObject(cs1, g1, l2, null, C_LockFactory.WAIT_FOREVER);
		checkLockCount(cs0, 1);
		checkLockCount(cs1, 2);

		lf.unlockGroup(cs0, g0);
		lf.unlockGroup(cs1, g1);
		checkLockCount(cs0, 0);
		checkLockCount(cs1, 0);

		PASS("S005");



	}	


	/**
		Single user API test 007.

		Tests on groups and compatibility spaces
		never seen by the lock manager.
		

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/
	void S007() throws StandardException, T_Fail {

		CompatibilitySpace cs = lf.createCompatibilitySpace(null);
		Object g0 = new Object();	// create an object for a lock group
		Object g1 = new Object();	// create an object for a lock group
		Lockable l0 = new T_L1();

		int count;

		// check we have no locks held
		checkLockCount(cs, 0);
		checkLockGroupCount(cs, g0, 0);

		lf.unlockGroup(cs, g0);
		lf.unlockGroup(cs, cs);
		lf.unlock(cs, g0, l0, null);

		lf.transfer(cs, g0, g1);
		lf.transfer(cs, g1, g0);

		if (lf.anyoneBlocked())
			throw T_Fail.testFailMsg("anyoneBlocked() returned true on an empty space");

		// check we have no locks held
		checkLockCount(cs, 0);
		checkLockGroupCount(cs, g0, 0);
		checkLockGroupCount(cs, g1, 0);

		PASS("S007");
	}

	/*
	** Multi-user tests.
	*/

	/**
		Multi-user test 001.

		Create two lockable objects and pass them off to two threads.
		Each thread will run lock the first object, set its value then lock
		the second object & set its value, yield and then release the lock
		on one and then on two. Various checks are made to ensure the
		values are as expected.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/

	void M001() throws StandardException, T_Fail {

		Lockable[] locks = new T_L1[2];
		locks[0] = new T_L1();
		locks[1] = new T_L1();

		T_User u1 = new T_User(1, lf, locks, ITERATIONS, 10 * ITERATIONS);
		T_User u2 = new T_User(1, lf, locks, ITERATIONS, 20 * ITERATIONS);
		Thread t1 = new Thread(u1);
		Thread t2 = new Thread(u2);

		t1.start();
		t2.start();

		try {
			t1.join();
			t2.join();
		} catch (InterruptedException ie) {
			throw T_Fail.exceptionFail(ie);
		}

		if (u1.error != null)
			throw T_Fail.exceptionFail(u1.error);
		if (u2.error != null)
			throw T_Fail.exceptionFail(u2.error);

		PASS("M001");
	}
	

	/**
		Multi-user test 002

		Create a single lockable and have three threads lock it, yield and
		then release it. The single lockable can only have one locker.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/

		
	void M002() throws StandardException, T_Fail {

		Lockable[] locks = new T_L1[1];
		locks[0] = new T_L1();

		T_User u1 = new T_User(2, lf, locks, ITERATIONS, 10 * ITERATIONS);
		T_User u2 = new T_User(2, lf, locks, ITERATIONS, 20 * ITERATIONS);
		T_User u3 = new T_User(2, lf, locks, ITERATIONS, 30 * ITERATIONS);
		Thread t1 = new Thread(u1);
		Thread t2 = new Thread(u2);
		Thread t3 = new Thread(u3);

		t1.start();
		t2.start();
		t3.start();

		try {
			t1.join();
			t2.join();
			t3.join();
		} catch (InterruptedException ie) {
			throw T_Fail.exceptionFail(ie);
		}

		if (u1.error != null)
			throw T_Fail.exceptionFail(u1.error);
		if (u2.error != null)
			throw T_Fail.exceptionFail(u2.error);
		if (u3.error != null)
			throw T_Fail.exceptionFail(u3.error);


		PASS("M002");
	}
	/**
		Multi-user test 003

		Create a single lockable and have three threads lock it, yield and
		then release it. The single lockable is a semaphore that can have two lockers.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/

		
	void M003() throws StandardException, T_Fail {

		Lockable[] locks = new Lockable[1];
		locks[0] = new T_L2(2);

		T_User u1 = new T_User(3, lf, locks, ITERATIONS, 0);
		T_User u2 = new T_User(3, lf, locks, ITERATIONS, 0);
		T_User u3 = new T_User(3, lf, locks, ITERATIONS, 0);
		Thread t1 = new Thread(u1);
		Thread t2 = new Thread(u2);
		Thread t3 = new Thread(u3);

		t1.start();
		t2.start();
		t3.start();

		try {
			t1.join();
			t2.join();
			t3.join();
		} catch (InterruptedException ie) {
			throw T_Fail.exceptionFail(ie);
		}

		if (u1.error != null)
			throw T_Fail.exceptionFail(u1.error);
		if (u2.error != null)
			throw T_Fail.exceptionFail(u2.error);
		if (u3.error != null)
			throw T_Fail.exceptionFail(u3.error);


		PASS("M003");
	}
	/**
		Multi-user test 004

		As M003 but each thread will lock the object twice, to ensure that
		lock manager grantes the lock when the compatability space and qualifier
		match.

		@exception StandardException	An exception thrown by a method of LockFactory
		@exception T_Fail	Some behaviour of the LockFactory is incorrect
	*/

		
	void M004() throws StandardException, T_Fail {

		Lockable[] locks = new Lockable[1];
		locks[0] = new T_L2(2);

		T_User u1 = new T_User(4, lf, locks, ITERATIONS, 0);
		T_User u2 = new T_User(4, lf, locks, ITERATIONS, 0);
		T_User u3 = new T_User(4, lf, locks, ITERATIONS, 0);
		Thread t1 = new Thread(u1);
		Thread t2 = new Thread(u2);
		Thread t3 = new Thread(u3);

		t1.start();
		t2.start();
		t3.start();

		try {
			t1.join();
			t2.join();
			t3.join();
		} catch (InterruptedException ie) {
			throw  T_Fail.exceptionFail(ie);
		}

		if (u1.error != null)
			throw  T_Fail.exceptionFail(u1.error);
		if (u2.error != null)
			throw T_Fail.exceptionFail(u2.error);
		if (u3.error != null)
			throw T_Fail.exceptionFail(u3.error);


		PASS("M004");
	}



	/*
	** Utility functions
	*/

	/**
		Check to see if the total number of locks we have is as expected.

		@exception T_Fail	Number of locks is not as expected.
	*/
	void checkLockCount(CompatibilitySpace cs, int expected) throws T_Fail {
		boolean expect = expected != 0;
		boolean got = lf.areLocksHeld(cs);
		if (got != expect)
			throw T_Fail.testFailMsg("Expected lock count (" + expect + "), got (" + got + ")");
	}

	/**
		Check to see if the number of locks in a group we have is as expected.

		@exception T_Fail	Number of locks is not as expected.
	*/

	void checkLockGroupCount(CompatibilitySpace cs, Object group, int expected)
			throws T_Fail {
		boolean expect = expected != 0;
		boolean got = lf.areLocksHeld(cs, group);
		if (got != expect)
			throw T_Fail.testFailMsg("Expected lock count (" + expect + "), got (" + got + ")");
	}

}


