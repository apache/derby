/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_DaemonService

   Copyright 1997, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.services;

import org.apache.derbyTesting.unitTests.harness.T_Fail;
import org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.daemon.*;

import java.util.Random;
import java.util.Vector;
/**
	This test exercices the DaemonFactory and DaemonService implementation
*/
public class T_DaemonService extends T_MultiThreadedIterations 
{
	private static DaemonService testDaemon;
	private static Random random;

	/*
	 * fields for testing serviceable, one per test object
	 */
	private Vector serviceRecord; // a vectory of T_Serviceable

	public T_DaemonService()
	{
		super();
		serviceRecord = new Vector(9, 1);
		random = new Random();
	}


	/*
	** Methods required by T_Generic
	*/

	protected String getModuleToTestProtocolName() {
		return org.apache.derby.iapi.reference.Module.DaemonFactory;
	}

	/**
	** Methods required by T_MultiIterations
	** @exception T_Fail unexpected behaviour from the API
	*/
	protected void setupTest() throws T_Fail
	{

		DaemonFactory daemonFactory;
		try {
			daemonFactory = (DaemonFactory)Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.DaemonFactory);
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}
		if (daemonFactory == null)
			throw T_Fail.testFailMsg("cannot find daemon factory " + org.apache.derby.iapi.reference.Module.DaemonFactory);
			
		try
		{
			testDaemon = daemonFactory.createNewDaemon("testDaemon");
		}
		catch (StandardException se)
		{
			throw T_Fail.exceptionFail(se);
		}
		if (testDaemon == null)
			throw T_Fail.testFailMsg("cannot create new Daemon Service");


	}


	/**
	** Methods required by T_MultiThreadedIterations
	** @exception T_Fail unexpected behaviour from the API
	*/
	protected void joinSetupTest() throws T_Fail
	{
		if (testDaemon == null)
			throw T_Fail.testFailMsg("test deamon not set");
	}

	protected T_MultiThreadedIterations newTestObject()
	{
		return new T_DaemonService(); // clone myself
	}


	/**
		@exception T_Fail - test failed
	*/
	protected void runTestSet() throws T_Fail
	{
		// we don't want t_checkStatus() to hang because of
		// unsubscribed records from a previous, failed iteration
		// (DERBY-989)
		serviceRecord.clear();

		try
		{
			/* test basic DaemonService interface */
			T01(testDaemon);	// basic subscription
			T02(testDaemon);	// basic enqueue
			T03(testDaemon);	// mixture of everything

			t_checkStatus(testDaemon);	// make sure all serviceables I created got serviced
		}
		catch (StandardException se)
		{
			throw T_Fail.exceptionFail(se);			
		}

	}

	/*
	 *  tests
	 */

	/* test 1 - basic subscription */
	private void T01(DaemonService daemon) throws T_Fail, StandardException
	{
		// add a couple of subscriptions to the deamon
		T_Serviceable s1 = new T_Serviceable(false);  // not on demand
		serviceRecord.addElement(s1);
		int clientNumber1 = daemon.subscribe(s1, false);
		s1.setClientNumber(clientNumber1);

		T_Serviceable s2 = new T_Serviceable(true);  // on demand only
		serviceRecord.addElement(s2);
		int clientNumber2 = daemon.subscribe(s2, true);
		s2.setClientNumber(clientNumber2);

		daemon.serviceNow(clientNumber2); // s2 should be serviced exactly once

		s2.t_wait(1); // wait for s2 to be serviced

		randomSleep();

		// don't demand service, let daemon service it by itself
		s1.t_wait(1); // wait for s1 to be serviced

		s2.t_check(1);  // s2 should be serviced exactly once

		PASS("T01");

		randomSleep();
	}

	/* test 1 - basic enqueue */
	private void T02(DaemonService daemon) throws T_Fail, StandardException
	{
		int requeue = 10;

		T_Serviceable e1 = new T_Serviceable(1); // service now and don't requeue
		serviceRecord.addElement(e1);
		daemon.enqueue(e1, true);

		T_Serviceable e2 = new T_Serviceable(requeue); // service now and requeue
		serviceRecord.addElement(e2);
		daemon.enqueue(e2, true);

		T_Serviceable e3 = new T_Serviceable(1); // don't requeue
		serviceRecord.addElement(e3);
		daemon.enqueue(e3, false);

		T_Serviceable e4 = new T_Serviceable(requeue); // requeue
		serviceRecord.addElement(e4);
		daemon.enqueue(e4, false);

		randomSleep();

		e1.t_wait(1);				// make sure they are all serviced at least once
		e2.t_wait(1);
		e3.t_wait(1);
		e4.t_wait(1);

		e2.t_wait(requeue);	// e2 and e4 are requeued
		e4.t_wait(requeue);	// e2 and e4 are requeued

		// meanwhile, e1 and e3 should not be service more than once
		e1.t_check(1);
		e3.t_check(1);

		PASS("T02");

		randomSleep();
	}

	/* test 4 - mixture */
	private void T03(DaemonService daemon) throws T_Fail, StandardException
	{
		T_Serviceable s1 = new T_Serviceable(false);  // unsubscribe this laster
		serviceRecord.addElement(s1);
		int sub1 = daemon.subscribe(s1, false);

		T_Serviceable e1 = new T_Serviceable(1); 
		serviceRecord.addElement(e1);
		daemon.enqueue(e1, false); // enqueue the same thing 5 times
		daemon.enqueue(e1, false);
		daemon.enqueue(e1, false);
		daemon.enqueue(e1, false);
		daemon.enqueue(e1, false);

		T_Serviceable s2 = new T_Serviceable(false); // not on demand
		serviceRecord.addElement(s2);
		int sub2 = daemon.subscribe(s2, false); 
		int realsub2 = daemon.subscribe(s2, false); 
		s2.setClientNumber(realsub2);

		daemon.unsubscribe(sub1);
		daemon.unsubscribe(sub2); // it has another subscriptions

		int save;
		synchronized(s1)
		{
			save = s1.timesServiced;
		}
		daemon.serviceNow(sub1); // should be silently igored

		randomSleep();

		e1.t_wait(5);			// it is enqueued 5 times, it should be serviced 5 times

		daemon.serviceNow(sub1); // should be silently igored

		s2.t_wait(3);		// wait long enough for it to be serviced at least 3 times

		daemon.serviceNow(sub1); // should be silently igored

		synchronized(s1)
		{
			// DERBY-989: The client should not be serviced after it
			// unsubscribes. However, it might have been in the
			// process of being serviced when unsubscribe() was
			// called. Therefore, performWork() can run even after the
			// save variable was initialized, but only once.
			int diff = s1.timesServiced - save;
			// Check that the client has not been serviced more than
			// once after it unsubscribed.
			T_Fail.T_ASSERT((diff == 0 || diff == 1),
							"unsubscribed continue to get serviced");

			// unsubscribed can subscribe again
			s1.timesServiced = 0;
		}
		
		sub1 = daemon.subscribe(s1, false); // resubscribe
		s1.setClientNumber(sub1);
		daemon.serviceNow(sub1);
		s1.t_wait(1);

		// e1 should not be serviced for > 5 times
		e1.t_check(5);

		PASS("T03");
		randomSleep();

	}

	private void t_checkStatus(DaemonService daemon) throws T_Fail
	{
		for (int i = 0; i < serviceRecord.size(); i++)
		{
			T_Serviceable check = (T_Serviceable)serviceRecord.elementAt(i);
			if (check != null)
			{
				if (check.subscribed)
				{
					if (check.onDemandOnly)
						check.t_check(1);
					else
						check.t_wait(10); // sooner or later, it will be serviced this many times

					daemon.unsubscribe(check.getClientNumber());
				}
				else			// enqueued
				{
					check.t_wait(check.timesRequeue);
				}
			}
		}
		PASS("T_CheckStatus");
	}

	private void randomSleep()
		 throws StandardException
	{
		// randomly sleep for a bit if this is a multi-threaded test to make it more interesting
		if (getNumThreads() > 1)
		{
			int nap = random.nextInt()%100;
			if (nap < 0) nap = -nap;
			try
			{
				Thread.sleep(nap);
			}
			catch (InterruptedException ie)
			{
				throw StandardException.interrupt(ie);
			}
		}	
	}

}
