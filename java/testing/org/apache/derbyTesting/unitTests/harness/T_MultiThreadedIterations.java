/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations

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

package org.apache.derbyTesting.unitTests.harness;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;

import java.util.Properties;

/**
	Abstract class which executes T_MultiIterations. This allows multiple
	threads running T_MultiIterations.

	This allows the setup to be performed once, and then the
	test itself to be run with multiple threads for a number of iterations. 
	The number of threads and iterations are set by the property 
	derby.unittests.numThreads and derby.unittests.iterations
	and default to 1.
	<P>
	Statistics are provided about each iteration in the error log. The statistics
	are time for each iteration, used and total memory changes per iteration.

	@see T_Generic
*/
public abstract class T_MultiThreadedIterations extends T_MultiIterations implements Runnable
{
	protected int threadNumber = 0;

	static volatile boolean inError = false;

	static int numThreads = 1;
	static int iterations = 1;

	Throwable error = null;
	static Thread[] TestThreads;
	static T_MultiThreadedIterations[] TestObjects;

	protected T_MultiThreadedIterations()
	{
		super();
	}

	/**
	  Run the test. The test should raise an exception if it
	  fails. runTests should return if the tests pass.

	  @exception T_Fail Test code throws these
	  */
	protected void runTests() throws T_Fail 
	{
		/*
		** The property name for the number of iterations is
		** derby.className.numThreads.  For example, if the test
		** class is derby.com.package.to.test.T_Tester,
		** the property name is derby.T_Tester.numThreads.
		*/
		String myClass = this.getClass().getName();
		String noPackage = myClass.substring(myClass.lastIndexOf('.') + 1);
		String propertyName = "derby." + noPackage + ".numThreads";

		String nthread = PropertyUtil.getSystemProperty(propertyName);
		if (nthread != null) {
			try {
					numThreads = Integer.parseInt(nthread);
			} catch (NumberFormatException nfe) {
				numThreads = 1;
			}
			if (numThreads <= 0)
				numThreads = 1;
		}

		if (numThreads == 1)	// just use this thread
			super.runTests();	// use T_MultiIterations runtest
		else
		{
			// start numThreads new threads, each with its own test object
			TestThreads = new Thread[numThreads];
			TestObjects = new T_MultiThreadedIterations[numThreads];

			inError = false;

			for (int i = 0; i < numThreads; i++)
			{
				TestObjects[i] = newTestObject();
				TestObjects[i].out = this.out;

				TestThreads[i] = new Thread(TestObjects[i], "Thread_" + i);
			}

			// use the first test object to setup the test
			TestObjects[0].setupTest();
			TestObjects[0].threadNumber = 0;

			// make the other test objects to join in the setup
			for (int i = 1; i < numThreads; i++)
			{
				TestObjects[i].threadNumber = i;
				TestObjects[i].joinSetupTest();
			}

			// now run them 
			propertyName = "derby." + noPackage + ".iterations";

			String iter = PropertyUtil.getSystemProperty(propertyName);
			if (iter != null) {
				try {
					iterations = Integer.parseInt(iter);
				} catch (NumberFormatException nfe) {
					// leave at one
				}
				if (iterations <= 0)
					iterations = 1;
			}

			for (int i = 0; i < numThreads; i++)
			{
				TestThreads[i].start();
			}

			// wait for the threads to end
			try
			{
				for (int i = 0; i < numThreads; i++)
				{
					TestThreads[i].join();
				}
			}
			catch (InterruptedException ie) {
				throw T_Fail.exceptionFail(ie);
			}

			// report error
			for (int i = 0; i < numThreads; i++)
			{
				if (TestObjects[i].error != null)
					throw T_Fail.exceptionFail(TestObjects[i].error);
			}
		}
	}

	/*
	 * run each worker test thread
	 */
	public void run()
	{
		String threadName = "[" + Thread.currentThread().getName() + "] ";

		out.println(threadName + "started");

		try
		{

			for (int i = 0; i < iterations; i++) 
			{
				Runtime.getRuntime().gc();
				long btm = Runtime.getRuntime().totalMemory();
				long bfm = Runtime.getRuntime().freeMemory();
				long bum = btm - bfm;

				long start = System. currentTimeMillis();

				runTestSet();
				long end = System. currentTimeMillis();

				Runtime.getRuntime().gc();
				long atm = Runtime.getRuntime().totalMemory();
				long afm = Runtime.getRuntime().freeMemory();
				long aum = atm - afm;

				out.println(threadName + "Iteration " + i + " took " + (end - start) + "ms");
				out.println(threadName + "Total memory increased by " + (atm - btm) + " is " + atm);
				out.println(threadName + "Used  memory increased by " + (aum - bum) + " is " + aum);
			}
		}
		catch (ThreadDeath death) // some other thread has died and want to see my stack 
		{
			out.println(threadName + "caught thread death, printing stack");
			death.printStackTrace(out.getPrintWriter());
			Thread.dumpStack();

			throw death;
		}
		catch (Throwable t)
		{
			error = t;
		}

		if (error == null)
			out.println(threadName + "finished with no error");
		else if (!inError)
		{
			inError = true;

			error.printStackTrace(out.getPrintWriter());
			for (int i = 0; i < numThreads; i++)
			{
				if (this != TestObjects[i]) // don't kill myself again
					TestThreads[i].interrupt();
			}
		}
	}

	/*
	 * multi threaded test abstract methods
	 */

	/* 
	 * joins an existing setup - do whatever remaining setup the test may need
	 * to do given that setupTest has already been run by another test object
	 *
	 * This call will be executed in the main (parent) thread
	 */
	protected abstract void joinSetupTest() throws T_Fail;

	/*
	 * make a new test object instance
	 */
	protected abstract T_MultiThreadedIterations newTestObject();


	/*
	 * class specific method
	 */
	protected int getNumThreads()
	{
		return numThreads;
	}
}
