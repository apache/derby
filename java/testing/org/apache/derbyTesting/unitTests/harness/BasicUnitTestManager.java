/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.BasicUnitTestManager

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

package org.apache.derbyTesting.unitTests.harness;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derbyTesting.unitTests.harness.UnitTest;
import org.apache.derbyTesting.unitTests.harness.UnitTestConstants;
import org.apache.derbyTesting.unitTests.harness.UnitTestManager;
import org.apache.derbyTesting.unitTests.harness.T_Bomb;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.util.Hashtable;

public class BasicUnitTestManager implements UnitTestManager, ModuleControl
{
	private Vector vectorOfTests;
	private Hashtable	namesOfTests;

	private	static	boolean	alreadyRun = false;
	private HeaderPrintWriter output;
	private HeaderPrintWriter currentOutput;
	private int testType = UnitTestConstants.TYPE_COMPLETE;
	private int testDuration = UnitTestConstants.DURATION_FOREVER;
	private boolean reportOutputOn = true;
	private boolean performanceReportOn = false;
	private ContextService contextService;
	private boolean runForever = false; 

	/*
	** Constructor
	*/

	public BasicUnitTestManager() {
	}

	/*
	** Methods of ModuleControl
	*/
	public void boot(boolean create, Properties startParams)
		 throws StandardException
	{
		boolean	testStatus = true;

		// startParams should define output, for now
		// use the sytem trace stream. If that doesn't exist
		// then use a null stream.

		output = Monitor.getStream();

		contextService = ContextService.getFactory();

		this.currentOutput = output;

		vectorOfTests = new Vector();
		namesOfTests = new Hashtable();

		findTests(startParams, startParams);
		try {
			findTests(System.getProperties(), startParams);
		} catch (SecurityException se) {
		}
		findTests(Monitor.getMonitor().getApplicationProperties(), startParams);

		if ( !alreadyRun )
		{
			testStatus = runTests();
			alreadyRun = true;
		}

		if (!testStatus) {

			// try to print out that the shutdown is occurring.
			System.out.println("Shutting down due to unit test failure.");
			output.printlnWithHeader("Shutting down due to unit test failure, see log for more information.");

			Monitor.getMonitor().shutdown();
		}
	}

	public void stop(){
		return;
	}

	public synchronized void registerTest(UnitTest objectToTest, String testName){

		// only add the new test if it isn't already there.
		// otherwise you will upset me.
		if ( !namesOfTests.containsKey( testName ) )
		{
			vectorOfTests.addElement(objectToTest);
			namesOfTests.put( testName, testName );
		}
	}

	private void findTests(Properties testList, Properties startParams) {

		if (testList == null)
			return;

		for (Enumeration e = testList.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
			if (key.startsWith("derby.module.test.")) {
				String unitTestClass = testList.getProperty(key);

				try {
					Object unitTest =
						Monitor.bootServiceModule(false, this, unitTestClass,
												  startParams);
					if (unitTest instanceof UnitTest) {
						registerTest((UnitTest) unitTest, unitTestClass);
					} else if (unitTest != null) {
					System.out.println("class does not implement UnitTest " +
									   unitTestClass);
					}
				} catch (StandardException se) {
					System.out.println("exception booting " + unitTestClass);
					System.out.println(se.toString());
					se.printStackTrace(System.out);
				}
			}
		}
	}

	/**
	 *	emitAMessage
	 *
	 *	Convenience routine to emit messages. This routine only works
	 *  for messages provided by this package.
	 *
	 *  @see	UnitTestConstants for supported durations.
	 **/
	private void emitAMessage(String message){

	   currentOutput.printlnWithHeader(message);
	}

	private boolean runATest(UnitTest aTest){

		boolean result;

		String thisTestName = aTest.getClass().getName();
		Date startTime = null, endTime;

		// push a new context manager
		ContextManager cm = null;
		if (contextService != null) {
			cm = contextService.newContextManager();
			contextService.setCurrentContextManager(cm);
		}

		if (performanceReportOn)
			startTime = new Date();

		try{
			emitAMessage("Starting test  '" + thisTestName + "'.");
			result = aTest.Execute(currentOutput);
			if (result == true)
				emitAMessage("Test '" + thisTestName + "' passed");
			else
				emitAMessage("Test '" + thisTestName + "' failed");

		} catch (Throwable t) {
			if (t instanceof ThreadDeath)
			{
				t.printStackTrace(output.getPrintWriter());
				Runtime.getRuntime().exit(1);
			}

			result = false;
			String  msg = t.getMessage();
			if (msg == null) msg = t.getClass().getName();
			emitAMessage("Test '" + thisTestName + "' failed with exception '" + msg +"'.");
			t.printStackTrace(output.getPrintWriter());
		} finally {

			if (contextService != null) {
				//
				//Assure the completed test does not stick around
				//cm.cleanupOnError
				//	(BasicUnitTestDatabaseException.cleanUp());
				contextService.resetCurrentContextManager(cm);
			}
		}

		if (performanceReportOn){
			endTime = new Date();
			emitAMessage("Test '" + thisTestName + "' took " + new Long(endTime.getTime() - startTime.getTime()) + " milliseconds.");
		}

		return result;
	}

	// STUB: Verify its ok this is synchronized.
	public synchronized boolean runTests(){

		boolean result = true;
		int passCount = 0;
		int failCount = 0;
		int skipCount = 0;
		boolean runTests = true;

		if (SanityManager.DEBUG)
		{
			runTests =
				!SanityManager.DEBUG_ON(UnitTestManager.SKIP_UNIT_TESTS);
			runForever =
				SanityManager.DEBUG_ON(UnitTestManager.RUN_FOREVER);
		}
		if (runTests) {

		if (!runForever) T_Bomb.makeBomb();
		for(int ix = vectorOfTests.size() - 1; ix >= 0 ; ix--){

			UnitTest thisTest =
				((UnitTest)vectorOfTests.elementAt(ix));
			if (thisTest.UnitTestDuration() <= this.testDuration &&
				thisTest.UnitTestType() <= this.testType){
				if (runATest(thisTest))
					passCount++;
				else
					failCount++;
				vectorOfTests.removeElementAt(ix);
			}
			else{
				skipCount++;
			}
		}
		emitAMessage("Test Summary - Run " + (passCount+failCount) +
			", Passed " + passCount + ", Failed " + failCount + ", Skipped " + skipCount + ".");
		}
		else {
			emitAMessage("Tests not run.");
		}
		return (failCount == 0);
	}


	public boolean runTests(int testType, int testDuration){
		//STUB: Sanity check for type/duration
		this.testType = testType;
		this.testDuration = testDuration;
		return runTests();
	}


	public void setTestDuration(int testDuration){
		//STUB: Sanity check for type/duration
		this.testDuration = testDuration;
		return;
	}


	public void setTestType(int testType){
		//STUB: Sanity check for type/duration
		this.testType = testType;
		return;
	}


	public void setPerformanceReportOn(boolean performanceReportOn){
		this.performanceReportOn = performanceReportOn;
		return;
	}	
}

