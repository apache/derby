/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.T_Generic

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

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derbyTesting.unitTests.harness.UnitTest;
import org.apache.derbyTesting.unitTests.harness.UnitTestConstants;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;

import java.util.Properties;

/**
	Abstract class which executes a unit test.

	<P>To write a test,	extend this class with a class which implements the two
	abstract methods:
<UL>	
    <LI>runTests
	<LI>setUp
</UL>
	@see UnitTest
	@see ModuleControl
*/
public abstract class T_Generic implements UnitTest, ModuleControl
{
	/**
	  The unqualified name for the module to test. This is set by the generic
	  code.
	  */
	protected String shortModuleToTestName;

	/**
	  The start parameters for your test. This is set by generic code.
	  */
	protected Properties startParams;

	/**
	  The HeaderPrintWriter for test output. This is set by the
	  generic code.
	  */
	protected HeaderPrintWriter out;

	protected T_Generic()
	{
	}

	/*
	** Public methods of ModuleControl
	*/

	/**
	  ModuleControl.start
	  
	  @see ModuleControl#boot
	  @exception StandardException Module cannot be started.
	  */
	public void boot(boolean create, Properties startParams)
		 throws StandardException
	{
		shortModuleToTestName =
			getModuleToTestProtocolName()
			.substring(getModuleToTestProtocolName().lastIndexOf('.')+1);

		this.startParams = startParams;
	}

	/**
	  ModuleControl.stop
	  
	  @see ModuleControl#stop
	  */
	public void stop() {
	}

	/*
	** Public methods of UnitTest
	*/
	/**
	  UnitTest.Execute
	  
	  @see UnitTest#Execute
	  */
	public boolean Execute(HeaderPrintWriter out)
	{
		this.out = out;

		String myClass = this.getClass().getName();
		String testName = myClass.substring(myClass.lastIndexOf('.') + 1);

		System.out.println("-- Unit Test " + testName + " starting");

		try
		{
			runTests();
		}
		
		catch (Throwable t)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
			FAIL(t.toString());
			t.printStackTrace(out.getPrintWriter());
			return false;
		}

		System.out.println("-- Unit Test " + testName + " finished");

		return true;
	}

	/**
	  UnitTest.UnitTestDuration
	  
	  @return UnitTestConstants.DURATION_MICRO
	  @see UnitTest#UnitTestDuration
	  @see UnitTestConstants
	  */
	public int UnitTestDuration() {
		return UnitTestConstants.DURATION_MICRO;
	}

	/**
	  UnitTest.UnitTestType
	  
	  @return UnitTestConstants.TYPE_COMMON
	  @see UnitTest#UnitTestType
	  @see UnitTestConstants
	  */
	public int UnitTestType() {
		return UnitTestConstants.TYPE_COMMON;
	}

	/**
	  Emit a message indicating why the test failed.

	  RESOLVE: Should this be localized?

	  @param msg the message.
	  @return false
	*/
	protected boolean FAIL(String msg) {
		out.println("[" + Thread.currentThread().getName() + "] FAIL - " + msg);
		return false;
	}

	/**
	  Emit a message saying the test passed.
	  You may use this to emit messages indicating individual test cases
	  within a unit test passed.

	  <P>RESOLVE:Localize this.
	  @param testName the test which passed.
	  @return true
	  */
	protected boolean PASS(String testName) {
		out.println("[" + Thread.currentThread().getName() + "] Pass - "+shortModuleToTestName +" " + testName);
		return true;
	}

	/**
		Emit a message during a unit test run, indent the message
		to allow the PASS/FAIL messages to stand out.
	*/
	public void REPORT(String msg) {
		out.println("[" + Thread.currentThread().getName() + "]     " + msg);
	}

	
	/**
	  Abstract methods to implement for your test.
	  */
	
	/**
	  Run the test. The test should raise an exception if it
	  fails. runTests should return if the tests pass.

	  @exception Exception Test code throws these
	  */
	protected abstract void runTests() throws Exception;

	/**
	  Get the name of the protocol for the module to test.
	  This is the 'factory.MODULE' variable.
	  
	  'moduleName' to the name of the module to test. 

	  */
	protected abstract String getModuleToTestProtocolName();
}
