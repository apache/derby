/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.T_Fail

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

/**
	Exception used to throw for errors in a unit test.
*/
public class T_Fail extends Exception  {

	private Throwable nested;

	/**
	  Create a T_Fail exception which carries a message.

	  @param messageId An Id for an error message for this exception.
	  */
	private T_Fail(String message) {
		super(message);
	}

	/**
		return a T_Fail exception to indicate the configuration does
		not specify the module to test.

		@return The exception.
	*/
	public static T_Fail moduleToTestIdNotFound()
	{
		return new T_Fail("Test failed because the configuration does not include the MODULE_TO_TEST_IDENT attribute.");
	}

	/**
		return a T_Fail exception to indicate the configuration does
		not contain the module to test.

		@return The exception.
	*/
	public static T_Fail moduleToTestNotFound(String moduleToTest)
	{
		return new T_Fail("Test failed due to failure loading " + moduleToTest);
	}

	/**
	  return a T_Fail exception to indicate the test failed due
	  to an exception.

	  <P>Note: Since the Test Service catches all exceptions this
	  seems to be of limited value.

	  @return The exception.
	*/
	public static T_Fail exceptionFail(Throwable e)
	{
		T_Fail tf = new T_Fail("The test failed with an exception: " + e.toString());
		tf.nested = e;
		return tf;
	}

	/**
	  return a T_Fail exception to indicate the test failed.

	  @return the exception.
	  */
	public static T_Fail testFail()
	{
		return new T_Fail("The test failed");
	}

	/**
	  return a T_Fail exception which includes a user message indicating
	  why a test failed.

	  @return The exception.
	*/
	public static T_Fail testFailMsg(String message)
	{
		return new T_Fail("Test failed - " + message);
	}

	/**
	  Check a test condition. If it is false, throw a T_Fail exception.

	  @param mustBeTrue The condition.
	  @exception T_Fail A test failure exception
	  */
	public static final void T_ASSERT(boolean mustBeTrue)
		 throws T_Fail
	{
		if (!mustBeTrue)
			throw testFail();
	}

	/**
	  Check a test condition. If it is false, throw a T_Fail exception which
	  includes a message.

	  @param mustBeTrue The condition.
	  @param msg A message describing the failue.
	  @exception T_Fail A test failure exception
	  */
	public static final void T_ASSERT(boolean mustBeTrue,String msg)
		 throws T_Fail
	{
		if (!mustBeTrue)
			throw testFailMsg(msg);
	}
}
