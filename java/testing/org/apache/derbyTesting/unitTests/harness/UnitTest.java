/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.UnitTest

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

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

/**
 * The UnitTest interface is implemented by the class
 * that tests a class.  Such a class has the name XUnitTest
 * and is the test for the class named X.
 * <p>
 *
 * @version 0.1
 * @author Ames
 */


public interface UnitTest 
{
	/**
	 * Execute the test.
	 *
	 * @param out	A HeaderPrintWriter the test may use for tracing.
	 *				To disable tracing the caller may provide a
	 *				HeaderPrintWriter which throws away all the data
	 *				the test writes.
	 *
	 * @return	true iff the test passes
	 */
	public boolean Execute (HeaderPrintWriter out);

	/**
	 *	UnitTestDuration
	 *
	 *  @return	The tests duration. 
	 *
	 *  @see	UnitTestConstants
	 **/
	public int UnitTestDuration();

	/**
	 *	UnitTestDuration
	 *
	 *  @return	The tests duration. 
	 *
	 *  @see	UnitTestConstants
	 **/
	public int UnitTestType();

}

