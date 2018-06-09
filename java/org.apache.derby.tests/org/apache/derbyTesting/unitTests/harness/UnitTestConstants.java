/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.UnitTestConstants

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

/**
 * UnitTestConstants contains the constants for the
 * unit tests to use when registering and running
 * the tests.
 *
 */
public interface UnitTestConstants 
{
	/**
	  * the duration of a test can be from MICRO to FOREVER.
	  * <p>
	  * MICRO means the test is practically nothing more than
	  * the call; a simple field examination, for example.
	  */
	static final int DURATION_MICRO = 0;
	/**
	  * SHORT means the test is less than a second.
	  */
	static final int DURATION_SHORT = 1;
	/**
	  * MEDIUM means the test is less than 30 seconds.
	  */
	static final int DURATION_MEDIUM = 2;
	/**
	  * LONG means the test might take 1-5 minutes.
	  */
	static final int DURATION_LONG = 3;
	/**
	  * FOREVER means the test takes more than 5 minutes,
	  * or could loop forever if it fails.
	  */
	static final int DURATION_FOREVER = 4;
	
	/**
	  * The TYPE of test says what sort of completeness it
	  * tests; its thoroughness.
	  * <p>
	  * Note the types given here are ordered from simple to
	  * comprehensive. Each category of tests includes 
	  * tests in earlier, simpler catagories. Thus all SANITY
	  * tests are also BASIC tests.
	  */
	
	/**
	  * SANITY means the test is simply a check that
	  * the system is running.  Little more than a consistency
	  * check would be done.
	  */
	static final int TYPE_SANITY = 0;
	/**
	  * BASIC means the test is a basic check that
	  * the system is working. A single, very common construct
	  * would be verified to be working.
	  */
	static final int TYPE_BASIC = 1;
	/**
	  * COMMON means the test verify that the most common 
	  * cases of use of this object are working properly.
	  */
	static final int TYPE_COMMON = 2;
	/**
	  * COMPLETE means that the tests verify that the
	  * object is performing all expected functionality
	  * correctly.
	  */
	static final int TYPE_COMPLETE = 3;
	
	
}// UnitTestConstants

