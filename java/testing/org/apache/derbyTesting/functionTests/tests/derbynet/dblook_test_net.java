/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.dblook_test_net

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derbyTesting.functionTests.tests.tools.dblook_test;

public class dblook_test_net extends dblook_test {

	// This test runs dblook on a test database using
	// a connection to the Network Server.

	public static void main (String [] args) {

		System.out.println("\n-= Start dblook (net server) Test. =-");
		separator = System.getProperty("file.separator");
		testDirectory = "dblook_test_net/";
		new dblook_test_net().doTest();
		System.out.println("\n[ Done. ]\n");

	}

	/* **********************************************
	 * doTest
	 * Run a test of the dblook utility using
	 * Network Server.
	 ****/

	protected void doTest() {

		try {

			createTestDatabase();

			// Don't let error stream ruin the diff.
			System.err.close();

			// The only test we need to run is the one for
			// Network Server; see functionTests/tools/
			// dblook_test.java.
			runTest(3, testDBName, testDBName + "_new");

		} catch (Exception e) {
			System.out.println("-=- FAILED: to complete the test:");
			e.printStackTrace();
		}

	}

}
