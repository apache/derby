/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.dblook_test_net

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
		renameDbLookLog("dblook_test_net");
//IC see: https://issues.apache.org/jira/browse/DERBY-3458

	}

	/* **********************************************
	 * doTest
	 * Run a test of the dblook utility using
	 * Network Server.
	 ****/

	protected void doTest() {

		try {

			createTestDatabase(dbCreationScript_1);
//IC see: https://issues.apache.org/jira/browse/DERBY-90

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
