/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.tests.derbynet
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derbyTesting.functionTests.tests.tools.dblook_test;

public class dblook_test_net extends dblook_test {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

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
