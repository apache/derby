/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi._Suite

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test; 
import junit.framework.TestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.jdbcapi
 *
 */
public class _Suite extends BaseTestCase  {

	/**
	 * Use suite method instead.
	 */
	private _Suite(String name) {
		super(name);
	}

	public static Test suite() {

		TestSuite suite = new TestSuite("jdbcapi");

		suite.addTest(ConcurrencyTest.suite());
		suite.addTest(HoldabilityTest.suite());
		suite.addTest(ProcedureTest.suite());
		suite.addTest(SURQueryMixTest.suite());
		suite.addTest(SURTest.suite());
		suite.addTest(UpdateXXXTest.suite());
		suite.addTestSuite(URCoveringIndexTest.class);
        suite.addTest(ResultSetCloseTest.suite());
        suite.addTest(DataSourcePropertiesTest.suite());
		
		// Tests that are compiled using 1.4 target need to
		// be added this way, otherwise creating the suite
		// will throw an invalid class version error
		if (JDBC.vmSupportsJDBC3() || JDBC.vmSupportsJSR169())
		{
			suite.addTest(ScrollResultSetTest.suite());
		}
        
        if (TestConfiguration.runningInDerbyHarness())
            return new NetworkServerTestSetup(suite);
        
        return suite;
	}
}
