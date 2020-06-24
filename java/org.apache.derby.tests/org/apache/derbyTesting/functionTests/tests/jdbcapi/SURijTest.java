/*
 * Derby - Class 
 * org.apache.derbyTesting.functionTests.tests.jdbcapi.SURijTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 *	Test that runs the SURTest_ij.sql script and compares the output 
 *	to SURTest_ij.out.
 */
public final class SURijTest extends ScriptTestCase {

	/**
	 * The test script
	 */
	private static final String[] TESTS = { "SURTest_ij" };
	
	
	/**
	 * Constructor that runs a single script.
	 * 
	 * @param script - the name of the script
	 */
	private SURijTest(String script) {
		super(script);
	}

	
	/**
	 * Return the suite that runs the script.
	 */
	public static Test suite() {

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("SURijTest");
		suite.addTest(TestConfiguration
				.clientServerDecorator(new CleanDatabaseTestSetup(
						new SURijTest(TESTS[0]))));
        suite.addTest(new CleanDatabaseTestSetup(
                        new SURijTest(TESTS[0])));
		return suite;
	}
}
