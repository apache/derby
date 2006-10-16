/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LangScripts
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
package org.apache.derbyTesting.functionTests.tests.lang;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * LangScripts runs SQL scripts (.sql files) in the lang package
 * and compares the output to a canon file in the
 * standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program to run one or more
 * language based SQL scripts as tests.
 *
 */
public final class LangScripts extends ScriptTestCase {
	
	/**
	 * Language SQL scripts (.sql files) that run under all configurations.
	 */
	private static final String[] SQL_LANGUAGE_TESTS = {
		"case",
		"constantExpression",
		};

    /**
     * Language SQL scripts (.sql files) that run under Derby's client
     * and emebdded configurations.
     */
    private static final String[] DERBY_TESTS = {
        "bit2",
        "derived",
        };
    
    /**
     * Language SQL scripts (.sql files) that only run in embedded.
     */
    private static final String[] EMBEDDED_TESTS = {
        "arithmetic",
        "depend",
        "union",
        };	

	/**
	 * Run a set of language SQL scripts (.sql files) passed in on the
	 * command line. Note the .sql suffix must not be provided as
     * part of the script name.
	 * <code>
	 * example
	 * java org.apache.derbyTesting.functionTests.tests.lang.LangScripts case union
	 * </code>
	 */
	public static void main(String[] args)
	{
		junit.textui.TestRunner.run(getSuite(args));
	}

	/**
	 * Return the suite that runs all the langauge SQL scripts.
	 */
	public static Test suite() {
        
        TestSuite suite = new TestSuite();
        suite.addTest(getSuite(SQL_LANGUAGE_TESTS));
        suite.addTest(getSuite(DERBY_TESTS));
        suite.addTest(getSuite(EMBEDDED_TESTS));
        
        // Set up the scripts run with the network client
        TestSuite clientTests = new TestSuite();
        clientTests.addTest(getSuite(SQL_LANGUAGE_TESTS));
        clientTests.addTest(getSuite(DERBY_TESTS));
        Test client = TestConfiguration.derbyClientServerDecorator(clientTests);
        
        // add those client tests into the top-level suite.
        suite.addTest(client);

        return suite;
    }
    
	/*
	 * A single JUnit test that runs a single language SQL script.
	 */
	private LangScripts(String langTest){
		super(langTest);
	}
	
    /**
     * Return a suite of language SQL tests from the list of
     * script names. Each test is surrounded in a decorator
     * that cleans the database.
      */
	private static Test getSuite(String[] list)
	{
        TestSuite suite = new TestSuite();
        for (int i = 0; i < list.length; i++)
            suite.addTest(
            		new CleanDatabaseTestSetup(
            		new LangScripts(list[i])));

        return getIJConfig(suite);
    }
}
