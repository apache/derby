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

public final class LangScripts extends ScriptTestCase {
	
	/**
	 * Language SQL scripts that run under all configurations.
	 */
	private static final String[] SQL_LANGUAGE_TESTS = {
		"case",
		"constantExpression",
		};

    /**
     * Language SQL scripts that run under Derby's clients configurations.
     */
    private static final String[] DERBY_TESTS = {
        "bit2",
        "derived",
        };
    
    /**
     * Language SQL scripts that only run in embedded.
     */
    private static final String[] EMBEDDED_TESTS = {
        "arithmetic",
        "depend",
        "union",
        };	

	/**
	 * Run a set of language SQL scripts passed in on the
	 * command line.
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
        
        if (usingEmbedded() || usingDerbyNetClient())
            suite.addTest(getSuite(DERBY_TESTS));
        
        if (usingEmbedded())
            suite.addTest(getSuite(EMBEDDED_TESTS));
        
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
