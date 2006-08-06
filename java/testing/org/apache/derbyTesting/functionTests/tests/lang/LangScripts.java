/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LangScripts
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

public final class LangScripts extends ScriptTestCase {
	
	/**
	 * All the langauge SQL scripts to be run as JUnit tests.
	 */
	private static final String[] SQL_LANGUAGE_TESTS = {
		"arithmetic",
		"bit2",
		"case",
		"union",
		};
	
	/**
	 * Run a set of langauge SQL scripts passed in on the
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
    	return getSuite(SQL_LANGUAGE_TESTS);
    }
    
	/*
	 * A single JUnit test that runs a single language SQL script.
	 */
	private LangScripts(String langTest){
		super(langTest);
	}
	
    /**
     * Return a suite of language SQL tests from the list of
     * script names.
      */
	private static Test getSuite(String[] list)
	{
        TestSuite suite = new TestSuite();
        for (int i = 0; i < list.length; i++)
            suite.addTest(new LangScripts(list[i]));

        return getIJConfig(suite);
    }
}
