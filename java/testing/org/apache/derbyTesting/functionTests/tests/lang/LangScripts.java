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

public class LangScripts extends ScriptTestCase {
	
	private static final String[] SQL_LANGUAGE_TESTS = {
		"arithmetic",
		"bit2",
		"case",
		"union",
		};
	
	LangScripts(String langTest){
		super(langTest);
	}

	/**
	 * SQL scripts in the lang folder.
	 */
	protected String getArea() {
		return "lang";
	}
	
    public static Test suite() {
        TestSuite suite = new TestSuite();
        for (int i = 0; i < SQL_LANGUAGE_TESTS.length; i++)
            suite.addTest(new LangScripts(SQL_LANGUAGE_TESTS[i]));

        return getIJConfig(suite);
    }
}
