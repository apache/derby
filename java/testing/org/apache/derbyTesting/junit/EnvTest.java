/*
 *
 * Derby - Class org.apache.derbyTesting.junit.EnvTest
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
package org.apache.derbyTesting.junit;

import junit.framework.TestCase;

/**
 * Simple Junit "test" that runs a number of fixtures to
 * show the environment a test would run in.
 * A fixture changes its name based upon the return
 * of a method that checks for some environmental condition,
 * e.g. does this vm support JDBC 3.
 * Meant as a simple aid to help determine any environment problems.
 *
 */
public class EnvTest extends TestCase {
	
	public EnvTest(String name)
	{
		super(name);
	}
	/*
	** Tests of the JDBC.vmSupportsXXX to see which JDBC support is available.
	*/
	public void testJSR169() {
		setName(JDBC.vmSupportsJSR169() + "_vmSupportsJSR169()");
	}
	public void testJDBC3() {
		setName(JDBC.vmSupportsJDBC3() + "_vmSupportsJDBC3()");
	}
	public void testJDBC4() {
		setName(JDBC.vmSupportsJDBC4() + "_vmSupportsJDBC4()");
	}
	/*
	** Tests of the Derby.hasXXX to see which Derby code is
	** available for the tests.
	*/
	public void testHasServer() {
		setName(Derby.hasServer() + "_hasServer");
	}
	public void testHasClient() {
		setName(Derby.hasClient() + "_hasClient");
	}
	public void testHasEmbedded() {
		setName(Derby.hasEmbedded() + "_hasEmbedded");
	}
	public void testHasTools() {
		setName(Derby.hasTools() + "_hasTools");
	}
    /*
    ** XML related tests
    */
    public void testClasspathHasXalanAndJAXP() {
        setName(XML.classpathHasXalanAndJAXP() + "_classpathHasXalanAndJAXP");
    }
    public void testClasspathMeetsXMLReqs() {
        setName(XML.classpathMeetsXMLReqs() + "_classpathMeetsXMLReqs");
    }
}
