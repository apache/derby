/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.ScriptTestCase
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
package org.apache.derbyTesting.functionTests.util;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;

import org.apache.derbyTesting.junit.Derby;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Run a .sql script as a test comparing it to
 * a master output file.
 *
 */
public abstract class ScriptTestCase extends CanonTestCase {
	
	private final String inputEncoding;
	private final String user;

	/**
	 * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection()
	 * @param script Base name of the .sql script
	 * excluding the .sql suffix.
	 */
	public ScriptTestCase(String script)
	{
		super(script);
		inputEncoding = "US-ASCII";
		user = null;
	}
	
    /**
     * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection() with a
     * different encoding
     * @param script Base name of the .sql script
     * excluding the .sql suffix.
     */
    public ScriptTestCase(String script, String encoding)
    {
        super(script);
        inputEncoding = encoding;
		user = null;
    }

    /**
     * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection() with a
     * different encoding.
     * @param script     Base name of the .sql script
     *                   excluding the .sql suffix.
     * @param encoding   Run using encoding if not null, else use "US-ASCII".
     * @param user       Run script as user
     */
    public ScriptTestCase(String script, String encoding, String user)
    {
        super(script);

		if (encoding != null) {
			inputEncoding = encoding;
		} else {
			inputEncoding = "US-ASCII";
		}

		this.user = user;
    }

    /**
	 * Return the folder (last element of the package) where
	 * the .sql script lives, e.g. lang.
	 */
	protected String getArea() {
		
		String name =  getClass().getName();
		
		int lastDot = name.lastIndexOf('.');
		
		name = name.substring(0, lastDot);
		
		lastDot = name.lastIndexOf('.');
		
		return name.substring(lastDot+1);
	}
		
	/**
	 * Get a decorator to setup the ij in order
	 * to run the test. A sub-class must decorate
	 * its suite using this call.
	 */
	public static Test getIJConfig(Test test)
	{
        // Need the tools to run the scripts as this
        // test uses ij as the script runner.
        if (!Derby.hasTools())
            return new TestSuite("empty: no tools support");
            
		// No decorator needed currently.
		return test;
	}
	
	/**
	 * Run the test, using the resource as the input.
	 * Compare to the master file using a very simple
	 * line by line comparision. Fails at the first
	 * difference. If a failure occurs the output
	 * is written into the current directory as
	 * testScript.out, otherwise the output is only
	 * kept in memory.
	 * @throws Throwable 
	 */
	public void runTest() throws Throwable
	{
		String resource =
			"org/apache/derbyTesting/functionTests/tests/"
			+ getArea() + "/"
			+ getName() + ".sql";
		
		String canon =
			"org/apache/derbyTesting/functionTests/master/"
			+ getName() + ".out";

		URL sql = getTestResource(resource);
		assertNotNull("SQL script missing: " + resource, sql);
		
		InputStream sqlIn = openTestResource(sql);

		Connection conn;

		if (user != null) {
			conn = openUserConnection(user);
		} else {
			conn = getConnection();
		}

		org.apache.derby.tools.ij.runScript(
				conn,
				sqlIn,
				inputEncoding,
                getOutputStream(),
				outputEncoding);
		
		if (!conn.isClosed() && !conn.getAutoCommit())
		    conn.commit();
		
		sqlIn.close();
        
        this.compareCanon(canon);
	}
}
