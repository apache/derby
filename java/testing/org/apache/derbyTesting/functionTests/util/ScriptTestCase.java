/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.ScriptTestCase
 *
 * Copyright 2005,2006 The Apache Software Foundation or its 
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
package org.apache.derbyTesting.functionTests.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * Run a .sql script as a test comparing it to
 * a master output file.
 *
 */
public abstract class ScriptTestCase extends BaseJDBCTestCase {

	/**
	 * Create a ScriptTestCase to run a single test. 
	 * @param script Base name of the .sql script
	 * excluding the .sql suffix.
	 */
	public ScriptTestCase(String script)
	{
		super(script);
	}
	
	/**
	 * Return the folder (last element of the package) where
	 * the .sql script lives, e.g. lang.
	 * @return
	 */
	protected abstract String getArea();
	
	/**
	 * Get a decorator to setup the ij in order
	 * to run the test. A sub-class must decorate
	 * its suite using this call.
	 */
	public static TestSetup getIJConfig(Test test)
	{
		Properties ij = new Properties();

		//TODO: set from configuration.
		ij.setProperty("ij.driver", "org.apache.derby.jdbc.EmbeddedDriver");
		ij.setProperty("ij.database", "jdbc:derby:wombat");

		ij.setProperty("ij.showNoConnectionsAtStart", "true");
		ij.setProperty("ij.showNoCountForSelect", "true");
			
		return new SystemPropertyTestSetup(test, ij);
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
			"/org/apache/derbyTesting/functionTests/tests/"
			+ getArea() + "/"
			+ getName() + ".sql";
		
		String canon =
			"org/apache/derbyTesting/functionTests/master/"
			+ getName() + ".out";
		
		PrintStream originalOut = System.out;
		
		ByteArrayOutputStream rawBytes;
		try {
			
			 rawBytes = new ByteArrayOutputStream(20 * 1024);
			
			PrintStream printOut = new PrintStream(rawBytes);
			System.setOut(printOut);
		
			org.apache.derby.tools.ij.main(new String[] {"-fr", resource});
			
			printOut.flush();
			
		} finally {
			System.setOut(originalOut);
		}
		
		byte[] testRawBytes = rawBytes.toByteArray();
		
		try {
			URL canonURL = getTestResource(canon);
			assertNotNull("No master file " + canon, canonURL);
				
			InputStream canonStream = getTestResource(canon).openStream();
			
			BufferedReader cannonReader = new BufferedReader(
					new InputStreamReader(canonStream));
			
			BufferedReader testOutput = new BufferedReader(
					new InputStreamReader(
					new ByteArrayInputStream(testRawBytes)));
			
			testOutput.readLine();
			
			for (int lineNumber = 1; ; lineNumber++)
			{
				String testLine = testOutput.readLine();
				
				// Skip blank lines.
				if ("".equals(testLine))
					continue;
				
				String canonLine = cannonReader.readLine();
					
				if (canonLine == null && testLine == null)
					break;
				
				if (canonLine == null)
					fail("More output from test than expected");
				
				if (testLine == null)
					fail("Less output from test than expected, stoped at line"
							+ lineNumber);
										
				assertEquals("Output at line " + lineNumber, canonLine, testLine);
			}
			
			cannonReader.close();
			testOutput.close();
		} catch (Throwable t) {
			FileOutputStream outFile = new FileOutputStream(getName() + ".out");
			outFile.write(testRawBytes);
			outFile.flush();
			outFile.close();
			throw t;
		}
	}
}
