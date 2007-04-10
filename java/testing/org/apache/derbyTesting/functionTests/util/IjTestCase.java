/*
 * Derby - Class org.apache.derbyTesting.functionTests.util.ExecIjTestCase
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.junit.SupportFilesSetup;


/**
 * Run a .sql script via ij's main method and compare with a canon.
 * 
 * Tests that extend this class should always wrap their suite with
 * a SupportFilesSetup so that the extinout directory where ij will
 * write the test output is created. 
 */
public class IjTestCase extends ScriptTestCase {

	String scriptName;
	String outfileName;
    File outfile;
	
    /**
     * Create a script testcase that runs the .sql script with the
     * given name. The name should not include the ".sql" suffix.
     */
	public IjTestCase(String name) {
		super(name);
		scriptName = getName() + ".sql";
		outfileName = SupportFilesSetup.EXTINOUT + "/" + getName() + ".out";
		outfile = new File(outfileName);
	}
	
	public void setUp() throws Exception{
	    super.setUp();
		setSystemProperty("ij.outfile", outfileName);
		setSystemProperty("ij.defaultResourcePackage",
				"/org/apache/derbyTesting/functionTests/tests/"
				+ getArea() + "/");
	}
	
	public void tearDown() throws Exception {
		super.tearDown();
		removeSystemProperty("ij.outfile");
		removeSystemProperty("ij.defaultResourcePackage");
	}
	
	/**
	 * Run a .sql test, calling ij's main method.
	 * Then, take the output filre and read it into our OutputStream
	 * so that it can be compared via compareCanon().
	 */
	public void runTest() throws Throwable {
		String [] args = { "-fr", scriptName };
		ij.main(args);
		
		String canon =
			"org/apache/derbyTesting/functionTests/master/"
			+ getName() + ".out";
		
		final File out = outfile;
		FileInputStream fis = (FileInputStream) AccessController.doPrivileged(new PrivilegedAction() {
			public Object run() {
				FileInputStream fis = null;
				try {
					fis = new FileInputStream(out);
				} catch (FileNotFoundException e) {
					fail("Could not open ij output file.");
				}				
				return fis;
			}
		});
		OutputStream os = getOutputStream();
		int b;
		while ((b = fis.read()) != -1) {
			os.write(b);
		}
		fis.close();
		
		Boolean deleted = (Boolean) AccessController.doPrivileged(new PrivilegedAction() {
			public Object run() {
				boolean d = outfile.delete();
				
				return new Boolean(d);
			}
		});
		
		if (!deleted.booleanValue())
			println("Could not delete outfile for " + scriptName);
		
		this.compareCanon(canon);
	}
}
