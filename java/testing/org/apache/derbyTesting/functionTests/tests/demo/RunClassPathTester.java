/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.demo.RunClassPathTester

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.demo;


/**
 * @author janet
 **/
public class RunClassPathTester { 

	public static void main(String args[]) {

		String arg2 = "embedded";
		String env = System.getProperty("theframework", "embedded");
		String[] newargs = new String[3];
		newargs[0] = "-cp";
		newargs[1] = arg2;
		newargs[2] = "SimpleApp.class";

		org.apache.derby.tools.sysinfo.main(newargs);
	}
	
	private RunClassPathTester() {} // no instances

}
