/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testProtocol

   Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derby.impl.drda.TestProto;


/**
	This tests protocol errors and protocol not used by JCC or derbyclient
*/

public class testProtocol { 

	private static final String DEFAULT_FILENAME = "protocol.tests";
	
	// constructor
	private testProtocol() {}
	

	/**
	 * main routine
	 */
    public static void main(String[] args) {
		
		executeFile(DEFAULT_FILENAME);
	}
	/**
	 * Execute a command file against a network server
	 *
	 * @param filename name of file to execute
	 */
	public static void executeFile(String filename)
	{
		TestProto t = new TestProto(filename);
	}
}
