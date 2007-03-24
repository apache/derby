/*

   Derby - Class org.apache.derby.impl.drda.TestFile

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.drda;

import org.apache.derby.drda.NetworkServerControl;

/**
	This tests protocol errors and protocol not used by JCC
	The file containing the test is given as an argument
*/

public class TestFile{

	
	// constructor
	private TestFile() {}
	

	/**
	 * main routine
	 */
    public static void main(String[] args) {
		
		if (args.length != 1)
		{
			System.err.println ("Usage: testFile <filename>");
			System.exit(1);
		}
		executeFile(args[0]);
	}
	/**
	 * Execute a command file against a network server
	 *
	 * @param filename name of file to execute
	 */
	public static void executeFile(String filename)
	{
		TestProto t = new TestProto(filename, NetworkServerControl.DEFAULT_PORTNUMBER);
	}
}
