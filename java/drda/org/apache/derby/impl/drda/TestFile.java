/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.drda
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.drda;


/**
	This tests protocol errors and protocol not used by JCC
	The file containing the test is given as an argument
*/
	/**
		IBM Copyright &copy notice.
	*/


public class TestFile{ private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

	
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
		TestProto t = new TestProto(filename);
	}
}
