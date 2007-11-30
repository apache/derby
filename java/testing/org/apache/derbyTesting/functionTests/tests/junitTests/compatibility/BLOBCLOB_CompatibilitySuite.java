/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.BLOBCLOB_CompatibilitySuite

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
/**
 * <p>
 * This is the JUnit suite verifying compatibility of Derby clients and
 * servers across Derby version levels and supported VMs. When you want
 * to add a new class of tests to this suite, just add the classname to
 * the accumulator in suite().
 * </p>
 *
 */

package org.apache.derbyTesting.functionTests.tests.junitTests.compatibility;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.Compat_BlobClob4BlobTest;

public	class	BLOBCLOB_CompatibilitySuite	extends	CompatibilitySuite
{

	/////////////////////////////////////////////////////////////
	//
	//	JUnit BEHAVIOR
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * JUnit boilerplate which adds as test cases all public methods
	 * whose names start with the string "test" in the named classes.
	 * When you want to add a new class of tests, just wire it into
	 * this suite.
	 * </p>
	 */
    public static Test suite()
    {
        TestSuite testSuite = new TestSuite();
        
        testSuite.addTest(Compat_BlobClob4BlobTest.suite()); // ONLY REAL DIFFERENCE FROM CompatibilitySuite

        return testSuite;
    }


	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINT
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Run JDBC compatibility tests using either the specified client or
	 * the client that is visible
	 * on the classpath. If there is more than one client on the classpath,
	 * exits with an error.
	 * </p>
	 *
	 * <ul>
	 * <li>arg[ 0 ] = required name of database to connect to</li>
	 * <li>arg[ 1 ] = optional driver to use. if not specified, we'll look for a
	 *                client on the classpath</li>
	 * </ul>
	 */
	public static void main( String args[] )
		throws Exception
	{
            int exitStatus = FAILURE_EXIT;

            if (
                parseDebug() &&
                parseArgs( args ) &&
                parseVMLevel() &&
                findClient() &&
                findServer()
		)
            {		
                Test t = suite(); 
                println("#      BLOBCLOB_CompatibilitySuite.main() will run suite with  " 
                    + t.countTestCases() + " testcases.");
            
                TestResult result = junit.textui.TestRunner.run( t );

                exitStatus = result.errorCount() + result.failureCount();
            }

            Runtime.getRuntime().exit( exitStatus );
	}

	/////////////////////////////////////////////////////////////
	//
	//	INNER CLASSES
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * This helper class exposes an entry point for creating an empty database.
	 * </p>
	 */
	public	static	final	class	Creator
	{
		private	static	BLOBCLOB_CompatibilitySuite	_driver = new BLOBCLOB_CompatibilitySuite();
		
		/**
		 * <p>
		 * Wait for server to come up, then create the database.
		 * </p>
		 *
		 * <ul>
		 * <li>args[ 0 ] = name of database to create.</li>
		 * </ul>
		 */
		public	static	void	main( String[] args )
			throws Exception
		{
			String		databaseName = args[ 0 ];
			findClient();
			_driver.createDB( databaseName );
		}
		
	}

}
