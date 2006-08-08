/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMapping

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
 * Run the compatibility tests against the embedded server.
 * </p>
 *
 * @author Rick
 */
package org.apache.derbyTesting.functionTests.tests.junitTests.derbyNet;

import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite;

public	class	CompatibilityTest
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	public	static	final	String	DATABASE_NAME = "wombat";
	public	static	final	String	NETWORK_CLIENT_NAME = "org.apache.derby.jdbc.ClientDriver";
	
	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINT
	//
	/////////////////////////////////////////////////////////////

	public	static	final	void	main( String[] args )
		throws Exception
	{
		// create database
		ij.getPropertyArg( args );
		Connection conn = ij.startJBMS();

		CompatibilitySuite.main( new String[] { DATABASE_NAME, NETWORK_CLIENT_NAME } );
	}
	
	/////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////

}
