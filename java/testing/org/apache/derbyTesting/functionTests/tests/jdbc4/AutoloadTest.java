/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.AutoloadTest

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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
/**
 * <p>
 * This JUnit test verifies the autoloading of the jdbc driver under JDBC4.
 * This test must be run in its own VM because we want to verify that the
 * driver was not accidentally loaded by some other test.
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.*;
import java.util.*;
import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

public	class	AutoloadTest	extends	BaseJDBCTestCase
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	private	static	final	String	NONEXISTENT_DATABASE = "nonexistentDatabase";
	private	static	final	String	DRIVER_LIST = "jdbc.drivers";
	
	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	public	AutoloadTest( String name ) { super( name ); }

	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINT
	//
	/////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////
	//
	//	JUnit BEHAVIOR
	//
	/////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////
	//
	//	TEST ENTRY POINTS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Tests the autoloading of the client driver by JDBC 4. This behavior
	 * is described in section 10.2.1 of the JDBC 4 spec. The driver is
	 * autoloaded if we are running under jdk1.6 or later and one of the
	 * following is true:
	 * </p>
	 *
	 * <ul>
	 * <li>Classes are being loaded out of the Derby jar files.</li>
	 * <li>OR the system property jdbc.drivers names the drivers.</li>
	 * </ul>
	 */
	public	void	testAutoloading()
		throws Exception
	{
		//CONFIG.setVerbosity( true );
		
		//
		// We expect that the connection to the database will fail for
		// one reason or another.
		//
		if ( autoloading() )
		{
			println( "We ARE autoloading..." );

			//
			// The DriverManager should have autoloaded the client driver.
			// This means that the connection request is passed on to the
			// server. The server then determines that the database does
			// not exist. This raises a different error depending on whether
			// we're running embedded or with the Derby client.
			//
			if ( usingEmbedded() ) { failToConnect( "XJ004" ); }
			else { failToConnect( "08004" ); }
		}
		else
		{
			println( "We are NOT autoloading..." );

			//
			// We aren't autoloading the driver. The
			// DriverManager returns the following SQLState.
			//
			failToConnect( "08001" );
		}
	}

	/**
	 * <p>
	 * Verify that we fail to connect to the database for the expected
	 * reason.
	 * </p>
	 */
	private	void	failToConnect( String expectedSQLState )
		throws Exception
	{
		String			connectionURL = CONFIG.getJDBCUrl( NONEXISTENT_DATABASE );
		Properties		properties = new Properties();
		SQLException 	se = null;

		properties.put( "user", CONFIG.getUserName() );
		properties.put( "password", CONFIG.getUserPassword() );

		try {
			println( "Attempting to connect with this URL: '" + connectionURL + "'" );
			
			DriverManager.getConnection( connectionURL, properties );
		}
		catch ( SQLException e ) { se = e; }

		println( "Caught expected SQLException: " + se );
		
		assertSQLState( expectedSQLState, expectedSQLState, se );
	}

	/**
	 * <p>
	 * Return true if we expect that the DriverManager will autoload the client driver.
	 * </p>
	 */
	private	boolean	autoloading()
		throws Exception
	{
		//
		// DriverManager autoloads the client only as of JDBC4.
		//
		if ( !CONFIG.supportsJDBC4() )
		{
			println( "NOT JDBC4..." );
			return false;
		}

		//
		// The DriverManager will autoload drivers specified by the jdbc.drivers
		// property. 
		//
		if ( CONFIG.getSystemStartupProperty( DRIVER_LIST ) != null )
		{
			println( "Drivers list encoded in startup properties..." );
			return true;
		}

		//
		// The DriverManager will also look inside our jar files for
		// the generated file META-INF/services/java.sql.Driver. This file
		// will contain the name of the driver to load. So if we are running
		// this test against Derby jar files, we expect that the driver will
		// be autoloaded.
		//
		// Note that if we run with a security manager, we get permissions
		// exception at startup when the driver is autoloaded. This exception
		// is silently swallowed and the result is that the driver is not
		// loaded even though we expect it to be.
		//
		if ( CONFIG.loadingFromJars() )
		{
			println( "Loading from jars..." );
			return true;
		}

		//
		// OK, we've run out of options. We do not expect that the driver
		// will be autoloaded.
		//

		println( "None of the above. Not autoloading..." );
		
		return false;
	}

}

