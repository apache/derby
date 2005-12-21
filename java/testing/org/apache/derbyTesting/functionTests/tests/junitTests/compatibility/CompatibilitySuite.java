/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.CompatibilitySuite

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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
 * This is the JUnit suite verifying compatibility of Derby clients and
 * servers across Derby version levels and supported VMs. When you want
 * to add a new class of tests to this suite, just add the classname to
 * the accumulator in suite().
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.tests.junitTests.compatibility;

import java.io.*;
import java.sql.*;
import java.util.*;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.DerbyJUnitTest;

public	class	CompatibilitySuite	extends	DerbyJUnitTest
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	// Supported versions of the db2jcc client.
	public	static	final	Version	IBM_2_4 = new Version( 2, 4 );

	// Supported versions of Derby.
	public	static	final	Version	DRB_10_0 = new Version( 10, 0 );
	public	static	final	Version	DRB_10_1 = new Version( 10, 1 );
	public	static	final	Version	DRB_10_2 = new Version( 10, 2 );

	// Supported VM versions.
	public	static	final	Version	VM_1_3 = new Version( 1, 3 );
	public	static	final	Version	VM_1_4 = new Version( 1, 4 );
	public	static	final	Version	VM_1_5 = new Version( 1, 5 );

	public	static	final	String	SERVER_VERSION_FUNCTION = "getVMVersion";
	
	private	static	final			String	VERSION_PROPERTY = "java.version";

	private	static	final			int		EXPECTED_CLIENT_COUNT = 1;

	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	private	static	Driver		_driver;				// the corresponding jdbc driver
	private	static	Version		_clientVMLevel;			// level of client-side vm
	private	static	Version		_serverVMLevel;			// level of server vm
	private	static	Version		_driverLevel;			// client rev level
	private	static	Version		_serverLevel;			// server rev level

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
		TestSuite	testSuite = new TestSuite();

		testSuite.addTestSuite( JDBCDriverTest.class );

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
		int			exitStatus = FAILURE_EXIT;
		
		if (
			   parseDebug() &&
			   parseArgs( args ) &&
			   parseVMLevel() &&
			   findClient() &&
			   findServer()
		   )
		{		
			TestResult	result = junit.textui.TestRunner.run( suite() );
			
			exitStatus = result.errorCount() + result.failureCount();
		}

		Runtime.getRuntime().exit( exitStatus );
	}

	/////////////////////////////////////////////////////////////
	//
	//	PUBLIC BEHAVIOR
	//
	/////////////////////////////////////////////////////////////
	
	/**
	 * <p>
	 * Get the version of the server.
	 * </p>
	 */
	public	Version	getServerVersion() { return _serverLevel; }

	/**
	 * <p>
	 * Get the version of the client.
	 * </p>
	 */
	public	Version	getDriverVersion() { return _driverLevel; }

	/**
	 * <p>
	 * Get the vm level of the server.
	 * </p>
	 */
	public	static	Version	getServerVMVersion()	{ return _serverVMLevel; }

	/**
	 * <p>
	 * Get the vm level of the client.
	 * </p>
	 */
	public	Version	getClientVMVersion() { return _clientVMLevel; }

	/////////////////////////////////////////////////////////////
	//
	//	DATABASE-SIDE FUNCTIONS
	//
	/////////////////////////////////////////////////////////////
	
	/**
	 * <p>
	 * Get the vm level of the server.
	 * </p>
	 */
	public	static	String	getVMVersion()
	{
		return System.getProperty( VERSION_PROPERTY );
	}

	/////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////
	
	///////////////////
	//
	//	GENERAL MINIONS
	//
	///////////////////
	
	//////////////////////////
	//
	//	INITIALIZATION MINIONS
	//
	//////////////////////////
	
	//
	// Initialize client settings based on the client found.
	// Return true if one and only one client found, false otherwise.
	// We allow for the special case when we're running the embedded client
	// off the current compiled class tree rather than off product jars.
	//
	private	static	boolean	findClient()
		throws Exception
	{
		//
		// The client may have been specified on the command line.
		// In that case, we don't bother looking for a client on
		// the classpath.
		//
		if ( getClientSettings() != null ) { faultInDriver( getClientSettings() ); }
		else
		{
			String	currentClientName = null;
			int		legalCount = LEGAL_CLIENTS.length;
			int		foundCount = 0;

			for ( int i = 0; i < legalCount; i++ )
			{
				String[]	candidate = LEGAL_CLIENTS[ i ];

				if ( faultInDriver( candidate ) )
				{
					setClient( candidate );
					foundCount++;
				}
			}

			if ( foundCount != EXPECTED_CLIENT_COUNT )
			{
				throw new Exception( "Wrong number of drivers: " + foundCount );
			}
		}

		// Now make sure that the JDBC driver is what we expect

		try {
			_driver = DriverManager.getDriver( getClientSettings()[ DATABASE_URL ] );
			_driverLevel = new Version( _driver.getMajorVersion(), _driver.getMinorVersion() );
		}
		catch (SQLException e)
		{
			printStackTrace( e );
			
			throw new Exception
				( "Driver doesn't understand expected URL: " + getClientSettings()[ DATABASE_URL ] );
		}

		println
			(
			    "Driver " + _driver.getClass().getName() +
				" Version = " + _driverLevel
			);
		
		return true;
	}

	//
	// Initialize server settings. Assumes that you have called
	// findClient().
	//
	private	static	boolean	findServer()
		throws Exception
	{
		try {
			Connection			conn = getConnection();
			DatabaseMetaData	dmd = conn.getMetaData();
			String				dbProductVersion = dmd.getDatabaseProductVersion();

			_serverLevel = new Version( dbProductVersion );
			
			parseServerVMVersion( conn );
		}
		catch (Exception e)
		{
			printStackTrace( e );
			
			throw new Exception( "Error lookup up server info: " + e.getMessage() );
		}
		
		println( "Server Version = " + _serverLevel );

		return true;
	}

	private	static	boolean	parseVMLevel()
		throws Exception
	{
		String				vmVersion = getVMVersion();

		try {
			_clientVMLevel = new Version( vmVersion );
		}
		catch (NumberFormatException e)
		{
			throw new Exception( "Badly formatted vm version: " + vmVersion );
		}

		println( "VM Version = " + _clientVMLevel );

		return true;
	}

	private	static	boolean	parseArgs( String args[] )
		throws Exception
	{
		if ( ( args == null ) || (args.length == 0 ) )
		{ throw new Exception( "Missing database name." ); }
		
		setDatabaseName( args[ 0 ] );

		if ( (args.length > 1) && !"".equals( args[ 1 ] ) )
		{
			String	desiredClientName = args[ 1 ];
			int		count = LEGAL_CLIENTS.length;

			for ( int i = 0; i < count; i++ )
			{
				String[]	candidate = LEGAL_CLIENTS[ i ];

				if ( desiredClientName.equals( candidate[ DRIVER_NAME ] ) )
				{
					setClient( candidate );
					break;
				}
			}

			if ( getClientSettings() == null )
			{
				throw new Exception
					( "Could not find client " + desiredClientName + " on the classpath." );
			}
		}
			
		return true;
	}

	/**
	 * <p>
	 * Get the vm level of the server.
	 * </p>
	 */
	private	static	void	parseServerVMVersion( Connection conn )
		throws SQLException
	{
		dropFunction( conn, SERVER_VERSION_FUNCTION );

		PreparedStatement	ps = prepare
			(
			    conn,
				"create function " + SERVER_VERSION_FUNCTION + "() returns varchar(50)\n" +
				"parameter style java no sql language java\n" +
				"external name 'org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite.getVMVersion'"
			);
		ps.execute();
		close( ps );

		ps = prepare
			(
			    conn,
				"values " + SERVER_VERSION_FUNCTION + "()"
			);

		ResultSet	rs = ps.executeQuery();
		rs.next();
		String		rawVersion = rs.getString( 1 );
		close( rs );
		close( ps );

		_serverVMLevel = new Version( rawVersion );
			
		println( "Server VM Version = " + _serverVMLevel );
	}

	///////////////
	//
	//	SQL MINIONS
	//
	///////////////

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
		private	static	CompatibilitySuite	_driver = new CompatibilitySuite();
		
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

			CompatibilitySuite.findClient();
			
			_driver.createDB( databaseName );
		}
		
	}

	/**
	 * <p>
	 * A class for storing a major and minor version number. This class
	 * assumes that more capable versions compare greater than less capable versions.
	 * </p>
	 */
	public	static	final	class	Version	implements	Comparable
	{
		private	int	_major;
		private	int	_minor;

		public	Version( int major, int minor )
		{
			constructorMinion( major, minor );
		}

		public	Version( String desc )
			throws NumberFormatException
		{
			StringTokenizer		tokens = new StringTokenizer( desc, "." );

			constructorMinion
				(
				    java.lang.Integer.parseInt( tokens.nextToken() ),
					java.lang.Integer.parseInt( tokens.nextToken() )
				);
		}

		private	void	constructorMinion( int major, int minor )
		{
			_major = major;
			_minor = minor;
		}

		/**
		 * <p>
		 * Returns true if this Version is at least as advanced
		 * as that Version.
		 * </p>
		 */
		public	boolean	atLeast( Version that )
		{
			return this.compareTo( that ) > -1;
		}


		////////////////////////////////////////////////////////
		//
		//	Comparable BEHAVIOR
		//
		////////////////////////////////////////////////////////

		public	int	compareTo( Object other )
		{
			if ( other == null ) { return -1; }
			if ( !( other instanceof Version ) ) { return -1; }

			Version	that = (Version) other;

			if ( this._major < that._major ) { return -1; }
			if ( this._major > that._major ) { return 1; }

			return this._minor - that._minor;
		}

		////////////////////////////////////////////////////////
		//
		//	Object OVERLOADS
		//
		////////////////////////////////////////////////////////
		
		public	String	toString()
		{
			return Integer.toString( _major ) + '.' + Integer.toString( _minor );
		}

		public	boolean	equals( Object other )
		{
			return (compareTo( other ) == 0);
		}

		public	int	hashCode()
		{
			return _major ^ _minor;
		}
		
	}

	

}
