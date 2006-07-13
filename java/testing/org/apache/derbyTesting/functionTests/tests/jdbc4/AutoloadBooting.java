/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.AutoloadBooting

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
 * This JUnit test verifies driver autoloading does not boot the engine.
 * This test is only run embedded because we manually bring the server up and down.
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.*;
import java.util.*;
import junit.framework.*;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

public	class	AutoloadBooting	extends	BaseJDBCTestCase
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	private	static	final	String	HEADER_LINE = "-------------------------------------";
	private	static	final	String	SPACER_LINE = "-- ";
	private	static	final	String	DRIVER_FACTORY = "org.apache.derby.jdbc.InternalDriver";
	private	static	final	String	DRIVER_SERVICE = "jdbc";
	private	static	final	String	NONEXISTENT_DATABASE = "nonexistentDatabase";
	private	static	final	String	CLIENT_DRIVER_NAME = "org.apache.derby.jdbc.ClientDriver";
	private	static	final	int		SERVER_PORT = 1527;
	private	static	final	long	SLEEP_TIME_MILLIS = 5000L;
	private	static	final	int		PING_COUNT = 6;
	

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
	
	public	AutoloadBooting( String name ) { super( name ); }

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
	 * Tests that the JDBC driver module is not booted by the autoloading
	 * of drivers.
	 * </p>
	 */
	public	void	testBooting()
		throws Exception
	{
		//
		// Uncomment this line if you want to the test to describe
		// its progress.
		//
		//CONFIG.setVerbosity( true );

		//
		// Only run embedded.
		//
		if ( !usingEmbedded() )
		{
			println( "Not running in the embedded framework. Exitting..." );
			return;
		}

		vetInitialization();
		scenario1_3();
		scenario2();
	}

	/**
	 * <p>
	 * Make sure that things look right at initialization.
	 * </p>
	 */
	private	void	vetInitialization()
		throws Exception
	{
		printBanner( "Initialization" );

		//
		// The engine should not be up when we start.
		//
		embeddedEngineIsUp( "In the beginning...", false );
	}
	
	/**
	 * <p>
	 * Scenarios (1) and (3) from the problem cases attached to DERBY-1459:
	 * http://issues.apache.org/jira/secure/attachment/12336017/autoloading_scenarios.html.
	 * </p>
	 *
	 * <p>
	 * In scenario (1), we verify that the embedded engine does not boot when
	 * you request a connection from some driver other than the embedded driver.
	 * In scenario (3), we verify that the network server also does not
	 * accidentally come up if you request a connection from some driver
	 * other than the embedded driver.
	 * </p>
	 */
	private	void	scenario1_3()
		throws Exception
	{
		printBanner( "Scenarios 1 and 3" );
		
		embeddedEngineIsUp( "Before loading client driver...", false );

		//
		// Request the network server to come up if the engine boots.
		//
		requestNetworkServerBoot();
		
		//
		// The engine should not come up when we load the network client driver.
		//
		loadNetworkClientDriver();
		embeddedEngineIsUp( "After loading network client...", false );

		//
		// The network server should not be up.
		//
		ping( false );
	}

	/**
	 * <p>
	 * Scenario (2) from the problem cases attached to DERBY-1459:
	 * http://issues.apache.org/jira/secure/attachment/12336017/autoloading_scenarios.html.
	 * </p>
	 *
	 * <p>
	 * In this scenario, we verify that the engine boots when we instantiate the
	 * embedded driver. We also test that the network server comes up if
	 * we set the appropriate system property.
	 * </p>
	 */
	private	void	scenario2()
		throws Exception
	{
		printBanner( "Scenario 2" );

		embeddedEngineIsUp( "Before instantiating embedded driver...", false );

		//
		// Request the network server to come up.
		//
		requestNetworkServerBoot();
		
		//
		// The engine should come up when we manually instantiate the EmbeddedDriver.
		//
		instantiateEmbeddedDriver();
		embeddedEngineIsUp( "After instantiating EmbeddedDriver...", true );

		//
		// The network server should also be booted because we set the
		// requesting system property.
		//
		ping( true );
	
		//
		// Now bring down the server and the engine.
		//
		bringDownServer();
		shutdownDerby();
		embeddedEngineIsUp( "After bringing down server...", false );
	}
		
	/////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Verify whether the network server came up.
	 * </p>
	 */
	private	void	ping( boolean shouldBeUp )
		throws Exception
	{
		NetworkServerControl	controller = new NetworkServerControl();
		Exception				finalException = null;
		boolean					isUp = false;
		
		for ( int i = 0; i < PING_COUNT; i++ )
		{
			try {
				controller.ping();
				isUp = true;
				println( "Network server came up!" );
				
				break;
			}
			catch (Exception e)
			{
				finalException = e;
				println( "Network server still down!" );
			}
			
			Thread.sleep( SLEEP_TIME_MILLIS );
		}

		assertEquals( "Network Server status", shouldBeUp, isUp );
	}

	/**
	 * <p>
	 * Tear down the network server.
	 * </p>
	 */
	private	void	bringDownServer()
		throws Exception
	{
		NetworkServerControl	controller = new NetworkServerControl();

		controller.shutdown();
	}
	
	/**
	 * <p>
	 * Set the system property which requests the network server to boot.
	 * </p>
	 */
	private	void	requestNetworkServerBoot()
		throws Exception
	{
		setSystemProperty( "derby.drda.startNetworkServer", "true" );
	}

	/**
	 * <p>
	 * Bring down the engine.
	 * </p>
	 */
	private	void	shutdownDerby()
		throws Exception
	{
		// swallow the shutdown exception
		try{
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
		} catch (SQLException e) {}
	}
	
	/**
	 * <p>
	 * Print out the banner for a test scenario.
	 * </p>
	 */
	private	void	printBanner( String bannerText )
	{
		println( HEADER_LINE );
		println( SPACER_LINE );
		println( SPACER_LINE + bannerText );
		println( SPACER_LINE );
		println( HEADER_LINE );
	}

	/**
	 * <p>
	 * Verify whether the embedded JDBC driver (and engine) has booted.
	 * </p>
	 */
	private	void	embeddedEngineIsUp( String banner, boolean isUp )
	{
		Object		service = null;

		// We get an NPE if the service doesn't exist
		try {
			service = Monitor.findService( DRIVER_FACTORY, DRIVER_SERVICE );
		}
		catch (NullPointerException npe) {}

		boolean		actualState = (service != null);

		println( banner + " Engine's booted status should be " + isUp + ", and is " + actualState );
		
		assertEquals( "JDBC driver status", isUp, actualState );
	}

	/**
	 * <p>
	 * Load the embedded driver.
	 * </p>
	 */
	private	void	instantiateEmbeddedDriver()
		throws Exception
	{
		Class.forName( "org.apache.derby.jdbc.EmbeddedDriver" ).newInstance();
	}


	/**
	 * <p>
	 * Load the network client.
	 * </p>
	 */
	private	void	loadNetworkClientDriver()
		throws Exception
	{
		boolean		isAutoloading = !CONFIG.autoloading();
		
		//
		// Forcibly load the network client if we are not autoloading it.
		//
		if ( isAutoloading )
		{
			println( "Not autoloading, so forcibly faulting in the client driver." );

			Class.forName( CLIENT_DRIVER_NAME );
		}

		//
		// We should fail to get a connection to the nonexistent database.
		// However, this call should force the client driver to register itself.
		//
		String	clientURL = "jdbc:derby://localhost:"  + SERVER_PORT + "/" + NONEXISTENT_DATABASE;

		try {
			DriverManager.getConnection( clientURL );

			fail( "Should not have connected to " + clientURL );
		}
		catch (SQLException se)
		{
			println( "As expected, failed to connect to " + clientURL );
		}

		//
		// Verify that the client driver registered itself.
		//
		Driver		clientDriver = DriverManager.getDriver( clientURL );

		assertNotNull( "Client driver should be registered.", clientDriver );
		assertEquals
			( "Client driver has correct name.", CLIENT_DRIVER_NAME, clientDriver.getClass().getName() );
	}

}

