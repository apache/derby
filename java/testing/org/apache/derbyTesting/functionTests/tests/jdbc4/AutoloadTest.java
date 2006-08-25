/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.AutoloadTest

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
 * This JUnit test verifies the autoloading of the jdbc driver under JDBC4.
 * This test must be run in its own VM because we want to verify that the
 * driver was not accidentally loaded by some other test.
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.security.PrivilegedActionException;
import java.sql.*;
import java.util.*;
import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public	class	AutoloadTest	extends	BaseJDBCTestCase
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	private	static	final	String	NONEXISTENT_DATABASE = "nonexistentDatabase";
	
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
    
    /**
     * Only run a test if the driver will be auto-loaded.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite();
        
        // need DriverManager at least and Derby drivers
        // no interest in testing DB2's client.
        if (JDBC.vmSupportsJDBC2() &&
                (usingEmbedded() || usingDerbyNetClient()))
        {

            boolean autoloadingCurrentDriver = false;
            
            // Autoloading if the current driver is defined in the
            // system property jdbc.drivers, see java.sql.DriverManager.
            try {
                String jdbcDrivers = getSystemProperty("jdbc.drivers");
                if (jdbcDrivers != null)
                {
                    // Simple test to see if the driver class is
                    // in the value. Could get fancy and see if it is
                    // correctly formatted but not worth it.
                    String driver =
                        TestConfiguration.getCurrent().getJDBCClient().getJDBCDriverName();
                    
                    if (jdbcDrivers.indexOf(driver) != -1)
                        autoloadingCurrentDriver = true;
                }
                
            } catch (PrivilegedActionException e) {
                // can't read property, assume not autoloading.
            }
            
            // Also auto loading if this is JDBC 4 and loading from the
            // jar files, due to the required manifest entry.
            if (JDBC.vmSupportsJDBC4() &&
                    TestConfiguration.getCurrent().loadingFromJars())
                autoloadingCurrentDriver = true;
          
            if (autoloadingCurrentDriver)
                suite.addTestSuite(AutoloadTest.class);

        }
        
        System.out.println("TEST COUNT" + suite.countTestCases());
        
        return suite;
    }

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

		println( "We ARE autoloading..." );

		//
		// The DriverManager should have autoloaded the client driver.
		// This means that the connection request is passed on to the
		// server. The server then determines that the database does
		// not exist. This raises a different error depending on whether
		// we're running embedded or with the Derby client.
		//
        String expectedError =
            usingEmbedded() ? "XJ004" : "08004";
        
        failToConnect(expectedError);
       
        // Test we can connect successfully to a database!
        String url = getTestConfiguration().getJDBCUrl();
        url = url.concat(";create=true");
        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();
        DriverManager.getConnection(url, user, password).close();

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
		String			connectionURL = getTestConfiguration().getJDBCUrl( NONEXISTENT_DATABASE );
		Properties		properties = new Properties();
		SQLException 	se = null;

		properties.put( "user", getTestConfiguration().getUserName() );
		properties.put( "password", getTestConfiguration().getUserPassword() );

		try {
			println( "Attempting to connect with this URL: '" + connectionURL + "'" );
			
			DriverManager.getConnection( connectionURL, properties );
            
            fail("Connection succeed, expected to fail.");
		}
		catch ( SQLException e ) { se = e; }

		println( "Caught expected SQLException: " + se );

		assertSQLState( expectedSQLState, expectedSQLState, se );
	}


}

