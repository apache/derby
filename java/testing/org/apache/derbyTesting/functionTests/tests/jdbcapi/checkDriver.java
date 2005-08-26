/*

Derby - Class org.apache.derby.jdbc.EmbeddedDriver

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author marsden
 *
 * This test tests java.sql.Driver methods.
 * Right now it just tests acceptsURL.  
 * Tests for getPropertyInfo need to be added. as well as connection attributes
 * 
 */

public class checkDriver {

	private static String EMBEDDED_URL = "jdbc:derby:wombat;create=true";
	private static String CLIENT_URL = "jdbc:derby://localhost:1527/wombat;create=true";
	private static String JCC_URL = "jdbc:derby:net://localhost:1527/wombat;create=true";
	private static String INVALID_URL = "jdbc:db2j:wombat;create=true";
	
	// URLS to check.  New urls need to also be added to the acceptsUrl table
	private static String[] urls = new String[]
	{
	  	EMBEDDED_URL,
		CLIENT_URL,
		JCC_URL,
		INVALID_URL,
	};
	
	
	
	
	// The acceptsURLTable uses  the frameworkOffset column int he table 
	// to check for valid results for each framework
	private static int  frameworkOffset;
	
	
	private static int EMBEDDED_OFFSET = 0;
	private static int DERBYNETCLIENT_OFFSET = 1;
	private static int DERBYNET_OFFSET = 2;   // JCC
	
	static {
		if (TestUtil.isEmbeddedFramework())
			frameworkOffset = EMBEDDED_OFFSET;
		else if (TestUtil.isDerbyNetClientFramework())
			frameworkOffset = DERBYNETCLIENT_OFFSET;
		else if (TestUtil.isJCCFramework())
			frameworkOffset = DERBYNET_OFFSET; // JCC
	}
	
	// Table that shows whether tested urls should return true for acceptsURL
	// under the given framework
	private static boolean[][] acceptsURLTable = new boolean[][]
	{
	// Framework/url      EMBEDDED     DERBYNETCLIENT       DERBYNET (JCC)
	/*EMBEDDED_URL */  {   true      ,  false           ,   false    },
	/*CLIENT_URL   */  {   false     ,  true            ,   false    },     
	/* JCC_URL 	   */  {   false     ,  false           ,   true     },
	/* INVALID_URL */  {   false     ,  false           ,   false    } 
	};

			

	public static void main(String[] args) {
		try {
			Driver driver = loadAndCheckDriverForFramework();
			checkAcceptsURL(driver);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	

	/**
	 * 
	 * @param driver
	 */
	private static void checkAcceptsURL(Driver driver) throws SQLException{
		for (int u = 0; u < urls.length;u++)
		{
			String url = urls[u];
			//System.out.println("acceptsURLTable[" + u +"][" + frameworkOffset+ "]"); 					
			boolean expectedAcceptance = acceptsURLTable[u][frameworkOffset];
			boolean actualAcceptance = driver.acceptsURL(url);
			System.out.println("checking acceptsURL(" + url + ")" );
			assertExpectedURLAcceptance(url, expectedAcceptance, actualAcceptance);
					
		}
		
	}


	private static Driver loadAndCheckDriverForFramework() throws Exception	
	{	
		TestUtil.loadDriver();
			
		String frameworkURL = TestUtil.getJdbcUrlPrefix() + "wombat;create=true";
		TestUtil.loadDriver();
		
		// Test that we loaded the right driver by making a connection
		Driver driver = DriverManager.getDriver(frameworkURL);
		Properties props = new Properties();
		props.put("user","APP");
		props.put("password","xxx");
		Connection conn = driver.connect(frameworkURL,props);
		//System.out.println("Successfully made connection for  " + conn.getMetaData().getDriverName());
		conn.close();
		//System.out.println("jdbcCompliant = " +  driver.jdbcCompliant());	
		return driver;
	}
		

	
	
	private static void assertExpectedURLAcceptance(String url, boolean expectedAcceptance, 
				boolean actualAcceptance)
	{
		if (actualAcceptance != expectedAcceptance)
		{
			new Exception("FAILED acceptURL check. url = " + url  + 
						   " expectedAcceptance = " + expectedAcceptance +
						   " actualAcceptance = " + actualAcceptance).printStackTrace(System.out);
		}

	}
	
	
}