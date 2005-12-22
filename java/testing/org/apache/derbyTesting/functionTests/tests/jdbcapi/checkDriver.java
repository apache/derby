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

import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * @author marsden
 *
 * This test tests java.sql.Driver methods.
 * Right now it just tests acceptsURL and some attributes  
 * Tests for getPropertyInfo need to be added. as well as connection attributes
 * 
 */

public class checkDriver {

	private static String hostName;
	private static String EMBEDDED_URL = "jdbc:derby:wombat;create=true";
	private static String CLIENT_URL;
	private static String JCC_URL;
	private static String INVALID_URL = "jdbc:db2j:wombat;create=true";
	
	private static String DERBY_SYSTEM_HOME = System.getProperty("derby.system.home");
	
	private static String CLIENT_URL_WITH_COLON1; 
	private static String CLIENT_URL_WITH_COLON2;
	private static String CLIENT_URL_WITH_DOUBLE_QUOTES1;  
	private static String CLIENT_URL_WITH_DOUBLE_QUOTES2; 
	private static String CLIENT_URL_WITH_SINGLE_QUOTES1; 
	private static String CLIENT_URL_WITH_SINGLE_QUOTES2;
	
	/**
	 * url prefix for this framework
	 */
	private static String frameworkPrefix;
	
	// The acceptsURLTable uses  the frameworkOffset column int he table 
	// to check for valid results for each framework
	private static int  frameworkOffset;
	
	private static int EMBEDDED_OFFSET = 0;
	private static int DERBYNETCLIENT_OFFSET = 1;
	private static int DERBYNET_OFFSET = 2;   // JCC
	
	static {
		frameworkPrefix = TestUtil.getJdbcUrlPrefix();
		if (TestUtil.isEmbeddedFramework())
			frameworkOffset = EMBEDDED_OFFSET;
		else if (TestUtil.isDerbyNetClientFramework())
			frameworkOffset = DERBYNETCLIENT_OFFSET;
		else if (TestUtil.isJCCFramework())
			frameworkOffset = DERBYNET_OFFSET; // JCC
		
		hostName = TestUtil.getHostName();
		CLIENT_URL = "jdbc:derby://" + hostName + ":1527/wombat;create=true";
		JCC_URL = "jdbc:derby:net://" + hostName + ":1527/wombat;create=true";
		CLIENT_URL_WITH_COLON1 = "jdbc:derby://" + hostName + ":1527/wombat:create=true";
		CLIENT_URL_WITH_COLON2 = "jdbc:derby://" + hostName + ":1527/"+ DERBY_SYSTEM_HOME + File.separator +"wombat:create=true";
		CLIENT_URL_WITH_DOUBLE_QUOTES1 = "jdbc:derby://" + hostName + ":1527/\"wombat\";create=true"; 
		CLIENT_URL_WITH_DOUBLE_QUOTES2 = "jdbc:derby://" + hostName + ":1527/\"" + DERBY_SYSTEM_HOME + File.separator + "wombat\";create=true";
		CLIENT_URL_WITH_SINGLE_QUOTES1 = "jdbc:derby://" + hostName + ":1527/'" + DERBY_SYSTEM_HOME + File.separator + "wombat';create=true";
		CLIENT_URL_WITH_SINGLE_QUOTES2 = "jdbc:derby://" + hostName + ":1527/'wombat';create=true";
	}

	// URLS to check.  New urls need to also be added to the acceptsUrl table
	private static String[] urls = new String[]
	{
	  	EMBEDDED_URL,
		CLIENT_URL,
		JCC_URL,
		INVALID_URL,
	};
		
	//Client URLS
	private static String[] clientUrls = new String[]
	{
		CLIENT_URL_WITH_COLON1,
		//CLIENT_URL_WITH_COLON2,
		//CLIENT_URL_WITH_DOUBLE_QUOTES1,
		//CLIENT_URL_WITH_DOUBLE_QUOTES2,
		//CLIENT_URL_WITH_SINGLE_QUOTES1,
		CLIENT_URL_WITH_SINGLE_QUOTES2
	};
	
	
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
			testEmbeddedAttributes(driver);
			testClientAttributes(driver);
			doClientURLTest(driver);
		}
		catch (SQLException se)
		{
			while (se != null)
			{
				se.printStackTrace(System.out);
				se = se.getNextException();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
		
	}
	

	/**
	 * Tests that client side attributes cann be specified in either url or info argument to connect.
	 * DERBY"-530. 
	 * 
	 * TODO: Add more comprehensive client attribute testing and enhance to handle jcc attributes in url. 
	 * 
	 * @param driver
	 */
	private static void testClientAttributes(Driver driver) throws SQLException
	{
		if (!TestUtil.isDerbyNetClientFramework())
			return;
		
		System.out.println("\ntestClientAttributes()");
		Properties info = new Properties();

		// Note: we have to put the trace file in an absolute path because the 
		// test harness sets user.dir and this confuses the File api greatly.
		// We put it in DERBY_SYSTEM_HOME since that is always available when 
		// tests are run
		String traceDirectory = DERBY_SYSTEM_HOME
			+ File.separator;
		String traceFile= traceDirectory + "trace.out";
		
		//		 traceFile attribute in url
		testConnect(driver, frameworkPrefix + "testpropdb;traceFile=" + 
					traceFile,info);
		assertTraceFileExists(traceFile);
		
		traceFile = traceDirectory + "trace2.out";
		
		// traceFile attribute in property
		info.setProperty("traceFile",traceFile);
		testConnect(driver, frameworkPrefix + "testpropdb",info);
		assertTraceFileExists(traceFile);

	}



	/**
	 * Check that trace file exists in <framework> directory
	 * 
	 * @param filename Name of trace file
	 */
	private static void assertTraceFileExists(String filename) 
	{
		File traceFile = new File(filename);
		//System.out.println("user.dir=" + System.getProperty("user.dir"));
		//System.out.println("fullpath = " + traceFile.getAbsolutePath());
		boolean exists = traceFile.exists();
		if (! exists)
			new Exception("FAILED trace file: " + filename + " does not exist").printStackTrace(System.out); 
		else
			System.out.println(" trace file exists");
			
	}


	/**
	 * Tests that embedded attributes can be specified in either url or info argument to connect
	 * DERBY-530. Only valid for emebedded driver and client. JCC has a different url format for 
	 * embedded attributes
	 * 
	 * @param driver
	 */
	private static void testEmbeddedAttributes(Driver driver) throws SQLException
	{
		// JCC can't take embedded attributes in info or as normal url attributes,
		// so not tested here.
		if (TestUtil.isJCCFramework())
			return;
		
		System.out.println("\ntestEmbeddedAttributes()");
		Properties info = new Properties();
		// create attribute as property
		info.setProperty("create","true");
		testConnect(driver, frameworkPrefix + "testcreatedb1", info);
		
		// create attribute in url
		testConnect(driver, frameworkPrefix + "testcreatedb2;create=true", null);
		
		// user/password in properties
		// testpropdb was created in load and test driver
		info.clear();
		info.setProperty("user","APP");
		info.setProperty("password", "xxxx");
		testConnect(driver, frameworkPrefix + "testpropdb", info);
		
		// user/password  in url
		testConnect(driver, frameworkPrefix + "testpropdb;user=testuser;password=testpass", null);
		
		// user in url, password in property
		info.clear();
		info.setProperty("password","testpass");
		testConnect(driver,frameworkPrefix + "testpropdb;user=testusr",info);

		// different users in url and in properties. URL is the winner
		info.clear();
		info.setProperty("user","APP");
		info.setProperty("password","xxxx");
		testConnect(driver, frameworkPrefix + "testpropdb;user=testuser;password=testpass", null);
		
		// shutdown with properties
		info.clear();
		info.setProperty("shutdown","true");				
		try {
			testConnect(driver,frameworkPrefix + "testcreatedb1", info);
		} catch (SQLException se)
		{
			System.out.println("Expected Exception:" + se.getSQLState() + ":" + se.getMessage());
		}
	}
		

	/**
	 * Check that drivers accept the correct urls and reject those for other supported drivers.
	 * 
	 * @param driver  driver we are testing.
	 * 
	 * @throws SQLException
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


	/**
	 * Load the driver and check java.sql.Driver methods, 
	 * @return
	 * @throws Exception
	 */
	private static Driver loadAndCheckDriverForFramework() throws Exception	
	{	
		TestUtil.loadDriver();
		String frameworkURL = TestUtil.getJdbcUrlPrefix() + "testpropdb;create=true";
		
		// Test that we loaded the right driver by making a connection
		Driver driver = DriverManager.getDriver(frameworkURL);
		Properties props = new Properties();
		props.put("user","testuser");
		props.put("password","testpass");
		Connection conn = DriverManager.getConnection(frameworkURL, props);
		DatabaseMetaData dbmd = conn.getMetaData();
		System.out.println("jdbcCompliant() = " +  driver.jdbcCompliant());
		
		// Just check versions against database metadata to avoid more master updates.
		// Metadata test prints the actual version.
		
		int majorVersion = driver.getMajorVersion();
		if (majorVersion == dbmd.getDriverMajorVersion())
			System.out.println("driver.getMajorVersion() = EXPECTED VERSION");
		else 
			new Exception("FAILED: unexpected value for  getMajorVersion(): " +
						majorVersion).printStackTrace();
		
		int  minorVersion = driver.getMinorVersion();
		if (minorVersion == dbmd.getDriverMinorVersion())
			System.out.println("driver.getMinorVersion() = EXPECTED VERSION");
		else 
			new Exception("FAILED: unexpected value for getMinorVersion()" +
					minorVersion).printStackTrace(System.out);
		
		conn.close();
		return driver;
	}
		

	
	
	/**
	 * Check the actual return value of acceptsURL against the expected value and error and stack
	 * trace if they don't match
	 * 
	 * @param url URL that was checked for acceptsURL
	 * @param expectedAcceptance  expected return value 
	 * @param actualAcceptance    actual return value
	 * 
	 */
	private static void assertExpectedURLAcceptance(String url, boolean expectedAcceptance, 
				boolean actualAcceptance)
	{
		if (actualAcceptance != expectedAcceptance)
		{
			new Exception("FAILED acceptsURL check. url = " + url  + 
						   " expectedAcceptance = " + expectedAcceptance +
						   " actualAcceptance = " + actualAcceptance).printStackTrace(System.out);
		}

	}
	
	/**
	 * Tests client URLs to see connection is successful or the correct exception is thrown.
	 * 
	 * @param driver
	 * @throws SQLException
	 */
	private static void doClientURLTest(Driver driver){
		if (!TestUtil.isDerbyNetClientFramework())
			return;
		
		System.out.println("doClientURLTest()");
		Properties info = null;		//test with null Properties object

		for (int i = 0; i < clientUrls.length;i++)
		{
			String url = clientUrls[i];
			System.out.println("doClientURLTest with url: " + replaceSystemHome(url));
			try{
				Connection conn = testConnect(driver,url,info);
				if(conn != null)
					System.out.println("PASSED:Connection Successful with url: " + replaceSystemHome(url) );
			}
			catch(SQLException se){
				System.out.println("EXPECTED EXCEPTION:"+replaceSystemHome(se.getMessage()));
			}
		}
	}	
	
	/**
	 * Make  java.sql.Driver.connect(String url, Properties info call) and print the status of
	 * the connection.
	 * 
	 * @param driver   driver for framework
	 * @param url       url to pass to Driver.connect()
	 * @param info      properties to pass to Driver.Connect()
	 * 
	 * @throws SQLException on error.
	 */
	private static Connection testConnect(Driver driver, String url, Properties info) throws SQLException
	{
		String infoString = null;
		if (info != null)
			infoString = replaceSystemHome(info.toString());
		String urlString = replaceSystemHome(url);
		Connection conn = driver.connect(url,info);
		
		if(conn == null){
			System.out.println("Null connection returned for url "+urlString);
			return conn;
		}
		
		System.out.println("\nConnection info for connect(" + urlString + ", " + infoString +")");
		String getUrlValue = conn.getMetaData().getURL();
		// URL may include path of DERBY_SYSTEM_HOME for traceFile
		// filter it out.
		getUrlValue = replaceSystemHome(getUrlValue);
		System.out.println("getURL() = " + getUrlValue);
		System.out.println("getUserName() = " + conn.getMetaData().getUserName());
		// CURRENT SCHEMA should match getUserName()
		ResultSet rs = conn.createStatement().executeQuery("VALUES(CURRENT SCHEMA)");
		rs.next();
		System.out.println("CURRENT SCHEMA = " + rs.getString(1));
		conn.close();
		return conn;
	}


	/**
	 * @param origString
	 * 
	 * @return origString with derby.system.home path replaed with [DERBY_SYSTEM_HOME]
	 */
	private static String replaceSystemHome(String origString) {
		String replaceString = DERBY_SYSTEM_HOME + File.separator;
		int offset = origString.indexOf(replaceString);
		if (offset == -1)
			return origString;
		else
			return origString.substring(0,offset) + "[DERBY_SYSTEM_HOME]/"+ 
			origString.substring(offset + replaceString.length());
	}
	
}