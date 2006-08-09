/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.dataSourcePermissions_net

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.ConnectionPoolDataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionEvent;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.io.*;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Properties;

import javax.naming.*;
import javax.naming.directory.*;

import java.lang.reflect.*;

public class dataSourcePermissions_net extends org.apache.derbyTesting.functionTests.tests.jdbcapi.dataSourcePermissions
{

	private static int NETWORKSERVER_PORT;
	private static String hostName;
	private static NetworkServerControl networkServer = null;
    
    private SwitchablePrintStream consoleLogStream_;
    private PrintStream originalStream_;
    private FileOutputStream shutdownLogStream_;

    private SwitchablePrintStream consoleErrLogStream_;
    private PrintStream originalErrStream_;
    private FileOutputStream shutdownErrLogStream_;
    
	public static void main(String[] args) throws Exception {

		hostName = TestUtil.getHostName();
		if (hostName.equals("localhost"))
			NETWORKSERVER_PORT = 20000;
		else
			NETWORKSERVER_PORT = 1527;
		
		// Load harness properties.
		ij.getPropertyArg(args);

		// "runTest()" is going to try to connect to the database through
		// the server at port NETWORKSERVER_PORT.  Thus, we have to
		// start the server on that port before calling runTest.
		// Except when we are using a remote server, then we assume the port is 1527,
		// and we assume the server has been started already

		try {
			TestUtil.loadDriver();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		PrintStream originalStream = System.out;
		FileOutputStream shutdownLogStream = 
		    new FileOutputStream("dataSourcePermissions_net." + 
					 System.getProperty("framework","") + "." + 
					 "shutdown.std.log");
		SwitchablePrintStream consoleLogStream = 
		    new SwitchablePrintStream( originalStream );

		PrintStream originalErrStream = System.err;
		FileOutputStream shutdownErrLogStream = 
		    new FileOutputStream("dataSourcePermissions_net." + 
					 System.getProperty("framework","") + "." + 
					 "shutdown.err.log");
		SwitchablePrintStream consoleErrLogStream = 
		    new SwitchablePrintStream( originalErrStream );
		
		System.setOut( consoleLogStream );
		System.setErr( consoleErrLogStream );

		if (hostName.equals("localhost"))
		{
			// Start the NetworkServer on another thread
			networkServer = new NetworkServerControl(InetAddress.getByName("localhost"),NETWORKSERVER_PORT);
			networkServer.start(null);

			// Wait for the NetworkServer to start.
			if (!isServerStarted(networkServer, 60))
				System.exit(-1);
		}
		
		
		// Now, go ahead and run the test.
		try {
		    dataSourcePermissions_net tester = 
			new dataSourcePermissions_net( consoleLogStream ,
						       originalStream ,
						       shutdownLogStream ,
						       consoleErrLogStream ,
						       originalErrStream,
						       shutdownErrLogStream);
			tester.setProperties();
			tester.runTest();
			if (TestUtil.isDerbyNetClientFramework())
				tester.testClientDataSourceProperties();
			new dataSourcePermissions_net( consoleLogStream , 
						       originalStream ,
						       shutdownLogStream ,
						       consoleErrLogStream ,
						       originalErrStream ,
						       shutdownErrLogStream ).cleanUp();

		} catch (Exception e) {
		// if we catch an exception of some sort, we need to make sure to
		// close our streams before returning; otherwise, we can get
		// hangs in the harness.  SO, catching all exceptions here keeps
		// us from exiting before closing the necessary streams.
			System.out.println("FAIL - Exiting due to unexpected error: " +
				e.getMessage());
			e.printStackTrace();
		}

		// Shutdown the server.
		if (hostName.equals("localhost"))
		{
		    
			consoleLogStream.switchOutput( shutdownLogStream );
			consoleErrLogStream.switchOutput( shutdownErrLogStream );
			
			networkServer.shutdown();
			consoleLogStream.flush();
			
			consoleLogStream.switchOutput( originalStream );
			consoleErrLogStream.switchOutput( originalErrStream );
			
			// how do we do this with the new api?
			//networkServer.join();
			Thread.sleep(5000);
		}
		System.out.println("Completed dataSourcePermissions_net");
		
		originalStream.close();
		shutdownLogStream.close();

		originalErrStream.close();
		shutdownErrLogStream.close();

	}


	public dataSourcePermissions_net( SwitchablePrintStream consoleLogStream,
					  PrintStream originalStream,
					  FileOutputStream shutdownLogStream,
					  SwitchablePrintStream consoleErrLogStream,
					  PrintStream originalErrStream, 
					  FileOutputStream shutdownErrLogStream ) {
	    
	    consoleLogStream_ = consoleLogStream;
	    originalStream_ = originalStream;
	    shutdownLogStream_ = shutdownLogStream;

	    consoleErrLogStream_ = consoleErrLogStream;
	    originalErrStream_ = originalErrStream;
	    shutdownErrLogStream_ = shutdownErrLogStream;
	    
	}
    

	public void setProperties() {
		// Set required server properties.
		System.setProperty("database",
						   TestUtil.getJdbcUrlPrefix(hostName,
													 NETWORKSERVER_PORT) +
						   "wombat;create=true");
		System.setProperty("ij.user", "EDWARD");
		System.setProperty("ij.password", "noodle");

	}

	public String getJDBCUrl(String db, String attrs) {
		// this method is accessed from subclasses - need to establish hostName
		String hostName = TestUtil.getHostName();
		if (hostName.equals("localhost"))
			NETWORKSERVER_PORT = 20000;	
		else
			NETWORKSERVER_PORT = 1527;
		String s = TestUtil.getJdbcUrlPrefix(hostName, NETWORKSERVER_PORT)
			+ db;
		if (attrs != null)
			if (TestUtil.isJCCFramework())
				s = s + ":" + attrs + ";";
			else
				s = s + ";" + attrs;
		return s;

	}

	public javax.sql.DataSource getDS(String database, String user, String
									  password)
	{
		return getDS(database,user,password,null);
	}

	public javax.sql.DataSource getDS(String database, String user, String
									  password, Properties attrs)
	{

	if (attrs == null)
		attrs = new Properties();
	attrs.setProperty("databaseName", database);
	if (user != null)
		attrs.setProperty("user", user);
	if (password != null)
		attrs.setProperty("password", password);
	attrs = addRequiredAttributes(attrs);
	return TestUtil.getDataSource(attrs);
	}



	public javax.sql.ConnectionPoolDataSource getCPDS(String database, String user, String password) {
		Properties attrs = new Properties();
		attrs.setProperty("databaseName", database);
		if (user != null)
			attrs.setProperty("user", user);
		if (password != null)
			attrs.setProperty("password", password);
		attrs = addRequiredAttributes(attrs);
		return TestUtil.getConnectionPoolDataSource(attrs);
	}

	private Properties addRequiredAttributes(Properties attrs)
	{
		// this method is accessed from subclasses - need to establish hostName
		hostName = TestUtil.getHostName();
		if (TestUtil.isJCCFramework())
		{
			attrs.setProperty("driverType","4");
            /**
             * As per the fix of derby-410 servername should
             * default to localhost, but for jcc it's still needed  
             */
            attrs.setProperty("serverName",hostName);
		}
		/** 
		 * For a remote host of course it's also needed 
		 */
		if (!hostName.equals("localhost"))
		{
			attrs.setProperty("serverName",hostName);
			attrs.setProperty("portNumber", "1527");
		}
		else
			attrs.setProperty("portNumber", "20000");
		//attrs.setProperty("retrieveMessagesFromServerOnGetMessage","true");
		return attrs;
	}

	public boolean supportsUnicodeNames() {
		return false;
	}

	public boolean supportsPooling() {
		return true;
	}
	public boolean supportsXA() {
		return false;
	}

	public void start() {
	}

	public void shutdown() {
	    
	    try{
		try {

		    consoleLogStream_.switchOutput( shutdownLogStream_ );
		    consoleErrLogStream_.switchOutput( shutdownErrLogStream_ );
		    
		    DriverManager.getConnection(TestUtil.getJdbcUrlPrefix(hostName,
															  NETWORKSERVER_PORT) +
										"wombat;shutdown=true",
				"EDWARD", "noodle");
		    
		    consoleLogStream_.switchOutput( originalStream_ );
		    consoleErrLogStream_.switchOutput( originalErrStream_);
		    
			System.out.println("FAIL - Shutdown returned connection");

		} catch (SQLException sqle) {
		    consoleLogStream_.switchOutput( originalStream_ );
		    consoleErrLogStream_.switchOutput( originalErrStream_ );
		    
			System.out.println("EXPECTED SHUTDOWN " + sqle.getMessage());
		}
		
	    } catch (IOException ioe){
		originalStream_.println("Switching stream was failed. Could not complete test ... ");
	    }
		
	}

	protected static boolean isServerStarted(NetworkServerControl server, int ntries)
	{
		for (int i = 1; i <= ntries; i ++)
		{
			try {
				Thread.sleep(500);
				server.ping();
				return true;
			}
			catch (Exception e) {
				if (i == ntries)
					return false;
			}
		}
		return false;
	}

	/**
	 *  Test Client specific dataSource Properties
	 *
	 */
	public void testClientDataSourceProperties() throws SQLException
	{
		testRetrieveMessageText();
		testDescription();
        
        //Added for Derby-409
        testConnectionAttributes();
        
        //Added for Derby-406
        allUsernameAndPasswordTests();
	}

	/**
	 * Test property retrieveMessageText to retrieve message text
	 * Property defaults to true for Network Client but can be set to
	 * false to disable the procedure call.
	 */
	public void testRetrieveMessageText() throws SQLException
	{
		Connection conn;
		String retrieveMessageTextProperty = "retrieveMessageText";
		Class[] argType = { Boolean.TYPE };
		String methodName = TestUtil.getSetterName(retrieveMessageTextProperty);
		Object[] args;

		try {
			DataSource ds = getDS("wombat", "EDWARD", "noodle");
			Method sh = ds.getClass().getMethod(methodName, argType);
			args = new Boolean[] { new Boolean(false) };
			sh.invoke(ds, args);
			conn = ds.getConnection();
			checkMessageText(conn,"false");
			conn.close();

			// now try with retrieveMessageText = true
			ds = getDS("wombat", "EDWARD", "noodle");
			args = new Boolean[] { new Boolean(true) };
			sh.invoke(ds, args);
			conn = ds.getConnection();
			checkMessageText(conn,"true");
			conn.close();
		}
		catch (Exception e)
		{
			System.out.println("FAIL: testRetrieveMessageText() Unexpected Exception " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Test description property
	 */
	public void testDescription() throws SQLException
	{
		String descriptionProperty = "description";
		Class[] argType = { String.class};
		String setterMethodName = TestUtil.getSetterName(descriptionProperty);
		String getterMethodName = TestUtil.getGetterName(descriptionProperty);

		Object[] args;

		try {
			String setDescription = "Everything you ever wanted to know about this datasource";
			DataSource ds = getDS("wombat", "EDWARD", "noodle");
			// Set the description
			Method sh = ds.getClass().getMethod(setterMethodName, argType);
			args = new Object[] { new String(setDescription) };
			sh.invoke(ds, args);
			// Now check it
			sh = ds.getClass().getMethod(getterMethodName, null);
			String getDescription = (String) sh.invoke(ds, null);
			if (!setDescription.equals(getDescription))
				throw new Exception("getDescription() " + getDescription + 
									" does not match setDescription() ");
		}
		catch (Exception e)
		{
			System.out.println("FAIL: testDescription() Unexpected Exception " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void checkMessageText(Connection conn, String
								 retrieveMessageTextValue) throws SQLException
	{
		System.out.println("** checkMessageText() with retrieveMessageText= " +
						   retrieveMessageTextValue);

		try {
			conn.createStatement().executeQuery("SELECT * FROM APP.NOTTHERE");
		}
		catch (SQLException e)
		{
			String expectedSQLState = "42X05";
			String sqlState = e.getSQLState();
			if (sqlState == null || ! sqlState.equals(expectedSQLState))
			{
				System.out.println("Incorrect SQLState.  Got: " + sqlState +
								   " should be: " + expectedSQLState);
				throw e;
			}
			if (retrieveMessageTextValue.equals("true") )
				{
					if (e.getMessage().indexOf("does not exist") != -1)
						System.out.println("PASS: Message Text retrieved properly");
					else
					{
						System.out.println("FAIL: Message text was not retrieved");
						throw e;
					}
				}
			else
				// retrieveMessageTextValue is false
				if (e.getMessage().indexOf("does not exist") == -1)
				{
					System.out.println("PASS: Message text not retrieved");
				}
				else
				{
					System.out.println("FAIL: Message Text should not have been retrieved");
					throw e;
				}

		}
	}
       
    /**
     * Added for Derby-409
     * 
     * Designed to test combinations of attributes to insure that 
     * no exceptions are thrown. 
     */
    public void testConnectionAttributes() {
        try {
            System.out.println("Begin connection attribute tests");
            testDataSourceConnection("One attribute test: ", 
                    "EDWARD", "noodle", "create=true");
            testDataSourceConnection("Another different attribute: ", 
                    "EDWARD", "noodle", "tracefile=trace.out"); 
            testDataSourceConnection("Two Attributes: ", 
                    "EDWARD", "noodle", "create=true;tracefile=trace.out");
            System.out.println("End connection attribute tests");
        }
        catch (Exception e)
        {
            System.out.println("FAIL: testSetConnectionAttributes() Unexpected Exception " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }
    
    /**
     * Added for Derby-406
     * 
     * Tests DataSource with a number of different username/password
     * input combinations.
     */
    public void allUsernameAndPasswordTests() {
        
        try {
            System.out.println("Begin username and password tests");
            
            testDataSourceConnection("Normal test: ", "EDWARD", "noodle", null);
            
            testDataSourceConnection("No username or password, only attributes test: ", 
                    null, null, "user=EDWARD;password=noodle");
            
            testDataSourceConnection("Bogus username and password, good attributes test: ", 
                    "Whatis", "theMatrix?", "user=EDWARD;password=noodle");
            
            testDataSourceConnection("Username, password attribute test: ", 
                    "EDWARD", null, "password=noodle");
            
            testDataSourceConnection("Password, username attribute test: ", 
                    null, "noodle", "user=EDWARD");
            
            System.out.println("Turning off authentication");
            DataSource ds = getDS("wombat", "EDWARD", "noodle");
            Connection conn = ds.getConnection();
            CallableStatement cs = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
            cs.setString(1, "derby.connection.requireAuthentication");
            cs.setString(2, "false");
            cs.execute();
            cs.close();
            cs = null;
            conn.close();
            //We have to shut down before the changes will take effect.
            shutdown();
            start();
            
            testDataSourceConnection("Username, no password test: ", 
                    "EDWARD", null, null);
            
            testDataSourceConnection("No username, password test: ", 
                    null, "noodle", null);
            
            testDataSourceConnection("No username, no password test: ", 
                    null, null, null);
            
            System.out.println("Turning on authentication");
            ds = getDS("wombat", "EDWARD", "noodle");
            conn = ds.getConnection();
            cs = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
            cs.setString(1, "derby.connection.requireAuthentication");
            cs.setString(2, "true");
            cs.execute();
            cs.close();
            cs = null;
            conn.close();
            shutdown();
            start();
            
            System.out.println("End username and password tests");
        }
        catch (Exception e)
        {
            System.out.println("FAIL: allUsernameAndPasswordTests. Unexpected Exception " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }
    
    /**
     * A method that attempts to retrieve the connection via a datasource
     * with the given user, password and connection attributes.
     * 
     * @param testType A string description of the type of test
     * @param username The user
     * @param password The Password
     * @param attributes A string to be added to a properties object. A
     * null string means null Property object.
     * @throws SQLException
     */
    public void testDataSourceConnection(String testType, String username, String password, String attributes) throws SQLException {
        try {
            System.out.print(testType);
            Properties props = null;
            if (attributes != null) {
                props = new Properties();
                props.put("ConnectionAttributes", attributes);
            }
            DataSource ds = getDS("wombat", username, password, props);
            Connection conn = ds.getConnection();
            conn.close();
            System.out.println("PASS.");
        } catch (SQLException e) {
            System.out.println("FAIL. Unexpected Exception: ");
            e.printStackTrace(System.out);
        }
    }
}






