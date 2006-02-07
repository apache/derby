/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testSecMec

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import javax.sql.DataSource;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.io.*;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Properties;

import java.lang.reflect.*;

/**
 * This class tests the security mechanisms supported by Network Server
 * Network server supports SECMEC_EUSRIDPWD, SECMEC_USRIDPWD, SECMEC_USRIDONL
 * Note  - currently the SECMEC_EUSRIDPWD does not work with all versions of ibm142
 * because of the following reason
 * The DiffieHelman algorithm that is used here uses a prime of 32bytes and this is not 
 * supported by Sun JCE , but is supported in ibm141 and some latest versions of ibm142
 *
 */
public class testSecMec extends dataSourcePermissions_net

{

	private static int NETWORKSERVER_PORT;

	private static NetworkServerControl networkServer = null;

	public static void main(String[] args) throws Exception {

		// Load harness properties.
		ij.getPropertyArg(args);

		String hostName = TestUtil.getHostName();
		if (hostName.equals("localhost"))
			NETWORKSERVER_PORT = 20000;
		else
			NETWORKSERVER_PORT = 1527;

		// "runTest()" is going to try to connect to the database through
		// the server at port NETWORKSERVER_PORT.  Thus, we have to
		// start the server on that port before calling runTest.

		try {
			TestUtil.loadDriver();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Start the NetworkServer on another thread, unless it's a remote host
		if (hostName.equals("localhost"))
		{
			networkServer = new NetworkServerControl(InetAddress.getByName(hostName),NETWORKSERVER_PORT);
			networkServer.start(null);

			// Wait for the NetworkServer to start.
			if (!isServerStarted(networkServer, 60))
				System.exit(-1);
		}

		// Now, go ahead and run the test.
		try {
			testSecMec tester = new testSecMec();
			tester.runTest();

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
			networkServer.shutdown();
			// how do we do this with the new api?
			//networkServer.join();
			Thread.sleep(5000);
		}
		System.out.println("Completed testSecMec");

		System.out.close();
		System.err.close();

	}


	  // Indicates userid/encrypted password security mechanism.
	  static final short SECMEC_EUSRIDPWD = 0x09;

	  // Indicates userid only security mechanism.
	  static final short SECMEC_USRIDONL = 0x04;

	  // Indicates userid/encrypted password security mechanism.
	  static final short SECMEC_USRENCPWD = 0x07;

	  // Indicates userid/new password security mechanism.
	  static final short SECMEC_USRIDNWPWD = 0x05;

	  // Indicates userid/password security mechanism.
	  static final short SECMEC_USRIDPWD = 0x03;


	/**
	 *  Test cases for security mechanism
	 *  ---------------------------------------------------------------
	 *  T1 - default , no user			PASS (for derbyclient)
	 *  T2 - user only					PASS (for derbyclient)
	 *	T3 - user,password			PASS (only for derbynet)
	 *  T4 - user,password, security mechanism not set  FAIL
	 *  T5 - user,password, security mechanism set to SECMEC_USRIDPWD    PASS
	 *  T6 - user, security mechanism set to SECMEC_USRIDONL   PASS
	 *  T7 - user,password, security mechanism set to SECMEC_USRENCPWD   FAIL
	 *  Test with datasource as well as DriverManager
	 *  T8 - user,password security mechanism set to SECMEC_USRIDONL   PASS
	 *  Test with datasource as well as DriverManager
	 */

	protected void runTest()
	{
		// Test cases with get connection via drivermanager and using
		// different security mechanisms.
		// Network server supports SECMEC_USRIDPWD, SECMEC_USRIDONL,SECMEC_EUSRIDPWD
		System.out.println("Checking security mechanism authentication with DriverManager");
		getConnectionUsingDriverManager(getJDBCUrl("wombat;create=true","user=neelima;password=lee;securityMechanism="+SECMEC_USRIDPWD),"T4:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat",null),"T1:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=max"),"T2:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee"),"T3:");
                // Disable because ibm142 doesnt support DiffieHelman prime of 32 bytes
                // Also Sun JCE doesnt support it.
		//getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_EUSRIDPWD),"T5:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;securityMechanism="+SECMEC_USRIDONL),"T6:");
                
                // disable as ibm142 and sun jce doesnt support DH prime of 32 bytes
		//getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_USRENCPWD),"T7:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_USRIDONL),"T8:");

		getConnectionUsingDataSource();

	}

        /*
         * Get connection from datasource and also set security mechanism
         */

	public void getConnectionUsingDataSource()
	{
		// bug in jcc, throws error with null password
		//testSecurityMechanism("sarah",null,new Short(SECMEC_USRIDONL),"SECMEC_USRIDONL:");
		testSecurityMechanism("john","sarah",new Short(SECMEC_USRIDPWD),"SECMEC_USRIDPWD:");
                
		// Disable this test because ibm142, sun jce does not Diffie Helman prime of 32 bytes
                // and so this security mechanism wont work in that case
		//testSecurityMechanism("john","sarah",new Short(SECMEC_EUSRIDPWD),"SECMEC_EUSRIDPWD:");

	}

	public void testSecurityMechanism(String user, String password,Short secmec,String msg)
	{
		Connection conn;
		String securityMechanismProperty = "SecurityMechanism";
		Class[] argType = { Short.TYPE };
		String methodName = TestUtil.getSetterName(securityMechanismProperty);
		Object[] args = new Short[1];
		args[0] = secmec;

		try {
			DataSource ds = getDS("wombat", user,password);
			Method sh = ds.getClass().getMethod(methodName, argType);
			sh.invoke(ds, args);
			conn = ds.getConnection();
			conn.close();
			System.out.println(msg +" OK");
		}
		catch (Exception e)
		{
			System.out.println(msg +"EXCEPTION testSecurityMechanism()  " + e.getMessage());
		}
	}

	public void getConnectionUsingDriverManager(String dbUrl, String msg)
	{

		try
		{
			DriverManager.getConnection(dbUrl);
			System.out.println(msg +" "+dbUrl );
		}
		catch(SQLException sqle)
		{
			System.out.println(msg +" "+dbUrl +" - EXCEPTION "+ sqle.getMessage());
		}
	}


}
