/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.dataSourcePermissions_net

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
import java.io.*;
import java.net.InetAddress;
import java.util.Hashtable;

import javax.naming.*;
import javax.naming.directory.*;

import java.lang.reflect.*;

public class dataSourcePermissions_net extends org.apache.derbyTesting.functionTests.tests.jdbcapi.dataSourcePermissions
{

	private static final int NETWORKSERVER_PORT = 20000;

	private static NetworkServerControl networkServer = null;

	public static void main(String[] args) throws Exception {

		// Load harness properties.
		ij.getPropertyArg(args);

		// "runTest()" is going to try to connect to the database through
		// the server at port NETWORKSERVER_PORT.  Thus, we have to
		// start the server on that port before calling runTest.

		try {
			Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Start the NetworkServer on another thread
		networkServer = new NetworkServerControl(InetAddress.getByName("localhost"),NETWORKSERVER_PORT);
		networkServer.start(null);

		// Wait for the NetworkServer to start.
		if (!isServerStarted(networkServer, 60))
			System.exit(-1);

		// Now, go ahead and run the test.
		try {
			dataSourcePermissions_net tester = new dataSourcePermissions_net();
			tester.setProperties();
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
		networkServer.shutdown();
		// how do we do this with the new api?
		//networkServer.join();
		Thread.sleep(5000);
		System.out.println("Completed dataSourcePermissions_net");

		System.out.close();
		System.err.close();

	}


	public dataSourcePermissions_net() {
	}

	public void setProperties() {

		// Set required server properties.
		System.setProperty("database", "jdbc:derby:net://localhost:"
			+ NETWORKSERVER_PORT + "/wombat;create=true");
		System.setProperty("ij.user", "EDWARD");
		System.setProperty("ij.password", "noodle");

	}

	public String getJDBCUrl(String db, String attrs) {

		String s = "jdbc:derby:net://localhost:" + NETWORKSERVER_PORT + "/" + db;
		if (attrs != null)
			s = s + ":" + attrs + ";";

		return s;

	}

	public javax.sql.DataSource getDS(String database, String user, String
									  password)  
{
		return (javax.sql.DataSource) getDataSourceWithReflection("com.ibm.db2.jcc.DB2SimpleDataSource",
									database,user,password);

	}

	public Object getDataSourceWithReflection(String classname, String database,
											  String user, String password)
	{
		Class[] STRING_ARG_TYPE = {String.class};
		Class[] INT_ARG_TYPE = {Integer.TYPE};
		Object[] args = null;
		Object ds = null;
		Method sh = null;
		try {
		ds  = Class.forName(classname).newInstance();
			
			// Need to use reflection to load indirectly
			// setDatabaseName
			sh = ds.getClass().getMethod("setDatabaseName", STRING_ARG_TYPE);
			args = new String[] {database};
			sh.invoke(ds, args);
			if (user != null) {
				// setUser
				sh = ds.getClass().getMethod("setUser", STRING_ARG_TYPE);
				args = new String[] {user};
				sh.invoke(ds, args);
				// setPassword
				sh = ds.getClass().getMethod("setPassword", STRING_ARG_TYPE);
				args = new String[] {password};
				sh.invoke(ds, args);
			}
			
			// setServerName
			sh = ds.getClass().getMethod("setServerName", STRING_ARG_TYPE);
			args = new String[] {"localhost"};
			sh.invoke(ds, args);

			//setPortNumber
			sh = ds.getClass().getMethod("setPortNumber", INT_ARG_TYPE);
			args = new Integer[] {new Integer(NETWORKSERVER_PORT)};
			sh.invoke(ds, args);

			//setDriverType
			sh = ds.getClass().getMethod("setDriverType", INT_ARG_TYPE);
			args = new Integer[] {new Integer(4)};
			sh.invoke(ds, args);

		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return ds;
	}

	public javax.sql.ConnectionPoolDataSource getCPDS(String database, String user, String password) {
		
		return (javax.sql.ConnectionPoolDataSource) getDataSourceWithReflection("com.ibm.db2.jcc.DB2ConnectionPoolDataSource",database,user,password);

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

		try {
			DriverManager.getConnection("jdbc:derby:net://localhost:" +
				NETWORKSERVER_PORT + "/wombat;shutdown=true",
				"EDWARD", "noodle");
			System.out.println("FAIL - Shutdown returned connection");

		} catch (SQLException sqle) {
			System.out.println("EXPECTED SHUTDOWN " + sqle.getMessage());
		}

	}
	private static boolean isServerStarted(NetworkServerControl server, int ntries)
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

}


