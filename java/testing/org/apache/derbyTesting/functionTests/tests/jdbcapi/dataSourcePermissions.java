/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.dataSourcePermissions

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

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

import java.io.*;
import java.util.Hashtable;

import javax.naming.*;
import javax.naming.directory.*;

public class dataSourcePermissions
{

	private static final String zeus = "\u0396\u0395\u03A5\u03A3";
	private static final String apollo = "\u0391\u09A0\u039F\u039B\u039B\u039A\u0390";


	public static void main(String[] args) throws Exception {

		ij.getPropertyArg(args);
		new dataSourcePermissions().runTest();
		System.out.println("Completed dataSourcePermissions");

	}


	public dataSourcePermissions() {
	}

	protected void runTest() throws Exception {

		// Check the returned type of the JDBC Connections.
		Connection conn = ij.startJBMS();

		CallableStatement cs = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
		cs.setString(1, "derby.user.EDWARD");
		cs.setString(2, "noodle");
		cs.execute();

		cs.setString(1, "derby.user.FRANCES");
		cs.setString(2, "isabella");
		cs.execute();

		cs.setString(1, "derby.authentication.provider");
		cs.setString(2, "BUILTIN");
		cs.execute();

		// a greek unicode string
		cs.setString(1, "derby.user." + zeus);
		cs.setString(2, apollo);
		cs.execute();

		cs.setString(1, "derby.connection.requireAuthentication");
		cs.setString(2, "true");
		cs.execute();

		cs.close();

		conn.close();
		shutdown();

		start();

		System.out.println("Checking authentication with DriverManager");
		try {
			DriverManager.getConnection(getJDBCUrl("wombat", null));
			System.out.println("FAIL - get DriverManager connection without user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		try {
			DriverManager.getConnection(getJDBCUrl("wombat", "user=cynthia;password=sara"));
			System.out.println("FAIL - get DriverManager connection with invalid user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}
		
		checkConnection(DriverManager.getConnection(getJDBCUrl("wombat", "user=EDWARD;password=noodle")));
		checkConnection(DriverManager.getConnection(getJDBCUrl("wombat", "user=FRANCES;password=isabella")));

		if (supportsUnicodeNames()) {
			checkConnection(DriverManager.getConnection(getJDBCUrl("wombat", "user="+zeus+";password="+apollo)));
			checkConnection(DriverManager.getConnection(getJDBCUrl("wombat", null), zeus, apollo));
		}

		{
		System.out.println("Checking connections with DataSource");

		shutdown();

		System.out.println("data source with no default user");
		javax.sql.DataSource ds = getDS("wombat", null, null);

		try {
			ds.getConnection().close();
			System.out.println("FAIL - get DataSource connection with no user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		try {
			ds.getConnection("cynthia", "sara").close();
			System.out.println("FAIL - get DataSource connection with invalid user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		checkConnection(ds.getConnection("EDWARD", "noodle"));
		checkConnection(ds.getConnection("FRANCES", "isabella"));
		if (supportsUnicodeNames()) {
			checkConnection(ds.getConnection(zeus, apollo));
		}

		shutdown();

		System.out.println("data source with invalid default user");
		ds = getDS("wombat", "EDWARD", "sara");
		try {
			ds.getConnection().close();
			System.out.println("FAIL - get DataSource connection with no user and invalid defaults");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}
		checkConnection(ds.getConnection("FRANCES", "isabella"));
		shutdown();

		System.out.println("data source with valid default user");
		ds = getDS("wombat", "EDWARD", "noodle");

		checkConnection(ds.getConnection());
		checkConnection(ds.getConnection("FRANCES", "isabella"));
		shutdown();
		}


		if (supportsPooling()) {

		System.out.println("Checking connections with ConnectionPoolDataSource");

		System.out.println("ConnectionPoolDataSource with no default user");
		javax.sql.ConnectionPoolDataSource ds = getCPDS("wombat", null, null);

		try {
			ds.getPooledConnection().close();
			System.out.println("FAIL - get ConnectionPoolDataSource connection with no user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		try {
			ds.getPooledConnection("cynthia", "sara").close();
			System.out.println("FAIL - get ConnectionPoolDataSource connection with invalid user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		checkConnection(ds.getPooledConnection("EDWARD", "noodle"));
		checkConnection(ds.getPooledConnection("FRANCES", "isabella"));
		if (supportsUnicodeNames()) {
			checkConnection(ds.getPooledConnection(zeus, apollo));
		}
		shutdown();

		System.out.println("ConnectionPoolDataSource with invalid default user");
		ds = getCPDS("wombat", "EDWARD", "sara");
		try {
			ds.getPooledConnection().close();
			System.out.println("FAIL - get ConnectionPoolDataSource connection with no user and invalid defaults");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}
		checkConnection(ds.getPooledConnection("FRANCES", "isabella"));
		shutdown();

		System.out.println("ConnectionPoolDataSource with valid default user");
		ds = getCPDS("wombat", "EDWARD", "noodle");

		checkConnection(ds.getPooledConnection());
		checkConnection(ds.getPooledConnection("FRANCES", "isabella"));
		shutdown();
		}



		if (supportsXA()) {

		System.out.println("Checking connections with XADataSource");

		System.out.println("XADataSource with no default user");
		EmbeddedXADataSource ds = new EmbeddedXADataSource();
		ds.setDatabaseName("wombat");

		try {
			ds.getXAConnection().close();
			System.out.println("FAIL - get XADataSource connection with no user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		try {
			ds.getXAConnection("cynthia", "sara").close();
			System.out.println("FAIL - get XADataSource connection with invalid user");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}

		checkConnection(ds.getXAConnection("EDWARD", "noodle"));
		checkConnection(ds.getXAConnection("FRANCES", "isabella"));
		if (supportsUnicodeNames()) {
			checkConnection(ds.getXAConnection(zeus, apollo));
		}

		shutdown();

		System.out.println("XADataSource with invalid default user");
		ds = new EmbeddedXADataSource();
		ds.setDatabaseName("wombat");
		ds.setUser("edward");
		ds.setPassword("sara");
		try {
			ds.getXAConnection().close();
			System.out.println("FAIL - get XADataSource connection with no user and invalid defaults");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED CONNFAIL " + sqle.getMessage());
		}
		checkConnection(ds.getXAConnection("FRANCES", "isabella"));
		shutdown();

		System.out.println("XADataSource with valid default user");
		ds = new EmbeddedXADataSource();
		ds.setDatabaseName("wombat");
		ds.setUser("EDWARD");
		ds.setPassword("noodle");

		checkConnection(ds.getXAConnection());
		checkConnection(ds.getXAConnection("FRANCES", "isabella"));

		shutdown();
		}
	}

	private static void checkConnection(Connection conn) throws SQLException {
		checkConnection("DS", conn);
	}

	private static void checkConnection(String tag, Connection conn) throws SQLException {
		ResultSet rs = conn.createStatement().executeQuery("values current_user");
		rs.next();
		String who = rs.getString(1);
		rs.close();
		conn.close();

		if (zeus.equals(who))
			who = "GREEK ZEUS";

		System.out.println(tag + " connected as " + who);
	}
	private static void checkConnection(javax.sql.PooledConnection pc) throws SQLException {
		checkConnection("CP", pc.getConnection());
		pc.close();

	}
	private static void checkConnection(javax.sql.XAConnection xac) throws SQLException {
		checkConnection("XA", xac.getConnection());
		xac.close();
	}

	/*
	**	Allow sub-classes to pick different implementations for (say) the network server.
	*/

	public String getJDBCUrl(String db, String attrs) {
		String s = "jdbc:derby:" + db;

		if (attrs != null)
			s = s + ";" + attrs;

		return s;

	}
	public javax.sql.DataSource getDS(String database, String user, String password) {
		
		EmbeddedDataSource ds = new EmbeddedDataSource();
		ds.setDatabaseName(database);
		if (user != null) {
			ds.setUser(user);
			ds.setPassword(password);
		}

		return ds;
	}

	public javax.sql.ConnectionPoolDataSource getCPDS(String database, String user, String password) {
		
		EmbeddedConnectionPoolDataSource ds = new EmbeddedConnectionPoolDataSource();
		ds.setDatabaseName(database);
		if (user != null) {
			ds.setUser(user);
			ds.setPassword(password);
		}

		return ds;
	}



	public boolean supportsUnicodeNames() {
		return true;
	}


	public boolean supportsPooling() {
		return true;
	}
	public boolean supportsXA() {
		return true;
	}

	public void start() {
		new org.apache.derby.jdbc.EmbeddedDriver();
	}

	public void shutdown() {
		try {
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
			System.out.println("FAIL - Shutdown returned connection");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SHUTDOWN " + sqle.getMessage());
		}
	}
}
