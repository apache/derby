/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.closed

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
	Test execution of closed JDBC objects. Executing or accessing a closed
	object should report that it is closed.
	<p>
	Note that alot of this behavior is not very specifically specified in
	the JDBC guide, so this test is local to our own handler. Running JBMS
	under other handlers (such as weblogic) may produce different results due
	to how they cache data and reuse client-side objects.

	@author ames
 */
public class closed implements Runnable {


	public static void main(String[] args) {
		System.out.println("Test closed starting");
		boolean passed = true;

		try {
			Connection conn;

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			String url = conn.getMetaData().getURL();

			// want all tests to run regardless of intermediate errors
			passed = testStatement(conn) && passed;

			passed = testPreparedStatement(conn) && passed;

			passed = testResultSet(conn) && passed;

			// this test needs to be last, because the connection will
			// be closed by it.
			passed = testConnection(conn) && passed;

			if (!conn.isClosed()) {
				passed = false;
				System.out.println("FAIL -- connection not closed by test");
				conn.close();
			}

			// shutdown the database
			passed = shutdownTest(url, url + ";shutdown=true");


			// shutdown the system
			passed = shutdownTest(url, "jdbc:derby:;shutdown=true");


		} catch (Throwable e) {
			passed = false;
			System.out.println("FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, e);
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test closed finished");
	}

	static boolean shutdownTest(String url, String shutdownUrl) throws SQLException {

		boolean passed = true;

		Connection c1 = DriverManager.getConnection(url);
		Connection c2 = DriverManager.getConnection(url);
		Connection c3a = DriverManager.getConnection(url);
		Connection c3b = DriverManager.getConnection(url);

		try {
			c3a.createStatement().execute("DROP TABLE CLOSED.LOCKME");
		} catch (SQLException sqle) {
		}
		try {
			c3a.createStatement().execute("DROP PROCEDURE SLEEP");
		} catch (SQLException sqle) {
		}

		c3a.createStatement().execute("CREATE TABLE CLOSED.LOCKME(i int)");
		
		c3a.createStatement().execute("create procedure sleep(t INTEGER) dynamic result sets 0 language java external name 'java.lang.Thread.sleep' parameter style java");
		c3a.setAutoCommit(false);
		c3a.createStatement().execute("LOCK TABLE CLOSED.LOCKME IN SHARE MODE");
		
		closed r2 = new closed(c2, "CALL sleep(10000)");
		closed r3 = new closed(c3b, "LOCK TABLE CLOSED.LOCKME IN EXCLUSIVE MODE");

		Thread t2 = new Thread(r2);
		t2.start();
		Thread t3 = new Thread(r3);
		t3.start();

		try {
			Thread.currentThread().sleep(2000);
		} catch (InterruptedException ie) {
			System.out.println(ie);
		}

		SQLException s = null;
		try {
			DriverManager.getConnection(shutdownUrl);
		} catch (SQLException sqle) {
			s = sqle;
		}

		try {
			t2.join();
		} catch (InterruptedException ie) {
			System.out.println(ie);
		}
		try {
			t3.join();
		} catch (InterruptedException ie) {
			System.out.println(ie);
		}

		System.out.println(r2.result);
		System.out.println(r3.result);

		if (s != null)
			JDBCDisplayUtil.ShowException(System.out, s);

		if (!c1.isClosed()) {
			passed = false;
			System.out.println("FAIL -- connection not shutdown " + shutdownUrl);
			c1.close();
		}
		if (!c2.isClosed()) {
			passed = false;
			System.out.println("FAIL -- active connection not shutdown " + shutdownUrl);
			c2.close();
		}

		System.out.println("Shutdown test completed " + shutdownUrl);
		return passed;
	}

	// for the shutdown test
	private Connection cc;
	private String sql;
	String result;
	private closed(Connection cc, String sql) {
		this.cc = cc;
		this.sql = sql;
	}
	public void run() {

		try {
			cc.createStatement().execute(sql);
			result = "Sleep thread completed " + sql;
		} catch (SQLException sqle) {

			// this is to avoid different cannons for different JVMs since
			// an java.lang.InterruptedException is thrown.
			StringBuffer sb = new StringBuffer();
			sb.append(sql);
			sb.append(" - ");
			sb.append(sqle.getSQLState());
			while (sqle != null)
			{
				if (sqle != null) {
					sb.append(", ");
					sb.append(sqle.getSQLState());
					sb.append(" -- ");
					if (sqle.getMessage().indexOf("InterruptedException") != -1)
						sb.append("InterruptedException");
					else
					{
						sb.append(sqle.getMessage());
						sqle.printStackTrace();
					}
				} else {
					sb.append(sqle.getMessage());
				}
				sqle  = sqle.getNextException();
			}
			result = sb.toString();
		}
	}

	static boolean testStatement(Connection conn) throws SQLException {
		Statement s;
		boolean passed = true;

		s = conn.createStatement();
		s.execute("create table t (i int)");
		s.execute("create table s (i int)");

		try {
			s.execute("create table u (i int)");
		} catch (SQLException se) {
			// out impl lets you execute from closed, as stmt object is reusable
			// after it is closed.
			passed = false; // won't pass unless caught
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on execute of closed statement");
		return passed;
	}

	static boolean testPreparedStatement(Connection conn) throws SQLException {
		PreparedStatement ps;
		boolean passed = true;

		ps = conn.prepareStatement("insert into t values (1)");
		ps.execute();
		ps.execute();
		ps.close();

		try {
			passed = false; // won't pass unless caught
			ps.execute();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on execute of closed prepared statement");

		return passed;
	}


	static boolean testResultSet(Connection conn) throws SQLException {
		PreparedStatement ps;
		Statement s;
		ResultSet rs;
		boolean passed = true;

		// first, get a few values into a table:
		ps = conn.prepareStatement("insert into s values (1)");
		ps.execute();
		ps.execute();
		ps.execute();
		ps.execute();
		ps.execute();
		ps.close();

		s = conn.createStatement();
		rs = s.executeQuery("select * from s");

		rs.next();
		rs.next();
		rs.close();

		try {
			passed = false; // won't pass unless caught
			rs.next();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on next of closed result set");

		// now see that rs after statement closed is closed also
		rs = s.executeQuery("select * from s");

		rs.next();
		rs.next();
		s.close();

		try {
			passed = false; // won't pass unless caught
			rs.next();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on next of result set with closed statement");

		return passed;
	}

	static boolean testConnection(Connection conn) throws SQLException {
		DatabaseMetaData dmd;
		ResultSet rs;
		Statement s;
		PreparedStatement ps;
		boolean passed = true;

		dmd = conn.getMetaData();
		s = conn.createStatement();
		ps = conn.prepareStatement("create table w (i int)");

		rs = dmd.getTables("%","%","%",null); // should work

		conn.close();

		// should not be able to execute an existing statement
		try {
			passed = false; // won't pass unless caught
			s.execute("create table x (i int)");
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on statement execute after connection close");

		// should not be able to execute an existing prepared statement
		try {
			passed = false; // won't pass unless caught
			ps.execute();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on prepared statement execute after connection close");

		// should not be able to create a statement...
		try {
			passed = false; // won't pass unless caught
			s = conn.createStatement();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on statement creation after connection close");

		// should not be able to prepare a statement...
		try {
			passed = false; // won't pass unless caught
			ps = conn.prepareStatement("create table z (i int)");
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on statement preparation after connection close");

		// should not be able to see metadata info...
		try {
			passed = false; // won't pass unless caught
			rs.next();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on metadata reading after connection close");

		// should not be able to get any more metadata info...
		try {
			passed = false; // won't pass unless caught
			rs = dmd.getColumns("%","%","%","%");
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on metadata collecting after connection close");

		// should not be able to get metadata object...
		try {
			passed = false; // won't pass unless caught
			dmd = conn.getMetaData();
		} catch (SQLException se) {
			passed = true;
			// could verify exception #...
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		if (!passed)
			System.out.println("FAIL -- no error on getting metadata after connection close");

		return passed;
	}

}
