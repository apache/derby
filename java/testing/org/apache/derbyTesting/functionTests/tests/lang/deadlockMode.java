/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.deadlockMode

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
	This tests for deadlock which can occur if two threads get a 
	row lock before getting a table lock on the same table.  This can
	happen if the lock obtained by the insert, update or delete result set
	is a smaller range than the table scan result set.  The insert, update or
	delete result set lock is obtained first.  For example, if the insert, update
	or delete result set obtain a row lock and then the table scan obtains a
	table scan lock, deadlock can occur since two threads can obtain the row lock
	and then both thread will want the same table lock.
 */

public class deadlockMode {

	private static final int THREAD_COUNT = 20;
	private static boolean passed = false;
	private Object syncObject = new Object();
	private int doneCount;
	private deadlockMode() {}

	public static void main(String[] args) {
		System.out.println("Test deadlockMode starting");
		
		deadlockMode tester = new deadlockMode();

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();

			setup(conn, true);
			passed = true;
			tester.runtest();
			teardown(conn);
			conn.close();


		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			passed = false;
			JDBCDisplayUtil.ShowException(System.out,e);
			e.printStackTrace();
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test cursor finished");
	}
	/**
	 * This method creates THREAD_COUNT threads which will all try to
	 * update the same table 
	 */
	private void runtest() throws InterruptedException
	{
		Thread [] t = new Thread[THREAD_COUNT];
		for (int i = 0; i < THREAD_COUNT; i++)
		{	
		 	t[i] = new Thread(new Runnable() {
				public void run() {startnew(); }});
			t[i].start();
		}
		boolean notdone = true;
		while(notdone)
		{
			synchronized(syncObject)
			{
				if (doneCount == THREAD_COUNT)
					notdone = false;
				else
					syncObject.wait();
			}
		}
	}
	/**
	 * Keep track of how many are done so we can wait until all the
	 * threads are finished before saying we have passed
	 */
	private void done()
	{
		System.out.println("Done Thread");
		synchronized(syncObject)
		{
			doneCount++;
			syncObject.notify();
		}
	}
	/**
	 * This method creates a connection, loads the query into cache using
  	 * READ_COMMITTED and then tries to execute the query using SERIALIZABLE.
	 * If we don't update the lock mode based on the isolation level at
	 * execution time, the query can deadlock with other threads
	 * SERALIZABLE requires a table lock mode.  READ_COMMITTED uses a row lock.
	 */
	private void startnew()
	{
		Connection conn = null;
		try {
			// make the initial connection.
			conn = ij.startJBMS();
			System.out.println("Starting thread");

			Statement stmt = conn.createStatement();
			// execute a query to load cache
			stmt.executeUpdate("update t set i = 456 where i = 456");
			// set isolation level to serializable
			conn.setAutoCommit(false);
			stmt.execute("set isolation serializable");
			for (int i = 0; i < 100 ; i++)
			{
				stmt.executeUpdate("update t set i = 456 where i = 456");
				conn.commit();
			}
			done();
			
		} catch (Throwable e) {
			synchronized(syncObject)
			{
				System.out.println("FAIL: exception thrown:");
				passed = false;
				JDBCDisplayUtil.ShowException(System.out,e);
				e.printStackTrace();
				done();
			}
		}
		finally {
			try {
				if (conn != null) {
					conn.rollback();
					conn.close();
				}
			} catch (SQLException sqle) {
				System.out.println("FAIL: exception thrown:");
				passed = false;
				JDBCDisplayUtil.ShowException(System.out,sqle);
				sqle.printStackTrace();
			}
		}
	}
	/**
	 * set up the table for the test
	 */
	static void setup(Connection conn, boolean first) throws SQLException {
		Statement stmt = conn.createStatement();

		if (first) {
			verifyCount(
				stmt.executeUpdate("create table t (i int)"),
				0);

		} else {
			verifyBoolean(
				stmt.execute("delete from t"),
				false);
		}

		verifyCount(
		    stmt.executeUpdate("insert into t values (1956)"),
			1);

		verifyCount(
		    stmt.executeUpdate("insert into t values (456)"),
			1);

		verifyCount(
		    stmt.executeUpdate("insert into t values (180)"),
			1);

		verifyCount(
		    stmt.executeUpdate("insert into t values (3)"),
			1);

		stmt.close();

		System.out.println("PASS: setup complete");
	}

	/**
	 * clean up
	 */ 
	static void teardown(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();

		verifyCount(
		    stmt.executeUpdate("drop table t"),
			0);

		stmt.close();

		System.out.println("PASS: teardown complete");
	}
	/** 
	 * verify row count
	 */
	static void verifyCount(int count, int expect) throws SQLException {
		if (count!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+count+" rows");
			throw new SQLException("Wrong number of rows returned");
		}
		else
			System.out.println("PASS: expected and got "+count+
							   (count == 1? " row":" rows"));
	}

	/**
	 * verify boolean value
	 */
	static void verifyBoolean(boolean got, boolean expect) throws SQLException {
		if (got!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+got);
			throw new SQLException("Wrong boolean returned");
		}
		else
			System.out.println("PASS: expected and got "+got);
	}
}
