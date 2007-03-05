/*

 Derby - Class org.apache.derbyTesting.system.nstest.tester.TesterObject

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

package org.apache.derbyTesting.system.nstest.tester;

import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Connection;

import org.apache.derbyTesting.system.nstest.NsTest;
import org.apache.derbyTesting.system.nstest.utils.DbUtil;

/**
 * TesterObject - The base tester class for all the testers
 */
public class TesterObject {

	private String thread_id;

	protected Connection connex = null;

	protected DbUtil dbutil;

	// *******************************************************************************
	//
	// Constructor. Get's the name of the thread running this for use in
	// messages
	//
	// *******************************************************************************
	public TesterObject(String name) {
		this.thread_id = name;
		dbutil = new DbUtil(getThread_id());
		System.out.println("==========> " + getThread_id()
				+ " THREAD starting <======");
	}

	// *******************************************************************************
	//
	// Gets the connection to the database. Implemented so that threads that
	// need to
	// frequently get a connection can just call this method instead.
	//
	// *******************************************************************************

	public Connection getConnection() {
		Connection conn = null;
		String jdbcurl = "";
		try {
			System.out.println(getThread_id()
					+ " is getting a connection to the database...");

			if (NsTest.embeddedMode) {
				jdbcurl = NsTest.embedDbURL + ";" + NsTest.bootPwd;
			} else {
				if (NsTest.driver_type.equalsIgnoreCase("DerbyClient"))
					jdbcurl = NsTest.clientDbURL + ";" + NsTest.bootPwd;

			}
			System.out.println("-->Thread " + getThread_id()
					+ " starting with url " + jdbcurl + " <--");
			conn = DriverManager.getConnection(jdbcurl, NsTest.prop);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("FAIL: " + getThread_id()
					+ " could not get the database connection");
			printException("Failed getting database connection using "
					+ jdbcurl, e);
		}
		// for statistical purposes, add one to the num of connections makde
		NsTest.addStats(NsTest.CONNECTIONS_MADE, 1);
		System.out.println("Connection number: " + NsTest.numConnections);
		return conn; // null if there was a problem, else a valid connection
	}

	// *******************************************************************************
	//
	// Sets the isolation level to that indicated.
	//
	// *******************************************************************************
	public void setIsolationLevel(int level) {
		try {
			connex.setTransactionIsolation(level);
		} catch (Exception e) {
			System.out.println("FAIL: " + getThread_id()
					+ " could not set isolation level");
			printException("setting transaction isolation", e);
		}
	}

	// *******************************************************************************
	//
	// Closes the connection to the database. Implemented so that threads that
	// need to
	// frequently close their connection can just call this method instead.
	//
	// *******************************************************************************
	public void closeConnection() {
		try {
			System.out.println(getThread_id()
					+ " is closing it's connection to the database...");
			connex.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + getThread_id()
					+ " could not close the database connection");
			printException("closing database connection", e);
		}
	}

	// ******************************************************************************************
	//
	// This method will do a basic Insert/Delete/Update operation. We randomly
	// decide whether
	// we want to do either an Insert, a delete or an update
	//
	//
	// *******************************************************************************************
	public void doIUDOperation() {
		// decide Insert, Update or Delete
		int decider = (int) (Math.random() * 100) % 3;

		switch (decider) {

		case 0: // do an Insert
			try {
				int numInsert = dbutil.add_one_row(connex, getThread_id());
				if (numInsert == 1)
					NsTest.addStats(NsTest.INSERT, 1);
				else
					NsTest.addStats(NsTest.FAILED_INSERT, 1);
			} catch (Exception e) {
				printException("executing add_one_row()", e);
			}

			break;

		case 1: // do an update

			try {
				int numUpdate = dbutil.update_one_row(connex, getThread_id());
				if (numUpdate == 1)
					NsTest.addStats(NsTest.UPDATE, 1);
				else
					NsTest.addStats(NsTest.FAILED_UPDATE, 1);
			} catch (Exception e) {
				printException("executing update_one_row", e);
			}
			break;

		case 2: // do a delete

			try {
				int numDelete = dbutil.delete_one_row(connex, getThread_id());
				if (numDelete == 1)
					NsTest.addStats(NsTest.DELETE, 1);
				else
					NsTest.addStats(NsTest.FAILED_DELETE, 1);
			} catch (Exception e) {
				printException("executing delete_one_row()", e);
			}
			break;

		}// end of switch(decider)
	}// end of method doIUDOperation()

	// ******************************************************************************************
	//
	// This method will do a basic Select operation based on the following
	// criteria
	// The query should return approximately nstest.MAX_LOW_STRESS number of
	// rows that we
	// loop through via a result set and perform operations (getXX calls) in
	// order to ensure
	// that data flows properly. The method will
	// return the total number of rows selected. Note that we do not touch rows
	// with serialkey
	// less than nstest.NUM_UNTOUCHED_ROWS, and the selects will be based on the
	// parameter passed
	// in, viz numRowsToSelect which is <= nstest.NUM_UNTOUCHED_ROWS
	//
	// *******************************************************************************************
	public int doSelectOperation(int numRowsToSelect) throws SQLException {

		int numRowsSelected = 0;
		ResultSet rSet = null;
		Statement s = null;

		System.out.println(getThread_id() + " is selecting " + numRowsToSelect
				+ " rows");
		try {
			// create the statement

			s = connex.createStatement();
			// Execute the query
			rSet = s
			.executeQuery("select id, t_char,"
					+ " t_date, t_decimal, t_decimal_nn, t_double, "
					+ " t_float, t_int, t_longint, t_numeric_large,"
					+ " t_real, t_smallint, t_time, t_timestamp,"
					+ " t_varchar, serialkey from nstesttab where serialkey <= "
					+ numRowsToSelect);
		} catch (Exception e) {
			System.out
			.println("FAIL: doSelectOperation() had problems creating/executing query");
			printException(
					"FAIL: doSelectOperation() had problems creating/executing query",
					e);
			s.close();
		}

		// Now work over the returned ResultSet and keep track of number of rows
		// returned
		// We execute the getXXX methods on each of the selected columns so that
		// data flow out
		// from the network server is also tested.
		try {
			while (rSet.next()) {
				// get value of column id
				int id1 = rSet.getInt(1);

				// get value of column t_char
				String str1 = rSet.getString(2);

				// get value of column t_date
				Date dt = rSet.getDate(3);

				// get value of column t_decimal
				double doub1 = rSet.getDouble(4);

				// get value of column t_decimal_nn
				double doub2 = rSet.getDouble(5);

				// get value of column t_double
				double doub3 = rSet.getDouble(6);

				// get value of column t_float
				float flt1 = rSet.getFloat(7);

				// get value of column t_int
				int id2 = rSet.getInt(8);

				// get value of column t_longint
				long lg1 = rSet.getLong(9);

				// get value of column t_numeric_large
				double doub4 = rSet.getDouble(10);

				// get value of column t_real
				float flt2 = rSet.getFloat(11);

				// get value of column t_smallint
				int id3 = rSet.getInt(12);

				// get value of column t_time
				Time tm = rSet.getTime(13);

				// get value of column t_timestamp
				Timestamp tstmp = rSet.getTimestamp(14);

				// get value of column t_varchar
				String str2 = rSet.getString(15);

				// get value of column serialkey
				long lg2 = rSet.getLong(16);

				numRowsSelected++;
			}
			NsTest.addStats(NsTest.SELECT, 1);
			System.out.println(this.thread_id + " selected " + numRowsSelected
					+ " rows");
		} catch (Exception e) {
			System.out
			.println("FAIL: doSelectOperation() had problems working over the ResultSet");
			NsTest.addStats(NsTest.FAILED_SELECT, 1);
			printException("processing ResultSet during row data retrieval", e);
			rSet.close();
			s.close();
			System.out.println("Closed the select statement");
		}

		// close the ResultSet and statement and release it's resources.
		try {
			if ((rSet != null) && (s != null)) {
				rSet.close();
				s.close();
				System.out.println("Closed the select statement");
			}
		} catch (Exception e) {
			System.out
			.println("FAIL: doSelectOperation() had problems closing the ResultSet");
			printException("closing ResultSet of query to get row data", e);
		}

		return numRowsSelected;
	}// end of doSelectOperation()

	// *******************************************************************************
	//
	// This starts the acutal test operations
	//
	// *******************************************************************************
	public void startTesting() {

		// This method needs to be overridden by the child classes in order for
		// a Tester to
		// be able to do work. The specifics such as how often the connection is
		// opened and
		// closed and how many transactions are done etc etc which form
		// individual test cases or
		// sorts are left to the child class to implement in their overridden
		// version of this
		// method

	}// end of startTesting()

	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
	public synchronized void printException(String where, Exception e) {
		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;

			if (se.getSQLState().equals("40001"))
				System.out.println("TObj --> deadlocked detected");
			if (se.getSQLState().equals("40XL1"))
				System.out.println("TObj --> lock timeout exception");
			if (se.getSQLState().equals("23500"))
				System.out.println("TObj --> duplicate key violation");
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
		}
		if (e.getMessage() == null) {
			System.out.println("TObj -->NULL error message detected");
			System.out.println("TObj -->Here is the NULL exception - "
					+ e.toString());
			System.out.println("TObj -->Stack trace of the NULL exception - ");
			e.printStackTrace(System.out);
		}
		System.out.println("TObj -->At this point - " + where
				+ ", exception thrown was : " + e.getMessage());
	}

	public String getTimestamp() {
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		return ts.toString();
	}

	public String getThread_id() {
		return thread_id;
	}

}
